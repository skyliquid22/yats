"""Alpaca broker adapter — order management and real-time bar streaming.

Implements PRD §13.2 Broker Adapter (Alpaca):
- Market order submission (v1)
- Status polling and fill handling
- Error normalization
- Paper/live endpoint separation (refuses live unless production tier)
- WebSocket streaming with auto-reconnect and exponential backoff

Also provides SimBrokerAdapter for shadow sim mode (PRD §13.3).
"""

from __future__ import annotations

import enum
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Protocol

logger = logging.getLogger(__name__)

RETRY_DELAYS = [1, 5, 30]  # seconds — exponential backoff


# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------


class OrderSide(enum.Enum):
    BUY = "buy"
    SELL = "sell"


class OrderStatus(enum.Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    REJECTED = "rejected"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


@dataclass
class OrderRequest:
    """Market order request."""

    symbol: str
    side: OrderSide
    notional: float  # USD notional value
    client_order_id: str | None = None


@dataclass
class Fill:
    """A fill from the broker."""

    symbol: str
    side: OrderSide
    filled_qty: float
    filled_avg_price: float
    filled_at: datetime
    commission: float = 0.0


@dataclass
class OrderResult:
    """Result of an order submission or poll."""

    order_id: str
    client_order_id: str
    symbol: str
    side: OrderSide
    status: OrderStatus
    submitted_at: datetime
    filled_qty: float = 0.0
    filled_avg_price: float = 0.0
    filled_at: datetime | None = None
    reject_reason: str = ""


class BrokerError(Exception):
    """Normalized broker error."""

    def __init__(self, message: str, *, code: str = "", retryable: bool = False):
        super().__init__(message)
        self.code = code
        self.retryable = retryable


# ---------------------------------------------------------------------------
# Broker adapter protocol
# ---------------------------------------------------------------------------


class BrokerAdapter(Protocol):
    """Protocol for broker adapters (PRD §13.2)."""

    def submit_order(self, request: OrderRequest) -> OrderResult: ...

    def get_order_status(self, order_id: str) -> OrderResult: ...

    def cancel_order(self, order_id: str) -> OrderResult: ...

    def get_account(self) -> dict[str, Any]: ...


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PAPER_BASE_URL = "https://paper-api.alpaca.markets"
LIVE_BASE_URL = "https://api.alpaca.markets"
PAPER_STREAM_URL = "wss://stream.data.sandbox.alpaca.markets/v2/iex"
LIVE_STREAM_URL = "wss://stream.data.alpaca.markets/v2/iex"


@dataclass(frozen=True)
class AlpacaBrokerConfig:
    """Alpaca broker adapter configuration.

    Credentials come from environment variables APCA_API_KEY_ID and
    APCA_API_SECRET_KEY (matching alpaca-py conventions).
    """

    api_key: str = ""
    api_secret: str = ""
    paper: bool = True  # Paper trading by default
    production_tier: bool = False  # Must be True to allow live endpoint
    max_retries: int = 3

    @property
    def base_url(self) -> str:
        return PAPER_BASE_URL if self.paper else LIVE_BASE_URL

    @property
    def stream_url(self) -> str:
        return PAPER_STREAM_URL if self.paper else LIVE_STREAM_URL

    def __post_init__(self) -> None:
        if not self.paper and not self.production_tier:
            raise ValueError(
                "Refusing live endpoint: production_tier must be True "
                "to use live trading. Set paper=True for paper trading."
            )

    @classmethod
    def from_env(
        cls,
        *,
        paper: bool = True,
        production_tier: bool = False,
    ) -> AlpacaBrokerConfig:
        """Create config from environment variables."""
        return cls(
            api_key=os.environ.get("APCA_API_KEY_ID", ""),
            api_secret=os.environ.get("APCA_API_SECRET_KEY", ""),
            paper=paper,
            production_tier=production_tier,
        )


# ---------------------------------------------------------------------------
# Alpaca broker adapter (real API)
# ---------------------------------------------------------------------------


def _normalize_alpaca_status(status_str: str) -> OrderStatus:
    """Map Alpaca order status string to OrderStatus enum."""
    mapping = {
        "new": OrderStatus.SUBMITTED,
        "accepted": OrderStatus.SUBMITTED,
        "pending_new": OrderStatus.PENDING,
        "accepted_for_bidding": OrderStatus.SUBMITTED,
        "partially_filled": OrderStatus.PARTIALLY_FILLED,
        "filled": OrderStatus.FILLED,
        "done_for_day": OrderStatus.FILLED,
        "canceled": OrderStatus.CANCELLED,
        "expired": OrderStatus.EXPIRED,
        "replaced": OrderStatus.SUBMITTED,
        "pending_cancel": OrderStatus.SUBMITTED,
        "pending_replace": OrderStatus.SUBMITTED,
        "stopped": OrderStatus.FILLED,
        "rejected": OrderStatus.REJECTED,
        "suspended": OrderStatus.REJECTED,
        "calculated": OrderStatus.SUBMITTED,
    }
    return mapping.get(status_str.lower(), OrderStatus.REJECTED)


class AlpacaBrokerAdapter:
    """Alpaca Trading API broker adapter.

    Uses alpaca-py TradingClient for order management.
    Market orders only (v1).
    """

    def __init__(self, config: AlpacaBrokerConfig) -> None:
        self._config = config
        self._validate_credentials()
        self._client = self._create_client()

    def _validate_credentials(self) -> None:
        if not self._config.api_key or not self._config.api_secret:
            raise BrokerError(
                "Missing Alpaca credentials: set APCA_API_KEY_ID and "
                "APCA_API_SECRET_KEY environment variables",
                code="AUTH_MISSING",
            )

    def _create_client(self) -> Any:
        """Create alpaca-py TradingClient."""
        from alpaca.trading.client import TradingClient

        return TradingClient(
            api_key=self._config.api_key,
            secret_key=self._config.api_secret,
            paper=self._config.paper,
        )

    def submit_order(self, request: OrderRequest) -> OrderResult:
        """Submit a market order to Alpaca."""
        from alpaca.trading.enums import OrderSide as AlpacaSide
        from alpaca.trading.enums import TimeInForce
        from alpaca.trading.requests import MarketOrderRequest

        alpaca_side = (
            AlpacaSide.BUY if request.side == OrderSide.BUY else AlpacaSide.SELL
        )

        order_req = MarketOrderRequest(
            symbol=request.symbol,
            notional=round(request.notional, 2),
            side=alpaca_side,
            time_in_force=TimeInForce.DAY,
            client_order_id=request.client_order_id,
        )

        last_exc: Exception | None = None
        for attempt, delay in enumerate(RETRY_DELAYS[: self._config.max_retries]):
            try:
                order = self._client.submit_order(order_data=order_req)
                return self._order_to_result(order)
            except Exception as exc:
                last_exc = exc
                error_str = str(exc)

                # Non-retryable errors
                if "insufficient" in error_str.lower():
                    raise BrokerError(
                        f"Insufficient funds: {error_str}",
                        code="INSUFFICIENT_FUNDS",
                        retryable=False,
                    ) from exc
                if "forbidden" in error_str.lower():
                    raise BrokerError(
                        f"Forbidden: {error_str}",
                        code="FORBIDDEN",
                        retryable=False,
                    ) from exc

                if attempt < self._config.max_retries - 1:
                    logger.warning(
                        "Order submit failed (attempt %d/%d): %s — retrying in %ds",
                        attempt + 1,
                        self._config.max_retries,
                        exc,
                        delay,
                    )
                    time.sleep(delay)

        raise BrokerError(
            f"Order submission failed after {self._config.max_retries} attempts: {last_exc}",
            code="SUBMIT_FAILED",
            retryable=False,
        ) from last_exc

    def get_order_status(self, order_id: str) -> OrderResult:
        """Poll order status from Alpaca."""
        try:
            order = self._client.get_order_by_id(order_id)
            return self._order_to_result(order)
        except Exception as exc:
            raise BrokerError(
                f"Failed to get order status: {exc}",
                code="STATUS_POLL_FAILED",
                retryable=True,
            ) from exc

    def cancel_order(self, order_id: str) -> OrderResult:
        """Cancel an order."""
        try:
            self._client.cancel_order_by_id(order_id)
            return self.get_order_status(order_id)
        except Exception as exc:
            raise BrokerError(
                f"Failed to cancel order: {exc}",
                code="CANCEL_FAILED",
                retryable=True,
            ) from exc

    def get_account(self) -> dict[str, Any]:
        """Get account information."""
        try:
            acct = self._client.get_account()
            return {
                "equity": float(acct.equity),
                "buying_power": float(acct.buying_power),
                "cash": float(acct.cash),
                "portfolio_value": float(acct.portfolio_value),
                "currency": acct.currency,
                "status": acct.status.value if acct.status else "unknown",
            }
        except Exception as exc:
            raise BrokerError(
                f"Failed to get account: {exc}",
                code="ACCOUNT_FAILED",
                retryable=True,
            ) from exc

    def _order_to_result(self, order: Any) -> OrderResult:
        """Convert alpaca-py Order to OrderResult."""
        status = _normalize_alpaca_status(
            order.status.value if hasattr(order.status, "value") else str(order.status)
        )

        filled_at = None
        if order.filled_at is not None:
            filled_at = (
                order.filled_at
                if isinstance(order.filled_at, datetime)
                else datetime.fromisoformat(str(order.filled_at))
            )

        return OrderResult(
            order_id=str(order.id),
            client_order_id=str(order.client_order_id or ""),
            symbol=order.symbol,
            side=OrderSide.BUY
            if (order.side.value if hasattr(order.side, "value") else str(order.side)).lower() == "buy"
            else OrderSide.SELL,
            status=status,
            submitted_at=order.submitted_at or datetime.now(timezone.utc),
            filled_qty=float(order.filled_qty or 0),
            filled_avg_price=float(order.filled_avg_price or 0),
            filled_at=filled_at,
            reject_reason="",
        )


# ---------------------------------------------------------------------------
# Sim broker adapter (deterministic fills for shadow mode)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SimBrokerConfig:
    """Configuration for simulated broker (shadow/sim mode).

    PRD §13.3: SimBrokerAdapter fills at snapshot price + configurable slippage.
    """

    slippage_model: str = "flat"  # "flat" or "volume_scaled"
    slippage_bp: float = 5.0  # basis points
    transaction_cost_bp: float = 5.0  # basis points

    def __post_init__(self) -> None:
        if self.slippage_model not in ("flat", "volume_scaled"):
            raise ValueError(
                f"slippage_model must be 'flat' or 'volume_scaled', "
                f"got '{self.slippage_model}'"
            )
        if self.slippage_bp < 0:
            raise ValueError("slippage_bp must be >= 0")
        if self.transaction_cost_bp < 0:
            raise ValueError("transaction_cost_bp must be >= 0")


@dataclass
class SimFill:
    """Result of a simulated fill."""

    symbol: str
    side: OrderSide
    requested_notional: float
    fill_price: float
    slippage_bps: float
    transaction_cost: float


class SimBrokerAdapter:
    """Deterministic broker adapter for shadow sim mode.

    Fills at snapshot price + configurable slippage (flat bps or volume-scaled).
    Transaction costs applied as configurable basis points.
    PRD §13.3.
    """

    def __init__(self, config: SimBrokerConfig) -> None:
        self._config = config
        self._order_count = 0

    def fill_order(
        self,
        symbol: str,
        side: OrderSide,
        notional: float,
        snapshot_price: float,
        volume: float = 0.0,
    ) -> SimFill:
        """Fill an order deterministically at snapshot price + slippage.

        Args:
            symbol: Ticker symbol.
            side: Buy or sell.
            notional: USD notional value of the order.
            snapshot_price: Current market price from snapshot.
            volume: Trading volume (used for volume_scaled slippage).

        Returns:
            SimFill with the execution details.
        """
        self._order_count += 1

        slippage_frac = self._compute_slippage(volume)
        slippage_bps = slippage_frac * 10_000

        # Apply slippage: buys pay more, sells receive less
        if side == OrderSide.BUY:
            fill_price = snapshot_price * (1.0 + slippage_frac)
        else:
            fill_price = snapshot_price * (1.0 - slippage_frac)

        # Transaction cost as basis points of notional
        transaction_cost = notional * self._config.transaction_cost_bp / 10_000

        return SimFill(
            symbol=symbol,
            side=side,
            requested_notional=notional,
            fill_price=fill_price,
            slippage_bps=slippage_bps,
            transaction_cost=transaction_cost,
        )

    def _compute_slippage(self, volume: float) -> float:
        """Compute slippage fraction based on configured model."""
        base_frac = self._config.slippage_bp / 10_000

        if self._config.slippage_model == "flat":
            return base_frac

        # volume_scaled: scale by inverse of volume (higher volume = less slippage)
        if volume > 0:
            scale = min(1_000_000 / volume, 5.0)  # cap at 5x
            return base_frac * scale
        return base_frac * 5.0  # max slippage if no volume

    @property
    def order_count(self) -> int:
        return self._order_count


# ---------------------------------------------------------------------------
# WebSocket streaming adapter
# ---------------------------------------------------------------------------


class AlpacaStreamAdapter:
    """WebSocket adapter for real-time bar streaming from Alpaca.

    PRD §13.2: WebSocket connection for real-time bar streaming
    with auto-reconnect and exponential backoff.
    """

    MAX_RECONNECT_DELAY = 60  # seconds

    def __init__(
        self,
        config: AlpacaBrokerConfig,
        symbols: list[str],
        on_bar: Callable[[dict[str, Any]], None],
    ) -> None:
        self._config = config
        self._symbols = symbols
        self._on_bar = on_bar
        self._stream: Any = None
        self._thread: threading.Thread | None = None
        self._running = False
        self._reconnect_count = 0

    def start(self) -> None:
        """Start streaming bars in a background thread."""
        if self._running:
            logger.warning("Stream already running")
            return

        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop,
            name="alpaca-stream",
            daemon=True,
        )
        self._thread.start()
        logger.info("Started Alpaca bar stream for %d symbols", len(self._symbols))

    def stop(self) -> None:
        """Stop streaming."""
        self._running = False
        if self._stream is not None:
            try:
                self._stream.stop()
            except Exception:
                pass
        if self._thread is not None:
            self._thread.join(timeout=10)
            self._thread = None
        logger.info("Stopped Alpaca bar stream")

    def _run_loop(self) -> None:
        """Run the stream with auto-reconnect and exponential backoff."""
        while self._running:
            try:
                self._connect_and_subscribe()
                self._reconnect_count = 0  # reset on successful connection
            except Exception as exc:
                if not self._running:
                    break
                self._reconnect_count += 1
                delay = min(
                    2 ** self._reconnect_count,
                    self.MAX_RECONNECT_DELAY,
                )
                logger.warning(
                    "Stream disconnected (attempt %d): %s — reconnecting in %ds",
                    self._reconnect_count,
                    exc,
                    delay,
                )
                time.sleep(delay)

    def _connect_and_subscribe(self) -> None:
        """Create stream, subscribe to bars, and run."""
        from alpaca.data.live import StockDataStream

        self._stream = StockDataStream(
            api_key=self._config.api_key,
            secret_key=self._config.api_secret,
            feed="iex" if self._config.paper else "sip",
        )

        @self._stream.on("bars")
        async def on_bar(bar: Any) -> None:
            try:
                bar_dict = {
                    "symbol": bar.symbol,
                    "timestamp": bar.timestamp.isoformat()
                    if hasattr(bar.timestamp, "isoformat")
                    else str(bar.timestamp),
                    "open": float(bar.open),
                    "high": float(bar.high),
                    "low": float(bar.low),
                    "close": float(bar.close),
                    "volume": int(bar.volume),
                    "vwap": float(bar.vwap) if hasattr(bar, "vwap") else 0.0,
                    "trade_count": int(bar.trade_count)
                    if hasattr(bar, "trade_count")
                    else 0,
                }
                self._on_bar(bar_dict)
            except Exception as exc:
                logger.error("Error processing bar: %s", exc)

        self._stream.subscribe_bars(on_bar, *self._symbols)
        self._stream.run()

    @property
    def is_running(self) -> bool:
        return self._running
