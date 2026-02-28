"""State writers — positions, orders, portfolio snapshots to QuestDB.

Implements PRD §13.4 State Management:
- PositionWriter: updates positions table after fills
- OrderWriter: records order lifecycle events
- PortfolioStateWriter: periodic NAV snapshots

All writers use QuestDB ILP (InfluxDB Line Protocol) for writes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from questdb.ingress import Protocol, Sender, TimestampNanos

from research.execution.broker_alpaca import Fill, OrderResult, OrderSide, OrderStatus

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StateWriterConfig:
    """Connection config for state writers."""

    ilp_host: str = "localhost"
    ilp_port: int = 9009


# ---------------------------------------------------------------------------
# Position tracking (in-memory + QuestDB snapshot)
# ---------------------------------------------------------------------------


@dataclass
class Position:
    """In-memory position state for a single symbol."""

    symbol: str
    quantity: float = 0.0
    avg_entry_price: float = 0.0
    realized_pnl: float = 0.0
    cost_basis: float = 0.0  # total cost basis for avg price calc

    def apply_fill(self, fill: Fill) -> None:
        """Update position from a fill."""
        if fill.side == OrderSide.BUY:
            # Buying: increase quantity, update avg entry price
            new_cost = fill.filled_qty * fill.filled_avg_price
            self.cost_basis += new_cost
            self.quantity += fill.filled_qty
            if self.quantity > 0:
                self.avg_entry_price = self.cost_basis / self.quantity
        else:
            # Selling: reduce quantity, realize PnL
            if self.quantity > 0 and self.avg_entry_price > 0:
                sold_qty = min(fill.filled_qty, self.quantity)
                pnl = sold_qty * (fill.filled_avg_price - self.avg_entry_price)
                self.realized_pnl += pnl
                self.cost_basis -= sold_qty * self.avg_entry_price
            self.quantity -= fill.filled_qty

        # Subtract commission from realized PnL
        self.realized_pnl -= fill.commission

    def notional(self, current_price: float) -> float:
        """Current notional value."""
        return self.quantity * current_price

    def unrealized_pnl(self, current_price: float) -> float:
        """Unrealized PnL at current price."""
        if self.quantity == 0 or self.avg_entry_price == 0:
            return 0.0
        return self.quantity * (current_price - self.avg_entry_price)


class PositionWriter:
    """Writes position snapshots to QuestDB positions table after fills.

    Maintains in-memory position state and flushes snapshots to QuestDB
    after each fill. The snapshot is authoritative for fast reads; on
    restart, it is validated against aggregated fills from the orders table
    (see recovery.py).
    """

    def __init__(
        self,
        config: StateWriterConfig,
        experiment_id: str,
        mode: str,
    ) -> None:
        self._config = config
        self._experiment_id = experiment_id
        self._mode = mode
        self._positions: dict[str, Position] = {}

    @property
    def positions(self) -> dict[str, Position]:
        return self._positions

    def apply_fill(
        self,
        fill: Fill,
        current_prices: dict[str, float] | None = None,
    ) -> None:
        """Apply a fill and write updated position snapshot to QuestDB."""
        if fill.symbol not in self._positions:
            self._positions[fill.symbol] = Position(symbol=fill.symbol)

        pos = self._positions[fill.symbol]
        pos.apply_fill(fill)

        # Write snapshot
        current_price = (current_prices or {}).get(fill.symbol, fill.filled_avg_price)
        self._write_snapshot(pos, current_price)

        # Remove closed positions from tracking
        if abs(pos.quantity) < 1e-10:
            del self._positions[fill.symbol]

    def restore_positions(self, positions: dict[str, Position]) -> None:
        """Restore positions from crash recovery."""
        self._positions = dict(positions)

    def _write_snapshot(self, pos: Position, current_price: float) -> None:
        """Write a single position snapshot row to QuestDB."""
        ts = TimestampNanos(int(datetime.now(timezone.utc).timestamp() * 1_000_000_000))

        with Sender(Protocol.Tcp, self._config.ilp_host, self._config.ilp_port) as sender:
            sender.row(
                "positions",
                symbols={
                    "experiment_id": self._experiment_id,
                    "mode": self._mode,
                    "symbol": pos.symbol,
                },
                columns={
                    "quantity": pos.quantity,
                    "avg_entry_price": pos.avg_entry_price,
                    "notional": pos.notional(current_price),
                    "unrealized_pnl": pos.unrealized_pnl(current_price),
                    "realized_pnl": pos.realized_pnl,
                },
                at=ts,
            )
            sender.flush()


# ---------------------------------------------------------------------------
# Order writer
# ---------------------------------------------------------------------------


class OrderWriter:
    """Records order lifecycle events to QuestDB orders table.

    Every order submission, fill, rejection, and cancellation gets a row.
    The orders table is the authoritative ledger for position reconstruction.
    """

    def __init__(
        self,
        config: StateWriterConfig,
        experiment_id: str,
        mode: str,
        dagster_run_id: str = "",
    ) -> None:
        self._config = config
        self._experiment_id = experiment_id
        self._mode = mode
        self._dagster_run_id = dagster_run_id

    def write_order(
        self,
        result: OrderResult,
        *,
        quantity: float = 0.0,
        order_type: str = "market",
        fill_price: float = 0.0,
        fill_quantity: float = 0.0,
        slippage_bps: float = 0.0,
        fees: float = 0.0,
        risk_check_result: str = "pass",
        risk_check_details: str = "{}",
    ) -> None:
        """Write an order event row to QuestDB."""
        ts = TimestampNanos(
            int((result.submitted_at or datetime.now(timezone.utc)).timestamp() * 1_000_000_000)
        )

        status_str = result.status.value if isinstance(result.status, OrderStatus) else str(result.status)
        side_str = result.side.value if isinstance(result.side, OrderSide) else str(result.side)

        with Sender(Protocol.Tcp, self._config.ilp_host, self._config.ilp_port) as sender:
            sender.row(
                "orders",
                symbols={
                    "experiment_id": self._experiment_id,
                    "mode": self._mode,
                    "symbol": result.symbol,
                    "side": side_str,
                    "order_type": order_type,
                    "status": status_str,
                    "risk_check_result": risk_check_result,
                },
                columns={
                    "order_id": result.client_order_id or result.order_id,
                    "quantity": quantity,
                    "fill_price": fill_price,
                    "fill_quantity": fill_quantity,
                    "slippage_bps": slippage_bps,
                    "fees": fees,
                    "risk_check_details": risk_check_details,
                    "broker_order_id": result.order_id,
                    "dagster_run_id": self._dagster_run_id,
                },
                at=ts,
            )
            sender.flush()

        logger.debug(
            "Wrote order: %s %s %s status=%s",
            result.symbol, side_str, result.order_id, status_str,
        )

    def write_fill(
        self,
        result: OrderResult,
        fill: Fill,
        *,
        slippage_bps: float = 0.0,
        risk_check_result: str = "pass",
        risk_check_details: str = "{}",
    ) -> None:
        """Convenience: write a filled order event."""
        self.write_order(
            result,
            quantity=fill.filled_qty,
            fill_price=fill.filled_avg_price,
            fill_quantity=fill.filled_qty,
            slippage_bps=slippage_bps,
            fees=fill.commission,
            risk_check_result=risk_check_result,
            risk_check_details=risk_check_details,
        )


# ---------------------------------------------------------------------------
# Portfolio state writer (NAV snapshots)
# ---------------------------------------------------------------------------


@dataclass
class PortfolioSnapshot:
    """In-memory portfolio state for NAV tracking."""

    nav: float = 0.0
    cash: float = 0.0
    gross_exposure: float = 0.0
    net_exposure: float = 0.0
    leverage: float = 0.0
    num_positions: int = 0
    daily_pnl: float = 0.0
    peak_nav: float = 0.0
    drawdown: float = 0.0


class PortfolioStateWriter:
    """Writes periodic NAV snapshots to QuestDB portfolio_state table.

    On restart, the most recent snapshot + subsequent fills are used to
    reconstruct portfolio state (see recovery.py).
    """

    def __init__(
        self,
        config: StateWriterConfig,
        experiment_id: str,
        mode: str,
        dagster_run_id: str = "",
    ) -> None:
        self._config = config
        self._experiment_id = experiment_id
        self._mode = mode
        self._dagster_run_id = dagster_run_id
        self._peak_nav: float = 0.0

    def write_snapshot(
        self,
        *,
        positions: dict[str, Position],
        cash: float,
        current_prices: dict[str, float],
        daily_pnl: float = 0.0,
    ) -> PortfolioSnapshot:
        """Compute and write a portfolio state snapshot.

        Returns the computed snapshot for caller use.
        """
        # Compute portfolio metrics
        gross_exposure = 0.0
        net_exposure = 0.0
        for sym, pos in positions.items():
            price = current_prices.get(sym, pos.avg_entry_price)
            notional = pos.quantity * price
            gross_exposure += abs(notional)
            net_exposure += notional

        nav = gross_exposure + cash
        if nav > self._peak_nav:
            self._peak_nav = nav

        drawdown = 0.0
        if self._peak_nav > 0:
            drawdown = (nav - self._peak_nav) / self._peak_nav

        leverage = gross_exposure / nav if nav > 0 else 0.0

        snapshot = PortfolioSnapshot(
            nav=nav,
            cash=cash,
            gross_exposure=gross_exposure,
            net_exposure=net_exposure,
            leverage=leverage,
            num_positions=len(positions),
            daily_pnl=daily_pnl,
            peak_nav=self._peak_nav,
            drawdown=drawdown,
        )

        self._write_to_questdb(snapshot)
        return snapshot

    def restore_peak_nav(self, peak_nav: float) -> None:
        """Restore peak NAV from crash recovery."""
        self._peak_nav = peak_nav

    def _write_to_questdb(self, snap: PortfolioSnapshot) -> None:
        """Write snapshot row to QuestDB."""
        ts = TimestampNanos(int(datetime.now(timezone.utc).timestamp() * 1_000_000_000))

        with Sender(Protocol.Tcp, self._config.ilp_host, self._config.ilp_port) as sender:
            sender.row(
                "portfolio_state",
                symbols={
                    "experiment_id": self._experiment_id,
                    "mode": self._mode,
                },
                columns={
                    "nav": snap.nav,
                    "cash": snap.cash,
                    "gross_exposure": snap.gross_exposure,
                    "net_exposure": snap.net_exposure,
                    "leverage": snap.leverage,
                    "num_positions": snap.num_positions,
                    "daily_pnl": snap.daily_pnl,
                    "peak_nav": snap.peak_nav,
                    "drawdown": snap.drawdown,
                    "dagster_run_id": self._dagster_run_id,
                },
                at=ts,
            )
            sender.flush()

        logger.info(
            "Wrote portfolio_state: experiment=%s nav=%.2f positions=%d drawdown=%.4f",
            self._experiment_id, snap.nav, snap.num_positions, snap.drawdown,
        )
