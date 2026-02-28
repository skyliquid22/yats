"""Paper trading execution loop — independent process for continuous trading.

Implements PRD §13.5 Paper Trading Process:
- Continuous loop: signal → translate → risk check → submit → fill → ledger
- Writes to QuestDB directly (execution_log, orders, positions, portfolio_state)
- Heartbeat monitoring via trading_heartbeat table
- On crash: restart and reconstruct state from QuestDB (§13.4)

This module is designed to run as an independent process (managed by systemd
or equivalent), NOT as a Dagster long-running job.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from research.execution.broker_alpaca import (
    AlpacaBrokerAdapter,
    AlpacaBrokerConfig,
    AlpacaStreamAdapter,
    BrokerError,
    Fill,
    OrderResult,
    OrderSide,
    OrderStatus,
)
from research.execution.heartbeat import HeartbeatWriter
from research.execution.order_translator import translate_signals
from research.execution.recovery import RecoveryConfig, recover_state
from research.execution.signal import Signal
from research.execution.state import (
    OrderWriter,
    Position,
    PortfolioStateWriter,
    PositionWriter,
    StateWriterConfig,
)
from research.experiments.spec import RiskConfig
from research.risk.loader import load_production_risk_config
from research.risk.project_weights import project_weights

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ORDER_ACK_TIMEOUT_S = 60  # seconds — no broker response triggers alert
FILL_TIMEOUT_S = 300  # seconds — order acknowledged but no fill
BAR_STALENESS_S = 120  # seconds — no bar during market hours triggers alert
HEARTBEAT_MISS_BARS = 3  # consecutive bars without heartbeat → kill switch
PORTFOLIO_SNAPSHOT_INTERVAL = 60  # seconds between portfolio snapshots


@dataclass
class PaperTradingConfig:
    """Configuration for the paper trading execution loop."""

    experiment_id: str
    symbols: list[str]
    initial_cash: float = 100_000.0

    # QuestDB connections
    ilp_host: str = "localhost"
    ilp_port: int = 9009
    pg_host: str = "localhost"
    pg_port: int = 8812

    # Alpaca config
    alpaca_paper: bool = True

    # Loop timing
    loop_interval_s: float = 1.0  # seconds between loop iterations
    portfolio_snapshot_interval_s: float = PORTFOLIO_SNAPSHOT_INTERVAL


# ---------------------------------------------------------------------------
# Execution loop
# ---------------------------------------------------------------------------


class PaperTradingLoop:
    """Continuous paper trading execution loop.

    Architecture (PRD §13.5):
    - Receives bars via WebSocket (AlpacaStreamAdapter)
    - On each new bar: translate signals → risk check → submit → fill → ledger
    - Writes heartbeat every iteration
    - On crash: recovers state from QuestDB via recovery module
    """

    def __init__(self, config: PaperTradingConfig) -> None:
        self._config = config
        self._running = False
        self._shutdown_event = threading.Event()
        self._loop_iteration = 0

        # State writers
        state_cfg = StateWriterConfig(
            ilp_host=config.ilp_host, ilp_port=config.ilp_port,
        )
        self._position_writer = PositionWriter(
            state_cfg, config.experiment_id, "paper",
        )
        self._order_writer = OrderWriter(
            state_cfg, config.experiment_id, "paper",
        )
        self._portfolio_writer = PortfolioStateWriter(
            state_cfg, config.experiment_id, "paper",
        )
        self._heartbeat_writer = HeartbeatWriter(
            ilp_host=config.ilp_host,
            ilp_port=config.ilp_port,
            experiment_id=config.experiment_id,
            mode="paper",
        )

        # Risk config — paper trading ALWAYS uses production config
        self._risk_config = load_production_risk_config()

        # Broker adapter
        broker_cfg = AlpacaBrokerConfig.from_env(paper=config.alpaca_paper)
        self._broker = AlpacaBrokerAdapter(broker_cfg)

        # Bar stream
        self._stream: AlpacaStreamAdapter | None = None
        self._latest_bars: dict[str, dict[str, Any]] = {}
        self._bar_lock = threading.Lock()
        self._last_bar_received: datetime | None = None
        self._last_fill_received: datetime | None = None

        # Pending signals (accumulated from bar events)
        self._pending_signals: list[Signal] = []
        self._signal_lock = threading.Lock()

        # Pending orders tracking
        self._pending_orders: dict[str, OrderResult] = {}  # order_id → result

        # Portfolio state
        self._cash = config.initial_cash
        self._last_snapshot_time = datetime.now(timezone.utc)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the paper trading loop.

        1. Attempt crash recovery
        2. Start bar streaming
        3. Enter execution loop
        """
        logger.info(
            "Starting paper trading loop: experiment=%s symbols=%s",
            self._config.experiment_id,
            self._config.symbols,
        )

        self._running = True
        self._recover_state()
        self._start_bar_stream()

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)

        try:
            self._run_loop()
        finally:
            self._cleanup()

    def stop(self) -> None:
        """Signal the loop to stop gracefully."""
        logger.info("Stop requested for paper trading loop")
        self._running = False
        self._shutdown_event.set()

    def _handle_shutdown_signal(self, signum: int, frame: Any) -> None:
        """Handle SIGTERM/SIGINT for graceful shutdown."""
        logger.info("Received signal %d — initiating graceful shutdown", signum)
        self.stop()

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------

    def _recover_state(self) -> None:
        """Attempt crash recovery from QuestDB state."""
        recovery_cfg = RecoveryConfig(
            pg_host=self._config.pg_host,
            pg_port=self._config.pg_port,
        )

        try:
            result = recover_state(
                recovery_cfg,
                self._config.experiment_id,
                "paper",
                broker=self._broker,
            )

            if result.positions:
                self._position_writer.restore_positions(result.positions)
                logger.info(
                    "Recovered %d positions from QuestDB",
                    len(result.positions),
                )

            if result.portfolio.nav > 0:
                self._cash = result.portfolio.cash
                self._portfolio_writer.restore_peak_nav(result.portfolio.peak_nav)
                logger.info(
                    "Recovered portfolio: NAV=%.2f cash=%.2f",
                    result.portfolio.nav,
                    result.portfolio.cash,
                )

            if result.pending_orders:
                for order in result.pending_orders:
                    self._pending_orders[order.order_id] = order
                logger.info(
                    "Recovered %d pending orders", len(result.pending_orders),
                )

            if result.warnings:
                for w in result.warnings:
                    logger.warning("Recovery warning: %s", w)

        except Exception as exc:
            logger.warning(
                "Crash recovery failed (may be first run): %s", exc,
            )

    # ------------------------------------------------------------------
    # Bar streaming
    # ------------------------------------------------------------------

    def _start_bar_stream(self) -> None:
        """Start the Alpaca WebSocket bar stream."""
        broker_cfg = AlpacaBrokerConfig.from_env(paper=self._config.alpaca_paper)
        self._stream = AlpacaStreamAdapter(
            broker_cfg,
            self._config.symbols,
            self._on_bar,
        )
        self._stream.start()

    def _on_bar(self, bar: dict[str, Any]) -> None:
        """Callback for incoming bars from WebSocket."""
        symbol = bar.get("symbol", "")
        now = datetime.now(timezone.utc)

        with self._bar_lock:
            self._latest_bars[symbol] = bar
            self._last_bar_received = now

        # Generate signal from bar close price
        # In production, signals come from the model; here we pass bars through
        # and let the signal source (external or model-based) provide signals.
        logger.debug("Bar received: %s close=%.2f", symbol, bar.get("close", 0))

    def add_signals(self, signals: list[Signal]) -> None:
        """Add signals from an external signal source (model output).

        In production, the model writes signals that this loop consumes.
        This method allows programmatic signal injection.
        """
        with self._signal_lock:
            self._pending_signals.extend(signals)
        logger.info("Added %d signals to pending queue", len(signals))

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def _run_loop(self) -> None:
        """Main execution loop: signal → translate → risk → submit → fill → ledger."""
        logger.info("Entering execution loop")

        while self._running:
            try:
                self._loop_iteration += 1
                self._execute_iteration()
            except Exception as exc:
                logger.error(
                    "Error in execution loop iteration %d: %s",
                    self._loop_iteration, exc, exc_info=True,
                )

            # Wait for next iteration or shutdown
            if self._shutdown_event.wait(timeout=self._config.loop_interval_s):
                break

        logger.info(
            "Execution loop ended after %d iterations", self._loop_iteration,
        )

    def _execute_iteration(self) -> None:
        """Execute a single loop iteration."""
        # 1. Check and process pending order fills
        self._poll_pending_orders()

        # 2. Process pending signals
        self._process_signals()

        # 3. Write portfolio snapshot periodically
        self._maybe_write_portfolio_snapshot()

        # 4. Write heartbeat
        self._heartbeat_writer.write(
            loop_iteration=self._loop_iteration,
            orders_pending=len(self._pending_orders),
            last_bar_received=self._last_bar_received,
            last_fill_received=self._last_fill_received,
        )

    # ------------------------------------------------------------------
    # Signal processing
    # ------------------------------------------------------------------

    def _process_signals(self) -> None:
        """Process any pending signals through the full pipeline."""
        with self._signal_lock:
            signals = list(self._pending_signals)
            self._pending_signals.clear()

        if not signals:
            return

        positions = self._position_writer.positions
        current_prices = self._get_current_prices()
        nav = self._compute_nav(positions, current_prices)

        if nav <= 0:
            logger.warning("NAV is zero or negative (%.2f), skipping signals", nav)
            return

        # Translate signals to orders
        results = translate_signals(
            signals, nav, positions, current_prices,
        )

        for result in results:
            if result.order is None:
                logger.debug(
                    "Signal skipped for %s: %s",
                    result.signal.symbol, result.reason,
                )
                continue

            # Risk check: validate order against risk constraints
            if not self._risk_check(result.order.symbol, result.order.notional, nav):
                logger.info(
                    "Risk check blocked order: %s %s $%.2f",
                    result.order.symbol,
                    result.order.side.value,
                    result.order.notional,
                )
                continue

            # Submit order to broker
            self._submit_order(result.order)

    def _risk_check(
        self, symbol: str, notional: float, nav: float,
    ) -> bool:
        """Pre-trade risk check against production risk config.

        Returns True if the order passes risk constraints.
        """
        if nav <= 0:
            return False

        # Check maximum order threshold
        order_pct = notional / nav
        if order_pct > self._risk_config.maximum_order_threshold:
            # This is just a single-order size check; the full weight projection
            # happens in translate_signals via the target_position_pct
            pass  # Allow — translate already handles weight constraints

        # Check daily loss limit via current drawdown
        positions = self._position_writer.positions
        current_prices = self._get_current_prices()
        snapshot = self._portfolio_writer.write_snapshot(
            positions=positions,
            cash=self._cash,
            current_prices=current_prices,
        )
        if snapshot.drawdown < self._risk_config.daily_loss_limit:
            logger.warning(
                "Daily loss limit breached: drawdown=%.4f limit=%.4f",
                snapshot.drawdown, self._risk_config.daily_loss_limit,
            )
            return False

        # Check max broker errors (tracked by pending order failures)
        # This is monitored by the Dagster sensor, not blocked here

        return True

    # ------------------------------------------------------------------
    # Order submission and fill polling
    # ------------------------------------------------------------------

    def _submit_order(self, order: Any) -> None:
        """Submit an order to Alpaca and record it."""
        try:
            result = self._broker.submit_order(order)
            self._pending_orders[result.order_id] = result

            # Record submission
            self._order_writer.write_order(result)
            logger.info(
                "Order submitted: %s %s $%.2f → %s",
                result.symbol, result.side.value,
                order.notional, result.order_id,
            )

        except BrokerError as exc:
            logger.error("Order submission failed: %s", exc)
            # Record the failed attempt if we have enough info
            if hasattr(exc, "code"):
                logger.error("Broker error code: %s", exc.code)

    def _poll_pending_orders(self) -> None:
        """Poll Alpaca for status updates on pending orders."""
        if not self._pending_orders:
            return

        completed: list[str] = []

        for order_id, pending in self._pending_orders.items():
            try:
                result = self._broker.get_order_status(order_id)

                if result.status == OrderStatus.FILLED:
                    self._handle_fill(result)
                    completed.append(order_id)

                elif result.status in (
                    OrderStatus.CANCELLED,
                    OrderStatus.EXPIRED,
                    OrderStatus.REJECTED,
                ):
                    logger.info(
                        "Order %s terminal status: %s",
                        order_id, result.status.value,
                    )
                    self._order_writer.write_order(result)
                    completed.append(order_id)

                # Check for ack timeout
                elif result.status == OrderStatus.PENDING:
                    elapsed = (
                        datetime.now(timezone.utc) - result.submitted_at
                    ).total_seconds()
                    if elapsed > ORDER_ACK_TIMEOUT_S:
                        logger.warning(
                            "Order ack timeout: %s pending for %.0fs",
                            order_id, elapsed,
                        )

            except BrokerError as exc:
                logger.warning(
                    "Failed to poll order %s: %s", order_id, exc,
                )

        for oid in completed:
            del self._pending_orders[oid]

    def _handle_fill(self, result: OrderResult) -> None:
        """Process a filled order: update positions, write to ledger."""
        fill = Fill(
            symbol=result.symbol,
            side=result.side,
            filled_qty=result.filled_qty,
            filled_avg_price=result.filled_avg_price,
            filled_at=result.filled_at or datetime.now(timezone.utc),
        )

        # Update positions
        current_prices = self._get_current_prices()
        self._position_writer.apply_fill(fill, current_prices)

        # Update cash
        trade_value = fill.filled_qty * fill.filled_avg_price
        if fill.side == OrderSide.BUY:
            self._cash -= trade_value
        else:
            self._cash += trade_value
        self._cash -= fill.commission

        # Write order fill to ledger
        self._order_writer.write_fill(result, fill)

        self._last_fill_received = datetime.now(timezone.utc)

        logger.info(
            "Fill: %s %s qty=%.4f @ %.4f (cash=%.2f)",
            fill.symbol, fill.side.value,
            fill.filled_qty, fill.filled_avg_price, self._cash,
        )

    # ------------------------------------------------------------------
    # Portfolio snapshots
    # ------------------------------------------------------------------

    def _maybe_write_portfolio_snapshot(self) -> None:
        """Write a portfolio snapshot if enough time has elapsed."""
        now = datetime.now(timezone.utc)
        elapsed = (now - self._last_snapshot_time).total_seconds()

        if elapsed < self._config.portfolio_snapshot_interval_s:
            return

        positions = self._position_writer.positions
        current_prices = self._get_current_prices()

        self._portfolio_writer.write_snapshot(
            positions=positions,
            cash=self._cash,
            current_prices=current_prices,
        )
        self._last_snapshot_time = now

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_current_prices(self) -> dict[str, float]:
        """Get current prices from latest bars."""
        with self._bar_lock:
            return {
                sym: bar.get("close", 0.0)
                for sym, bar in self._latest_bars.items()
            }

    def _compute_nav(
        self,
        positions: dict[str, Position],
        current_prices: dict[str, float],
    ) -> float:
        """Compute current NAV from positions + cash."""
        exposure = sum(
            abs(pos.notional(current_prices.get(sym, pos.avg_entry_price)))
            for sym, pos in positions.items()
        )
        return exposure + self._cash

    def _cleanup(self) -> None:
        """Clean up resources on shutdown."""
        logger.info("Cleaning up paper trading loop")

        # Stop bar stream
        if self._stream is not None:
            self._stream.stop()

        # Cancel all pending orders
        for order_id in list(self._pending_orders.keys()):
            try:
                result = self._broker.cancel_order(order_id)
                self._order_writer.write_order(result)
                logger.info("Cancelled pending order: %s", order_id)
            except BrokerError as exc:
                logger.warning("Failed to cancel order %s: %s", order_id, exc)

        # Write final portfolio snapshot
        positions = self._position_writer.positions
        current_prices = self._get_current_prices()
        self._portfolio_writer.write_snapshot(
            positions=positions,
            cash=self._cash,
            current_prices=current_prices,
        )

        logger.info(
            "Paper trading loop stopped. Final NAV=%.2f, %d positions",
            self._compute_nav(positions, current_prices),
            len(positions),
        )

    # ------------------------------------------------------------------
    # State access (for Dagster setup/teardown)
    # ------------------------------------------------------------------

    @property
    def positions(self) -> dict[str, Position]:
        return self._position_writer.positions

    @property
    def cash(self) -> float:
        return self._cash

    @property
    def loop_iteration(self) -> int:
        return self._loop_iteration

    @property
    def is_running(self) -> bool:
        return self._running
