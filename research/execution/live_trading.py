"""Live trading execution loop — independent process for live Alpaca trading.

Implements PRD §13.5 Live Trading Process (lines 1375-1381):
- Same architecture as paper_trading: Dagster handles setup/teardown,
  independent Python process runs execution loop against LIVE Alpaca endpoint.
- Gate: managing_partner approval required before launch.
- Trigger: yats.execution.promote_live

This module mirrors PaperTradingLoop with two critical differences:
1. Broker connects to live Alpaca endpoint (production_tier=True)
2. All state writes use mode="live" for isolation from paper data
"""

from __future__ import annotations

import json
import logging
import signal
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from research.execution.broker_alpaca import (
    AlpacaBrokerAdapter,
    AlpacaBrokerConfig,
    AlpacaStreamAdapter,
    BrokerError,
    Fill,
    OrderRequest,
    OrderResult,
    OrderSide,
    OrderStatus,
)
from research.execution.heartbeat import HeartbeatWriter
from research.execution.kill_switch import (
    KillSwitchStateMachine,
    KillTrigger,
    TradingState,
)
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

ORDER_ACK_TIMEOUT_S = 60
FILL_TIMEOUT_S = 300
BAR_STALENESS_S = 120
HEARTBEAT_MISS_BARS = 3
PORTFOLIO_SNAPSHOT_INTERVAL = 60


@dataclass
class LiveTradingConfig:
    """Configuration for the live trading execution loop."""

    experiment_id: str
    symbols: list[str]
    initial_cash: float = 100_000.0

    # QuestDB connections
    ilp_host: str = "localhost"
    ilp_port: int = 9009
    pg_host: str = "localhost"
    pg_port: int = 8812

    # Loop timing
    loop_interval_s: float = 1.0
    portfolio_snapshot_interval_s: float = PORTFOLIO_SNAPSHOT_INTERVAL

    # Managing partner acknowledgment (must be True)
    managing_partner_ack: bool = False


# ---------------------------------------------------------------------------
# Execution loop
# ---------------------------------------------------------------------------


class LiveTradingLoop:
    """Continuous live trading execution loop.

    Architecture mirrors PaperTradingLoop (PRD §13.5) with live Alpaca
    endpoint and production_tier=True. The managing_partner_ack gate
    is enforced at construction time.
    """

    def __init__(self, config: LiveTradingConfig) -> None:
        if not config.managing_partner_ack:
            raise ValueError(
                "Live trading requires managing_partner_ack=True. "
                "This gate cannot be bypassed."
            )

        self._config = config
        self._running = False
        self._shutdown_event = threading.Event()
        self._loop_iteration = 0

        # State writers — mode="live" isolates from paper data
        state_cfg = StateWriterConfig(
            ilp_host=config.ilp_host, ilp_port=config.ilp_port,
        )
        self._position_writer = PositionWriter(
            state_cfg, config.experiment_id, "live",
        )
        self._order_writer = OrderWriter(
            state_cfg, config.experiment_id, "live",
        )
        self._portfolio_writer = PortfolioStateWriter(
            state_cfg, config.experiment_id, "live",
        )
        self._heartbeat_writer = HeartbeatWriter(
            ilp_host=config.ilp_host,
            ilp_port=config.ilp_port,
            experiment_id=config.experiment_id,
            mode="live",
        )

        # Risk config — live trading ALWAYS uses production config
        self._risk_config = load_production_risk_config()

        # Broker adapter — LIVE endpoint, production_tier=True
        broker_cfg = AlpacaBrokerConfig.from_env(
            paper=False, production_tier=True,
        )
        self._broker = AlpacaBrokerAdapter(broker_cfg)

        # Kill switch state machine
        self._kill_switch = KillSwitchStateMachine(
            config.experiment_id,
            "live",
            ilp_host=config.ilp_host,
            ilp_port=config.ilp_port,
            on_halt=self._handle_halt,
            on_resume=self._handle_resume_verification,
        )

        # Bar stream
        self._stream: AlpacaStreamAdapter | None = None
        self._latest_bars: dict[str, dict[str, Any]] = {}
        self._bar_lock = threading.Lock()
        self._last_bar_received: datetime | None = None
        self._last_fill_received: datetime | None = None

        # Pending signals
        self._pending_signals: list[Signal] = []
        self._signal_lock = threading.Lock()

        # Pending orders tracking
        self._pending_orders: dict[str, OrderResult] = {}

        # Portfolio state
        self._cash = config.initial_cash
        self._last_snapshot_time = datetime.now(timezone.utc)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the live trading loop.

        1. Attempt crash recovery
        2. Start bar streaming (live endpoint)
        3. Enter execution loop
        """
        logger.info(
            "Starting LIVE trading loop: experiment=%s symbols=%s",
            self._config.experiment_id,
            self._config.symbols,
        )

        self._running = True
        self._recover_state()
        self._start_bar_stream()

        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)

        try:
            self._run_loop()
        finally:
            self._cleanup()

    def stop(self) -> None:
        """Signal the loop to stop gracefully."""
        logger.info("Stop requested for live trading loop")
        self._running = False
        self._shutdown_event.set()

    def _handle_shutdown_signal(self, signum: int, frame: Any) -> None:
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
                "live",
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
        """Start the Alpaca WebSocket bar stream (live endpoint)."""
        broker_cfg = AlpacaBrokerConfig.from_env(
            paper=False, production_tier=True,
        )
        self._stream = AlpacaStreamAdapter(
            broker_cfg,
            self._config.symbols,
            self._on_bar,
        )
        self._stream.start()

    def _on_bar(self, bar: dict[str, Any]) -> None:
        symbol = bar.get("symbol", "")
        now = datetime.now(timezone.utc)

        with self._bar_lock:
            self._latest_bars[symbol] = bar
            self._last_bar_received = now

        logger.debug("Bar received: %s close=%.2f", symbol, bar.get("close", 0))

    def add_signals(self, signals: list[Signal]) -> None:
        """Add signals from an external signal source."""
        with self._signal_lock:
            self._pending_signals.extend(signals)
        logger.info("Added %d signals to pending queue", len(signals))

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def _run_loop(self) -> None:
        logger.info("Entering LIVE execution loop")

        while self._running:
            try:
                self._loop_iteration += 1
                self._execute_iteration()
            except Exception as exc:
                logger.error(
                    "Error in live execution loop iteration %d: %s",
                    self._loop_iteration, exc, exc_info=True,
                )

            if self._shutdown_event.wait(timeout=self._config.loop_interval_s):
                break

        logger.info(
            "Live execution loop ended after %d iterations", self._loop_iteration,
        )

    def _execute_iteration(self) -> None:
        self._poll_pending_orders()
        self._evaluate_kill_triggers()

        if self._kill_switch.is_trading:
            self._process_signals()

        self._maybe_write_portfolio_snapshot()

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

            if not self._risk_check(result.order.symbol, result.order.notional, nav):
                logger.info(
                    "Risk check blocked order: %s %s $%.2f",
                    result.order.symbol,
                    result.order.side.value,
                    result.order.notional,
                )
                continue

            self._submit_order(result.order)

    def _risk_check(
        self, symbol: str, notional: float, nav: float,
    ) -> bool:
        if nav <= 0:
            return False

        if not self._kill_switch.is_trading:
            return False

        order_pct = notional / nav
        min_threshold = getattr(
            self._risk_config, "minimum_order_threshold", 0.01,
        )
        if order_pct < min_threshold:
            logger.debug(
                "Order below minimum threshold: %.4f < %.4f",
                order_pct, min_threshold,
            )
            return False

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

        return True

    # ------------------------------------------------------------------
    # Kill switch
    # ------------------------------------------------------------------

    def _evaluate_kill_triggers(self) -> None:
        if not self._kill_switch.is_trading:
            return

        positions = self._position_writer.positions
        current_prices = self._get_current_prices()
        snapshot = self._portfolio_writer.write_snapshot(
            positions=positions,
            cash=self._cash,
            current_prices=current_prices,
        )

        data_staleness_s = 0.0
        if self._last_bar_received is not None:
            data_staleness_s = (
                datetime.now(timezone.utc) - self._last_bar_received
            ).total_seconds()

        trigger = self._kill_switch.evaluate_triggers(
            daily_pnl=snapshot.daily_pnl,
            drawdown=snapshot.drawdown,
            daily_loss_limit=self._risk_config.daily_loss_limit,
            trailing_drawdown_limit=self._risk_config.trailing_drawdown_limit,
            max_broker_errors=getattr(self._risk_config, "max_broker_errors", 5),
            data_staleness_s=data_staleness_s,
            data_staleness_threshold=getattr(
                self._risk_config, "data_staleness_threshold", 300.0,
            ),
        )

        if trigger is not None:
            details = json.dumps({
                "drawdown": snapshot.drawdown,
                "daily_pnl": snapshot.daily_pnl,
                "nav": snapshot.nav,
                "data_staleness_s": data_staleness_s,
                "broker_errors": self._kill_switch.consecutive_broker_errors,
            })
            self._kill_switch.trigger_halt(trigger, details=details)

    def _handle_halt(self, trigger: KillTrigger, details: str) -> None:
        logger.warning(
            "Executing LIVE halt procedure: trigger=%s experiment=%s",
            trigger.value, self._config.experiment_id,
        )

        for order_id in list(self._pending_orders.keys()):
            try:
                result = self._broker.cancel_order(order_id)
                self._order_writer.write_order(result)
                logger.info("Halt: cancelled order %s", order_id)
            except BrokerError as exc:
                logger.warning("Halt: failed to cancel order %s: %s", order_id, exc)

        self._poll_pending_orders()

        with self._signal_lock:
            discarded = len(self._pending_signals)
            self._pending_signals.clear()
        if discarded:
            logger.info("Halt: discarded %d pending signals", discarded)

    def _handle_resume_verification(self) -> bool:
        if self._pending_orders:
            logger.warning(
                "Resume verification failed: %d pending orders",
                len(self._pending_orders),
            )
            return False

        positions = self._position_writer.positions
        current_prices = self._get_current_prices()
        snapshot = self._portfolio_writer.write_snapshot(
            positions=positions,
            cash=self._cash,
            current_prices=current_prices,
        )

        if snapshot.drawdown < self._risk_config.trailing_drawdown_limit:
            logger.warning(
                "Resume verification failed: drawdown=%.4f exceeds limit=%.4f",
                snapshot.drawdown, self._risk_config.trailing_drawdown_limit,
            )
            return False

        logger.info(
            "Resume verification passed: NAV=%.2f drawdown=%.4f pending_orders=0",
            snapshot.nav, snapshot.drawdown,
        )
        return True

    def halt_trading(
        self,
        *,
        triggered_by: str = "manual",
        details: str = "{}",
    ) -> bool:
        result = self._kill_switch.trigger_halt(
            KillTrigger.MANUAL_HALT,
            triggered_by=triggered_by,
            details=details,
        )
        return result.success

    def flatten_positions(
        self,
        *,
        triggered_by: str = "manual",
        details: str = "{}",
    ) -> bool:
        result = self._kill_switch.trigger_halt(
            KillTrigger.MANUAL_FLATTEN,
            triggered_by=triggered_by,
            details=details,
        )
        if not result.success:
            return False

        positions = self._position_writer.positions
        current_prices = self._get_current_prices()

        for sym, pos in positions.items():
            if abs(pos.quantity) < 1e-10:
                continue

            side = OrderSide.SELL if pos.quantity > 0 else OrderSide.BUY
            notional = abs(pos.notional(
                current_prices.get(sym, pos.avg_entry_price),
            ))

            if notional < 1.0:
                continue

            order = OrderRequest(
                symbol=sym,
                side=side,
                notional=notional,
            )
            try:
                order_result = self._broker.submit_order(order)
                self._pending_orders[order_result.order_id] = order_result
                self._order_writer.write_order(order_result)
                logger.info(
                    "Flatten: submitted close order %s %s $%.2f",
                    sym, side.value, notional,
                )
            except BrokerError as exc:
                logger.error("Flatten: failed to close %s: %s", sym, exc)

        return True

    def resume_trading(self, *, approved_by: str) -> bool:
        """Resume trading after halt. Requires managing_partner approval for live."""
        result = self._kill_switch.trigger_resume(approved_by=approved_by)
        return result.success

    # ------------------------------------------------------------------
    # Order submission and fill polling
    # ------------------------------------------------------------------

    def _submit_order(self, order: Any) -> None:
        try:
            result = self._broker.submit_order(order)
            self._pending_orders[result.order_id] = result
            self._kill_switch.record_broker_success()
            self._order_writer.write_order(result)
            logger.info(
                "Order submitted: %s %s $%.2f → %s",
                result.symbol, result.side.value,
                order.notional, result.order_id,
            )
        except BrokerError as exc:
            self._kill_switch.record_broker_error()
            logger.error("Order submission failed: %s", exc)
            if hasattr(exc, "code"):
                logger.error("Broker error code: %s", exc.code)

    def _poll_pending_orders(self) -> None:
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
                self._kill_switch.record_broker_error()
                logger.warning(
                    "Failed to poll order %s: %s", order_id, exc,
                )

        for oid in completed:
            del self._pending_orders[oid]

    def _handle_fill(self, result: OrderResult) -> None:
        fill = Fill(
            symbol=result.symbol,
            side=result.side,
            filled_qty=result.filled_qty,
            filled_avg_price=result.filled_avg_price,
            filled_at=result.filled_at or datetime.now(timezone.utc),
        )

        current_prices = self._get_current_prices()
        self._position_writer.apply_fill(fill, current_prices)

        trade_value = fill.filled_qty * fill.filled_avg_price
        if fill.side == OrderSide.BUY:
            self._cash -= trade_value
        else:
            self._cash += trade_value
        self._cash -= fill.commission

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
        exposure = sum(
            abs(pos.notional(current_prices.get(sym, pos.avg_entry_price)))
            for sym, pos in positions.items()
        )
        return exposure + self._cash

    def _cleanup(self) -> None:
        logger.info("Cleaning up live trading loop")

        if self._stream is not None:
            self._stream.stop()

        for order_id in list(self._pending_orders.keys()):
            try:
                result = self._broker.cancel_order(order_id)
                self._order_writer.write_order(result)
                logger.info("Cancelled pending order: %s", order_id)
            except BrokerError as exc:
                logger.warning("Failed to cancel order %s: %s", order_id, exc)

        positions = self._position_writer.positions
        current_prices = self._get_current_prices()
        self._portfolio_writer.write_snapshot(
            positions=positions,
            cash=self._cash,
            current_prices=current_prices,
        )

        logger.info(
            "Live trading loop stopped. Final NAV=%.2f, %d positions",
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

    @property
    def trading_state(self) -> TradingState:
        return self._kill_switch.state

    @property
    def kill_switch(self) -> KillSwitchStateMachine:
        return self._kill_switch
