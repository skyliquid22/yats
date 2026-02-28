"""Kill switch state machine — trading halt/resume lifecycle.

Implements PRD §13.5 Kill Switches and §24.6 Kill Switch State Machine:
- State machine: TRADING → HALTING → HALTED → RESUMING → TRADING
- Automatic triggers: daily_loss, trailing_drawdown, data_integrity,
  broker_instability, risk_engine_failure
- Manual triggers: halt_trading, flatten_positions
- Mutex on state transitions — at most one in progress
- Duplicate halt calls are idempotent
- State transitions logged to kill_switches table and audit_trail
"""

from __future__ import annotations

import enum
import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from questdb.ingress import Protocol, Sender, TimestampNanos

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------


class TradingState(enum.Enum):
    """Kill switch state machine states (PRD §24.6)."""

    TRADING = "trading"
    HALTING = "halting"
    HALTED = "halted"
    RESUMING = "resuming"


class KillTrigger(enum.Enum):
    """Automatic and manual kill switch triggers (PRD §13.5)."""

    DAILY_LOSS = "daily_loss"
    TRAILING_DRAWDOWN = "trailing_drawdown"
    DATA_INTEGRITY = "data_integrity"
    BROKER_INSTABILITY = "broker_instability"
    RISK_ENGINE_FAILURE = "risk_engine_failure"
    MANUAL_HALT = "manual"
    MANUAL_FLATTEN = "manual_flatten"


class KillAction(enum.Enum):
    """Kill switch action type."""

    HALT = "halt"
    FLATTEN = "flatten"


@dataclass(frozen=True)
class TransitionResult:
    """Result of a state transition attempt."""

    success: bool
    from_state: TradingState
    to_state: TradingState
    message: str = ""


@dataclass(frozen=True)
class KillSwitchEvent:
    """A logged kill switch event."""

    timestamp: datetime
    experiment_id: str
    mode: str
    trigger: KillTrigger
    action: KillAction
    triggered_by: str
    details: str  # JSON string
    resolved: bool = False
    resolved_at: datetime | None = None
    resolved_by: str = ""


# ---------------------------------------------------------------------------
# Kill switch state machine
# ---------------------------------------------------------------------------


class KillSwitchStateMachine:
    """Trading execution kill switch state machine (PRD §24.6).

    Manages the TRADING → HALTING → HALTED → RESUMING → TRADING lifecycle.
    Thread-safe via mutex on all state transitions.

    Args:
        experiment_id: Experiment identifier.
        mode: Trading mode (paper/live).
        ilp_host: QuestDB ILP host.
        ilp_port: QuestDB ILP port.
        on_halt: Callback invoked when entering HALTING state.
            Receives (trigger, details) and should cancel orders + settle fills.
        on_resume: Callback invoked when entering RESUMING state.
            Should verify orders settled, no pending fills, risk state clean.
            Returns True if verification passes, False to remain HALTED.
    """

    def __init__(
        self,
        experiment_id: str,
        mode: str,
        *,
        ilp_host: str = "localhost",
        ilp_port: int = 9009,
        on_halt: Callable[[KillTrigger, str], None] | None = None,
        on_resume: Callable[[], bool] | None = None,
    ) -> None:
        self._experiment_id = experiment_id
        self._mode = mode
        self._ilp_host = ilp_host
        self._ilp_port = ilp_port
        self._on_halt = on_halt
        self._on_resume = on_resume

        self._state = TradingState.TRADING
        self._lock = threading.Lock()
        self._consecutive_broker_errors = 0
        self._active_event: KillSwitchEvent | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def state(self) -> TradingState:
        return self._state

    @property
    def is_trading(self) -> bool:
        return self._state == TradingState.TRADING

    @property
    def is_halted(self) -> bool:
        return self._state in (TradingState.HALTING, TradingState.HALTED)

    @property
    def consecutive_broker_errors(self) -> int:
        return self._consecutive_broker_errors

    def record_broker_error(self) -> None:
        """Record a consecutive broker error. Resets on success."""
        self._consecutive_broker_errors += 1

    def record_broker_success(self) -> None:
        """Reset consecutive broker error counter."""
        self._consecutive_broker_errors = 0

    # ------------------------------------------------------------------
    # Trigger evaluation
    # ------------------------------------------------------------------

    def evaluate_triggers(
        self,
        *,
        daily_pnl: float = 0.0,
        drawdown: float = 0.0,
        daily_loss_limit: float = -1.0,
        trailing_drawdown_limit: float = -1.0,
        max_broker_errors: int = 5,
        data_staleness_s: float = 0.0,
        data_staleness_threshold: float = 300.0,
    ) -> KillTrigger | None:
        """Evaluate automatic kill switch triggers.

        Returns the first triggered condition, or None if all clear.
        Only evaluates when in TRADING state.
        """
        if self._state != TradingState.TRADING:
            return None

        # Daily loss breach
        if daily_loss_limit < 0 and daily_pnl < daily_loss_limit:
            return KillTrigger.DAILY_LOSS

        # Trailing drawdown breach
        if trailing_drawdown_limit < 0 and drawdown < trailing_drawdown_limit:
            return KillTrigger.TRAILING_DRAWDOWN

        # Data integrity violation (stale data)
        if data_staleness_threshold > 0 and data_staleness_s > data_staleness_threshold:
            return KillTrigger.DATA_INTEGRITY

        # Broker instability
        if self._consecutive_broker_errors > max_broker_errors:
            return KillTrigger.BROKER_INSTABILITY

        return None

    # ------------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------------

    def trigger_halt(
        self,
        trigger: KillTrigger,
        *,
        triggered_by: str = "system",
        details: str = "{}",
    ) -> TransitionResult:
        """Trigger a trading halt (TRADING → HALTING → HALTED).

        Idempotent: returns success if already in HALTING or HALTED state.
        Thread-safe via mutex.
        """
        with self._lock:
            # Idempotent: already halting/halted
            if self._state in (TradingState.HALTING, TradingState.HALTED):
                logger.info(
                    "Halt requested but already in %s state (idempotent)",
                    self._state.value,
                )
                return TransitionResult(
                    success=True,
                    from_state=self._state,
                    to_state=self._state,
                    message="already halted/halting",
                )

            if self._state == TradingState.RESUMING:
                # Race: halt wins over resume
                logger.warning("Halt requested during RESUMING — halt wins")

            prev = self._state
            action = (
                KillAction.FLATTEN
                if trigger == KillTrigger.MANUAL_FLATTEN
                else KillAction.HALT
            )

            # Transition to HALTING
            self._state = TradingState.HALTING
            event = KillSwitchEvent(
                timestamp=datetime.now(timezone.utc),
                experiment_id=self._experiment_id,
                mode=self._mode,
                trigger=trigger,
                action=action,
                triggered_by=triggered_by,
                details=details,
            )
            self._active_event = event
            self._write_kill_switch_event(event)
            self._write_audit_trail("kill_switch.halt", triggered_by, details)

            logger.warning(
                "Kill switch activated: %s → HALTING trigger=%s by=%s",
                prev.value, trigger.value, triggered_by,
            )

        # Execute halt callback outside the lock to avoid deadlock
        if self._on_halt is not None:
            try:
                self._on_halt(trigger, details)
            except Exception as exc:
                logger.error("Halt callback failed: %s", exc, exc_info=True)

        # Transition to HALTED
        with self._lock:
            self._state = TradingState.HALTED
            logger.info("Kill switch: HALTING → HALTED")

        return TransitionResult(
            success=True,
            from_state=prev,
            to_state=TradingState.HALTED,
            message=f"halted by {trigger.value}",
        )

    def trigger_resume(
        self,
        *,
        approved_by: str,
    ) -> TransitionResult:
        """Attempt to resume trading (HALTED → RESUMING → TRADING).

        Resume requires approval (managing_partner for production, PM for paper).
        Verification callback must pass before transitioning to TRADING.
        If verification fails, remains HALTED.
        """
        with self._lock:
            if self._state != TradingState.HALTED:
                msg = f"cannot resume from {self._state.value} state"
                logger.warning("Resume rejected: %s", msg)
                return TransitionResult(
                    success=False,
                    from_state=self._state,
                    to_state=self._state,
                    message=msg,
                )

            prev = self._state
            self._state = TradingState.RESUMING

            logger.info(
                "Kill switch: HALTED → RESUMING (approved by %s)", approved_by,
            )

        # Run verification callback outside the lock
        verified = True
        if self._on_resume is not None:
            try:
                verified = self._on_resume()
            except Exception as exc:
                logger.error("Resume verification failed: %s", exc, exc_info=True)
                verified = False

        with self._lock:
            if not verified:
                self._state = TradingState.HALTED
                msg = "verification failed — remaining HALTED"
                logger.warning("Kill switch: RESUMING → HALTED (%s)", msg)
                self._write_audit_trail(
                    "kill_switch.resume_failed", approved_by,
                    '{"reason": "verification_failed"}',
                )
                return TransitionResult(
                    success=False,
                    from_state=prev,
                    to_state=TradingState.HALTED,
                    message=msg,
                )

            # Verification passed — transition to TRADING
            self._state = TradingState.TRADING
            self._consecutive_broker_errors = 0

            # Resolve the active event
            if self._active_event is not None:
                self._write_kill_switch_resolved(approved_by)
                self._active_event = None

            self._write_audit_trail(
                "kill_switch.resume", approved_by,
                '{"result": "trading_resumed"}',
            )

            logger.info(
                "Kill switch: RESUMING → TRADING (resumed by %s)", approved_by,
            )

        return TransitionResult(
            success=True,
            from_state=prev,
            to_state=TradingState.TRADING,
            message=f"resumed by {approved_by}",
        )

    # ------------------------------------------------------------------
    # QuestDB writes
    # ------------------------------------------------------------------

    def _write_kill_switch_event(self, event: KillSwitchEvent) -> None:
        """Write a kill switch event to the kill_switches table."""
        ts = TimestampNanos(int(event.timestamp.timestamp() * 1_000_000_000))

        try:
            with Sender(Protocol.Tcp, self._ilp_host, self._ilp_port) as sender:
                sender.row(
                    "kill_switches",
                    symbols={
                        "experiment_id": event.experiment_id,
                        "mode": event.mode,
                        "trigger": event.trigger.value,
                        "action": event.action.value,
                        "triggered_by": event.triggered_by,
                    },
                    columns={
                        "details": event.details,
                        "resolved": False,
                    },
                    at=ts,
                )
                sender.flush()
        except Exception as exc:
            logger.error("Failed to write kill_switches event: %s", exc)

    def _write_kill_switch_resolved(self, resolved_by: str) -> None:
        """Write a resolution event to the kill_switches table."""
        now = datetime.now(timezone.utc)
        ts = TimestampNanos(int(now.timestamp() * 1_000_000_000))

        try:
            with Sender(Protocol.Tcp, self._ilp_host, self._ilp_port) as sender:
                sender.row(
                    "kill_switches",
                    symbols={
                        "experiment_id": self._experiment_id,
                        "mode": self._mode,
                        "trigger": self._active_event.trigger.value
                        if self._active_event
                        else "unknown",
                        "action": "resume",
                        "triggered_by": resolved_by,
                    },
                    columns={
                        "details": '{"action": "resolved"}',
                        "resolved": True,
                        "resolved_by": resolved_by,
                        "resolved_at": TimestampNanos(int(now.timestamp() * 1_000_000_000)),
                    },
                    at=ts,
                )
                sender.flush()
        except Exception as exc:
            logger.error("Failed to write kill_switches resolved: %s", exc)

    def _write_audit_trail(
        self, tool_name: str, invoker: str, details: str,
    ) -> None:
        """Write an audit trail event."""
        now = datetime.now(timezone.utc)
        ts = TimestampNanos(int(now.timestamp() * 1_000_000_000))

        try:
            with Sender(Protocol.Tcp, self._ilp_host, self._ilp_port) as sender:
                sender.row(
                    "audit_trail",
                    symbols={
                        "tool_name": tool_name,
                        "invoker": invoker,
                        "experiment_id": self._experiment_id,
                        "mode": self._mode,
                        "result_status": "ok",
                    },
                    columns={
                        "parameters": details,
                        "result_summary": f"state={self._state.value}",
                        "duration_ms": 0,
                    },
                    at=ts,
                )
                sender.flush()
        except Exception as exc:
            logger.error("Failed to write audit_trail: %s", exc)
