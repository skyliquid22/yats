"""Tests for kill switch state machine (PRD §24.6)."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import pytest

from research.execution.kill_switch import (
    KillAction,
    KillSwitchStateMachine,
    KillTrigger,
    TradingState,
    TransitionResult,
)


@pytest.fixture
def ks() -> KillSwitchStateMachine:
    """Kill switch with mocked QuestDB writes."""
    with patch("research.execution.kill_switch.Sender"):
        return KillSwitchStateMachine(
            "test-exp",
            "paper",
            ilp_host="localhost",
            ilp_port=9009,
        )


class TestTradingStateTransitions:
    def test_initial_state_is_trading(self, ks: KillSwitchStateMachine) -> None:
        assert ks.state == TradingState.TRADING
        assert ks.is_trading
        assert not ks.is_halted

    def test_halt_transitions_to_halted(self, ks: KillSwitchStateMachine) -> None:
        result = ks.trigger_halt(KillTrigger.DAILY_LOSS)
        assert result.success
        assert result.to_state == TradingState.HALTED
        assert ks.state == TradingState.HALTED
        assert ks.is_halted
        assert not ks.is_trading

    def test_halt_is_idempotent(self, ks: KillSwitchStateMachine) -> None:
        ks.trigger_halt(KillTrigger.DAILY_LOSS)
        result = ks.trigger_halt(KillTrigger.TRAILING_DRAWDOWN)
        assert result.success
        assert result.message == "already halted/halting"
        assert ks.state == TradingState.HALTED

    def test_resume_from_halted(self, ks: KillSwitchStateMachine) -> None:
        ks.trigger_halt(KillTrigger.MANUAL_HALT)
        result = ks.trigger_resume(approved_by="pm")
        assert result.success
        assert result.to_state == TradingState.TRADING
        assert ks.is_trading

    def test_resume_fails_if_not_halted(self, ks: KillSwitchStateMachine) -> None:
        result = ks.trigger_resume(approved_by="pm")
        assert not result.success
        assert "cannot resume from trading" in result.message

    def test_resume_fails_if_verification_fails(self) -> None:
        with patch("research.execution.kill_switch.Sender"):
            ks = KillSwitchStateMachine(
                "test-exp", "paper",
                on_resume=lambda: False,
            )
        ks.trigger_halt(KillTrigger.DAILY_LOSS)
        result = ks.trigger_resume(approved_by="pm")
        assert not result.success
        assert ks.state == TradingState.HALTED

    def test_halt_callback_invoked(self) -> None:
        callback = MagicMock()
        with patch("research.execution.kill_switch.Sender"):
            ks = KillSwitchStateMachine(
                "test-exp", "paper", on_halt=callback,
            )
        ks.trigger_halt(KillTrigger.BROKER_INSTABILITY, details='{"count": 6}')
        callback.assert_called_once_with(
            KillTrigger.BROKER_INSTABILITY, '{"count": 6}',
        )

    def test_resume_resets_broker_errors(self, ks: KillSwitchStateMachine) -> None:
        ks.record_broker_error()
        ks.record_broker_error()
        assert ks.consecutive_broker_errors == 2
        ks.trigger_halt(KillTrigger.BROKER_INSTABILITY)
        ks.trigger_resume(approved_by="pm")
        assert ks.consecutive_broker_errors == 0

    def test_flatten_action(self, ks: KillSwitchStateMachine) -> None:
        result = ks.trigger_halt(KillTrigger.MANUAL_FLATTEN)
        assert result.success
        assert ks.state == TradingState.HALTED


class TestTriggerEvaluation:
    def test_no_trigger_when_healthy(self, ks: KillSwitchStateMachine) -> None:
        trigger = ks.evaluate_triggers(
            daily_pnl=0.01,
            drawdown=-0.05,
            daily_loss_limit=-0.05,
            trailing_drawdown_limit=-0.20,
        )
        assert trigger is None

    def test_daily_loss_trigger(self, ks: KillSwitchStateMachine) -> None:
        trigger = ks.evaluate_triggers(
            daily_pnl=-0.06,
            drawdown=-0.05,
            daily_loss_limit=-0.05,
        )
        assert trigger == KillTrigger.DAILY_LOSS

    def test_trailing_drawdown_trigger(self, ks: KillSwitchStateMachine) -> None:
        trigger = ks.evaluate_triggers(
            daily_pnl=0.0,
            drawdown=-0.25,
            trailing_drawdown_limit=-0.20,
        )
        assert trigger == KillTrigger.TRAILING_DRAWDOWN

    def test_data_integrity_trigger(self, ks: KillSwitchStateMachine) -> None:
        trigger = ks.evaluate_triggers(
            data_staleness_s=400.0,
            data_staleness_threshold=300.0,
        )
        assert trigger == KillTrigger.DATA_INTEGRITY

    def test_broker_instability_trigger(self, ks: KillSwitchStateMachine) -> None:
        for _ in range(6):
            ks.record_broker_error()
        trigger = ks.evaluate_triggers(max_broker_errors=5)
        assert trigger == KillTrigger.BROKER_INSTABILITY

    def test_no_trigger_when_halted(self, ks: KillSwitchStateMachine) -> None:
        ks.trigger_halt(KillTrigger.DAILY_LOSS)
        trigger = ks.evaluate_triggers(
            daily_pnl=-0.10,
            daily_loss_limit=-0.05,
        )
        assert trigger is None

    def test_broker_success_resets_counter(self, ks: KillSwitchStateMachine) -> None:
        ks.record_broker_error()
        ks.record_broker_error()
        ks.record_broker_success()
        assert ks.consecutive_broker_errors == 0


class TestThreadSafety:
    def test_concurrent_halt_calls(self) -> None:
        """Multiple threads calling halt should not corrupt state."""
        with patch("research.execution.kill_switch.Sender"):
            ks = KillSwitchStateMachine("test-exp", "paper")

        results: list[TransitionResult] = []
        errors: list[Exception] = []

        def halt_worker() -> None:
            try:
                r = ks.trigger_halt(KillTrigger.DAILY_LOSS)
                results.append(r)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=halt_worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert all(r.success for r in results)
        assert ks.state == TradingState.HALTED
