"""Unit tests for risk engine integration in the paper trading loop.

Tests that _process_signals correctly:
- Calls project_weights_full before order translation
- Logs risk decisions via RiskDecisionsWriter
- Honors SIZE_REDUCE decisions (smaller adjusted weights → smaller orders)
- Honors HALT decisions (triggers kill switch, no orders submitted)
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from research.execution.paper_trading import PaperTradingConfig, PaperTradingLoop
from research.execution.signal import Signal
from research.experiments.spec import RiskConfig
from research.risk.project_weights import Decision, RiskDecision, RiskResult


@pytest.fixture
def config():
    return PaperTradingConfig(
        experiment_id="test-risk-simple",
        symbols=["AAPL", "MSFT"],
        initial_cash=100_000.0,
    )


@pytest.fixture
def loop(config):
    with (
        patch("research.execution.paper_trading.load_production_risk_config") as mock_risk,
        patch("research.execution.paper_trading.AlpacaBrokerAdapter") as mock_broker_cls,
        patch("research.execution.paper_trading.AlpacaBrokerConfig.from_env"),
    ):
        mock_risk.return_value = RiskConfig()
        mock_broker_cls.return_value = MagicMock()
        lp = PaperTradingLoop(config)

    lp._risk_writer = MagicMock()
    lp._broker = MagicMock()
    lp._order_writer = MagicMock()
    lp._position_writer = MagicMock()
    lp._position_writer.positions = {}
    lp._portfolio_writer = MagicMock()
    lp._portfolio_writer.write_snapshot.return_value = MagicMock(drawdown=-0.01)
    lp._get_current_prices = MagicMock(return_value={"AAPL": 150.0, "MSFT": 300.0})
    lp._compute_nav = MagicMock(return_value=100_000.0)
    lp._kill_switch = MagicMock()
    lp._kill_switch.is_trading = True
    return lp


def _signal(symbol: str, pct: float) -> Signal:
    return Signal(
        timestamp=datetime(2024, 6, 15, tzinfo=timezone.utc),
        symbol=symbol,
        target_position_pct=pct,
        model_version="test-v1",
    )


class TestRiskEngineIsCalled:
    def test_project_weights_full_called_once(self, loop):
        loop.add_signals([_signal("AAPL", 0.3)])

        with patch("research.execution.paper_trading.project_weights_full") as mock_pwf:
            mock_pwf.return_value = RiskResult(
                weights=np.array([0.3]),
                decisions=[RiskDecision(rule_id="max_gross_exposure", decision=Decision.PASS)],
            )
            loop._process_signals()

        mock_pwf.assert_called_once()

    def test_target_weights_passed_to_risk_engine(self, loop):
        loop.add_signals([_signal("AAPL", 0.25), _signal("MSFT", 0.15)])

        with patch("research.execution.paper_trading.project_weights_full") as mock_pwf:
            mock_pwf.return_value = RiskResult(
                weights=np.array([0.25, 0.15]),
                decisions=[],
            )
            loop._process_signals()

        raw_weights = mock_pwf.call_args[0][0]
        # Order may vary; check both values are present
        assert sorted(raw_weights.tolist()) == pytest.approx(sorted([0.25, 0.15]))

    def test_risk_decisions_logged_after_engine(self, loop):
        loop.add_signals([_signal("AAPL", 0.1)])

        with patch("research.execution.paper_trading.project_weights_full") as mock_pwf:
            mock_pwf.return_value = RiskResult(
                weights=np.array([0.1]),
                decisions=[RiskDecision(rule_id="max_gross_exposure", decision=Decision.PASS)],
            )
            loop._process_signals()

        loop._risk_writer.log_result.assert_called_once()

    def test_empty_signals_skips_risk_engine(self, loop):
        with patch("research.execution.paper_trading.project_weights_full") as mock_pwf:
            loop._process_signals()

        mock_pwf.assert_not_called()
        loop._risk_writer.log_result.assert_not_called()


class TestSizeReduceDecision:
    def test_adjusted_weights_passed_to_translator(self, loop):
        """SIZE_REDUCE: translate_signals receives the reduced weights, not the raw ones."""
        loop.add_signals([_signal("AAPL", 0.5)])

        with patch("research.execution.paper_trading.project_weights_full") as mock_pwf:
            mock_pwf.return_value = RiskResult(
                weights=np.array([0.3]),
                decisions=[
                    RiskDecision(
                        rule_id="max_gross_exposure",
                        decision=Decision.SIZE_REDUCE,
                        details={"total": 0.5, "limit": 0.3},
                    )
                ],
            )
            with patch("research.execution.paper_trading.translate_signals") as mock_ts:
                mock_ts.return_value = []
                loop._process_signals()

        assert mock_ts.called
        adjusted = mock_ts.call_args[0][0]
        assert len(adjusted) == 1
        assert adjusted[0].target_position_pct == pytest.approx(0.3)
        assert adjusted[0].symbol == "AAPL"

    def test_original_signal_fields_preserved(self, loop):
        """SIZE_REDUCE: symbol, model_version, confidence_score are preserved."""
        sig = Signal(
            timestamp=datetime(2024, 6, 15, tzinfo=timezone.utc),
            symbol="AAPL",
            target_position_pct=0.5,
            model_version="policy-v2",
            confidence_score=0.85,
        )
        loop.add_signals([sig])

        with patch("research.execution.paper_trading.project_weights_full") as mock_pwf:
            mock_pwf.return_value = RiskResult(
                weights=np.array([0.3]),
                decisions=[
                    RiskDecision(rule_id="max_gross_exposure", decision=Decision.SIZE_REDUCE)
                ],
            )
            with patch("research.execution.paper_trading.translate_signals") as mock_ts:
                mock_ts.return_value = []
                loop._process_signals()

        adjusted = mock_ts.call_args[0][0]
        assert adjusted[0].model_version == "policy-v2"
        assert adjusted[0].confidence_score == pytest.approx(0.85)


class TestHaltDecision:
    def test_halt_in_decisions_triggers_kill_switch(self, loop):
        loop.add_signals([_signal("AAPL", 0.9)])

        with patch("research.execution.paper_trading.project_weights_full") as mock_pwf:
            mock_pwf.return_value = RiskResult(
                weights=np.array([0.9]),
                decisions=[
                    RiskDecision(
                        rule_id="max_gross_exposure",
                        decision=Decision.HALT,
                    )
                ],
            )
            loop._process_signals()

        loop._kill_switch.trigger_halt.assert_called_once()
        loop._broker.submit_order.assert_not_called()

    def test_halted_flag_triggers_kill_switch(self, loop):
        loop.add_signals([_signal("AAPL", 0.9)])

        with patch("research.execution.paper_trading.project_weights_full") as mock_pwf:
            mock_pwf.return_value = RiskResult(
                weights=np.array([0.9]),
                decisions=[],
                halted=True,
            )
            loop._process_signals()

        loop._kill_switch.trigger_halt.assert_called_once()
        loop._broker.submit_order.assert_not_called()

    def test_halt_logs_decisions_before_stopping(self, loop):
        """risk_decisions are logged even when a halt is triggered."""
        loop.add_signals([_signal("AAPL", 0.9)])

        with patch("research.execution.paper_trading.project_weights_full") as mock_pwf:
            mock_pwf.return_value = RiskResult(
                weights=np.array([0.9]),
                decisions=[RiskDecision(rule_id="max_gross_exposure", decision=Decision.HALT)],
            )
            loop._process_signals()

        loop._risk_writer.log_result.assert_called_once()


class TestRejectDecision:
    def test_reject_zeroes_weight_no_order_for_symbol(self, loop):
        """REJECT: weight set to 0 means no order for that symbol."""
        loop.add_signals([_signal("AAPL", 0.4)])

        with patch("research.execution.paper_trading.project_weights_full") as mock_pwf:
            mock_pwf.return_value = RiskResult(
                weights=np.array([0.0]),
                decisions=[
                    RiskDecision(rule_id="max_active_positions", decision=Decision.REJECT)
                ],
            )
            with patch("research.execution.paper_trading.translate_signals") as mock_ts:
                mock_ts.return_value = []
                loop._process_signals()

        adjusted = mock_ts.call_args[0][0]
        assert adjusted[0].target_position_pct == pytest.approx(0.0)
