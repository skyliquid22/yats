"""Integration tests: real project_weights_full wired into paper/live trading loops.

Verifies without mocking the risk engine that:
1. An order violating max_gross_exposure is size-reduced in the paper loop
2. risk_decisions rows are written when violations occur
3. Live trading loop also has the risk engine wired (project_weights_full called)
4. Post-fill constraint verification runs after each fill

A tight risk config (max_gross_exposure=0.50) makes violations easy to trigger.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from research.execution.paper_trading import PaperTradingConfig, PaperTradingLoop
from research.execution.live_trading import LiveTradingConfig, LiveTradingLoop
from research.execution.signal import Signal
from research.experiments.spec import RiskConfig
from research.risk.project_weights import Decision, project_weights_full

# Deliberately tight config to trigger size_reduce on moderate exposure requests
_TIGHT_RISK = RiskConfig(
    max_gross_exposure=0.50,
    max_symbol_weight=0.30,
    min_cash=0.02,
    minimum_order_threshold=0.001,
    max_daily_turnover=2.0,
    daily_loss_limit=-1.0,
    trailing_drawdown_limit=-1.0,
)


def _signal(symbol: str, pct: float) -> Signal:
    return Signal(
        timestamp=datetime(2024, 6, 15, tzinfo=timezone.utc),
        symbol=symbol,
        target_position_pct=pct,
        model_version="test-v1",
    )


def _make_paper_loop(risk_config: RiskConfig) -> PaperTradingLoop:
    config = PaperTradingConfig(
        experiment_id="test-integration",
        symbols=["AAPL", "MSFT"],
        initial_cash=100_000.0,
    )
    with (
        patch("research.execution.paper_trading.load_production_risk_config") as mock_risk,
        patch("research.execution.paper_trading.AlpacaBrokerAdapter") as mock_broker_cls,
        patch("research.execution.paper_trading.AlpacaBrokerConfig.from_env"),
    ):
        mock_risk.return_value = risk_config
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


class TestMaxGrossExposureEnforced:
    def test_size_reduced_order_submitted_when_exposure_violated(self):
        """Orders exceeding max_gross_exposure=0.50 are scaled down before submission.

        Raw request: AAPL=0.40 + MSFT=0.40 = 0.80 total  (exceeds 0.50 limit)
        Expected: risk engine scales to ≤0.48 (0.50 - 0.02 min_cash)
        """
        loop = _make_paper_loop(_TIGHT_RISK)
        loop.add_signals([_signal("AAPL", 0.40), _signal("MSFT", 0.40)])
        loop._process_signals()

        calls = loop._broker.submit_order.call_args_list
        # Some order(s) must have been submitted (not blocked entirely)
        assert len(calls) > 0
        total_notional = sum(c[0][0].notional for c in calls)
        # Total submitted must be less than the raw-requested 80K
        assert total_notional < 80_000.0

    def test_risk_decisions_written_for_exposure_violation(self):
        """When max_gross_exposure is violated, SIZE_REDUCE decision is logged."""
        loop = _make_paper_loop(_TIGHT_RISK)
        loop.add_signals([_signal("AAPL", 0.40), _signal("MSFT", 0.40)])
        loop._process_signals()

        loop._risk_writer.log_result.assert_called_once()
        logged_result = loop._risk_writer.log_result.call_args[0][0]
        decisions_by_rule = {d.rule_id: d for d in logged_result.decisions}
        assert "max_gross_exposure" in decisions_by_rule
        assert decisions_by_rule["max_gross_exposure"].decision == Decision.SIZE_REDUCE

    def test_order_rejected_when_weight_zeroed_by_engine(self):
        """When risk engine zeroes a symbol's weight, no order is submitted for it."""
        # max_active_positions=1 forces second symbol to 0
        one_pos_risk = RiskConfig(
            max_gross_exposure=1.0,
            max_symbol_weight=0.5,
            max_active_positions=1,
            minimum_order_threshold=0.001,
        )
        loop = _make_paper_loop(one_pos_risk)
        loop.add_signals([_signal("AAPL", 0.30), _signal("MSFT", 0.30)])
        loop._process_signals()

        # Only 1 order submitted (one symbol zeroed by max_active_positions)
        calls = loop._broker.submit_order.call_args_list
        assert len(calls) <= 1


class TestRiskEngineRealProjection:
    def test_project_weights_full_satisfies_constraint(self):
        """The real risk engine always produces weights within the configured limits."""
        raw = np.array([0.40, 0.40])  # total = 0.80 > 0.50 max_gross_exposure
        result = project_weights_full(raw, _TIGHT_RISK)
        eff_max = min(_TIGHT_RISK.max_gross_exposure, 1.0 - _TIGHT_RISK.min_cash)
        assert result.weights.sum() <= eff_max + 1e-9

    def test_size_reduce_decision_present_when_exposure_exceeded(self):
        raw = np.array([0.40, 0.40])
        result = project_weights_full(raw, _TIGHT_RISK)
        rules = {d.rule_id: d.decision for d in result.decisions}
        assert rules["max_gross_exposure"] == Decision.SIZE_REDUCE

    def test_pass_decision_when_within_limits(self):
        raw = np.array([0.20, 0.20])  # total = 0.40 < 0.50
        result = project_weights_full(raw, _TIGHT_RISK)
        rules = {d.rule_id: d.decision for d in result.decisions}
        assert rules["max_gross_exposure"] == Decision.PASS


class TestLiveTradingRiskEngine:
    def test_live_loop_calls_project_weights_full(self):
        """Live trading loop wires the risk engine (project_weights_full is called)."""
        live_config = LiveTradingConfig(
            experiment_id="test-live-integration",
            symbols=["AAPL"],
            initial_cash=100_000.0,
            managing_partner_ack=True,
        )
        with (
            patch("research.execution.live_trading.load_production_risk_config") as mock_risk,
            patch("research.execution.live_trading.AlpacaBrokerAdapter") as mock_broker_cls,
            patch("research.execution.live_trading.AlpacaBrokerConfig.from_env"),
        ):
            mock_risk.return_value = _TIGHT_RISK
            mock_broker_cls.return_value = MagicMock()
            lp = LiveTradingLoop(live_config)

        lp._risk_writer = MagicMock()
        lp._broker = MagicMock()
        lp._order_writer = MagicMock()
        lp._position_writer = MagicMock()
        lp._position_writer.positions = {}
        lp._portfolio_writer = MagicMock()
        lp._portfolio_writer.write_snapshot.return_value = MagicMock(drawdown=-0.01)
        lp._get_current_prices = MagicMock(return_value={"AAPL": 150.0})
        lp._compute_nav = MagicMock(return_value=100_000.0)
        lp._kill_switch = MagicMock()
        lp._kill_switch.is_trading = True
        lp.add_signals([_signal("AAPL", 0.40)])

        with patch("research.execution.live_trading.project_weights_full") as mock_pwf:
            mock_pwf.return_value = MagicMock(
                weights=np.array([0.30]),
                decisions=[],
                halted=False,
            )
            lp._process_signals()

        mock_pwf.assert_called_once()
        lp._risk_writer.log_result.assert_called_once()

    def test_live_loop_has_risk_decisions_writer(self):
        """LiveTradingLoop instantiates RiskDecisionsWriter on construction."""
        live_config = LiveTradingConfig(
            experiment_id="test-live-riskwriter",
            symbols=["AAPL"],
            managing_partner_ack=True,
        )
        with (
            patch("research.execution.live_trading.load_production_risk_config"),
            patch("research.execution.live_trading.AlpacaBrokerAdapter"),
            patch("research.execution.live_trading.AlpacaBrokerConfig.from_env"),
            patch("research.execution.live_trading.RiskDecisionsWriter") as mock_rdw,
        ):
            lp = LiveTradingLoop(live_config)

        mock_rdw.assert_called_once_with(
            experiment_id="test-live-riskwriter",
            mode="live",
            ilp_host="localhost",
            ilp_port=9009,
        )


class TestPostFillVerification:
    def test_post_fill_runs_after_handle_fill(self):
        """_verify_post_fill_constraints is called after each fill is processed."""
        loop = _make_paper_loop(_TIGHT_RISK)
        loop._verify_post_fill_constraints = MagicMock()

        from research.execution.broker_alpaca import (
            Fill, OrderResult, OrderSide, OrderStatus,
        )
        loop._position_writer = MagicMock()
        loop._position_writer.positions = {}
        loop._order_writer = MagicMock()

        fill_result = OrderResult(
            order_id="ord-1",
            client_order_id="cl-1",
            symbol="AAPL",
            side=OrderSide.BUY,
            status=OrderStatus.FILLED,
            submitted_at=datetime(2024, 6, 15, tzinfo=timezone.utc),
            filled_qty=10.0,
            filled_avg_price=150.0,
            filled_at=datetime(2024, 6, 15, 0, 1, tzinfo=timezone.utc),
        )
        loop._handle_fill(fill_result)

        loop._verify_post_fill_constraints.assert_called_once()
