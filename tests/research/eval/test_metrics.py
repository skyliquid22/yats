"""Tests for research.eval.metrics — metric computation."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.eval.metrics import (
    ANNUALIZATION_FACTOR,
    PerformanceMetrics,
    TradingMetrics,
    SafetyMetrics,
    compute_sharpe,
    compute_sortino,
    compute_max_drawdown,
    compute_calmar,
    compute_total_return,
    compute_annualized_return,
    compute_performance_metrics,
    compute_turnover,
    compute_avg_holding_period,
    compute_win_rate,
    compute_profit_factor,
    compute_trading_metrics,
    compute_safety_metrics,
)


# ---------------------------------------------------------------------------
# Sharpe
# ---------------------------------------------------------------------------

class TestSharpe:
    def test_positive_returns(self):
        returns = pd.Series([0.01, 0.02, 0.01, 0.015, 0.005] * 50)
        sharpe = compute_sharpe(returns)
        assert sharpe > 0

    def test_zero_std_returns_zero(self):
        returns = pd.Series([0.01] * 100)
        assert compute_sharpe(returns) == 0.0

    def test_empty_returns_zero(self):
        assert compute_sharpe(pd.Series(dtype=float)) == 0.0

    def test_single_return(self):
        assert compute_sharpe(pd.Series([0.01])) == 0.0

    def test_annualization(self):
        # Known values: mean=0.001, std=0.01 → sharpe = 0.001/0.01 * sqrt(252) ≈ 1.587
        rng = np.random.default_rng(42)
        returns = pd.Series(rng.normal(0.001, 0.01, 1000))
        sharpe = compute_sharpe(returns)
        expected = returns.mean() / returns.std() * np.sqrt(252)
        assert abs(sharpe - expected) < 1e-10


# ---------------------------------------------------------------------------
# Sortino
# ---------------------------------------------------------------------------

class TestSortino:
    def test_positive_returns(self):
        returns = pd.Series([0.01, 0.02, -0.005, 0.015, -0.002] * 50)
        sortino = compute_sortino(returns)
        assert sortino > 0

    def test_no_downside_returns_inf(self):
        returns = pd.Series([0.01, 0.02, 0.03])
        sortino = compute_sortino(returns)
        assert sortino == float("inf")

    def test_empty_returns_zero(self):
        assert compute_sortino(pd.Series(dtype=float)) == 0.0


# ---------------------------------------------------------------------------
# Max drawdown
# ---------------------------------------------------------------------------

class TestMaxDrawdown:
    def test_simple_drawdown(self):
        # Equity: 100, 110, 90, 95
        equity = pd.Series([100.0, 110.0, 90.0, 95.0])
        dd, dur = compute_max_drawdown(equity)
        # Max drawdown from 110 to 90 = (90-110)/110 = -0.1818...
        assert abs(dd - (-20.0 / 110.0)) < 1e-10
        assert dur == 2  # days 2 and 3 are below peak

    def test_no_drawdown(self):
        equity = pd.Series([100.0, 110.0, 120.0, 130.0])
        dd, dur = compute_max_drawdown(equity)
        assert dd == 0.0
        assert dur == 0

    def test_empty(self):
        dd, dur = compute_max_drawdown(pd.Series(dtype=float))
        assert dd == 0.0
        assert dur == 0


# ---------------------------------------------------------------------------
# Calmar
# ---------------------------------------------------------------------------

class TestCalmar:
    def test_positive(self):
        assert compute_calmar(0.10, -0.05) == pytest.approx(2.0)

    def test_zero_drawdown_positive_return(self):
        assert compute_calmar(0.10, 0.0) == float("inf")

    def test_zero_drawdown_zero_return(self):
        assert compute_calmar(0.0, 0.0) == 0.0


# ---------------------------------------------------------------------------
# Total / annualized return
# ---------------------------------------------------------------------------

class TestReturns:
    def test_total_return(self):
        equity = pd.Series([100.0, 110.0, 121.0])
        assert compute_total_return(equity) == pytest.approx(0.21)

    def test_annualized_return(self):
        # 21% over 252 days = annualized 21%
        ann = compute_annualized_return(0.21, 252)
        assert abs(ann - 0.21) < 1e-10

    def test_annualized_return_two_years(self):
        # 44% over 504 days (2 years) → sqrt(1.44) - 1 = 0.2
        ann = compute_annualized_return(0.44, 504)
        expected = (1.44 ** 0.5) - 1.0
        assert abs(ann - expected) < 1e-10


# ---------------------------------------------------------------------------
# Performance metrics (combined)
# ---------------------------------------------------------------------------

class TestPerformanceMetrics:
    def test_full_computation(self):
        rng = np.random.default_rng(42)
        returns = pd.Series(rng.normal(0.0005, 0.01, 500))
        equity = (1.0 + returns).cumprod()
        perf = compute_performance_metrics(returns, equity)

        assert isinstance(perf, PerformanceMetrics)
        assert isinstance(perf.sharpe, float)
        assert isinstance(perf.max_drawdown, float)
        assert perf.max_drawdown <= 0
        assert perf.max_drawdown_duration >= 0


# ---------------------------------------------------------------------------
# Trading metrics
# ---------------------------------------------------------------------------

class TestTradingMetrics:
    def test_turnover(self):
        weights = pd.DataFrame({
            "A": [0.5, 0.6, 0.4, 0.5],
            "B": [0.5, 0.4, 0.6, 0.5],
        })
        turnover = compute_turnover(weights)
        # Day 1→2: |0.1|+|0.1| = 0.2; Day 2→3: 0.4; Day 3→4: 0.2
        assert len(turnover) == 3
        assert turnover.iloc[0] == pytest.approx(0.2)
        assert turnover.iloc[1] == pytest.approx(0.4)

    def test_win_rate(self):
        pnl = pd.Series([0.01, -0.005, 0.02, -0.01, 0.015])
        assert compute_win_rate(pnl) == pytest.approx(0.6)

    def test_profit_factor(self):
        pnl = pd.Series([0.01, -0.005, 0.02, -0.01, 0.015])
        gross_profit = 0.01 + 0.02 + 0.015
        gross_loss = 0.005 + 0.01
        assert compute_profit_factor(pnl) == pytest.approx(gross_profit / gross_loss)

    def test_full_trading_metrics(self):
        weights = pd.DataFrame({
            "A": [0.5, 0.6, 0.4],
            "B": [0.5, 0.4, 0.6],
        })
        pnl = pd.Series([0.01, -0.005, 0.02])
        tm = compute_trading_metrics(weights, pnl)
        assert isinstance(tm, TradingMetrics)
        assert tm.turnover_1d_mean > 0
        assert 0 <= tm.win_rate <= 1


# ---------------------------------------------------------------------------
# Safety metrics
# ---------------------------------------------------------------------------

class TestSafetyMetrics:
    def test_clean_data(self):
        weights = pd.DataFrame({"A": [0.5, 0.5], "B": [0.5, 0.5]})
        returns = pd.DataFrame({"A": [0.01, -0.01], "B": [0.02, -0.02]})
        sm = compute_safety_metrics(weights, returns)
        assert sm.nan_inf_violations == 0
        assert sm.constraint_violations == 0

    def test_nan_detection(self):
        weights = pd.DataFrame({"A": [0.5, np.nan], "B": [0.5, 0.5]})
        returns = pd.DataFrame({"A": [0.01, 0.01], "B": [0.02, np.inf]})
        sm = compute_safety_metrics(weights, returns)
        assert sm.nan_inf_violations == 2  # 1 nan in weights + 1 inf in returns

    def test_constraint_violation(self):
        weights = pd.DataFrame({"A": [0.8, 0.8], "B": [0.8, 0.8]})
        returns = pd.DataFrame({"A": [0.01, 0.01], "B": [0.01, 0.01]})
        sm = compute_safety_metrics(
            weights, returns,
            max_gross_exposure=1.0,
            max_symbol_weight=0.5,
        )
        assert sm.constraint_violations > 0
