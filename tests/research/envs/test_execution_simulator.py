"""Tests for ExecutionSimulator — fill/slippage/missed-fill behavior."""

from __future__ import annotations

import random

import numpy as np
import pytest

from research.envs.execution_simulator import ExecutionResult, ExecutionSimulator
from research.experiments.spec import ExecutionSimConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ohlc(
    symbols: list[str],
    close: float = 100.0,
    spread: float = 0.01,
) -> dict[str, dict[str, float]]:
    """Build OHLC row data for testing."""
    return {
        sym: {
            "open": close,
            "high": close * (1 + spread),
            "low": close * (1 - spread),
            "close": close,
            "volume": 1_000_000.0,
        }
        for sym in symbols
    }


# ---------------------------------------------------------------------------
# Basic behavior
# ---------------------------------------------------------------------------

class TestExecutionSimulator:
    def test_no_trade_returns_prev_weights(self):
        cfg = ExecutionSimConfig(enabled=True, slippage_bp=5.0)
        sim = ExecutionSimulator(cfg, rng=random.Random(42))

        prev = np.array([0.5, 0.5])
        target = np.array([0.5, 0.5])  # no change
        ohlc = _make_ohlc(["AAPL", "MSFT"])

        result = sim.simulate(prev, target, ohlc, 1000.0)
        np.testing.assert_array_almost_equal(result.realized_weights, prev)
        assert result.missed_fill_ratio == 0.0
        assert result.execution_slippage_bps == 0.0
        assert result.order_type_counts["hold"] == 2

    def test_buy_order_type(self):
        cfg = ExecutionSimConfig(enabled=True, fill_probability=1.0)
        sim = ExecutionSimulator(cfg, rng=random.Random(42))

        prev = np.array([0.0])
        target = np.array([0.5])
        ohlc = _make_ohlc(["AAPL"])

        result = sim.simulate(prev, target, ohlc, 1000.0)
        assert result.order_type_counts["buy"] == 1
        assert result.order_type_counts["sell"] == 0

    def test_sell_order_type(self):
        cfg = ExecutionSimConfig(enabled=True, fill_probability=1.0)
        sim = ExecutionSimulator(cfg, rng=random.Random(42))

        prev = np.array([0.5])
        target = np.array([0.1])
        ohlc = _make_ohlc(["AAPL"])

        result = sim.simulate(prev, target, ohlc, 1000.0)
        assert result.order_type_counts["sell"] == 1

    def test_full_fill_with_prob_1(self):
        cfg = ExecutionSimConfig(
            enabled=True, fill_probability=1.0, slippage_bp=0.0,
        )
        sim = ExecutionSimulator(cfg, rng=random.Random(42))

        prev = np.array([0.0, 0.0])
        target = np.array([0.3, 0.5])
        ohlc = _make_ohlc(["AAPL", "MSFT"])

        result = sim.simulate(prev, target, ohlc, 1000.0)
        np.testing.assert_array_almost_equal(result.realized_weights, target)

    def test_zero_fill_probability_misses_all(self):
        cfg = ExecutionSimConfig(
            enabled=True, fill_probability=0.0, slippage_bp=5.0,
        )
        sim = ExecutionSimulator(cfg, rng=random.Random(42))

        prev = np.array([0.0, 0.0])
        target = np.array([0.5, 0.5])
        ohlc = _make_ohlc(["AAPL", "MSFT"])

        result = sim.simulate(prev, target, ohlc, 1000.0)
        # All fills missed — weights stay at prev
        np.testing.assert_array_almost_equal(result.realized_weights, prev)
        assert result.missed_fill_ratio == 1.0
        assert result.unfilled_notional > 0

    def test_result_type(self):
        cfg = ExecutionSimConfig(enabled=True)
        sim = ExecutionSimulator(cfg, rng=random.Random(42))

        result = sim.simulate(
            np.array([0.0]), np.array([0.5]),
            _make_ohlc(["AAPL"]), 1000.0,
        )
        assert isinstance(result, ExecutionResult)
        assert isinstance(result.realized_weights, np.ndarray)
        assert isinstance(result.execution_slippage_bps, float)
        assert isinstance(result.missed_fill_ratio, float)


# ---------------------------------------------------------------------------
# Slippage models
# ---------------------------------------------------------------------------

class TestSlippageModels:
    def test_flat_slippage(self):
        cfg = ExecutionSimConfig(
            enabled=True, slippage_model="flat",
            slippage_bp=10.0, fill_probability=1.0,
        )
        sim = ExecutionSimulator(cfg, rng=random.Random(42))

        result = sim.simulate(
            np.array([0.0]), np.array([0.5]),
            _make_ohlc(["AAPL"]), 1000.0,
        )
        # Slippage should be approximately the configured bp
        assert result.execution_slippage_bps >= 0

    def test_volume_scaled_high_volume_less_slippage(self):
        cfg = ExecutionSimConfig(
            enabled=True, slippage_model="volume_scaled",
            slippage_bp=10.0, fill_probability=1.0,
        )
        sim = ExecutionSimulator(cfg, rng=random.Random(42))

        # High volume
        ohlc_high = {"AAPL": {
            "open": 100, "high": 101, "low": 99, "close": 100,
            "volume": 10_000_000.0,
        }}
        r_high = sim.simulate(
            np.array([0.0]), np.array([0.5]), ohlc_high, 1000.0,
        )

        sim2 = ExecutionSimulator(cfg, rng=random.Random(42))
        # Low volume
        ohlc_low = {"AAPL": {
            "open": 100, "high": 101, "low": 99, "close": 100,
            "volume": 100.0,
        }}
        r_low = sim2.simulate(
            np.array([0.0]), np.array([0.5]), ohlc_low, 1000.0,
        )

        # Lower volume should produce higher slippage
        assert r_low.execution_slippage_bps >= r_high.execution_slippage_bps


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

class TestExecutionSimConfigValidation:
    def test_invalid_slippage_model(self):
        with pytest.raises(ValueError, match="slippage_model"):
            ExecutionSimConfig(enabled=True, slippage_model="invalid")

    def test_negative_slippage_bp(self):
        with pytest.raises(ValueError, match="slippage_bp"):
            ExecutionSimConfig(enabled=True, slippage_bp=-1.0)

    def test_fill_probability_out_of_range(self):
        with pytest.raises(ValueError, match="fill_probability"):
            ExecutionSimConfig(enabled=True, fill_probability=1.5)

    def test_range_shrink_zero(self):
        with pytest.raises(ValueError, match="range_shrink"):
            ExecutionSimConfig(enabled=True, range_shrink=0.0)
