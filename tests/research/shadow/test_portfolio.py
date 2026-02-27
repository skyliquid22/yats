"""Tests for research.shadow.portfolio — ShadowPortfolio."""

from __future__ import annotations

import numpy as np
import pytest

from research.shadow.portfolio import ShadowPortfolio


class TestConstruction:
    def test_default(self):
        p = ShadowPortfolio(symbols=("AAPL", "MSFT"))
        assert p.portfolio_value == 1_000_000.0
        assert p.cash == 0.0
        assert p.peak_value == 1_000_000.0
        np.testing.assert_array_equal(p.weights, np.zeros(2))
        assert p.positions == {"AAPL": 0.0, "MSFT": 0.0}

    def test_custom_value(self):
        p = ShadowPortfolio(symbols=("A",), portfolio_value=500.0)
        assert p.portfolio_value == 500.0
        assert p.peak_value == 500.0


class TestApplyStep:
    def test_positive_return_no_cost(self):
        p = ShadowPortfolio(symbols=("A", "B"), portfolio_value=1000.0)
        weights = np.array([0.5, 0.5])
        # 1% return, no cost
        p.apply_step(weights, weighted_return=0.01, cost=0.0)
        assert p.portfolio_value == pytest.approx(1010.0)
        np.testing.assert_array_equal(p.weights, weights)
        assert p.peak_value == pytest.approx(1010.0)

    def test_cost_reduces_value(self):
        p = ShadowPortfolio(symbols=("A",), portfolio_value=1000.0)
        weights = np.array([1.0])
        # 0% return, 1% cost
        p.apply_step(weights, weighted_return=0.0, cost=0.01)
        assert p.portfolio_value == pytest.approx(990.0)

    def test_negative_return_updates_drawdown(self):
        p = ShadowPortfolio(symbols=("A",), portfolio_value=1000.0)
        p.peak_value = 1000.0
        weights = np.array([1.0])
        p.apply_step(weights, weighted_return=-0.05, cost=0.0)
        assert p.portfolio_value == pytest.approx(950.0)
        assert p.drawdown == pytest.approx(-0.05)
        assert p.peak_value == 1000.0  # peak doesn't drop

    def test_positions_updated(self):
        p = ShadowPortfolio(symbols=("A", "B"), portfolio_value=1000.0)
        weights = np.array([0.6, 0.4])
        p.apply_step(weights, weighted_return=0.0, cost=0.0)
        assert p.positions["A"] == pytest.approx(600.0)
        assert p.positions["B"] == pytest.approx(400.0)

    def test_cash_tracks_unallocated(self):
        p = ShadowPortfolio(symbols=("A",), portfolio_value=1000.0)
        weights = np.array([0.8])
        p.apply_step(weights, weighted_return=0.0, cost=0.0)
        assert p.cash == pytest.approx(200.0)


class TestDrawdown:
    def test_no_drawdown(self):
        p = ShadowPortfolio(symbols=("A",), portfolio_value=1000.0)
        assert p.drawdown == 0.0

    def test_drawdown_from_peak(self):
        p = ShadowPortfolio(symbols=("A",), portfolio_value=900.0, peak_value=1000.0)
        assert p.drawdown == pytest.approx(-0.1)


class TestSerialization:
    def test_roundtrip(self):
        p = ShadowPortfolio(
            symbols=("AAPL", "MSFT"),
            portfolio_value=105000.0,
            cash=5000.0,
            peak_value=106000.0,
            weights=np.array([0.5, 0.45]),
            positions={"AAPL": 52500.0, "MSFT": 47250.0},
        )
        state = p.to_state_dict()
        restored = ShadowPortfolio.from_state_dict(("AAPL", "MSFT"), state)

        assert restored.portfolio_value == pytest.approx(105000.0)
        assert restored.cash == pytest.approx(5000.0)
        assert restored.peak_value == pytest.approx(106000.0)
        np.testing.assert_array_almost_equal(restored.weights, [0.5, 0.45])
        assert restored.positions["AAPL"] == pytest.approx(52500.0)
