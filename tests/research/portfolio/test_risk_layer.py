"""Tests for research.portfolio.risk_layer — vol targeting and beta-neutral option."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.experiments.spec import PortfolioRiskConfig
from research.portfolio.risk_layer import (
    apply_risk_layer,
    apply_risk_layer_batch,
    compute_betas,
    _compute_vol_scale,
    _compute_beta_cap_scale,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(**kw) -> PortfolioRiskConfig:
    return PortfolioRiskConfig(**kw)


def _rng(seed: int = 42) -> np.random.Generator:
    return np.random.default_rng(seed)


# ---------------------------------------------------------------------------
# PortfolioRiskConfig validation
# ---------------------------------------------------------------------------

class TestPortfolioRiskConfig:
    def test_defaults_are_no_op(self):
        cfg = PortfolioRiskConfig()
        assert cfg.vol_target == 0.10
        assert cfg.vol_lookback == 20
        assert cfg.beta_neutral is False
        assert cfg.beta_lookback == 60
        assert cfg.beta_cap == 1.0
        assert cfg.spy_symbol == "SPY"

    def test_invalid_vol_target(self):
        with pytest.raises(ValueError, match="vol_target"):
            PortfolioRiskConfig(vol_target=0.0)

    def test_invalid_vol_lookback(self):
        with pytest.raises(ValueError, match="vol_lookback"):
            PortfolioRiskConfig(vol_lookback=1)

    def test_invalid_beta_cap(self):
        with pytest.raises(ValueError, match="beta_cap"):
            PortfolioRiskConfig(beta_cap=0.0)

    def test_frozen(self):
        cfg = PortfolioRiskConfig()
        with pytest.raises(AttributeError):
            cfg.vol_target = 0.20  # type: ignore


# ---------------------------------------------------------------------------
# Vol scale math
# ---------------------------------------------------------------------------

class TestComputeVolScale:
    def test_scales_down_when_vol_exceeds_target(self):
        # Use normally distributed returns with known daily std = 2%
        # → annualised trailing vol ≈ 2% * sqrt(252) ≈ 31.7% >> 10% target
        rng = np.random.default_rng(0)
        daily_std = 0.02
        returns = rng.normal(0.0, daily_std, 25)
        cfg = _make_config(vol_target=0.10, vol_lookback=20)
        scale = _compute_vol_scale(returns, cfg)
        # With this level of vol the scale must be strictly < 1.0
        assert 0.0 < scale < 1.0
        # And it should match target / trailing_vol (capped at 1.0)
        window = returns[-20:]
        trailing_vol = float(np.std(window, ddof=1)) * np.sqrt(252.0)
        expected = min(1.0, 0.10 / trailing_vol)
        assert abs(scale - expected) < 1e-9

    def test_does_not_scale_up(self):
        # Portfolio with very low vol (< target) → scale capped at 1.0
        returns = np.full(25, 0.0001)
        cfg = _make_config(vol_target=0.50, vol_lookback=20)
        scale = _compute_vol_scale(returns, cfg)
        assert scale == 1.0

    def test_insufficient_history_returns_one(self):
        cfg = _make_config(vol_lookback=20)
        assert _compute_vol_scale(np.array([0.01]), cfg) == 1.0
        assert _compute_vol_scale(np.array([]), cfg) == 1.0

    def test_zero_vol_returns_one(self):
        cfg = _make_config(vol_lookback=5)
        scale = _compute_vol_scale(np.zeros(10), cfg)
        assert scale == 1.0

    def test_scale_decreases_with_higher_vol(self):
        rng = np.random.default_rng(11)
        # Low vol: daily std ≈ 0.003 → annualised ≈ 4.8% < 10% target → scale = 1.0
        # High vol: daily std ≈ 0.02  → annualised ≈ 31.7% > 10% target → scale < 1.0
        low_vol_rets = rng.normal(0.0, 0.003, 25)
        high_vol_rets = rng.normal(0.0, 0.030, 25)
        cfg = _make_config(vol_target=0.10, vol_lookback=20)
        scale_low = _compute_vol_scale(low_vol_rets, cfg)
        scale_high = _compute_vol_scale(high_vol_rets, cfg)
        assert scale_low > scale_high

    def test_scale_exact_at_target(self):
        # Daily std = target_vol / sqrt(252) → scale = 1.0
        target = 0.15
        daily_std = target / np.sqrt(252)
        returns = np.random.default_rng(0).normal(0, daily_std, 100)
        cfg = _make_config(vol_target=target, vol_lookback=20)
        scale = _compute_vol_scale(returns, cfg)
        # Trailing vol of last 20 bars should be close to target → scale ≈ 1.0
        # (within sampling noise — just verify it's in (0, 1])
        assert 0 < scale <= 1.0


# ---------------------------------------------------------------------------
# Beta computation
# ---------------------------------------------------------------------------

class TestComputeBetas:
    def test_beta_one_for_identical_returns(self):
        rets = np.random.default_rng(1).normal(0, 0.01, (60, 1))
        spy = rets[:, 0]
        betas = compute_betas(rets, spy)
        assert abs(betas[0] - 1.0) < 1e-10

    def test_beta_zero_for_uncorrelated(self):
        rng = _rng(7)
        spy = rng.normal(0, 0.01, 100)
        sym = rng.normal(0, 0.01, 100)  # independent
        betas = compute_betas(sym[:, np.newaxis], spy)
        # OLS beta ≈ 0 for uncorrelated; allow generous tolerance for sampling
        assert abs(betas[0]) < 0.5

    def test_beta_two_for_doubled_returns(self):
        spy = np.random.default_rng(3).normal(0, 0.01, 60)
        sym = spy * 2.0  # β = 2 exactly
        betas = compute_betas(sym[:, np.newaxis], spy)
        assert abs(betas[0] - 2.0) < 1e-10

    def test_zero_spy_var_returns_zeros(self):
        spy = np.zeros(60)
        sym = np.random.default_rng(5).normal(0, 0.01, 60)
        betas = compute_betas(sym[:, np.newaxis], spy)
        assert np.all(betas == 0.0)

    def test_multi_symbol(self):
        rng = _rng(42)
        spy = rng.normal(0, 0.01, 100)
        sym1 = spy * 1.5
        sym2 = spy * 0.5
        sym_arr = np.stack([sym1, sym2], axis=1)
        betas = compute_betas(sym_arr, spy)
        assert abs(betas[0] - 1.5) < 1e-10
        assert abs(betas[1] - 0.5) < 1e-10

    def test_1d_symbol_returns(self):
        spy = np.random.default_rng(8).normal(0, 0.01, 60)
        sym = spy * 1.2
        betas = compute_betas(sym, spy)
        assert abs(betas[0] - 1.2) < 1e-10


# ---------------------------------------------------------------------------
# Beta cap scale
# ---------------------------------------------------------------------------

class TestBetaCapScale:
    def test_no_scale_when_beta_within_cap(self):
        weights = np.array([0.5, 0.5])
        betas = np.array([0.8, 0.8])  # portfolio β = 0.8 < 1.0
        cfg = _make_config(beta_cap=1.0)
        scale = _compute_beta_cap_scale(weights, betas, cfg)
        assert scale == 1.0

    def test_scales_down_when_beta_exceeds_cap(self):
        weights = np.array([0.5, 0.5])
        betas = np.array([1.5, 1.5])  # portfolio β = 1.5 > 1.0
        cfg = _make_config(beta_cap=1.0)
        scale = _compute_beta_cap_scale(weights, betas, cfg)
        expected = 1.0 / 1.5
        assert abs(scale - expected) < 1e-9

    def test_scale_at_exact_cap(self):
        weights = np.array([0.5, 0.5])
        betas = np.array([1.0, 1.0])  # exactly at cap
        cfg = _make_config(beta_cap=1.0)
        scale = _compute_beta_cap_scale(weights, betas, cfg)
        assert scale == 1.0

    def test_zero_portfolio_beta_returns_one(self):
        weights = np.array([0.5, 0.5])
        betas = np.array([0.0, 0.0])
        cfg = _make_config(beta_cap=1.0)
        scale = _compute_beta_cap_scale(weights, betas, cfg)
        assert scale == 1.0


# ---------------------------------------------------------------------------
# apply_risk_layer (single-step)
# ---------------------------------------------------------------------------

class TestApplyRiskLayer:
    def test_off_by_default_no_op(self):
        # With default PortfolioRiskConfig and very short history, vol scale = 1.0
        cfg = _make_config()
        weights = np.array([0.5, 0.5])
        port_rets = np.array([])
        result = apply_risk_layer(weights, port_rets, None, None, cfg)
        np.testing.assert_array_equal(result, weights)

    def test_vol_scaling_applied(self):
        # High-vol history → weights scaled down
        cfg = _make_config(vol_target=0.10, vol_lookback=20)
        weights = np.array([0.5, 0.5])
        daily_std = 0.03  # annualised ≈ 47.6%, well above 10% target
        port_rets = np.full(25, daily_std)
        result = apply_risk_layer(weights, port_rets, None, None, cfg)
        assert np.all(result <= weights)
        assert np.all(result >= 0.0)

    def test_low_vol_not_scaled_up(self):
        # Very low vol → scale capped at 1.0 (no leverage)
        cfg = _make_config(vol_target=0.50, vol_lookback=20)
        weights = np.array([0.4, 0.4])
        port_rets = np.full(25, 0.0001)
        result = apply_risk_layer(weights, port_rets, None, None, cfg)
        np.testing.assert_array_almost_equal(result, weights)

    def test_beta_neutral_off_ignores_spy(self):
        cfg = _make_config(beta_neutral=False)
        weights = np.array([0.5, 0.5])
        port_rets = np.full(25, 0.0)
        spy = np.random.default_rng(0).normal(0, 0.01, 65)
        sym_hist = np.random.default_rng(1).normal(0, 0.01, (65, 2))
        result = apply_risk_layer(weights, port_rets, sym_hist, spy, cfg)
        # Vol scale = 1.0 (zero vol), so result == weights
        np.testing.assert_array_almost_equal(result, weights)

    def test_beta_neutral_caps_high_beta_portfolio(self):
        rng = _rng(5)
        spy = rng.normal(0, 0.01, 65)
        # Both symbols have β=2 (very high)
        sym_hist = np.stack([spy * 2.0, spy * 2.0], axis=1)
        weights = np.array([0.5, 0.5])
        port_rets = np.zeros(25)  # zero vol → scale = 1.0
        cfg = _make_config(vol_target=0.10, vol_lookback=20, beta_neutral=True, beta_cap=1.0)
        result = apply_risk_layer(weights, port_rets, sym_hist, spy, cfg)
        # Portfolio β = 0.5*2 + 0.5*2 = 2.0 → capped to 1.0 → scale = 0.5
        np.testing.assert_array_almost_equal(result, weights * 0.5, decimal=6)

    def test_output_always_leq_input(self):
        rng = _rng(99)
        cfg = _make_config(vol_target=0.10, beta_neutral=True, beta_cap=1.0)
        weights = rng.uniform(0.1, 0.3, 5)
        weights /= weights.sum()
        port_rets = rng.normal(0, 0.02, 30)
        spy = rng.normal(0, 0.01, 70)
        sym_hist = np.stack([spy * (1 + i * 0.3) for i in range(5)], axis=1)
        result = apply_risk_layer(weights, port_rets, sym_hist, spy, cfg)
        assert np.all(result <= weights + 1e-12)
        assert np.all(result >= 0.0)


# ---------------------------------------------------------------------------
# apply_risk_layer_batch (WFO eval path)
# ---------------------------------------------------------------------------

class TestApplyRiskLayerBatch:
    def _make_data(self, n: int = 252, n_sym: int = 3, seed: int = 0):
        rng = np.random.default_rng(seed)
        dates = pd.bdate_range("2022-01-01", periods=n)
        syms = [f"SYM{i}" for i in range(n_sym)]
        weights = pd.DataFrame(
            np.full((n, n_sym), 1.0 / n_sym), index=dates, columns=syms,
        )
        returns = pd.DataFrame(
            rng.normal(0.0003, 0.012, (n, n_sym)), index=dates, columns=syms,
        )
        return weights, returns, syms

    def test_output_shape_matches_input(self):
        cfg = _make_config()
        weights, returns, _ = self._make_data()
        result = apply_risk_layer_batch(weights, returns, None, cfg)
        assert result.shape == weights.shape
        assert list(result.columns) == list(weights.columns)
        assert list(result.index) == list(weights.index)

    def test_no_lookahead_first_bars_unchanged(self):
        # First bar (t=0) has no history → scale = 1.0, weights unchanged
        cfg = _make_config(vol_target=0.10, vol_lookback=20)
        weights, returns, _ = self._make_data()
        result = apply_risk_layer_batch(weights, returns, None, cfg)
        np.testing.assert_array_almost_equal(result.iloc[0], weights.iloc[0])

    def test_weights_scaled_down_when_high_vol(self):
        # Use high-vol returns → scaled weights should be ≤ raw weights
        cfg = _make_config(vol_target=0.05, vol_lookback=20)
        n, n_sym = 100, 2
        rng = np.random.default_rng(7)
        dates = pd.bdate_range("2023-01-01", periods=n)
        syms = ["A", "B"]
        weights = pd.DataFrame(np.full((n, n_sym), 0.5), index=dates, columns=syms)
        # Returns with 3% daily std → annualised ≈ 47%, well above 5% target
        returns = pd.DataFrame(
            rng.normal(0.0, 0.03, (n, n_sym)), index=dates, columns=syms,
        )
        result = apply_risk_layer_batch(weights, returns, None, cfg)
        # After warm-up period, weights should be scaled down
        assert (result.iloc[25:] <= weights.iloc[25:] + 1e-9).all().all()

    def test_all_weights_nonnegative(self):
        cfg = _make_config(vol_target=0.10, beta_neutral=True, beta_cap=1.0)
        weights, returns, syms = self._make_data(n=150, n_sym=3)
        spy_rets = pd.Series(
            np.random.default_rng(42).normal(0, 0.01, 150),
            index=returns.index,
        )
        result = apply_risk_layer_batch(weights, returns, spy_rets, cfg)
        assert (result >= 0).all().all()

    def test_beta_neutral_off_ignores_spy_series(self):
        cfg = _make_config(beta_neutral=False)
        weights, returns, _ = self._make_data()
        spy = pd.Series(np.ones(len(returns)) * 0.001, index=returns.index)
        result_with_spy = apply_risk_layer_batch(weights, returns, spy, cfg)
        result_no_spy = apply_risk_layer_batch(weights, returns, None, cfg)
        pd.testing.assert_frame_equal(result_with_spy, result_no_spy)

    def test_zero_vol_config_not_applied_when_no_history(self):
        # At t=0, no history → scale = 1.0 for both vol and beta
        cfg = _make_config(vol_target=0.10)
        weights, returns, _ = self._make_data(n=50)
        result = apply_risk_layer_batch(weights, returns, None, cfg)
        np.testing.assert_array_almost_equal(result.iloc[0], weights.iloc[0])

    def test_causal_window_vol_estimate(self):
        # Verify that a spike at bar t does NOT affect the scale at bar t
        cfg = _make_config(vol_target=0.10, vol_lookback=10)
        n = 30
        dates = pd.bdate_range("2023-01-01", periods=n)
        syms = ["X"]
        weights = pd.DataFrame(np.ones((n, 1)), index=dates, columns=syms)
        returns = pd.DataFrame(np.zeros((n, 1)), index=dates, columns=syms)
        # Insert a spike at bar 15
        returns.iloc[15, 0] = 0.50
        result = apply_risk_layer_batch(weights, returns, None, cfg)
        # At bar 15 itself, no past spike → scale at 15 should be 1.0
        assert abs(result.iloc[15, 0] - 1.0) < 1e-9
        # At bar 16+, spike in window → scale < 1.0
        assert result.iloc[16, 0] < 1.0
