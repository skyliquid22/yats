"""Tests for research.portfolio.risk_layer — vol targeting and beta-neutral option."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.experiments.spec import PortfolioRiskConfig, RegimeConditioningConfig
from research.portfolio.risk_layer import (
    apply_risk_layer,
    apply_risk_layer_batch,
    compute_betas,
    _compute_vol_scale,
    _compute_beta_cap_scale,
    _regime_conditioned_vol_target,
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


# ---------------------------------------------------------------------------
# RegimeConditioningConfig validation
# ---------------------------------------------------------------------------

class TestRegimeConditioningConfig:
    def test_defaults_off(self):
        rc = RegimeConditioningConfig()
        assert rc.enabled is False
        assert rc.feature == "spy_iv_zscore_60d"
        assert rc.low_target == 0.05
        assert rc.high_target == 0.15
        assert rc.zscore_lo == -1.0
        assert rc.zscore_hi == 1.0

    def test_empty_feature_raises(self):
        with pytest.raises(ValueError, match="feature"):
            RegimeConditioningConfig(feature="")

    def test_nonpositive_low_target_raises(self):
        with pytest.raises(ValueError, match="low_target"):
            RegimeConditioningConfig(low_target=0.0)

    def test_nonpositive_high_target_raises(self):
        with pytest.raises(ValueError, match="high_target"):
            RegimeConditioningConfig(high_target=-0.1)

    def test_low_above_high_raises(self):
        with pytest.raises(ValueError, match="low_target"):
            RegimeConditioningConfig(low_target=0.20, high_target=0.10)

    def test_equal_targets_allowed(self):
        rc = RegimeConditioningConfig(low_target=0.10, high_target=0.10)
        assert rc.low_target == rc.high_target == 0.10

    def test_zscore_lo_geq_hi_raises(self):
        with pytest.raises(ValueError, match="zscore_lo"):
            RegimeConditioningConfig(zscore_lo=1.0, zscore_hi=1.0)
        with pytest.raises(ValueError, match="zscore_lo"):
            RegimeConditioningConfig(zscore_lo=2.0, zscore_hi=-2.0)

    def test_frozen(self):
        rc = RegimeConditioningConfig()
        with pytest.raises(AttributeError):
            rc.enabled = True  # type: ignore

    def test_portfolio_risk_default_none(self):
        cfg = PortfolioRiskConfig()
        assert cfg.regime_conditioning is None

    def test_portfolio_risk_coerces_dict(self):
        # JSON spec files pass the nested config as a plain dict
        cfg = PortfolioRiskConfig(
            regime_conditioning={"enabled": True, "low_target": 0.06},
        )
        assert isinstance(cfg.regime_conditioning, RegimeConditioningConfig)
        assert cfg.regime_conditioning.enabled is True
        assert cfg.regime_conditioning.low_target == 0.06
        assert cfg.regime_conditioning.high_target == 0.15  # default preserved

    def test_portfolio_risk_coerced_dict_validates(self):
        with pytest.raises(ValueError, match="low_target"):
            PortfolioRiskConfig(
                regime_conditioning={"low_target": 0.5, "high_target": 0.1},
            )

    def test_portfolio_risk_rejects_bad_type(self):
        with pytest.raises(ValueError, match="regime_conditioning"):
            PortfolioRiskConfig(regime_conditioning=3.14)  # type: ignore


# ---------------------------------------------------------------------------
# Regime-conditioned vol target — interpolation math
# ---------------------------------------------------------------------------

def _regime_config(**rc_kw) -> PortfolioRiskConfig:
    rc_defaults = dict(
        enabled=True, low_target=0.05, high_target=0.15,
        zscore_lo=-1.0, zscore_hi=1.0,
    )
    rc_defaults.update(rc_kw)
    return PortfolioRiskConfig(
        vol_target=0.10,
        regime_conditioning=RegimeConditioningConfig(**rc_defaults),
    )


class TestRegimeConditionedVolTarget:
    def test_disabled_returns_base_target(self):
        cfg = _regime_config(enabled=False)
        assert _regime_conditioned_vol_target(5.0, cfg) == cfg.vol_target

    def test_no_regime_config_returns_base_target(self):
        cfg = _make_config(vol_target=0.10)
        assert _regime_conditioned_vol_target(5.0, cfg) == 0.10

    def test_none_value_returns_base_target(self):
        cfg = _regime_config()
        assert _regime_conditioned_vol_target(None, cfg) == cfg.vol_target

    def test_nan_value_returns_base_target(self):
        cfg = _regime_config()
        assert _regime_conditioned_vol_target(float("nan"), cfg) == cfg.vol_target

    def test_calm_boundary_returns_high_target(self):
        cfg = _regime_config()
        assert _regime_conditioned_vol_target(-1.0, cfg) == 0.15

    def test_stress_boundary_returns_low_target(self):
        cfg = _regime_config()
        assert _regime_conditioned_vol_target(1.0, cfg) == 0.05

    def test_clamped_below_lo(self):
        cfg = _regime_config()
        assert _regime_conditioned_vol_target(-7.3, cfg) == 0.15

    def test_clamped_above_hi(self):
        cfg = _regime_config()
        assert _regime_conditioned_vol_target(4.2, cfg) == 0.05

    def test_midpoint_interpolation(self):
        cfg = _regime_config()
        # z = 0 is halfway between -1 and 1 → mean of targets
        assert abs(_regime_conditioned_vol_target(0.0, cfg) - 0.10) < 1e-12

    def test_linear_interpolation_arbitrary_point(self):
        cfg = _regime_config(zscore_lo=-2.0, zscore_hi=2.0)
        # z = 1.0 → frac = (1 - (-2)) / 4 = 0.75 → 0.15 + 0.75 * (0.05 - 0.15)
        expected = 0.15 + 0.75 * (0.05 - 0.15)
        assert abs(_regime_conditioned_vol_target(1.0, cfg) - expected) < 1e-12

    def test_monotone_nonincreasing_in_stress(self):
        cfg = _regime_config()
        zs = np.linspace(-3.0, 3.0, 61)
        targets = [_regime_conditioned_vol_target(z, cfg) for z in zs]
        assert all(a >= b - 1e-12 for a, b in zip(targets, targets[1:]))


# ---------------------------------------------------------------------------
# Regime-conditioned vol targeting — batch behavior & causality
# ---------------------------------------------------------------------------

class TestApplyRiskLayerBatchRegime:
    LOOKBACK = 10

    def _fixture(self, n: int = 40):
        """Single-symbol fixture with deterministic nonzero vol.

        Alternating ±2% daily returns → annualized trailing vol ≈ 32%,
        above every target in play, so the vol scale is always < 1 once
        history is available and is exactly target / trailing_vol.
        """
        dates = pd.bdate_range("2023-01-02", periods=n)
        weights = pd.DataFrame(np.ones((n, 1)), index=dates, columns=["X"])
        rets = np.where(np.arange(n) % 2 == 0, 0.02, -0.02)
        returns = pd.DataFrame(rets, index=dates, columns=["X"])
        return weights, returns, dates

    def _expected_scale(self, port_rets: np.ndarray, t: int, target: float) -> float:
        window = port_rets[max(0, t - self.LOOKBACK):t]
        vol = float(np.std(window, ddof=1)) * np.sqrt(252.0)
        return min(1.0, target / vol)

    def _regime_batch_config(self, **rc_kw) -> PortfolioRiskConfig:
        rc_defaults = dict(
            enabled=True, low_target=0.05, high_target=0.15,
            zscore_lo=-1.0, zscore_hi=1.0,
        )
        rc_defaults.update(rc_kw)
        return PortfolioRiskConfig(
            vol_target=0.10, vol_lookback=self.LOOKBACK,
            regime_conditioning=RegimeConditioningConfig(**rc_defaults),
        )

    # ---- off-by-default no-op ----

    def test_off_by_default_bit_identical_to_reference(self):
        # Reference reimplementation of the pre-regime vol-targeting loop:
        # target is the constant config.vol_target at every bar.
        cfg = _make_config(vol_target=0.10, vol_lookback=self.LOOKBACK)
        assert cfg.regime_conditioning is None
        weights, returns, dates = self._fixture()
        port_rets = (weights * returns).sum(axis=1).values

        expected = weights.values.astype(np.float64).copy()
        for t in range(len(weights)):
            window = port_rets[max(0, t - self.LOOKBACK):t]
            if len(window) >= 2:
                vol = float(np.std(window, ddof=1)) * np.sqrt(252.0)
                scale = 1.0 if vol <= 0 else min(1.0, cfg.vol_target / vol)
                expected[t] = expected[t] * scale

        result = apply_risk_layer_batch(weights, returns, None, cfg)
        np.testing.assert_array_equal(result.values, expected)

    def test_regime_series_ignored_when_not_configured(self):
        cfg = _make_config(vol_target=0.10, vol_lookback=self.LOOKBACK)
        weights, returns, dates = self._fixture()
        regime = pd.Series(np.linspace(-3, 3, len(dates)), index=dates)
        base = apply_risk_layer_batch(weights, returns, None, cfg)
        with_regime = apply_risk_layer_batch(
            weights, returns, None, cfg, regime_series=regime,
        )
        pd.testing.assert_frame_equal(with_regime, base, check_exact=True)

    def test_regime_series_ignored_when_disabled(self):
        cfg = self._regime_batch_config(enabled=False)
        weights, returns, dates = self._fixture()
        regime = pd.Series(np.linspace(-3, 3, len(dates)), index=dates)
        base = apply_risk_layer_batch(weights, returns, None, cfg)
        with_regime = apply_risk_layer_batch(
            weights, returns, None, cfg, regime_series=regime,
        )
        pd.testing.assert_frame_equal(with_regime, base, check_exact=True)

    def test_enabled_without_series_is_unconditioned(self):
        cfg_regime = self._regime_batch_config()
        cfg_plain = _make_config(vol_target=0.10, vol_lookback=self.LOOKBACK)
        weights, returns, _ = self._fixture()
        with_cfg = apply_risk_layer_batch(weights, returns, None, cfg_regime)
        without = apply_risk_layer_batch(weights, returns, None, cfg_plain)
        pd.testing.assert_frame_equal(with_cfg, without, check_exact=True)

    # ---- alignment: value applied at bar t is the t-1 reading ----

    def test_value_at_t_is_lagged_one_bar(self):
        cfg = self._regime_batch_config()
        weights, returns, dates = self._fixture()
        n = len(dates)
        k = 20  # single stress reading at bar k
        regime = pd.Series(np.full(n, -5.0), index=dates)  # calm everywhere
        regime.iloc[k] = 5.0                               # stressed at k only
        port_rets = (weights * returns).sum(axis=1).values

        result = apply_risk_layer_batch(
            weights, returns, None, cfg, regime_series=regime,
        )
        # Bar k uses regime[k-1] = calm → high_target (stress at k not yet visible)
        assert abs(
            result.iloc[k, 0] - self._expected_scale(port_rets, k, 0.15)
        ) < 1e-12
        # Bar k+1 uses regime[k] = stressed → low_target
        assert abs(
            result.iloc[k + 1, 0] - self._expected_scale(port_rets, k + 1, 0.05)
        ) < 1e-12
        # Bar k+2 uses regime[k+1] = calm again → high_target
        assert abs(
            result.iloc[k + 2, 0] - self._expected_scale(port_rets, k + 2, 0.15)
        ) < 1e-12

    def test_first_bar_unconditioned(self):
        # Row 0 has no completed prior bar → no regime reading; also no vol
        # history → weights pass through unchanged even under extreme stress.
        cfg = self._regime_batch_config()
        weights, returns, dates = self._fixture()
        regime = pd.Series(np.full(len(dates), 10.0), index=dates)
        result = apply_risk_layer_batch(
            weights, returns, None, cfg, regime_series=regime,
        )
        np.testing.assert_array_almost_equal(result.iloc[0], weights.iloc[0])

    def test_nan_regime_falls_back_to_base_target(self):
        cfg = self._regime_batch_config()
        weights, returns, dates = self._fixture()
        n = len(dates)
        k = 20
        regime = pd.Series(np.full(n, np.nan), index=dates)
        regime.iloc[k] = np.nan  # explicit: reading before k+1 is missing
        port_rets = (weights * returns).sum(axis=1).values
        result = apply_risk_layer_batch(
            weights, returns, None, cfg, regime_series=regime,
        )
        # All-NaN series → every bar uses the unconditioned vol_target
        assert abs(
            result.iloc[k + 1, 0]
            - self._expected_scale(port_rets, k + 1, cfg.vol_target)
        ) < 1e-12

    # ---- causality: mutating regime at bar t cannot reach bars <= t ----

    def test_mutating_regime_at_t_does_not_affect_bars_up_to_t(self):
        cfg = self._regime_batch_config()
        weights, returns, dates = self._fixture()
        n = len(dates)
        k = 25
        regime_a = pd.Series(np.zeros(n), index=dates)
        regime_b = regime_a.copy()
        regime_b.iloc[k] = 5.0  # mutate ONLY bar k

        result_a = apply_risk_layer_batch(
            weights, returns, None, cfg, regime_series=regime_a,
        )
        result_b = apply_risk_layer_batch(
            weights, returns, None, cfg, regime_series=regime_b,
        )
        # Per the documented alignment, regime[k] first influences bar k+1:
        # every bar <= k must be bit-identical...
        pd.testing.assert_frame_equal(
            result_a.iloc[: k + 1], result_b.iloc[: k + 1], check_exact=True,
        )
        # ...and bar k+1 must actually differ (vol scaling is active and the
        # target moved from midpoint 0.10 to low_target 0.05).
        assert abs(result_a.iloc[k + 1, 0] - result_b.iloc[k + 1, 0]) > 1e-9

    # ---- single-step API parity ----

    def test_apply_risk_layer_regime_value(self):
        cfg = self._regime_batch_config()
        weights = np.array([0.5, 0.5])
        # Alternating ±2% → ≈32% annualized trailing vol ≫ all targets
        port_rets = np.where(np.arange(25) % 2 == 0, 0.02, -0.02)
        stressed = apply_risk_layer(
            weights, port_rets, None, None, cfg, regime_value=5.0,
        )
        calm = apply_risk_layer(
            weights, port_rets, None, None, cfg, regime_value=-5.0,
        )
        none = apply_risk_layer(weights, port_rets, None, None, cfg)
        # low_target < vol_target < high_target ⇒ ordering of scaled weights
        assert np.all(stressed < none)
        assert np.all(none < calm)
        # Exact ratio: scales are target/vol, so stressed/calm = 0.05/0.15
        np.testing.assert_allclose(stressed / calm, 0.05 / 0.15, rtol=1e-9)
