"""Tests for research.eval.attribution — factor regression."""

from __future__ import annotations

import json
import tempfile
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from research.eval.attribution import compute_factor_attribution
from research.eval.evaluate import evaluate, evaluate_to_json
from research.experiments.spec import CostConfig, ExperimentSpec


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_spec(**overrides) -> ExperimentSpec:
    defaults = dict(
        experiment_name="test_attribution",
        symbols=("AAPL", "MSFT"),
        start_date=date(2023, 1, 1),
        end_date=date(2024, 1, 1),
        interval="daily",
        feature_set="core_v1",
        policy="equal_weight",
        policy_params={},
        cost_config=CostConfig(transaction_cost_bp=5.0),
        seed=42,
    )
    defaults.update(overrides)
    return ExperimentSpec(**defaults)


def _make_dates(n: int = 252) -> pd.DatetimeIndex:
    return pd.bdate_range("2023-01-01", periods=n)


def _synthetic_returns_with_known_factor(
    n: int = 500,
    true_alpha_daily: float = 0.0005,
    true_beta_market: float = 0.8,
    true_beta_momentum: float = 0.3,
    noise_std: float = 0.002,
    seed: int = 42,
) -> tuple[pd.Series, pd.DataFrame]:
    """Generate portfolio returns = alpha + beta_m*market + beta_mom*momentum + noise.

    Returns (portfolio_returns, factor_returns) with date index.
    """
    rng = np.random.default_rng(seed)
    dates = _make_dates(n)

    market = rng.normal(0.0004, 0.01, n)
    momentum = rng.normal(0.0002, 0.005, n)
    noise = rng.normal(0.0, noise_std, n)

    portfolio = true_alpha_daily + true_beta_market * market + true_beta_momentum * momentum + noise

    port_series = pd.Series(portfolio, index=dates, name="portfolio")
    factor_df = pd.DataFrame({"market": market, "momentum": momentum}, index=dates)

    return port_series, factor_df


# ---------------------------------------------------------------------------
# Unit tests for compute_factor_attribution
# ---------------------------------------------------------------------------

class TestComputeFactorAttribution:
    def test_recovers_known_alpha(self):
        """Regression should recover true alpha (annualised) closely."""
        true_alpha_daily = 0.001  # 25.2% annualised
        port, factors = _synthetic_returns_with_known_factor(
            n=1000, true_alpha_daily=true_alpha_daily, noise_std=0.001, seed=1,
        )
        result = compute_factor_attribution(port, factors)

        assert result["alpha_annualized"] is not None
        expected = true_alpha_daily * 252
        assert abs(result["alpha_annualized"] - expected) < 0.05  # within 5 pp

    def test_recovers_known_betas(self):
        """Regression should recover true factor betas closely."""
        true_beta_m = 0.7
        true_beta_mom = 0.4
        port, factors = _synthetic_returns_with_known_factor(
            n=1000, true_beta_market=true_beta_m, true_beta_momentum=true_beta_mom,
            noise_std=0.001, seed=2,
        )
        result = compute_factor_attribution(port, factors)

        assert result["betas"]["market"] is not None
        assert abs(result["betas"]["market"] - true_beta_m) < 0.05
        assert abs(result["betas"]["momentum"] - true_beta_mom) < 0.05

    def test_zero_alpha_not_significant(self):
        """Zero-alpha strategy should have |alpha_t_stat| < 2 (within noise)."""
        port, factors = _synthetic_returns_with_known_factor(
            n=1000, true_alpha_daily=0.0, noise_std=0.01, seed=3,
        )
        result = compute_factor_attribution(port, factors)

        assert result["alpha_t_stat"] is not None
        assert abs(result["alpha_t_stat"]) < 3.0  # noise driven, not > 3σ

    def test_significant_alpha_has_high_t_stat(self):
        """High-alpha, low-noise strategy should have large |alpha_t_stat|."""
        port, factors = _synthetic_returns_with_known_factor(
            n=1000, true_alpha_daily=0.002, noise_std=0.0005, seed=4,
        )
        result = compute_factor_attribution(port, factors)

        assert result["alpha_t_stat"] is not None
        assert result["alpha_t_stat"] > 2.0

    def test_r_squared_bounds(self):
        """R-squared must lie in [0, 1]."""
        port, factors = _synthetic_returns_with_known_factor(n=500, seed=5)
        result = compute_factor_attribution(port, factors)

        assert result["r_squared"] is not None
        assert 0.0 <= result["r_squared"] <= 1.0

    def test_high_r_squared_for_pure_factor(self):
        """Portfolio that is purely factor-driven should have high R2."""
        n = 500
        dates = _make_dates(n)
        rng = np.random.default_rng(6)
        market = rng.normal(0.0004, 0.01, n)
        # Portfolio = 0.9 * market (almost no idiosyncratic)
        portfolio = 0.9 * market + rng.normal(0.0, 0.0001, n)
        port = pd.Series(portfolio, index=dates)
        factors = pd.DataFrame({"market": market}, index=dates)

        result = compute_factor_attribution(port, factors)
        assert result["r_squared"] is not None
        assert result["r_squared"] > 0.95

    def test_returns_expected_keys(self):
        port, factors = _synthetic_returns_with_known_factor(n=300, seed=7)
        result = compute_factor_attribution(port, factors)

        for key in ["alpha_annualized", "alpha_t_stat", "betas", "r_squared", "n_obs", "factors_used"]:
            assert key in result, f"Missing key: {key}"

        assert "market" in result["betas"]
        assert "momentum" in result["betas"]

    def test_n_obs_matches_alignment(self):
        """n_obs should equal number of aligned dates."""
        n = 400
        port, factors = _synthetic_returns_with_known_factor(n=n, seed=8)
        result = compute_factor_attribution(port, factors)
        assert result["n_obs"] == n

    def test_factors_used_matches_columns(self):
        port, factors = _synthetic_returns_with_known_factor(n=300, seed=9)
        result = compute_factor_attribution(port, factors)
        assert result["factors_used"] == list(factors.columns)

    def test_partial_date_overlap(self):
        """Attribution uses only dates present in both series."""
        n = 300
        dates = _make_dates(n)
        rng = np.random.default_rng(10)
        market = rng.normal(0.0, 0.01, n)
        portfolio = 0.8 * market + rng.normal(0.0, 0.002, n)

        port = pd.Series(portfolio, index=dates)
        # Factor only covers last 200 dates
        factors = pd.DataFrame({"market": market[100:]}, index=dates[100:])

        result = compute_factor_attribution(port, factors)
        assert result["n_obs"] == 200

    def test_empty_attribution_when_too_few_obs(self):
        """Fewer observations than parameters + 1 should return null values."""
        dates = pd.bdate_range("2023-01-01", periods=2)
        port = pd.Series([0.01, -0.01], index=dates)
        factors = pd.DataFrame({"market": [0.005, -0.005], "momentum": [0.001, -0.001]}, index=dates)

        # 2 obs, 3 params (intercept + 2 betas) → insufficient dof
        result = compute_factor_attribution(port, factors)
        assert result["alpha_annualized"] is None
        assert result["alpha_t_stat"] is None
        assert result["n_obs"] == 0

    def test_single_factor(self):
        """Single factor regression should work."""
        n = 300
        dates = _make_dates(n)
        rng = np.random.default_rng(11)
        market = rng.normal(0.0004, 0.01, n)
        portfolio = 0.0003 + 0.7 * market + rng.normal(0.0, 0.002, n)

        port = pd.Series(portfolio, index=dates)
        factors = pd.DataFrame({"market": market}, index=dates)

        result = compute_factor_attribution(port, factors)
        assert result["alpha_annualized"] is not None
        assert "market" in result["betas"]
        assert "momentum" not in result["betas"]

    def test_json_serialisable(self):
        """Result dict must be JSON-serialisable."""
        port, factors = _synthetic_returns_with_known_factor(n=300, seed=12)
        result = compute_factor_attribution(port, factors)
        json.dumps(result)  # must not raise


# ---------------------------------------------------------------------------
# Integration: attribution in evaluate() output
# ---------------------------------------------------------------------------

class TestAttributionInEvaluate:
    def _make_data(self, n: int = 252, n_sym: int = 2, seed: int = 0):
        dates = _make_dates(n)
        syms = [f"SYM{i}" for i in range(n_sym)]
        rng = np.random.default_rng(seed)
        weights = pd.DataFrame(
            np.full((n, n_sym), 1.0 / n_sym), index=dates, columns=syms,
        )
        returns = pd.DataFrame(
            rng.normal(0.0005, 0.01, (n, n_sym)), index=dates, columns=syms,
        )
        return weights, returns, dates

    def test_attribution_none_when_no_factor_returns(self):
        spec = _make_spec()
        weights, returns, _ = self._make_data()
        result = evaluate(spec, weights, returns)

        assert "attribution" in result["performance"]
        assert result["performance"]["attribution"] is None

    def test_attribution_present_when_factor_returns_provided(self):
        spec = _make_spec()
        weights, returns, dates = self._make_data()

        rng = np.random.default_rng(42)
        n = len(dates)
        factor_returns = pd.DataFrame({
            "market": rng.normal(0.0004, 0.01, n),
            "momentum": rng.normal(0.0002, 0.005, n),
        }, index=dates)

        result = evaluate(spec, weights, returns, factor_returns=factor_returns)

        attr = result["performance"]["attribution"]
        assert attr is not None
        assert "alpha_annualized" in attr
        assert "alpha_t_stat" in attr
        assert "betas" in attr
        assert "market" in attr["betas"]
        assert "momentum" in attr["betas"]
        assert "r_squared" in attr
        assert "n_obs" in attr
        assert attr["n_obs"] > 0

    def test_known_alpha_recovered_through_evaluate(self):
        """evaluate() attribution should recover a planted alpha."""
        n = 500
        true_alpha_daily = 0.001
        true_beta = 0.6
        dates = _make_dates(n)
        rng = np.random.default_rng(99)
        syms = ["A"]

        market = rng.normal(0.0004, 0.01, n)
        port_returns = true_alpha_daily + true_beta * market + rng.normal(0.0, 0.001, n)

        weights = pd.DataFrame(np.ones((n, 1)), index=dates, columns=syms)
        returns = pd.DataFrame(port_returns[:, None], index=dates, columns=syms)
        factor_returns = pd.DataFrame({"market": market}, index=dates)

        spec = _make_spec()
        result = evaluate(spec, weights, returns, factor_returns=factor_returns)

        attr = result["performance"]["attribution"]
        expected_alpha = true_alpha_daily * 252
        assert abs(attr["alpha_annualized"] - expected_alpha) < 0.05
        assert abs(attr["betas"]["market"] - true_beta) < 0.05

    def test_attribution_in_evaluate_to_json(self):
        spec = _make_spec()
        weights, returns, dates = self._make_data()

        rng = np.random.default_rng(55)
        n = len(dates)
        factor_returns = pd.DataFrame({"market": rng.normal(0.0, 0.01, n)}, index=dates)

        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "metrics.json"
            result = evaluate_to_json(
                spec, weights, returns, out, factor_returns=factor_returns,
            )

            assert out.exists()
            loaded = json.loads(out.read_text())
            attr = loaded["performance"]["attribution"]
            assert attr is not None
            assert attr["alpha_annualized"] is not None
            assert "market" in attr["betas"]

    def test_attribution_absent_in_evaluate_to_json_without_factors(self):
        """evaluate_to_json without factor_returns → performance.attribution is null."""
        spec = _make_spec()
        weights, returns, _ = self._make_data()

        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "metrics.json"
            evaluate_to_json(spec, weights, returns, out)

            loaded = json.loads(out.read_text())
            assert loaded["performance"]["attribution"] is None
