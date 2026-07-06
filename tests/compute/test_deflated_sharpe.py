"""Tests for compute.stats.deflated_sharpe — PSR and DSR functions."""

from __future__ import annotations

import numpy as np
import pytest
from scipy import stats as sp_stats

from compute.stats.deflated_sharpe import (
    deflated_sharpe_ratio,
    probabilistic_sharpe_ratio,
)


class TestProbabilisticSharpeRatio:
    def test_returns_expected_keys(self):
        result = probabilistic_sharpe_ratio(
            observed_sharpe=1.0, n_observations=252,
            returns_skewness=0.0, returns_kurtosis=0.0,
        )
        for key in ("psr", "z_score", "benchmark_sharpe", "observed_sharpe"):
            assert key in result, f"Missing key: {key}"

    def test_psr_bounds(self):
        result = probabilistic_sharpe_ratio(
            observed_sharpe=1.5, n_observations=252,
            returns_skewness=-0.2, returns_kurtosis=1.0,
        )
        assert 0.0 <= result["psr"] <= 1.0

    def test_positive_sharpe_above_half(self):
        """PSR(0) with positive Sharpe must be > 0.5."""
        result = probabilistic_sharpe_ratio(
            observed_sharpe=1.0, n_observations=252,
            returns_skewness=0.0, returns_kurtosis=0.0,
        )
        assert result["psr"] > 0.5

    def test_negative_sharpe_below_half(self):
        """PSR(0) with negative Sharpe must be < 0.5."""
        result = probabilistic_sharpe_ratio(
            observed_sharpe=-1.0, n_observations=252,
            returns_skewness=0.0, returns_kurtosis=0.0,
        )
        assert result["psr"] < 0.5

    def test_zero_sharpe_near_half(self):
        """PSR(0) with zero Sharpe should be near 0.5."""
        result = probabilistic_sharpe_ratio(
            observed_sharpe=0.0, n_observations=252,
            returns_skewness=0.0, returns_kurtosis=0.0,
        )
        assert abs(result["psr"] - 0.5) < 0.01

    def test_single_observation_returns_neutral(self):
        result = probabilistic_sharpe_ratio(
            observed_sharpe=2.0, n_observations=1,
            returns_skewness=0.0, returns_kurtosis=0.0,
        )
        assert result["psr"] == pytest.approx(0.5)
        assert result["z_score"] == pytest.approx(0.0)

    def test_analytical_formula_match(self):
        """PSR must match manual formula: de-annualize → SE → CDF."""
        sr_ann = 1.2
        n = 500
        skew = -0.3
        kurt = 1.5
        bench = 0.0
        annualization = 252.0

        sr = sr_ann / np.sqrt(annualization)
        sr_star = bench / np.sqrt(annualization)
        variance = (1 - skew * sr + ((kurt - 1) / 4) * sr**2) / (n - 1)
        expected_z = (sr - sr_star) / np.sqrt(variance)
        expected_psr = float(sp_stats.norm.cdf(expected_z))

        result = probabilistic_sharpe_ratio(
            observed_sharpe=sr_ann, n_observations=n,
            returns_skewness=skew, returns_kurtosis=kurt,
            benchmark_sharpe=bench,
        )
        assert abs(result["psr"] - expected_psr) < 1e-10
        assert abs(result["z_score"] - expected_z) < 1e-10

    def test_custom_benchmark_sharpe(self):
        """PSR with non-zero benchmark must be lower than PSR with zero benchmark."""
        result_bench_0 = probabilistic_sharpe_ratio(
            observed_sharpe=1.0, n_observations=252,
            returns_skewness=0.0, returns_kurtosis=0.0,
            benchmark_sharpe=0.0,
        )
        result_bench_1 = probabilistic_sharpe_ratio(
            observed_sharpe=1.0, n_observations=252,
            returns_skewness=0.0, returns_kurtosis=0.0,
            benchmark_sharpe=1.0,
        )
        assert result_bench_0["psr"] > result_bench_1["psr"]

    def test_more_observations_higher_psr(self):
        """With the same Sharpe, more observations → tighter SE → higher PSR for SR > 0."""
        common = dict(observed_sharpe=0.8, returns_skewness=0.0, returns_kurtosis=0.0)
        r_small = probabilistic_sharpe_ratio(n_observations=50, **common)
        r_large = probabilistic_sharpe_ratio(n_observations=500, **common)
        assert r_large["psr"] > r_small["psr"]


class TestDeflatedSharpeRatio:
    def test_returns_expected_keys(self):
        result = deflated_sharpe_ratio(
            observed_sharpe=1.0, num_trials=10,
            returns_skewness=0.0, returns_kurtosis=0.0,
            n_observations=252,
        )
        for key in ("deflated_sharpe_ratio", "p_value", "is_significant", "observed_sharpe"):
            assert key in result, f"Missing key: {key}"

    def test_dsr_bounds(self):
        result = deflated_sharpe_ratio(
            observed_sharpe=1.5, num_trials=20,
            returns_skewness=0.0, returns_kurtosis=0.0,
            n_observations=252,
        )
        assert 0.0 <= result["deflated_sharpe_ratio"] <= 1.0

    def test_more_trials_lower_dsr(self):
        """With more trials, the expected max Sharpe rises, so DSR falls."""
        common = dict(
            observed_sharpe=1.0, returns_skewness=0.0,
            returns_kurtosis=0.0, n_observations=252,
        )
        r_few = deflated_sharpe_ratio(num_trials=2, **common)
        r_many = deflated_sharpe_ratio(num_trials=100, **common)
        assert r_few["deflated_sharpe_ratio"] > r_many["deflated_sharpe_ratio"]
