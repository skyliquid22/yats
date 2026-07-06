"""Tests for compute.stats.deflated_sharpe — DSR and sweep-level correction.

Tests required by ya-cnac1:
1. DSR decreases as N (trial count) rises holding observed SR fixed
2. DSR decreases as cross-config Sharpe variance rises
3. N=1 sweep degenerates gracefully to DSR ≈ PSR
4. Formula matches a hand-computed fixture
"""

from __future__ import annotations

import math

import numpy as np
import pytest

from compute.stats.deflated_sharpe import (
    _expected_max_sr_benchmark,
    compute_sweep_benchmark_sr,
    compute_sweep_dsr,
    deflated_sharpe_ratio,
    probabilistic_sharpe_ratio,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cfg(sharpe: float, skew: float = 0.0, kurt: float = 3.0, n_obs: int = 252) -> dict:
    return {"sharpe": sharpe, "skewness": skew, "kurtosis": kurt, "n_obs": n_obs}


# ---------------------------------------------------------------------------
# _expected_max_sr_benchmark
# ---------------------------------------------------------------------------


class TestExpectedMaxBenchmark:
    def test_n_one_returns_zero(self):
        assert _expected_max_sr_benchmark(1) == 0.0

    def test_n_zero_returns_zero(self):
        assert _expected_max_sr_benchmark(0) == 0.0

    def test_positive_for_n_gt_1(self):
        for n in [2, 5, 10, 100]:
            assert _expected_max_sr_benchmark(n) > 0.0

    def test_monotone_increasing(self):
        vals = [_expected_max_sr_benchmark(n) for n in [2, 5, 10, 50, 100]]
        for a, b in zip(vals, vals[1:]):
            assert a < b


# ---------------------------------------------------------------------------
# compute_sweep_benchmark_sr
# ---------------------------------------------------------------------------


class TestComputeSweepBenchmarkSr:
    def test_empty_returns_zero(self):
        assert compute_sweep_benchmark_sr([]) == 0.0

    def test_single_config_returns_zero(self):
        assert compute_sweep_benchmark_sr([1.5]) == 0.0

    def test_identical_configs_returns_zero(self):
        # Zero variance → SR0 = 0 regardless of N
        assert compute_sweep_benchmark_sr([1.0, 1.0, 1.0]) == 0.0

    def test_scales_with_variance(self):
        # Higher SR variance → higher SR0
        low_var = [1.0, 1.2]       # std ≈ 0.1
        high_var = [0.5, 2.5]      # std = 1.0
        assert compute_sweep_benchmark_sr(high_var) > compute_sweep_benchmark_sr(low_var)

    def test_scales_with_n(self):
        # Same std, more configs → higher SR0
        std = 0.5
        configs_4 = [1.0 + std, 1.0 - std, 1.0 + std, 1.0 - std]
        configs_8 = configs_4 * 2  # same std, twice as many
        assert compute_sweep_benchmark_sr(configs_8) > compute_sweep_benchmark_sr(configs_4)

    def test_fixture_n3(self):
        # SRs = [0.5, 1.0, 1.5], N=3
        # std = sqrt(1/6) ≈ 0.40825
        # E[max(3)] = (1-γ)*ppf(2/3) + γ*ppf(1-1/(3e))
        # SR0 = std * E[max(3)] ≈ 0.3482
        sr0 = compute_sweep_benchmark_sr([0.5, 1.0, 1.5])
        assert sr0 == pytest.approx(0.3482, abs=0.002)


# ---------------------------------------------------------------------------
# Required tests (ya-cnac1)
# ---------------------------------------------------------------------------


class TestSweepDsrDecreaseWithN:
    """Test 1: DSR decreases as N (trial count) rises holding observed SR fixed."""

    def test_dsr_decreases_as_n_rises(self):
        # Use symmetric configs: ±spread around mean, so std = spread regardless of N.
        # More configs → same std, higher E[max] → higher SR0 → lower DSR.
        spread = 0.5
        mean_sr = 1.0

        # N=4
        configs_4 = [
            _cfg(mean_sr + spread), _cfg(mean_sr - spread),
            _cfg(mean_sr + spread), _cfg(mean_sr - spread),
        ]
        # N=8 (same distribution, more trials)
        configs_8 = configs_4 * 2

        dsr_4 = compute_sweep_dsr(configs_4)
        dsr_8 = compute_sweep_dsr(configs_8)

        # Compare DSR for the best config (sharpe = mean + spread)
        best_idx = 0  # first config has sharpe = mean + spread
        assert dsr_4[best_idx]["dsr"] > dsr_8[best_idx]["dsr"]

    def test_monotone_over_n_via_benchmark(self):
        # More targeted: verify SR0 strictly increases as N increases with fixed std.
        std_sr = 0.5
        for n1, n2 in [(2, 5), (5, 10), (10, 20)]:
            # Build N configs with correct std (symmetric around mean)
            def _build(n: int) -> list[float]:
                half = n // 2
                return [1.0 + std_sr] * half + [1.0 - std_sr] * (n - half)

            sr0_small = compute_sweep_benchmark_sr(_build(n1))
            sr0_large = compute_sweep_benchmark_sr(_build(n2))
            assert sr0_small < sr0_large, f"SR0(N={n1}) should be < SR0(N={n2})"


class TestSweepDsrDecreaseWithVariance:
    """Test 2: DSR decreases as cross-config Sharpe variance rises."""

    def test_dsr_decreases_as_variance_rises(self):
        # Same N and observed SR, but different spread in SRs
        # Low variance: configs clustered
        configs_low = [_cfg(1.0), _cfg(1.5), _cfg(0.5), _cfg(1.0)]
        # High variance: configs spread widely (same N)
        configs_high = [_cfg(0.0), _cfg(1.5), _cfg(3.0), _cfg(0.0)]

        dsr_low = compute_sweep_dsr(configs_low)
        dsr_high = compute_sweep_dsr(configs_high)

        # Find DSR for the sharpe=1.5 config in each sweep
        dsr_best_low = next(r["dsr"] for r, c in zip(dsr_low, configs_low) if c["sharpe"] == 1.5)
        dsr_best_high = next(r["dsr"] for r, c in zip(dsr_high, configs_high) if c["sharpe"] == 1.5)

        assert dsr_best_low > dsr_best_high

    def test_benchmark_sr_increases_with_variance(self):
        # Same N, increasing variance → increasing SR0
        n = 4
        for std_low, std_high in [(0.1, 0.5), (0.5, 1.0), (1.0, 2.0)]:
            sr0_low = compute_sweep_benchmark_sr([1.0 + std_low, 1.0 - std_low] * (n // 2))
            sr0_high = compute_sweep_benchmark_sr([1.0 + std_high, 1.0 - std_high] * (n // 2))
            assert sr0_low < sr0_high


class TestN1Degenerate:
    """Test 3: N=1 sweep degenerates gracefully to DSR ≈ PSR."""

    def test_n1_dsr_equals_psr_with_zero_benchmark(self):
        sharpe, skew, kurt, n_obs = 1.5, 0.0, 3.0, 252

        sweep_result = compute_sweep_dsr([_cfg(sharpe, skew, kurt, n_obs)])
        dsr_n1 = sweep_result[0]["dsr"]

        # PSR with benchmark=0 (zero-skill benchmark)
        psr_result = probabilistic_sharpe_ratio(
            observed_sharpe=sharpe,
            benchmark_sharpe=0.0,
            n_observations=n_obs,
            returns_skewness=skew,
            returns_kurtosis=kurt,
        )

        assert dsr_n1 == pytest.approx(psr_result["dsr"])

    def test_n1_benchmark_is_zero(self):
        result = compute_sweep_dsr([_cfg(1.5)])
        assert result[0]["benchmark_sharpe"] == 0.0

    def test_n1_returns_single_element(self):
        result = compute_sweep_dsr([_cfg(1.5)])
        assert len(result) == 1

    def test_empty_sweep_returns_empty(self):
        assert compute_sweep_dsr([]) == []


class TestHandComputedFixture:
    """Test 4: Formula matches a hand-computed fixture."""

    def test_n3_sr0_and_dsr(self):
        # N=3, SRs = [0.5, 1.0, 1.5], skewness=0, kurtosis=3, n_obs=252
        #
        # std_sr = sqrt(((0.5-1)^2 + (1-1)^2 + (1.5-1)^2) / 3)
        #        = sqrt(0.5/3) = sqrt(1/6) ≈ 0.40825
        #
        # E[max(3)] = (1-0.5772)*ppf(2/3) + 0.5772*ppf(1-1/(3*e))
        #           ≈ 0.4228*0.4307 + 0.5772*1.1597 ≈ 0.8514
        #
        # SR0 = 0.40825 * 0.8514 ≈ 0.3476

        configs = [
            _cfg(0.5), _cfg(1.0), _cfg(1.5),
        ]
        results = compute_sweep_dsr(configs)

        # SR0 matches hand computation
        sr0 = results[0]["benchmark_sharpe"]
        assert sr0 == pytest.approx(0.3476, abs=0.005)

        # DSR increases monotonically with observed SR
        dsrs = [r["dsr"] for r in results]
        assert dsrs[0] < dsrs[1] < dsrs[2]

        # Best config (sharpe=1.5) exceeds benchmark → DSR > 0.5
        assert results[2]["dsr"] > 0.5

        # Worst config (sharpe=0.5) is above benchmark (0.5 > 0.35) → DSR > 0.5
        assert results[0]["dsr"] > 0.5

    def test_n5_dsr_for_best_config(self):
        # N=5, SRs = [0.5, 0.8, 1.2, 1.5, 2.0], normal returns, n_obs=252
        #
        # Hand computation (population variance):
        # mean = 1.2, var = 0.276, std = 0.52535
        # E[max(5)] ≈ 0.4228*ppf(0.8) + 0.5772*ppf(1-1/(5e))
        #           ≈ 0.4228*0.8416 + 0.5772*1.4469 ≈ 1.1908
        # SR0 = 0.52535 * 1.1908 ≈ 0.6255
        #
        # For sharpe=2.0: sr_deann = 2.0/sqrt(252)≈0.12599
        # sr_star_deann = 0.6255/sqrt(252)≈0.03940
        # dof = 251
        # se = sqrt((1 + 0.5*0.12599^2) / 251) ≈ 0.06326
        # z = (0.12599 - 0.03940) / 0.06326 ≈ 1.369
        # DSR = Φ(1.369) ≈ 0.914

        configs = [_cfg(sr) for sr in [0.5, 0.8, 1.2, 1.5, 2.0]]
        results = compute_sweep_dsr(configs)

        sr0 = results[0]["benchmark_sharpe"]
        assert sr0 == pytest.approx(0.6255, abs=0.01)

        dsr_best = results[-1]["dsr"]  # sharpe=2.0
        assert dsr_best == pytest.approx(0.914, abs=0.01)


# ---------------------------------------------------------------------------
# deflated_sharpe_ratio (original API preserved)
# ---------------------------------------------------------------------------


class TestDeflatedSharpeRatioApi:
    def test_returns_required_keys(self):
        result = deflated_sharpe_ratio(1.5, 10, 0.0, 3.0, 252)
        assert {"deflated_sharpe_ratio", "p_value", "is_significant",
                "expected_max_sharpe", "observed_sharpe", "num_trials"} <= result.keys()

    def test_high_sr_vs_few_trials_is_significant(self):
        result = deflated_sharpe_ratio(5.0, 2, 0.0, 3.0, 252)
        assert result["is_significant"]

    def test_low_sr_vs_many_trials_not_significant(self):
        result = deflated_sharpe_ratio(0.3, 1000, 0.0, 3.0, 252)
        assert not result["is_significant"]

    def test_dsr_increases_with_observed_sr(self):
        dsr_low = deflated_sharpe_ratio(0.5, 10, 0.0, 3.0, 252)["deflated_sharpe_ratio"]
        dsr_high = deflated_sharpe_ratio(3.0, 10, 0.0, 3.0, 252)["deflated_sharpe_ratio"]
        assert dsr_low < dsr_high

    def test_expected_max_sharpe_preserved(self):
        # expected_max_sharpe for N=10: sqrt(2*log(10)) - ... ≈ 1.7448
        result = deflated_sharpe_ratio(1.5, 10, 0.0, 3.0, 252)
        assert result["expected_max_sharpe"] == pytest.approx(1.7448, abs=0.001)


# ---------------------------------------------------------------------------
# probabilistic_sharpe_ratio
# ---------------------------------------------------------------------------


class TestProbabilisticSharpeRatio:
    def test_zero_benchmark_positive_sr(self):
        result = probabilistic_sharpe_ratio(1.5, 0.0, 252, 0.0, 3.0)
        assert result["dsr"] > 0.9  # well above zero benchmark

    def test_benchmark_equals_observed_gives_half(self):
        result = probabilistic_sharpe_ratio(1.5, 1.5, 252, 0.0, 3.0)
        assert result["dsr"] == pytest.approx(0.5, abs=0.05)

    def test_dsr_in_unit_interval(self):
        for obs in [0.0, 0.5, 1.0, 2.0]:
            result = probabilistic_sharpe_ratio(obs, 1.0, 252, 0.0, 3.0)
            assert 0.0 <= result["dsr"] <= 1.0

    def test_pvalue_plus_dsr_equals_one(self):
        result = probabilistic_sharpe_ratio(1.5, 0.5, 252, 0.0, 3.0)
        assert result["dsr"] + result["p_value"] == pytest.approx(1.0)
