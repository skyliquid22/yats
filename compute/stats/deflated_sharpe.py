#!/usr/bin/env python3
"""Deflated Sharpe Ratio — multiple testing correction.

Computes the probability that a Sharpe ratio is significant after
accounting for the number of trials (backtest configurations tested).

Bailey & de Prado (2014): "The Deflated Sharpe Ratio: Correcting for
Selection Bias, Backtest Overfitting and Non-Normality".
"""
import argparse
import json
import sys

import numpy as np
from scipy import stats as sp_stats

_EULER_MASCHERONI = 0.5772156649


def probabilistic_sharpe_ratio(
    observed_sharpe: float,
    benchmark_sharpe: float,
    n_observations: int,
    returns_skewness: float,
    returns_kurtosis: float,
    annualization: float = 252.0,
) -> dict:
    """PSR: probability that the observed SR exceeds the benchmark.

    Core computation shared by deflated_sharpe_ratio and compute_sweep_dsr.

    Args:
        observed_sharpe: Annualized observed Sharpe ratio.
        benchmark_sharpe: Annualized benchmark Sharpe ratio (SR*).
        n_observations: Number of OOS observations.
        returns_skewness: Returns skewness (γ₃).
        returns_kurtosis: Returns standard kurtosis (4th moment; 3 for normal).
        annualization: Periods per year (252 for daily returns).

    Returns:
        Dict with 'dsr' (PSR value in [0,1]), 'p_value', 'is_significant',
        'benchmark_sharpe', 'observed_sharpe', and 'z_score'.
    """
    sr = observed_sharpe / np.sqrt(annualization)
    sr_star = benchmark_sharpe / np.sqrt(annualization)

    # Standard error per Lo (2002) / Bailey & de Prado (2014) eq. 7.
    # Uses T-1 degrees of freedom per the paper.
    dof = max(n_observations - 1, 1)
    variance_term = 1.0 - returns_skewness * sr + ((returns_kurtosis - 1) / 4) * sr**2
    se_sr = np.sqrt(max(variance_term, 0.0) / dof)

    if se_sr <= 0:
        return {
            "dsr": 0.0,
            "p_value": 1.0,
            "is_significant": False,
            "benchmark_sharpe": float(benchmark_sharpe),
            "observed_sharpe": float(observed_sharpe),
        }

    z = (sr - sr_star) / se_sr
    psr = float(sp_stats.norm.cdf(z))

    return {
        "dsr": psr,
        "p_value": float(1.0 - psr),
        "is_significant": psr > 0.95,
        "benchmark_sharpe": float(benchmark_sharpe),
        "observed_sharpe": float(observed_sharpe),
        "z_score": float(z),
    }


def _expected_max_sr_benchmark(n_trials: int) -> float:
    """Expected maximum of N i.i.d. standard normals (Bailey & de Prado 2014).

    Formula: (1 - γ) · Φ⁻¹(1 - 1/N) + γ · Φ⁻¹(1 - 1/(N·e))

    where γ = Euler-Mascheroni constant ≈ 0.5772 and e = Euler's number.

    Returns 0.0 for N ≤ 1 (no multiple-testing correction needed).
    """
    if n_trials <= 1:
        return 0.0
    return float(
        (1.0 - _EULER_MASCHERONI) * sp_stats.norm.ppf(1.0 - 1.0 / n_trials)
        + _EULER_MASCHERONI * sp_stats.norm.ppf(1.0 - 1.0 / (n_trials * np.e))
    )


def compute_sweep_benchmark_sr(sharpe_ratios: list[float]) -> float:
    """Compute sweep-level DSR benchmark SR0.

    SR0 = sqrt(Var_SR) · E[max of N standard normals]

    where Var_SR is the population variance of per-config OOS Sharpe ratios
    and E[max] uses the Bailey & de Prado (2014) formula.

    Args:
        sharpe_ratios: Annualized OOS Sharpe ratios for each sweep config.

    Returns:
        Annualized benchmark SR0. Returns 0.0 for N ≤ 1 or zero variance.
    """
    n = len(sharpe_ratios)
    if n <= 1:
        return 0.0
    std_sr = float(np.std(sharpe_ratios))  # population std (ddof=0)
    return float(std_sr * _expected_max_sr_benchmark(n))


def compute_sweep_dsr(configs: list[dict]) -> list[dict]:
    """Compute Deflated Sharpe Ratio at sweep level (cross-config correction).

    After a sweep of N configs completes, computes the sweep-level benchmark
    SR0 from the cross-config Sharpe variance, then computes per-config
    DSR = PSR(observed_SR; SR0).

    Args:
        configs: List of per-config dicts, each with:
            - sharpe (float): annualized observed OOS Sharpe ratio
            - skewness (float): returns skewness
            - kurtosis (float): returns standard kurtosis (3 for normal)
            - n_obs (int): number of OOS observations

    Returns:
        List of result dicts (same length as configs), each with:
            - dsr: Deflated Sharpe Ratio probability
            - p_value: 1 - dsr
            - is_significant: dsr > 0.95
            - benchmark_sharpe: sweep-level SR0
            - observed_sharpe: per-config observed SR
            - z_score: PSR z-score
    """
    if not configs:
        return []

    sharpe_ratios = [float(c["sharpe"]) for c in configs]
    sr0 = compute_sweep_benchmark_sr(sharpe_ratios)

    results = []
    for cfg in configs:
        psr_result = probabilistic_sharpe_ratio(
            observed_sharpe=float(cfg["sharpe"]),
            benchmark_sharpe=sr0,
            n_observations=int(cfg["n_obs"]),
            returns_skewness=float(cfg["skewness"]),
            returns_kurtosis=float(cfg["kurtosis"]),
        )
        results.append(psr_result)

    return results


def deflated_sharpe_ratio(
    observed_sharpe: float,
    num_trials: int,
    returns_skewness: float,
    returns_kurtosis: float,
    n_observations: int,
    annualization: float = 252.0,
) -> dict:
    """Compute Deflated Sharpe Ratio per Bailey & de Prado (2014).

    Uses the EVT benchmark for the expected maximum Sharpe across num_trials
    independent backtest configurations.

    For sweep-level DSR that accounts for cross-config Sharpe variance,
    use compute_sweep_dsr() instead.

    Args:
        observed_sharpe: Annualized observed Sharpe ratio.
        num_trials: Number of independent backtest configurations tried.
        returns_skewness: Returns skewness.
        returns_kurtosis: Returns standard kurtosis (3 for normal distribution).
        n_observations: Number of OOS observations.
        annualization: Periods per year (252 for daily returns).
    """
    euler_mascheroni = 0.5772156649
    e_max_sharpe = np.sqrt(2 * np.log(num_trials)) - (
        (np.log(np.pi) + euler_mascheroni) / (2 * np.sqrt(2 * np.log(num_trials)))
    )
    # EVT formula yields a dimensionless expected-max value treated as an
    # annualized SR benchmark (same convention as observed_sharpe).
    benchmark_sharpe = float(e_max_sharpe)

    result = probabilistic_sharpe_ratio(
        observed_sharpe=observed_sharpe,
        benchmark_sharpe=benchmark_sharpe,
        n_observations=n_observations,
        returns_skewness=returns_skewness,
        returns_kurtosis=returns_kurtosis,
        annualization=annualization,
    )

    return {
        "deflated_sharpe_ratio": result["dsr"],
        "p_value": result["p_value"],
        "is_significant": result["is_significant"],
        "expected_max_sharpe": benchmark_sharpe,
        "observed_sharpe": observed_sharpe,
        "num_trials": num_trials,
        "z_score": result.get("z_score", 0.0),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Deflated Sharpe Ratio")
    parser.add_argument("--observed-sharpe", type=float, required=True, help="Observed Sharpe ratio")
    parser.add_argument("--num-trials", type=int, required=True, help="Number of backtest trials")
    parser.add_argument("--skewness", type=float, required=True, help="Returns skewness")
    parser.add_argument("--kurtosis", type=float, required=True, help="Returns standard kurtosis (3 for normal)")
    parser.add_argument("--n-observations", type=int, required=True, help="Number of observations")
    args = parser.parse_args()

    result = deflated_sharpe_ratio(
        observed_sharpe=args.observed_sharpe,
        num_trials=args.num_trials,
        returns_skewness=args.skewness,
        returns_kurtosis=args.kurtosis,
        n_observations=args.n_observations,
    )
    print(json.dumps(result))


if __name__ == "__main__":
    main()
