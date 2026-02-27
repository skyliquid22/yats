#!/usr/bin/env python3
"""Deflated Sharpe Ratio — multiple testing correction.

Computes the probability that a Sharpe ratio is significant after
accounting for the number of trials (backtest configurations tested).
"""
import argparse
import json
import sys

import numpy as np
from scipy import stats as sp_stats


def deflated_sharpe_ratio(
    observed_sharpe: float,
    num_trials: int,
    returns_skewness: float,
    returns_kurtosis: float,
    n_observations: int,
    annualization: float = 252.0,
) -> dict:
    """Compute Deflated Sharpe Ratio per Bailey & de Prado (2014)."""
    # Expected max Sharpe under null (multiple testing)
    euler_mascheroni = 0.5772156649
    e_max_sharpe = np.sqrt(2 * np.log(num_trials)) - (
        (np.log(np.pi) + euler_mascheroni) / (2 * np.sqrt(2 * np.log(num_trials)))
    )
    e_max_sharpe /= np.sqrt(annualization)  # de-annualize for comparison

    # Standard error of Sharpe ratio (Lo, 2002)
    sr = observed_sharpe / np.sqrt(annualization)  # de-annualized
    se_sr = np.sqrt(
        (1 - returns_skewness * sr + ((returns_kurtosis - 1) / 4) * sr**2) / n_observations
    )

    if se_sr <= 0:
        return {
            "deflated_sharpe_ratio": 0.0,
            "p_value": 1.0,
            "is_significant": False,
            "expected_max_sharpe": float(e_max_sharpe * np.sqrt(annualization)),
            "observed_sharpe": observed_sharpe,
            "num_trials": num_trials,
        }

    # PSR: prob that true Sharpe > expected max
    z = (sr - e_max_sharpe) / se_sr
    p_value = float(sp_stats.norm.cdf(z))

    return {
        "deflated_sharpe_ratio": float(p_value),
        "p_value": float(1.0 - p_value),
        "is_significant": p_value > 0.95,
        "expected_max_sharpe": float(e_max_sharpe * np.sqrt(annualization)),
        "observed_sharpe": observed_sharpe,
        "num_trials": num_trials,
        "z_score": float(z),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Deflated Sharpe Ratio")
    parser.add_argument("--observed-sharpe", type=float, required=True, help="Observed Sharpe ratio")
    parser.add_argument("--num-trials", type=int, required=True, help="Number of backtest trials")
    parser.add_argument("--skewness", type=float, required=True, help="Returns skewness")
    parser.add_argument("--kurtosis", type=float, required=True, help="Returns excess kurtosis")
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
