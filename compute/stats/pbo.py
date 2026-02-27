#!/usr/bin/env python3
"""Probability of Backtest Overfitting (PBO).

Implements the combinatorially symmetric cross-validation (CSCV) method
from Bailey et al. (2017) to estimate the probability that a backtest-
optimized strategy is overfit.
"""
import argparse
import json
import sys
from itertools import combinations

import numpy as np


def compute_pbo(
    returns_matrix: np.ndarray,
    n_partitions: int = 16,
    seed: int | None = None,
) -> dict:
    """Compute PBO via CSCV.

    Args:
        returns_matrix: (T, S) matrix — T time periods, S strategy variants
        n_partitions: Number of sub-samples (must be even, >= 4)
        seed: Random seed for reproducibility
    """
    rng = np.random.default_rng(seed)
    t, s = returns_matrix.shape

    if n_partitions % 2 != 0:
        n_partitions += 1
    if n_partitions < 4:
        n_partitions = 4

    # Partition time periods into n_partitions groups
    partition_size = t // n_partitions
    if partition_size < 2:
        return {"error": f"Insufficient data: {t} periods for {n_partitions} partitions"}

    indices = np.arange(t)
    rng.shuffle(indices)
    partitions = [indices[i * partition_size:(i + 1) * partition_size] for i in range(n_partitions)]

    half = n_partitions // 2
    combos = list(combinations(range(n_partitions), half))
    if len(combos) > 1000:
        # Sample combinations if too many
        combo_indices = rng.choice(len(combos), size=1000, replace=False)
        combos = [combos[i] for i in combo_indices]

    logit_values: list[float] = []

    for combo in combos:
        in_sample_idx = np.concatenate([partitions[i] for i in combo])
        oos_idx = np.concatenate([partitions[i] for i in range(n_partitions) if i not in combo])

        # IS performance: mean return per strategy
        is_perf = returns_matrix[in_sample_idx].mean(axis=0)
        best_is = int(np.argmax(is_perf))

        # OOS performance of IS-best strategy
        oos_perf = returns_matrix[oos_idx].mean(axis=0)
        oos_rank = float(np.sum(oos_perf >= oos_perf[best_is])) / s

        # Logit of rank (clipped to avoid inf)
        rank_clipped = np.clip(oos_rank, 0.01, 0.99)
        logit_values.append(float(np.log(rank_clipped / (1.0 - rank_clipped))))

    logit_arr = np.array(logit_values)
    pbo = float(np.mean(logit_arr < 0))  # fraction where IS-best underperforms OOS

    return {
        "pbo": pbo,
        "pbo_interpretation": "high" if pbo > 0.5 else "moderate" if pbo > 0.25 else "low",
        "n_combinations": len(combos),
        "n_partitions": n_partitions,
        "n_strategies": s,
        "n_periods": t,
        "logit_mean": float(logit_arr.mean()),
        "logit_std": float(logit_arr.std()),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Probability of Backtest Overfitting")
    parser.add_argument("--returns-file", required=True, help="Path to CSV: rows=periods, cols=strategies")
    parser.add_argument("--n-partitions", type=int, default=16, help="Number of CSCV partitions")
    parser.add_argument("--seed", type=int, default=None, help="Random seed")
    args = parser.parse_args()

    try:
        returns_matrix = np.loadtxt(args.returns_file, delimiter=",", skiprows=1)
    except Exception as e:
        print(json.dumps({"error": f"Failed to load returns file: {e}"}))
        sys.exit(1)

    if returns_matrix.ndim == 1:
        print(json.dumps({"error": "Need at least 2 strategy variants (columns)"}))
        sys.exit(1)

    result = compute_pbo(returns_matrix, n_partitions=args.n_partitions, seed=args.seed)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
