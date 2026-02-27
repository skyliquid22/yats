#!/usr/bin/env python3
"""Tail risk analysis — CVaR, worst drawdowns, bootstrap CIs.

Analyzes the tail risk properties of a return series.
"""
import argparse
import json
import sys

import numpy as np


def compute_cvar(returns: np.ndarray, alpha: float = 0.05) -> dict:
    """Compute VaR and CVaR (Expected Shortfall) at given confidence level."""
    sorted_returns = np.sort(returns)
    cutoff_idx = int(len(sorted_returns) * alpha)
    if cutoff_idx == 0:
        cutoff_idx = 1

    var = float(sorted_returns[cutoff_idx])
    cvar = float(sorted_returns[:cutoff_idx].mean())

    return {
        "var": var,
        "cvar": cvar,
        "alpha": alpha,
        "n_tail_observations": cutoff_idx,
    }


def worst_drawdowns(returns: np.ndarray, top_n: int = 5) -> list[dict]:
    """Find the worst drawdown periods."""
    cumulative = np.cumprod(1 + returns)
    running_max = np.maximum.accumulate(cumulative)
    drawdowns = (cumulative - running_max) / running_max

    # Find drawdown periods
    dd_list: list[dict] = []
    in_drawdown = False
    dd_start = 0
    worst_dd = 0.0
    worst_idx = 0

    for i in range(len(drawdowns)):
        if drawdowns[i] < 0 and not in_drawdown:
            in_drawdown = True
            dd_start = i
            worst_dd = drawdowns[i]
            worst_idx = i
        elif in_drawdown:
            if drawdowns[i] < worst_dd:
                worst_dd = drawdowns[i]
                worst_idx = i
            if drawdowns[i] >= 0 or i == len(drawdowns) - 1:
                in_drawdown = False
                dd_list.append({
                    "start_idx": int(dd_start),
                    "trough_idx": int(worst_idx),
                    "end_idx": int(i),
                    "max_drawdown": float(worst_dd),
                    "duration": int(i - dd_start),
                    "recovery_duration": int(i - worst_idx),
                })

    dd_list.sort(key=lambda x: x["max_drawdown"])
    return dd_list[:top_n]


def bootstrap_cvar_ci(
    returns: np.ndarray,
    alpha: float = 0.05,
    confidence: float = 0.95,
    n_bootstrap: int = 5000,
    seed: int | None = None,
) -> dict:
    """Bootstrap confidence interval for CVaR."""
    rng = np.random.default_rng(seed)
    cvars = np.empty(n_bootstrap)

    for i in range(n_bootstrap):
        sample = rng.choice(returns, size=len(returns), replace=True)
        sorted_sample = np.sort(sample)
        cutoff = max(1, int(len(sorted_sample) * alpha))
        cvars[i] = sorted_sample[:cutoff].mean()

    ci_alpha = 1.0 - confidence
    return {
        "cvar_ci_lower": float(np.percentile(cvars, 100 * ci_alpha / 2)),
        "cvar_ci_upper": float(np.percentile(cvars, 100 * (1 - ci_alpha / 2))),
        "cvar_bootstrap_mean": float(cvars.mean()),
        "confidence": confidence,
        "n_bootstrap": n_bootstrap,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Tail risk analysis")
    parser.add_argument("--symbol", required=True, help="Ticker symbol")
    parser.add_argument("--start-date", required=True, help="Start date ISO-8601")
    parser.add_argument("--end-date", required=True, help="End date ISO-8601")
    parser.add_argument("--alpha", type=float, default=0.05, help="VaR/CVaR confidence (default 0.05)")
    parser.add_argument("--top-drawdowns", type=int, default=5, help="Number of worst drawdowns")
    parser.add_argument("--seed", type=int, default=None, help="Random seed")
    args = parser.parse_args()

    import psycopg2
    conn = psycopg2.connect(
        host="localhost", port=8812, user="admin", password="quest", dbname="qdb"
    )
    try:
        sql = """
            SELECT close FROM canonical_equity_ohlcv
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp
        """
        with conn.cursor() as cur:
            cur.execute(sql, (args.symbol, args.start_date, args.end_date))
            rows = cur.fetchall()
    finally:
        conn.close()

    if len(rows) < 30:
        print(json.dumps({"error": f"Insufficient data: {len(rows)} prices (need >= 30)"}))
        sys.exit(1)

    prices = np.array([r[0] for r in rows], dtype=float)
    returns = np.diff(prices) / prices[:-1]

    cvar_result = compute_cvar(returns, alpha=args.alpha)
    drawdowns = worst_drawdowns(returns, top_n=args.top_drawdowns)
    ci_result = bootstrap_cvar_ci(returns, alpha=args.alpha, seed=args.seed)

    result = {
        "symbol": args.symbol,
        "observations": len(returns),
        **cvar_result,
        "cvar_confidence_interval": ci_result,
        "worst_drawdowns": drawdowns,
        "tail_statistics": {
            "skewness": float(np.mean(((returns - returns.mean()) / returns.std()) ** 3)),
            "kurtosis": float(np.mean(((returns - returns.mean()) / returns.std()) ** 4) - 3),
            "min_return": float(returns.min()),
            "max_return": float(returns.max()),
            "mean_return": float(returns.mean()),
            "std_return": float(returns.std()),
        },
    }
    print(json.dumps(result))


if __name__ == "__main__":
    main()
