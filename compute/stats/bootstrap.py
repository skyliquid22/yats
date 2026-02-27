#!/usr/bin/env python3
"""Bootstrap confidence interval for Sharpe ratio.

Reads returns from QuestDB and computes bootstrap CI as JSON.
"""
import argparse
import json
import sys

import numpy as np


def fetch_returns(symbol: str, start_date: str, end_date: str) -> np.ndarray:
    """Fetch daily returns from QuestDB canonical data."""
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
            cur.execute(sql, (symbol, start_date, end_date))
            rows = cur.fetchall()
        prices = np.array([r[0] for r in rows], dtype=float)
        return np.diff(np.log(prices))  # log returns
    finally:
        conn.close()


def bootstrap_sharpe(
    returns: np.ndarray,
    n_bootstrap: int = 10000,
    confidence: float = 0.95,
    annualization: float = 252.0,
    seed: int | None = None,
) -> dict:
    """Compute bootstrap CI for annualized Sharpe ratio."""
    rng = np.random.default_rng(seed)
    n = len(returns)
    sharpes = np.empty(n_bootstrap)

    for i in range(n_bootstrap):
        sample = rng.choice(returns, size=n, replace=True)
        mu = sample.mean()
        sigma = sample.std(ddof=1)
        sharpes[i] = (mu / sigma) * np.sqrt(annualization) if sigma > 0 else 0.0

    alpha = 1.0 - confidence
    lower = float(np.percentile(sharpes, 100 * alpha / 2))
    upper = float(np.percentile(sharpes, 100 * (1 - alpha / 2)))
    point_estimate = float((returns.mean() / returns.std(ddof=1)) * np.sqrt(annualization))

    return {
        "sharpe_ratio": point_estimate,
        "ci_lower": lower,
        "ci_upper": upper,
        "confidence": confidence,
        "n_bootstrap": n_bootstrap,
        "bootstrap_mean": float(sharpes.mean()),
        "bootstrap_std": float(sharpes.std()),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap Sharpe ratio CI")
    parser.add_argument("--symbol", required=True, help="Ticker symbol")
    parser.add_argument("--start-date", required=True, help="Start date ISO-8601")
    parser.add_argument("--end-date", required=True, help="End date ISO-8601")
    parser.add_argument("--n-bootstrap", type=int, default=10000, help="Bootstrap iterations")
    parser.add_argument("--confidence", type=float, default=0.95, help="Confidence level")
    parser.add_argument("--seed", type=int, default=None, help="Random seed")
    args = parser.parse_args()

    returns = fetch_returns(args.symbol, args.start_date, args.end_date)
    if len(returns) < 30:
        print(json.dumps({"error": f"Insufficient data: {len(returns)} returns (need >= 30)"}))
        sys.exit(1)

    result = bootstrap_sharpe(
        returns,
        n_bootstrap=args.n_bootstrap,
        confidence=args.confidence,
        seed=args.seed,
    )
    result["symbol"] = args.symbol
    result["observations"] = len(returns)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
