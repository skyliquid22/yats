#!/usr/bin/env python3
"""Strategy vs book correlation analysis.

Analyzes correlation between strategy returns and a reference book/index.
"""
import argparse
import json
import sys

import numpy as np


def rolling_correlation(x: np.ndarray, y: np.ndarray, window: int = 60) -> np.ndarray:
    """Compute rolling Pearson correlation."""
    n = len(x)
    if n < window:
        return np.array([])

    corrs = np.empty(n - window + 1)
    for i in range(n - window + 1):
        xi = x[i:i + window]
        yi = y[i:i + window]
        corrs[i] = np.corrcoef(xi, yi)[0, 1]
    return corrs


def compute_correlation_analysis(
    strategy_returns: np.ndarray,
    book_returns: np.ndarray,
    rolling_window: int = 60,
) -> dict:
    """Full correlation analysis between strategy and book."""
    # Overall correlation
    overall_corr = float(np.corrcoef(strategy_returns, book_returns)[0, 1])

    # Rolling correlation
    roll_corr = rolling_correlation(strategy_returns, book_returns, rolling_window)

    # Beta (regression slope)
    cov = np.cov(strategy_returns, book_returns)
    beta = float(cov[0, 1] / cov[1, 1]) if cov[1, 1] != 0 else 0.0

    # R-squared
    r_squared = overall_corr ** 2

    # Up/down market correlation
    up_mask = book_returns > 0
    down_mask = book_returns < 0

    up_corr = float(np.corrcoef(strategy_returns[up_mask], book_returns[up_mask])[0, 1]) if up_mask.sum() > 10 else None
    down_corr = float(np.corrcoef(strategy_returns[down_mask], book_returns[down_mask])[0, 1]) if down_mask.sum() > 10 else None

    result: dict = {
        "overall_correlation": overall_corr,
        "beta": beta,
        "r_squared": float(r_squared),
        "rolling_window": rolling_window,
    }

    if len(roll_corr) > 0:
        result["rolling_correlation"] = {
            "mean": float(roll_corr.mean()),
            "std": float(roll_corr.std()),
            "min": float(roll_corr.min()),
            "max": float(roll_corr.max()),
            "current": float(roll_corr[-1]),
        }

    if up_corr is not None:
        result["up_market_correlation"] = up_corr
    if down_corr is not None:
        result["down_market_correlation"] = down_corr

    # Tracking error
    diff = strategy_returns - book_returns
    result["tracking_error"] = float(diff.std() * np.sqrt(252))
    result["information_ratio"] = float(diff.mean() / diff.std() * np.sqrt(252)) if diff.std() > 0 else 0.0

    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Correlation analysis")
    parser.add_argument("--strategy-symbol", required=True, help="Strategy/ticker symbol")
    parser.add_argument("--book-symbol", required=True, help="Book/benchmark symbol")
    parser.add_argument("--start-date", required=True, help="Start date ISO-8601")
    parser.add_argument("--end-date", required=True, help="End date ISO-8601")
    parser.add_argument("--rolling-window", type=int, default=60, help="Rolling window days")
    args = parser.parse_args()

    import psycopg2
    conn = psycopg2.connect(
        host="localhost", port=8812, user="admin", password="quest", dbname="qdb"
    )

    def fetch_returns(symbol: str) -> np.ndarray:
        sql = """
            SELECT close FROM canonical_equity_ohlcv
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp
        """
        with conn.cursor() as cur:
            cur.execute(sql, (symbol, args.start_date, args.end_date))
            rows = cur.fetchall()
        prices = np.array([r[0] for r in rows], dtype=float)
        return np.diff(prices) / prices[:-1]

    try:
        strat_rets = fetch_returns(args.strategy_symbol)
        book_rets = fetch_returns(args.book_symbol)
    finally:
        conn.close()

    # Align lengths
    min_len = min(len(strat_rets), len(book_rets))
    if min_len < 30:
        print(json.dumps({"error": f"Insufficient overlapping data: {min_len} returns (need >= 30)"}))
        sys.exit(1)

    strat_rets = strat_rets[:min_len]
    book_rets = book_rets[:min_len]

    result = compute_correlation_analysis(strat_rets, book_rets, args.rolling_window)
    result["strategy_symbol"] = args.strategy_symbol
    result["book_symbol"] = args.book_symbol
    result["observations"] = min_len
    print(json.dumps(result))


if __name__ == "__main__":
    main()
