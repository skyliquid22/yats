#!/usr/bin/env python3
"""Augmented Dickey-Fuller stationarity test.

Reads a time series from QuestDB and returns ADF test results as JSON.
"""
import argparse
import json
import sys

import numpy as np
from statsmodels.tsa.stattools import adfuller


def fetch_series(symbol: str, column: str, start_date: str, end_date: str) -> np.ndarray:
    """Fetch a time series column from QuestDB canonical data."""
    import psycopg2

    conn = psycopg2.connect(
        host="localhost", port=8812, user="admin", password="quest", dbname="qdb"
    )
    try:
        sql = """
            SELECT {col} FROM canonical_equity_ohlcv
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp
        """.format(col=column)
        with conn.cursor() as cur:
            cur.execute(sql, (symbol, start_date, end_date))
            rows = cur.fetchall()
        return np.array([r[0] for r in rows], dtype=float)
    finally:
        conn.close()


def run_adf(series: np.ndarray, max_lags: int | None = None) -> dict:
    """Run ADF test and return structured results."""
    result = adfuller(series, maxlag=max_lags, autolag="AIC")
    adf_stat, p_value, used_lag, nobs, critical_values, icbest = result
    return {
        "adf_statistic": float(adf_stat),
        "p_value": float(p_value),
        "used_lag": int(used_lag),
        "nobs": int(nobs),
        "critical_values": {k: float(v) for k, v in critical_values.items()},
        "ic_best": float(icbest),
        "is_stationary_5pct": p_value < 0.05,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="ADF stationarity test")
    parser.add_argument("--symbol", required=True, help="Ticker symbol")
    parser.add_argument("--column", default="close", help="Column to test (default: close)")
    parser.add_argument("--start-date", required=True, help="Start date ISO-8601")
    parser.add_argument("--end-date", required=True, help="End date ISO-8601")
    parser.add_argument("--max-lags", type=int, default=None, help="Max lags (default: auto)")
    args = parser.parse_args()

    series = fetch_series(args.symbol, args.column, args.start_date, args.end_date)
    if len(series) < 20:
        print(json.dumps({"error": f"Insufficient data: {len(series)} observations (need >= 20)"}))
        sys.exit(1)

    result = run_adf(series, args.max_lags)
    result["symbol"] = args.symbol
    result["column"] = args.column
    result["observations"] = len(series)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
