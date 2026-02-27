#!/usr/bin/env python3
"""Stress test against historical scenarios.

Runs portfolio returns through historical stress scenarios and reports
impact metrics.
"""
import argparse
import json
import sys

import numpy as np

# Well-known historical stress scenarios (start, end, description)
SCENARIOS: dict[str, dict] = {
    "gfc_2008": {
        "name": "Global Financial Crisis",
        "start": "2008-09-01",
        "end": "2009-03-31",
        "description": "Lehman collapse and credit crisis",
    },
    "covid_2020": {
        "name": "COVID-19 Crash",
        "start": "2020-02-19",
        "end": "2020-03-23",
        "description": "Pandemic market crash",
    },
    "dotcom_2000": {
        "name": "Dot-com Bust",
        "start": "2000-03-10",
        "end": "2002-10-09",
        "description": "Technology bubble burst",
    },
    "flash_crash_2010": {
        "name": "Flash Crash",
        "start": "2010-05-06",
        "end": "2010-05-07",
        "description": "May 6 2010 flash crash",
    },
    "vix_2018": {
        "name": "Volmageddon",
        "start": "2018-02-02",
        "end": "2018-02-09",
        "description": "VIX short squeeze",
    },
}


def run_stress_test(
    returns: np.ndarray,
    scenario_returns: np.ndarray,
    weights: np.ndarray | None = None,
) -> dict:
    """Compute portfolio impact under a stress scenario."""
    if weights is None:
        weights = np.ones(returns.shape[1]) / returns.shape[1]

    port_returns = (scenario_returns * weights).sum(axis=1)
    cumulative = np.cumprod(1 + port_returns) - 1

    return {
        "total_return": float(cumulative[-1]) if len(cumulative) > 0 else 0.0,
        "max_drawdown": float(np.min(cumulative)) if len(cumulative) > 0 else 0.0,
        "worst_day": float(np.min(port_returns)) if len(port_returns) > 0 else 0.0,
        "best_day": float(np.max(port_returns)) if len(port_returns) > 0 else 0.0,
        "volatility": float(np.std(port_returns) * np.sqrt(252)) if len(port_returns) > 0 else 0.0,
        "n_days": len(port_returns),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run stress tests")
    parser.add_argument("--symbols", required=True, help="Comma-separated symbols")
    parser.add_argument("--scenario", help="Specific scenario key (or 'all')")
    parser.add_argument("--weights", help="Comma-separated portfolio weights")
    args = parser.parse_args()

    symbols = args.symbols.split(",")
    scenario_keys = list(SCENARIOS.keys()) if args.scenario == "all" or not args.scenario else [args.scenario]
    weights = np.array([float(w) for w in args.weights.split(",")]) if args.weights else None

    # Validate scenarios
    for key in scenario_keys:
        if key not in SCENARIOS:
            print(json.dumps({"error": f"Unknown scenario: {key}. Available: {list(SCENARIOS.keys())}"}))
            sys.exit(1)

    import psycopg2
    conn = psycopg2.connect(
        host="localhost", port=8812, user="admin", password="quest", dbname="qdb"
    )

    results: list[dict] = []
    try:
        for key in scenario_keys:
            scenario = SCENARIOS[key]
            scenario_data: list[np.ndarray] = []

            for symbol in symbols:
                sql = """
                    SELECT close FROM canonical_equity_ohlcv
                    WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
                    ORDER BY timestamp
                """
                with conn.cursor() as cur:
                    cur.execute(sql, (symbol, scenario["start"], scenario["end"]))
                    rows = cur.fetchall()

                if len(rows) < 2:
                    scenario_data.append(np.array([0.0]))
                    continue

                prices = np.array([r[0] for r in rows], dtype=float)
                rets = np.diff(prices) / prices[:-1]
                scenario_data.append(rets)

            # Align lengths
            min_len = min(len(r) for r in scenario_data)
            if min_len < 1:
                results.append({
                    "scenario": key,
                    **scenario,
                    "error": "Insufficient data for this scenario",
                })
                continue

            aligned = np.column_stack([r[:min_len] for r in scenario_data])
            impact = run_stress_test(aligned, aligned, weights)

            results.append({
                "scenario": key,
                **scenario,
                **impact,
            })
    finally:
        conn.close()

    print(json.dumps({"symbols": symbols, "results": results}))


if __name__ == "__main__":
    main()
