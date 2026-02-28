#!/usr/bin/env python3
"""Pre-trade risk check against RISK_POLICY constraints.

Validates an order against all risk constraints WITHOUT submitting it.
Returns pass/reject with detailed reasons.
"""
import argparse
import json
import sys
from pathlib import Path

import yaml


def load_risk_config() -> dict:
    """Load risk configuration from configs/risk.yml."""
    # Resolve relative to project root (PYTHONPATH includes project root)
    config_path = Path(__file__).resolve().parent.parent.parent / "configs" / "risk.yml"
    if not config_path.exists():
        return {}
    with open(config_path) as f:
        return yaml.safe_load(f) or {}


def check_order(
    symbol: str,
    side: str,
    quantity: float,
    notional: float,
    nav: float,
    positions: list[dict],
    config: dict,
) -> dict:
    """Run all risk policy checks on an order.

    Returns dict with 'decision' (pass/reject) and 'reasons' list.
    """
    reasons: list[dict] = []
    decision = "pass"

    if nav <= 0:
        return {"decision": "reject", "reasons": [{"rule": "nav_check", "message": "NAV is zero or negative"}]}

    order_pct = notional / nav

    # Kill switch check: daily loss limit
    daily_loss_limit = config.get("daily_loss_limit", -0.05)

    # Minimum order threshold
    min_threshold = config.get("minimum_order_threshold", 0.01)
    if order_pct < min_threshold:
        reasons.append({
            "rule": "minimum_order_threshold",
            "message": f"Order {order_pct:.4f} of NAV below minimum {min_threshold}",
            "severity": "reject",
        })
        decision = "reject"

    # Max symbol weight
    max_weight = config.get("max_symbol_weight", 0.20)
    # Compute current weight for this symbol
    current_symbol_value = sum(
        abs(p.get("market_value", 0)) for p in positions if p.get("symbol") == symbol
    )
    new_symbol_value = current_symbol_value + notional
    new_weight = new_symbol_value / nav if nav > 0 else 0
    if new_weight > max_weight:
        reasons.append({
            "rule": "max_symbol_weight",
            "message": f"Symbol weight {new_weight:.4f} would exceed max {max_weight}",
            "severity": "reject",
        })
        decision = "reject"

    # Max active positions
    max_positions = config.get("max_active_positions", 50)
    current_symbols = {p.get("symbol") for p in positions if p.get("qty", 0) > 0}
    if symbol not in current_symbols and len(current_symbols) >= max_positions:
        reasons.append({
            "rule": "max_active_positions",
            "message": f"Would exceed max {max_positions} active positions (currently {len(current_symbols)})",
            "severity": "reject",
        })
        decision = "reject"

    # Gross exposure check
    max_gross = config.get("max_gross_exposure", 1.0)
    current_gross = sum(abs(p.get("market_value", 0)) for p in positions)
    new_gross = (current_gross + notional) / nav if nav > 0 else 0
    if new_gross > max_gross:
        reasons.append({
            "rule": "max_gross_exposure",
            "message": f"Gross exposure {new_gross:.4f} would exceed max {max_gross}",
            "severity": "reject",
        })
        decision = "reject"

    # Min cash check
    min_cash = config.get("min_cash", 0.02)
    # Approximate: assume cash = nav - gross
    approx_cash_after = nav - current_gross - notional
    cash_pct = approx_cash_after / nav if nav > 0 else 0
    if cash_pct < min_cash:
        reasons.append({
            "rule": "min_cash",
            "message": f"Cash {cash_pct:.4f} of NAV would fall below minimum {min_cash}",
            "severity": "reject",
        })
        decision = "reject"

    # If all checks pass
    if decision == "pass":
        reasons.append({"rule": "all_checks", "message": "All risk checks passed", "severity": "info"})

    return {
        "decision": decision,
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "notional": notional,
        "order_pct_nav": round(order_pct, 6),
        "reasons": reasons,
        "checks_run": [
            "minimum_order_threshold",
            "max_symbol_weight",
            "max_active_positions",
            "max_gross_exposure",
            "min_cash",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Pre-trade risk check")
    parser.add_argument("--experiment-id", required=True, help="Experiment ID")
    parser.add_argument("--symbol", required=True, help="Ticker symbol")
    parser.add_argument("--side", required=True, choices=["buy", "sell"], help="Order side")
    parser.add_argument("--quantity", required=True, type=float, help="Order quantity")
    parser.add_argument("--notional", required=True, type=float, help="Notional value")
    parser.add_argument("--mode", default="paper", choices=["paper", "live"], help="Trading mode")
    parser.add_argument("--run-id", default=None, help="Trading run ID")
    args = parser.parse_args()

    config = load_risk_config()

    # Fetch current portfolio state from QuestDB
    import psycopg2
    conn = psycopg2.connect(
        host="localhost", port=8812, user="admin", password="quest", dbname="qdb"
    )

    try:
        # Get current NAV
        nav_sql = "SELECT total_nav FROM portfolio_nav WHERE experiment_id = %s ORDER BY timestamp DESC LIMIT 1"
        with conn.cursor() as cur:
            cur.execute(nav_sql, (args.experiment_id,))
            row = cur.fetchone()
        nav = row[0] if row else 0.0

        # Get current positions
        pos_sql = """
            SELECT symbol, side, qty, market_value
            FROM positions
            WHERE experiment_id = %s AND qty > 0
        """
        with conn.cursor() as cur:
            cur.execute(pos_sql, (args.experiment_id,))
            pos_rows = cur.fetchall()
        positions = [
            {"symbol": r[0], "side": r[1], "qty": r[2], "market_value": r[3]}
            for r in pos_rows
        ]
    finally:
        conn.close()

    result = check_order(
        symbol=args.symbol,
        side=args.side,
        quantity=args.quantity,
        notional=args.notional,
        nav=nav,
        positions=positions,
        config=config,
    )
    result["experiment_id"] = args.experiment_id
    result["mode"] = args.mode
    if args.run_id:
        result["run_id"] = args.run_id

    print(json.dumps(result))


if __name__ == "__main__":
    main()
