"""CLI entry points for evaluation operations.

Called from TypeScript MCP tools via PythonRunner.runModule.

Usage:
    python -m research.eval.cli metrics <experiment_id>
    python -m research.eval.cli regime-slices <experiment_id> [--labeling v1|v2]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from research.experiments import registry


def cmd_metrics(args: argparse.Namespace) -> None:
    """Fetch evaluation metrics for an experiment."""
    exp = registry.get(args.experiment_id)
    metrics_path = Path(exp["path"]) / "evaluation" / "metrics.json"

    if not metrics_path.exists():
        print(json.dumps({
            "error": f"No metrics found for experiment {args.experiment_id}",
            "experiment_id": args.experiment_id,
            "expected_path": str(metrics_path),
        }))
        return

    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    print(json.dumps({
        "experiment_id": args.experiment_id,
        "metrics": metrics,
        "metrics_path": str(metrics_path),
    }))


def cmd_regime_slices(args: argparse.Namespace) -> None:
    """Generate regime slice artifacts for an experiment."""
    exp = registry.get(args.experiment_id)
    metrics_path = Path(exp["path"]) / "evaluation" / "metrics.json"

    if not metrics_path.exists():
        print(json.dumps({
            "error": f"No metrics found for experiment {args.experiment_id}",
            "experiment_id": args.experiment_id,
        }))
        return

    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))

    # Extract regime performance from metrics
    regime_perf = metrics.get("performance_by_regime", {})
    regime_meta = metrics.get("regime", {})

    output = {
        "experiment_id": args.experiment_id,
        "labeling": regime_meta.get("labeling_version", args.labeling),
        "regime_config": regime_meta,
        "slices": regime_perf,
    }

    # Compute summary stats across regimes
    if regime_perf:
        sharpe_vals = [v.get("sharpe", 0) for v in regime_perf.values() if isinstance(v, dict)]
        output["summary"] = {
            "n_regimes": len(regime_perf),
            "best_regime": max(regime_perf, key=lambda k: regime_perf[k].get("sharpe", float("-inf"))) if regime_perf else None,
            "worst_regime": min(regime_perf, key=lambda k: regime_perf[k].get("sharpe", float("inf"))) if regime_perf else None,
            "sharpe_spread": round(max(sharpe_vals) - min(sharpe_vals), 6) if sharpe_vals else 0,
        }

    print(json.dumps(output))


def main() -> None:
    parser = argparse.ArgumentParser(prog="research.eval.cli")
    sub = parser.add_subparsers(dest="command", required=True)

    p_metrics = sub.add_parser("metrics")
    p_metrics.add_argument("experiment_id", help="Experiment ID")

    p_regime = sub.add_parser("regime-slices")
    p_regime.add_argument("experiment_id", help="Experiment ID")
    p_regime.add_argument("--labeling", default="v2", help="Labeling version (v1 or v2)")

    args = parser.parse_args()

    if args.command == "metrics":
        cmd_metrics(args)
    elif args.command == "regime-slices":
        cmd_regime_slices(args)


if __name__ == "__main__":
    main()
