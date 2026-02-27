"""CLI entry points for experiment registry operations.

Called from TypeScript MCP tools via PythonRunner.runModule.

Usage:
    python -m research.experiments.cli create --spec-json '{...}'
    python -m research.experiments.cli get <experiment_id>
    python -m research.experiments.cli compare <id_a> <id_b>
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from research.experiments import registry, spec as spec_mod


def _build_spec(spec_dict: dict[str, Any]) -> spec_mod.ExperimentSpec:
    """Construct an ExperimentSpec from a flat dict (JSON-parsed)."""
    # Resolve inheritance if "extends" is present
    spec_dict = spec_mod.resolve_inheritance(spec_dict)

    # Parse sub-configs
    cost_raw = spec_dict.get("cost_config", {})
    cost = spec_mod.CostConfig(**cost_raw) if isinstance(cost_raw, dict) else cost_raw

    eval_split = None
    if spec_dict.get("evaluation_split"):
        eval_split = spec_mod.EvaluationSplitConfig(**spec_dict["evaluation_split"])

    risk = spec_mod.RiskConfig()
    if spec_dict.get("risk_config"):
        risk = spec_mod.RiskConfig(**spec_dict["risk_config"])

    exec_sim = None
    if spec_dict.get("execution_sim"):
        exec_sim = spec_mod.ExecutionSimConfig(**spec_dict["execution_sim"])

    from datetime import date as date_type

    start = spec_dict["start_date"]
    end = spec_dict["end_date"]
    if isinstance(start, str):
        start = date_type.fromisoformat(start)
    if isinstance(end, str):
        end = date_type.fromisoformat(end)

    symbols = spec_dict["symbols"]
    if isinstance(symbols, list):
        symbols = tuple(symbols)

    return spec_mod.ExperimentSpec(
        experiment_name=spec_dict["experiment_name"],
        symbols=symbols,
        start_date=start,
        end_date=end,
        interval=spec_dict.get("interval", "daily"),
        feature_set=spec_dict["feature_set"],
        policy=spec_dict["policy"],
        policy_params=spec_dict.get("policy_params", {}),
        cost_config=cost,
        seed=spec_dict.get("seed", 42),
        evaluation_split=eval_split,
        risk_config=risk,
        execution_sim=exec_sim,
        notes=spec_dict.get("notes"),
        regime_feature_set=spec_dict.get("regime_feature_set"),
        regime_labeling=spec_dict.get("regime_labeling"),
        hierarchy_enabled=spec_dict.get("hierarchy_enabled", False),
        controller_config=spec_dict.get("controller_config"),
        allocator_by_mode=spec_dict.get("allocator_by_mode"),
    )


def cmd_create(args: argparse.Namespace) -> None:
    """Create an experiment from a JSON spec."""
    spec_dict = json.loads(args.spec_json)
    experiment_spec = _build_spec(spec_dict)
    experiment_id = registry.create(experiment_spec)
    print(json.dumps({"experiment_id": experiment_id}))


def cmd_get(args: argparse.Namespace) -> None:
    """Get experiment details."""
    result = registry.get(args.experiment_id)
    # Convert Path objects for JSON
    output = {
        "spec": result["spec"],
        "path": str(result["path"]),
        "artifacts": {k: str(v) for k, v in result["artifacts"].items()},
    }

    # Try to load metrics if available
    metrics_path = Path(result["path"]) / "evaluation" / "metrics.json"
    if metrics_path.exists():
        output["metrics"] = json.loads(metrics_path.read_text(encoding="utf-8"))

    # Check run status from runs/ dir
    runs_dir = Path(result["path"]) / "runs"
    if runs_dir.exists():
        run_files = list(runs_dir.glob("*.json"))
        output["run_count"] = len(run_files)

    print(json.dumps(output))


def cmd_compare(args: argparse.Namespace) -> None:
    """Compare two experiments side-by-side."""
    results = []
    for eid in [args.id_a, args.id_b]:
        exp = registry.get(eid)
        entry: dict[str, Any] = {
            "experiment_id": eid,
            "spec": exp["spec"],
        }
        metrics_path = Path(exp["path"]) / "evaluation" / "metrics.json"
        if metrics_path.exists():
            entry["metrics"] = json.loads(metrics_path.read_text(encoding="utf-8"))
        else:
            entry["metrics"] = None
        results.append(entry)

    # Build comparison report
    comparison: dict[str, Any] = {
        "experiments": [r["experiment_id"] for r in results],
        "specs": {r["experiment_id"]: r["spec"] for r in results},
    }

    # Diff specs
    spec_a, spec_b = results[0]["spec"], results[1]["spec"]
    diffs: dict[str, Any] = {}
    all_keys = set(spec_a.keys()) | set(spec_b.keys())
    for key in sorted(all_keys):
        va = spec_a.get(key)
        vb = spec_b.get(key)
        if va != vb:
            diffs[key] = {"a": va, "b": vb}
    comparison["spec_diffs"] = diffs

    # Compare metrics
    if results[0]["metrics"] and results[1]["metrics"]:
        ma = results[0]["metrics"]
        mb = results[1]["metrics"]
        perf_a = ma.get("performance", {})
        perf_b = mb.get("performance", {})
        metric_comparison: dict[str, Any] = {}
        for key in ("sharpe", "calmar", "sortino", "total_return", "annualized_return", "max_drawdown"):
            va = perf_a.get(key)
            vb = perf_b.get(key)
            if va is not None and vb is not None:
                metric_comparison[key] = {
                    "a": va, "b": vb,
                    "diff": round(vb - va, 6),
                }
        comparison["metric_comparison"] = metric_comparison
    else:
        comparison["metric_comparison"] = None
        comparison["note"] = "One or both experiments lack evaluation metrics"

    print(json.dumps(comparison))


def main() -> None:
    parser = argparse.ArgumentParser(prog="research.experiments.cli")
    sub = parser.add_subparsers(dest="command", required=True)

    p_create = sub.add_parser("create")
    p_create.add_argument("--spec-json", required=True, help="JSON experiment spec")

    p_get = sub.add_parser("get")
    p_get.add_argument("experiment_id", help="Experiment ID (SHA256 hex)")

    p_compare = sub.add_parser("compare")
    p_compare.add_argument("id_a", help="First experiment ID")
    p_compare.add_argument("id_b", help="Second experiment ID")

    args = parser.parse_args()

    if args.command == "create":
        cmd_create(args)
    elif args.command == "get":
        cmd_get(args)
    elif args.command == "compare":
        cmd_compare(args)


if __name__ == "__main__":
    main()
