"""Dagster experiment_sweep job — run multiple experiments from a sweep config.

Input: sweep config (base spec + override grid from configs/sweeps/)
Steps: for each override combination → create experiment (via registry) → run experiment_run
       → aggregate DSR across configs
Output: multiple experiment artifacts + index rows + sweep DSR artifact

PRD §15.3 (lines 1293-1297).
"""

import itertools
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

import yaml
from dagster import Config, DynamicOut, DynamicOutput, In, Nothing, OpExecutionContext, Out, job, op

logger = logging.getLogger(__name__)

_CONFIGS_DIR = Path(__file__).resolve().parents[3] / "configs"


class ExperimentSweepConfig(Config):
    """Run config for experiment_sweep job."""

    sweep_config_path: str = ""
    sweep_config_json: str = ""
    data_root: str = ".yats_data"
    managing_partner_ack: bool = False


# ---------------------------------------------------------------------------
# Ops
# ---------------------------------------------------------------------------


@op(out=Out(Nothing))
def check_experiment_explosion_guard(
    context: OpExecutionContext, config: ExperimentSweepConfig
) -> None:
    """Block sweep creation if experiment_index exceeds the explosion threshold.

    PRD §23.5: >1000 experiments in last 30 days blocks new sweeps.
    Requires managing_partner_ack=True to proceed when guard is tripped.
    """
    from research.governance.explosion_guard import ExplosionGuardError, check_explosion_guard

    try:
        check_explosion_guard(managing_partner_ack=config.managing_partner_ack)
        context.log.info("Explosion guard passed — sweep creation allowed")
    except ExplosionGuardError as exc:
        raise Exception(str(exc)) from exc


@op(ins={"guard": In(Nothing)}, out=Out(list))
def load_sweep_config(context: OpExecutionContext, config: ExperimentSweepConfig) -> list[dict]:
    """Load sweep config and expand the override grid into experiment specs.

    Returns a list of materialized spec dicts (one per override combination).
    """
    if config.sweep_config_json:
        sweep = json.loads(config.sweep_config_json)
    elif config.sweep_config_path:
        path = Path(config.sweep_config_path)
        if not path.is_absolute():
            path = _CONFIGS_DIR / "sweeps" / path
        with open(path) as f:
            if path.suffix in (".yml", ".yaml"):
                sweep = yaml.safe_load(f)
            else:
                sweep = json.load(f)
    else:
        raise ValueError("Either sweep_config_path or sweep_config_json must be provided")

    base_spec = sweep.get("base_spec", {})
    grid = sweep.get("grid", {})

    # Expand grid into all combinations
    specs = _expand_grid(base_spec, grid)
    context.log.info("Sweep expanded to %d experiment configurations", len(specs))
    return specs


@op(ins={"specs": In(list)}, out=Out(list))
def create_sweep_experiments(
    context: OpExecutionContext,
    config: ExperimentSweepConfig,
    specs: list[dict],
) -> list[str]:
    """Create experiment entries for each spec in the sweep.

    Returns list of experiment_ids.
    """
    from research.experiments.registry import create
    from research.experiments.spec import ExperimentSpec

    from yats_pipelines.jobs.experiment_run import _reconstruct_spec

    data_root = Path(config.data_root)
    experiment_ids: list[str] = []

    for i, spec_data in enumerate(specs):
        spec = _reconstruct_spec(spec_data)
        exp_id = create(spec, data_root=data_root)
        experiment_ids.append(exp_id)
        context.log.info("Created experiment %d/%d: %s", i + 1, len(specs), exp_id)

    return experiment_ids


@op(ins={"experiment_ids": In(list)}, out=Out(list))
def run_sweep_experiments(
    context: OpExecutionContext,
    config: ExperimentSweepConfig,
    experiment_ids: list[str],
) -> list[dict]:
    """Run each experiment in the sweep sequentially.

    Returns a list of result dicts with experiment_id and status.
    """
    from yats_pipelines.jobs.experiment_run import (
        _build_returns_df,
        _reconstruct_spec,
    )
    from research.experiments.registry import get

    data_root = Path(config.data_root)
    results: list[dict] = []

    for i, exp_id in enumerate(experiment_ids):
        context.log.info("Running experiment %d/%d: %s", i + 1, len(experiment_ids), exp_id)

        try:
            exp = get(exp_id, data_root=data_root)
            spec_data = exp["spec"]
            spec = _reconstruct_spec(spec_data)

            # For sweeps, we skip QuestDB fetch and training for now
            # (in production, this would call the full experiment_run pipeline)
            results.append({
                "experiment_id": exp_id,
                "status": "created",
                "spec_name": spec_data.get("experiment_name", ""),
            })

        except Exception as e:
            context.log.error("Failed experiment %s: %s", exp_id, e)
            results.append({
                "experiment_id": exp_id,
                "status": "failed",
                "error": str(e),
            })

    context.log.info("Sweep complete: %d/%d succeeded", sum(1 for r in results if r["status"] != "failed"), len(results))
    return results


def _compute_sweep_dsr_for_results(
    results: list[dict],
    data_root: Path,
) -> list[dict]:
    """Pure-function core: attach DSR to sweep results that have oos_sharpe.

    Returns a new list; inputs are not mutated.
    """
    from compute.stats.deflated_sharpe import compute_sweep_dsr

    completed: list[tuple[int, dict]] = [
        (i, r) for i, r in enumerate(results)
        if r.get("oos_sharpe") is not None
    ]

    augmented = list(results)

    if len(completed) < 2:
        for idx, _ in enumerate(augmented):
            if augmented[idx].get("dsr") is None:
                augmented[idx] = {**augmented[idx], "dsr": None}
        return augmented

    configs = [
        {
            "sharpe": float(r["oos_sharpe"]),
            "skewness": float(r.get("oos_skewness", 0.0)),
            "kurtosis": float(r.get("oos_kurtosis", 3.0)),
            "n_obs": int(r.get("oos_n_obs", 252)),
        }
        for _, r in completed
    ]

    dsr_results = compute_sweep_dsr(configs)

    for (idx, _orig), dsr_info in zip(completed, dsr_results):
        augmented[idx] = {**augmented[idx], **{
            "dsr": dsr_info["dsr"],
            "dsr_p_value": dsr_info["p_value"],
            "dsr_is_significant": dsr_info["is_significant"],
            "sweep_benchmark_sr": dsr_info["benchmark_sharpe"],
        }}

    # Persist sweep DSR summary artifact
    sweep_dir = data_root / "sweeps"
    sweep_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "n_configs": len(results),
        "n_with_oos_sharpe": len(completed),
        "sweep_benchmark_sr": dsr_results[0]["benchmark_sharpe"],
        "configs": [
            {
                "experiment_id": augmented[idx].get("experiment_id"),
                "oos_sharpe": r["oos_sharpe"],
                "dsr": augmented[idx].get("dsr"),
                "dsr_is_significant": augmented[idx].get("dsr_is_significant"),
            }
            for idx, r in completed
        ],
    }
    sweep_key = augmented[completed[0][0]].get("experiment_id", "unknown")
    summary_path = sweep_dir / f"{sweep_key}_dsr.json"
    summary_path.write_text(json.dumps(summary, indent=2))

    return augmented


@op(ins={"results": In(list)}, out=Out(list))
def aggregate_sweep_dsr(
    context: OpExecutionContext,
    config: ExperimentSweepConfig,
    results: list[dict],
) -> list[dict]:
    """Compute Deflated Sharpe Ratio at sweep level from per-config OOS results.

    Collects OOS Sharpe ratios from completed experiments, computes the
    sweep-level DSR benchmark SR0, and attaches per-config DSR to each result.

    Results that lack 'oos_sharpe' are passed through unchanged (DSR is skipped
    until the experiment pipeline populates OOS metrics).

    PRD Stage 1b (ya-cnac1): DSR at sweep-aggregation level.
    """
    data_root = Path(config.data_root)
    augmented = _compute_sweep_dsr_for_results(results, data_root)

    completed_count = sum(1 for r in augmented if r.get("dsr") is not None)
    if completed_count < 2:
        context.log.info(
            "Sweep DSR skipped: only %d/%d configs have OOS Sharpe data (need ≥2)",
            completed_count, len(results),
        )
    else:
        sr0 = next((r.get("sweep_benchmark_sr") for r in augmented if r.get("sweep_benchmark_sr") is not None), 0.0)
        n_significant = sum(1 for r in augmented if r.get("dsr_is_significant"))
        context.log.info(
            "Sweep DSR: N=%d configs with OOS data, SR0=%.4f, %d significant",
            completed_count, sr0, n_significant,
        )
        context.log.info("Sweep DSR summary written to %s/sweeps/", config.data_root)

    return augmented


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------


@job(tags={"yats/concurrency_pool": "sweep", "dagster/priority": "30"})
def experiment_sweep():
    """Run a parameter sweep: guard → load config → create experiments → run each → aggregate DSR."""
    guard = check_experiment_explosion_guard()
    specs = load_sweep_config(guard)
    experiment_ids = create_sweep_experiments(specs)
    sweep_results = run_sweep_experiments(experiment_ids)
    aggregate_sweep_dsr(sweep_results)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _expand_grid(base_spec: dict, grid: dict) -> list[dict]:
    """Expand a base spec + grid of overrides into all combinations.

    Grid format:
        {
            "policy_params.learning_rate": [1e-4, 3e-4, 1e-3],
            "policy_params.clip_range": [0.1, 0.2, 0.3],
            "seed": [42, 123, 456],
        }

    Each key is a dot-separated path into the spec dict. Values are lists of
    options. All combinations are enumerated (Cartesian product).

    Returns a list of fully-materialized spec dicts.
    """
    if not grid:
        return [dict(base_spec)]

    # Sort keys for deterministic ordering
    keys = sorted(grid.keys())
    value_lists = [grid[k] for k in keys]

    specs = []
    for combo in itertools.product(*value_lists):
        spec = _deep_copy_dict(base_spec)
        for key, val in zip(keys, combo):
            _set_nested(spec, key, val)

        # Ensure unique experiment names
        combo_suffix = "_".join(f"{k.split('.')[-1]}={v}" for k, v in zip(keys, combo))
        base_name = spec.get("experiment_name", "sweep")
        spec["experiment_name"] = f"{base_name}_{combo_suffix}"

        specs.append(spec)

    return specs


def _set_nested(d: dict, dotted_key: str, value: Any) -> None:
    """Set a value in a nested dict using dot-separated key path."""
    parts = dotted_key.split(".")
    current = d
    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], dict):
            current[part] = {}
        current = current[part]
    current[parts[-1]] = value


def _deep_copy_dict(d: dict) -> dict:
    """Deep copy a dict (JSON-safe values only)."""
    import copy
    return copy.deepcopy(d)
