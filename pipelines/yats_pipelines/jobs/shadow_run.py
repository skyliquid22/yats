"""Dagster shadow_run job — shadow execution replay.

Steps: load spec + policy -> build ReplayMarketDataSource -> run ShadowEngine
-> write artifacts under .yats_data/shadow/.

PRD §15.4 (lines 1299-1306).
"""

import logging
from pathlib import Path
from typing import Any

from dagster import Config, In, Nothing, OpExecutionContext, Out, job, op

logger = logging.getLogger(__name__)


class ShadowRunConfig(Config):
    """Run config for shadow_run job."""

    experiment_id: str
    execution_mode: str = "none"
    start_date: str = ""
    end_date: str = ""
    data_root: str = ".yats_data"
    initial_value: float = 1_000_000.0


# ---------------------------------------------------------------------------
# Ops
# ---------------------------------------------------------------------------


@op(out=Out(dict))
def load_shadow_spec(
    context: OpExecutionContext, config: ShadowRunConfig,
) -> dict:
    """Load experiment spec and verify it is promoted or allowlisted."""
    from research.experiments.registry import get

    data_root = Path(config.data_root)
    exp = get(config.experiment_id, data_root=data_root)
    spec_data = exp["spec"]
    context.log.info(
        "Loaded spec for shadow run %s (policy=%s)",
        config.experiment_id,
        spec_data.get("policy"),
    )
    return spec_data


@op(ins={"spec_data": In(dict)}, out=Out(dict))
def build_replay_source(
    context: OpExecutionContext, config: ShadowRunConfig, spec_data: dict,
) -> dict:
    """Build ReplayMarketDataSource and load snapshots."""
    from datetime import date

    from research.shadow.data_source import ReplayMarketDataSource

    # Override date range if provided
    start_date = spec_data.get("start_date", "")
    end_date = spec_data.get("end_date", "")
    if config.start_date:
        start_date = config.start_date
    if config.end_date:
        end_date = config.end_date

    symbols = sorted(spec_data.get("symbols", []))

    if isinstance(start_date, str) and start_date:
        start_date = date.fromisoformat(start_date)
    if isinstance(end_date, str) and end_date:
        end_date = date.fromisoformat(end_date)

    source = ReplayMarketDataSource(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        feature_set=spec_data.get("feature_set", "core_v1"),
        regime_feature_set=spec_data.get("regime_feature_set"),
    )
    snapshots = source.load_snapshots()
    context.log.info("Loaded %d snapshots for shadow replay", len(snapshots))

    # Serialize snapshots for passing between ops
    return {
        "snapshots": [s.to_dict() for s in snapshots],
        "snapshot_count": len(snapshots),
    }


@op(ins={"spec_data": In(dict), "replay_data": In(dict)}, out=Out(dict))
def run_shadow_engine(
    context: OpExecutionContext,
    config: ShadowRunConfig,
    spec_data: dict,
    replay_data: dict,
) -> dict:
    """Run ShadowEngine with direct rebalance (execution_mode=none)."""
    from datetime import date

    from research.experiments.spec import (
        CostConfig,
        ExperimentSpec,
        RiskConfig,
    )
    from research.shadow.data_source import Snapshot
    from research.shadow.engine import (
        ShadowEngine,
        ShadowRunConfig as EngineRunConfig,
        load_policy,
    )

    # Reconstruct spec
    spec = _reconstruct_spec(spec_data)

    # Reconstruct snapshots from serialized data
    raw_snapshots = replay_data["snapshots"]
    snapshots = [_reconstruct_snapshot(s) for s in raw_snapshots]

    if not snapshots:
        context.log.warning("No snapshots available — skipping shadow run")
        return {"summary": {}, "run_id": ""}

    # Set up run
    data_root = Path(config.data_root)
    import uuid
    run_id = uuid.uuid4().hex[:12]
    output_dir = data_root / "shadow" / config.experiment_id / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    engine_config = EngineRunConfig(
        experiment_id=config.experiment_id,
        run_id=run_id,
        output_dir=output_dir,
        initial_value=config.initial_value,
        execution_mode=config.execution_mode,
    )

    policy = load_policy(spec)

    # Set up QuestDB writer for unified execution tables (PRD §9.3)
    from research.shadow.questdb_writer import (
        ExecutionTableWriter,
        QuestDBWriterConfig,
    )
    dagster_run_id = context.run_id if hasattr(context, "run_id") else None
    questdb_writer = ExecutionTableWriter(
        config=QuestDBWriterConfig(),
        experiment_id=config.experiment_id,
        run_id=run_id,
        mode="shadow",
        dagster_run_id=dagster_run_id,
    )

    engine = ShadowEngine(
        spec, policy, snapshots, engine_config,
        questdb_writer=questdb_writer,
    )

    # Resume if state exists
    engine.resume()

    # Run
    summary = engine.run(dagster_run_id=dagster_run_id)

    context.log.info(
        "Shadow run complete: final_value=%.2f, return=%.4f",
        summary.get("final_value", 0),
        summary.get("total_return", 0),
    )
    return {"summary": summary, "run_id": run_id}


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------


@job
def shadow_run():
    """Shadow execution replay: load spec -> replay data -> run engine."""
    spec_data = load_shadow_spec()
    replay_data = build_replay_source(spec_data)
    run_shadow_engine(spec_data, replay_data)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reconstruct_spec(spec_data: dict) -> Any:
    """Reconstruct an ExperimentSpec from a spec dict.

    Reuses the same pattern as experiment_run.py.
    """
    from datetime import date

    from research.experiments.spec import (
        CostConfig,
        EvaluationSplitConfig,
        ExecutionSimConfig,
        ExperimentSpec,
        RiskConfig,
    )

    cost_raw = spec_data.get("cost_config", {})
    cost_config = CostConfig(**cost_raw) if isinstance(cost_raw, dict) else cost_raw

    risk_raw = spec_data.get("risk_config")
    if isinstance(risk_raw, dict):
        risk_config = RiskConfig(**risk_raw)
    elif risk_raw is None:
        risk_config = RiskConfig()
    else:
        risk_config = risk_raw

    eval_split_raw = spec_data.get("evaluation_split")
    eval_split = None
    if isinstance(eval_split_raw, dict):
        eval_split = EvaluationSplitConfig(**eval_split_raw)

    exec_sim_raw = spec_data.get("execution_sim")
    exec_sim = None
    if isinstance(exec_sim_raw, dict):
        exec_sim = ExecutionSimConfig(**exec_sim_raw)

    start_date = spec_data["start_date"]
    end_date = spec_data["end_date"]
    if isinstance(start_date, str):
        start_date = date.fromisoformat(start_date)
    if isinstance(end_date, str):
        end_date = date.fromisoformat(end_date)

    symbols = spec_data.get("symbols", [])
    if isinstance(symbols, list):
        symbols = tuple(symbols)

    return ExperimentSpec(
        experiment_name=spec_data["experiment_name"],
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        interval=spec_data.get("interval", "daily"),
        feature_set=spec_data["feature_set"],
        policy=spec_data["policy"],
        policy_params=spec_data.get("policy_params", {}),
        cost_config=cost_config,
        seed=spec_data.get("seed", 42),
        evaluation_split=eval_split,
        risk_config=risk_config,
        execution_sim=exec_sim,
        notes=spec_data.get("notes"),
        regime_feature_set=spec_data.get("regime_feature_set"),
        regime_labeling=spec_data.get("regime_labeling"),
        hierarchy_enabled=spec_data.get("hierarchy_enabled", False),
        controller_config=spec_data.get("controller_config"),
        allocator_by_mode=spec_data.get("allocator_by_mode"),
        regime_thresholds_hash=spec_data.get("regime_thresholds_hash", ""),
        regime_detector_version=spec_data.get("regime_detector_version", ""),
        regime_universe=tuple(spec_data.get("regime_universe", ())),
        feature_set_yaml_hash=spec_data.get("feature_set_yaml_hash", ""),
        risk_overrides=spec_data.get("risk_overrides"),
        _base_spec_hash=spec_data.get("_base_spec_hash"),
    )


def _reconstruct_snapshot(data: dict) -> Any:
    """Reconstruct a Snapshot from a serialized dict."""
    from datetime import datetime

    from research.shadow.data_source import Snapshot

    as_of = data["as_of"]
    if isinstance(as_of, str):
        as_of = datetime.fromisoformat(as_of)

    symbols = data["symbols"]
    if isinstance(symbols, list):
        symbols = tuple(symbols)

    return Snapshot(
        as_of=as_of,
        symbols=symbols,
        panel=data["panel"],
        regime_features=tuple(data.get("regime_features", ())),
        regime_feature_names=tuple(data.get("regime_feature_names", ())),
        observation_columns=tuple(data.get("observation_columns", ())),
    )
