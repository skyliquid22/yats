"""Main qualification runner.

Implements the full qualification pipeline following PRD Appendix D.1
gate evaluation order. Produces a QualificationReport.

PRD §10 (lines 784-836).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from research.promotion.criteria import (
    GateResult,
    evaluate_artifact_gates,
    evaluate_constraint_gates,
    evaluate_regression_gates,
)
from research.promotion.execution_criteria import (
    evaluate_execution_hard_gates,
    evaluate_execution_soft_gates,
)
from research.promotion.regime_criteria import (
    evaluate_regime_hard_gates,
    evaluate_regime_soft_gates,
)
from research.promotion.report import (
    QualificationReport,
    build_report,
    write_report,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Metric and execution data loading
# ---------------------------------------------------------------------------


def load_experiment_metrics(
    experiment_id: str,
    *,
    data_root: Path | None = None,
) -> dict[str, Any]:
    """Load evaluation/metrics.json for an experiment."""
    root = data_root or Path(".yats_data")
    metrics_path = root / "experiments" / experiment_id / "evaluation" / "metrics.json"
    if not metrics_path.exists():
        raise FileNotFoundError(
            f"Metrics not found for {experiment_id}: {metrics_path}"
        )
    return json.loads(metrics_path.read_text(encoding="utf-8"))


def load_execution_metrics(
    experiment_id: str,
    *,
    questdb_resource: Any | None = None,
) -> dict[str, Any] | None:
    """Load latest execution_metrics row from QuestDB for an experiment.

    Returns None if no execution metrics exist.
    """
    import psycopg2

    if questdb_resource is None:
        from pipelines.yats_pipelines.resources.questdb import QuestDBResource
        questdb_resource = QuestDBResource()

    conn = psycopg2.connect(
        host=questdb_resource.pg_host,
        port=questdb_resource.pg_port,
        user=questdb_resource.pg_user,
        password=questdb_resource.pg_password,
        database=questdb_resource.pg_database,
    )
    conn.autocommit = True

    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM execution_metrics "
            "WHERE experiment_id = %s "
            "ORDER BY timestamp DESC "
            "LIMIT 1",
            (experiment_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        col_names = [desc[0] for desc in cur.description]
        cur.close()
        return dict(zip(col_names, row))
    except Exception:
        logger.warning(
            "Could not query execution_metrics for %s", experiment_id,
            exc_info=True,
        )
        return None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Qualification-replay bypass (PRD §9.4, lines 750-768)
# ---------------------------------------------------------------------------


def trigger_qualification_replay(
    experiment_id: str,
    *,
    data_root: Path | None = None,
    dagster_run_id: str | None = None,
) -> dict[str, Any] | None:
    """Trigger shadow replay with qualification_replay=true bypass.

    This is the mechanism described in PRD §9.4: when qualification finds
    no execution evidence, it triggers shadow_run internally with the
    qualification_replay flag, bypassing the promotion gate.

    Returns execution_metrics dict after replay, or None on failure.
    """
    logger.info(
        "Triggering qualification replay for %s (bypass promotion gate)",
        experiment_id,
    )

    try:
        from research.experiments.registry import get
        from research.shadow.data_source import ReplayMarketDataSource
        from research.shadow.engine import (
            ShadowEngine,
            ShadowRunConfig as EngineRunConfig,
            load_policy,
        )
        from research.shadow.questdb_writer import (
            ExecutionTableWriter,
            QuestDBWriterConfig,
        )

        root = data_root or Path(".yats_data")
        exp = get(experiment_id, data_root=root)
        spec_data = exp["spec"]

        # Reconstruct spec
        from pipelines.yats_pipelines.jobs.shadow_run import _reconstruct_spec
        spec = _reconstruct_spec(spec_data)

        # Build data source
        from datetime import date
        symbols = sorted(spec_data.get("symbols", []))
        start_date = spec_data.get("start_date", "")
        end_date = spec_data.get("end_date", "")
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

        if not snapshots:
            logger.warning("No snapshots for qualification replay of %s", experiment_id)
            return None

        import uuid
        run_id = f"qr_{uuid.uuid4().hex[:10]}"
        output_dir = root / "shadow" / experiment_id / run_id
        output_dir.mkdir(parents=True, exist_ok=True)

        engine_config = EngineRunConfig(
            experiment_id=experiment_id,
            run_id=run_id,
            output_dir=output_dir,
            initial_value=1_000_000.0,
            qualification_replay=True,
        )

        policy = load_policy(spec)

        questdb_writer = ExecutionTableWriter(
            config=QuestDBWriterConfig(),
            experiment_id=experiment_id,
            run_id=run_id,
            mode="shadow",
            dagster_run_id=dagster_run_id,
        )

        engine = ShadowEngine(
            spec, policy, snapshots, engine_config,
            questdb_writer=questdb_writer,
        )
        engine.run(dagster_run_id=dagster_run_id)

        logger.info("Qualification replay complete for %s (run_id=%s)", experiment_id, run_id)

        # Reload execution metrics from QuestDB
        return load_execution_metrics(experiment_id)

    except Exception:
        logger.error(
            "Qualification replay failed for %s", experiment_id,
            exc_info=True,
        )
        return None


# ---------------------------------------------------------------------------
# Main qualification runner
# ---------------------------------------------------------------------------


def run_qualification(
    candidate_id: str,
    baseline_id: str,
    *,
    data_root: Path | None = None,
    questdb_resource: Any | None = None,
    dagster_run_id: str = "",
    config: dict[str, Any] | None = None,
) -> QualificationReport:
    """Run the full qualification pipeline.

    Follows PRD Appendix D.1 gate evaluation order:
    1. Regression gates (candidate vs baseline metrics)
    2. Constraint violations (NaN/Inf, max drawdown, turnover cap)
    3. Regime hard gates (high-vol drawdown/exposure)
    4. Regime soft gates (Sharpe degradation, stability)
    5. Artifact presence check
    6. Sweep robustness (optional, skipped for now)
    7. Execution evidence (locate or trigger shadow replay)
    8. Execution hard gates (fill rate, reject rate, slippage, halts)
    9. Execution soft gates (deltas vs baseline)
    10. Regime-execution diagnostics (optional, skipped for now)

    Args:
        candidate_id: Experiment ID for the candidate.
        baseline_id: Experiment ID for the baseline.
        data_root: Path to .yats_data root.
        questdb_resource: QuestDB resource for reading execution metrics.
        dagster_run_id: Dagster run ID for traceability.
        config: Optional config overrides (max_drawdown, max_turnover, etc.).

    Returns:
        QualificationReport with all gate results.
    """
    root = data_root or Path(".yats_data")
    all_gates: list[GateResult] = []
    warnings: list[str] = []

    skip_delta_checks = candidate_id == baseline_id
    if skip_delta_checks:
        warnings.append("baseline_is_candidate")
        logger.info("Baseline == candidate (%s): delta checks will be skipped", candidate_id)

    # Load metrics
    logger.info("Loading metrics for candidate=%s baseline=%s", candidate_id, baseline_id)
    candidate_metrics = load_experiment_metrics(candidate_id, data_root=root)
    baseline_metrics = load_experiment_metrics(baseline_id, data_root=root)

    # Determine hierarchy status from candidate spec
    candidate_spec_path = root / "experiments" / candidate_id / "spec" / "experiment_spec.json"
    hierarchy_enabled = False
    if candidate_spec_path.exists():
        spec_data = json.loads(candidate_spec_path.read_text(encoding="utf-8"))
        hierarchy_enabled = spec_data.get("hierarchy_enabled", False)

    # Get experiment path for artifact checks
    experiment_path = str(root / "experiments" / candidate_id)

    # --- Step 1: Regression gates ---
    logger.info("Step 1: Regression gates")
    all_gates.extend(evaluate_regression_gates(
        candidate_metrics, baseline_metrics,
        skip_delta_checks=skip_delta_checks,
    ))

    # --- Step 2: Constraint violations ---
    logger.info("Step 2: Constraint violations")
    all_gates.extend(evaluate_constraint_gates(
        candidate_metrics, config=config,
    ))

    # --- Step 3: Regime hard gates ---
    logger.info("Step 3: Regime hard gates")
    candidate_execution = load_execution_metrics(
        candidate_id, questdb_resource=questdb_resource,
    )
    baseline_execution = load_execution_metrics(
        baseline_id, questdb_resource=questdb_resource,
    )
    all_gates.extend(evaluate_regime_hard_gates(
        candidate_metrics, baseline_metrics,
        candidate_execution=candidate_execution,
        baseline_execution=baseline_execution,
        skip_delta_checks=skip_delta_checks,
    ))

    # --- Step 4: Regime soft gates ---
    logger.info("Step 4: Regime soft gates")
    all_gates.extend(evaluate_regime_soft_gates(
        candidate_metrics, baseline_metrics,
        hierarchy_enabled=hierarchy_enabled,
        skip_delta_checks=skip_delta_checks,
    ))

    # --- Step 5: Artifact presence ---
    logger.info("Step 5: Artifact presence")
    all_gates.extend(evaluate_artifact_gates(experiment_path))

    # --- Step 6: Sweep robustness (optional, skip) ---
    logger.info("Step 6: Sweep robustness (skipped)")

    # --- Step 7: Execution evidence ---
    logger.info("Step 7: Execution evidence")
    if candidate_execution is None:
        logger.info("No execution evidence for %s, triggering qualification replay", candidate_id)
        candidate_execution = trigger_qualification_replay(
            candidate_id, data_root=root, dagster_run_id=dagster_run_id,
        )
        if candidate_execution is not None:
            warnings.append("qualification_replay_triggered")
        else:
            all_gates.append(GateResult(
                name="execution_evidence", passed=False,
                value=None, baseline=None, threshold=None,
                gate_type="hard",
                detail="No execution evidence and replay failed",
            ))

    if candidate_execution is not None:
        all_gates.append(GateResult(
            name="execution_evidence", passed=True,
            value=None, baseline=None, threshold=None,
            gate_type="hard",
        ))

    # Also reload baseline execution if needed
    if baseline_execution is None and not skip_delta_checks:
        baseline_execution = load_execution_metrics(
            baseline_id, questdb_resource=questdb_resource,
        )

    # --- Step 8: Execution hard gates ---
    logger.info("Step 8: Execution hard gates")
    all_gates.extend(evaluate_execution_hard_gates(candidate_execution))

    # --- Step 9: Execution soft gates ---
    logger.info("Step 9: Execution soft gates")
    all_gates.extend(evaluate_execution_soft_gates(
        candidate_execution, baseline_execution,
        skip_delta_checks=skip_delta_checks,
    ))

    # --- Step 10: Regime-execution diagnostics (optional, skip) ---
    logger.info("Step 10: Regime-execution diagnostics (skipped)")

    # Build report
    report = build_report(
        experiment_id=candidate_id,
        baseline_id=baseline_id,
        all_gates=all_gates,
        warnings=warnings,
        dagster_run_id=dagster_run_id,
    )

    # Write report to filesystem
    report_path = root / "experiments" / candidate_id / "promotion" / "qualification_report.json"
    write_report(report, report_path)
    logger.info(
        "Qualification %s for %s: %d hard gates, %d soft warnings",
        "PASSED" if report.passed else "FAILED",
        candidate_id,
        sum(1 for g in all_gates if g.gate_type == "hard" and not g.passed),
        sum(1 for g in all_gates if g.gate_type == "soft" and not g.passed),
    )

    return report
