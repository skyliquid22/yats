"""Dagster qualify job — deterministic qualification gating.

Compares candidate experiment against baseline, evaluates all gates
in PRD Appendix D order, produces qualification_report.json, and
updates experiment_index.

PRD §10 (lines 784-836).
"""

import logging
from pathlib import Path
from typing import Any

from dagster import Config, In, Nothing, OpExecutionContext, Out, job, op

logger = logging.getLogger(__name__)


class QualifyConfig(Config):
    """Run config for qualify job."""

    candidate_id: str
    baseline_id: str
    data_root: str = ".yats_data"


# ---------------------------------------------------------------------------
# Ops
# ---------------------------------------------------------------------------


@op(out=Out(dict))
def load_candidate_spec(
    context: OpExecutionContext, config: QualifyConfig,
) -> dict:
    """Load and validate candidate experiment spec."""
    from research.experiments.registry import get

    data_root = Path(config.data_root)
    exp = get(config.candidate_id, data_root=data_root)
    spec_data = exp["spec"]
    context.log.info(
        "Loaded candidate spec %s (policy=%s)",
        config.candidate_id,
        spec_data.get("policy"),
    )
    return spec_data


@op(ins={"spec_data": In(dict)}, out=Out(dict))
def load_baseline_and_metrics(
    context: OpExecutionContext, config: QualifyConfig, spec_data: dict,
) -> dict:
    """Load candidate and baseline metrics, verify both exist."""
    from research.promotion.qualify import load_experiment_metrics

    data_root = Path(config.data_root)
    candidate_metrics = load_experiment_metrics(
        config.candidate_id, data_root=data_root,
    )
    baseline_metrics = load_experiment_metrics(
        config.baseline_id, data_root=data_root,
    )

    context.log.info(
        "Loaded metrics: candidate=%s (sharpe=%.4f) baseline=%s (sharpe=%.4f)",
        config.candidate_id,
        candidate_metrics.get("performance", {}).get("sharpe", 0),
        config.baseline_id,
        baseline_metrics.get("performance", {}).get("sharpe", 0),
    )

    return {
        "candidate_metrics": candidate_metrics,
        "baseline_metrics": baseline_metrics,
        "spec_data": spec_data,
    }


@op(ins={"context_data": In(dict)}, out=Out(dict))
def evaluate_gates(
    context: OpExecutionContext, config: QualifyConfig, context_data: dict,
) -> dict:
    """Run full qualification gate evaluation."""
    from research.promotion.qualify import run_qualification

    data_root = Path(config.data_root)
    dagster_run_id = context.run_id if hasattr(context, "run_id") else ""

    report = run_qualification(
        candidate_id=config.candidate_id,
        baseline_id=config.baseline_id,
        data_root=data_root,
        dagster_run_id=dagster_run_id,
    )

    context.log.info(
        "Qualification %s for %s (hard_gates=%d, soft_gates=%d, "
        "execution_gates=%d, regime_gates=%d, warnings=%d)",
        "PASSED" if report.passed else "FAILED",
        config.candidate_id,
        len(report.hard_gates),
        len(report.soft_gates),
        len(report.execution_gates),
        len(report.regime_gates),
        len(report.warnings),
    )

    return {
        "experiment_id": report.experiment_id,
        "baseline_id": report.baseline_id,
        "passed": report.passed,
        "hard_gates": report.hard_gates,
        "soft_gates": report.soft_gates,
        "execution_gates": report.execution_gates,
        "regime_gates": report.regime_gates,
        "warnings": report.warnings,
        "timestamp": report.timestamp,
        "dagster_run_id": report.dagster_run_id,
    }


@op(ins={"report_data": In(dict)}, out=Out(Nothing))
def update_experiment_index(
    context: OpExecutionContext, config: QualifyConfig, report_data: dict,
) -> None:
    """Update experiment_index with qualification status."""
    from research.experiments.registry import get, write_index_row
    from research.experiments.spec import ExperimentSpec

    from pipelines.yats_pipelines.jobs.shadow_run import _reconstruct_spec

    data_root = Path(config.data_root)
    exp = get(config.candidate_id, data_root=data_root)
    spec_data = exp["spec"]
    spec = _reconstruct_spec(spec_data)

    # Load existing metrics for the index row
    candidate_metrics = report_data.get("candidate_metrics")
    if candidate_metrics is None:
        from research.promotion.qualify import load_experiment_metrics
        try:
            full_metrics = load_experiment_metrics(
                config.candidate_id, data_root=data_root,
            )
            candidate_metrics = full_metrics.get("performance", {})
            candidate_metrics.update(full_metrics.get("trading", {}))
        except FileNotFoundError:
            candidate_metrics = {}

    qualification_status = "passed" if report_data["passed"] else "failed"

    write_index_row(
        spec,
        metrics=candidate_metrics,
        dagster_run_id=report_data.get("dagster_run_id", ""),
        qualification_status=qualification_status,
        data_root=data_root,
    )

    context.log.info(
        "Updated experiment_index: %s qualification_status=%s",
        config.candidate_id,
        qualification_status,
    )


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------


@job
def qualify():
    """Qualification pipeline: load specs -> evaluate gates -> update index."""
    spec_data = load_candidate_spec()
    context_data = load_baseline_and_metrics(spec_data)
    report_data = evaluate_gates(context_data)
    update_experiment_index(report_data)
