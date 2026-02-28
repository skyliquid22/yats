"""Dagster promote job — tiered promotion pipeline.

Promotes an experiment through research → candidate → production tiers
with hard gates for qualification, tier ordering, risk overrides,
baseline integrity, and data drift.

PRD §11 (lines 839-868), §24 (lines 2179-2226).
"""

import logging
from pathlib import Path
from typing import Any

from dagster import Config, In, Nothing, OpExecutionContext, Out, job, op

logger = logging.getLogger(__name__)


class PromoteConfig(Config):
    """Run config for promote job."""

    experiment_id: str
    target_tier: str = "research"
    promotion_reason: str = ""
    promoted_by: str = "dagster"
    managing_partner_ack: bool = False
    data_root: str = ".yats_data"


# ---------------------------------------------------------------------------
# Ops
# ---------------------------------------------------------------------------


@op(out=Out(dict))
def verify_qualification(
    context: OpExecutionContext, config: PromoteConfig,
) -> dict:
    """Verify qualification passed and load the report."""
    from research.promotion.promote import check_qualification_passed

    data_root = Path(config.data_root)
    report = check_qualification_passed(config.experiment_id, data_root=data_root)

    context.log.info(
        "Qualification verified for %s: passed=%s",
        config.experiment_id,
        report.get("passed"),
    )
    return report


@op(ins={"qualification_report": In(dict)}, out=Out(dict))
def check_promotion_gates(
    context: OpExecutionContext,
    config: PromoteConfig,
    qualification_report: dict,
) -> dict:
    """Run all promotion hard gates."""
    from research.promotion.promote import (
        check_baseline_integrity,
        check_data_drift,
        check_production_approval,
        check_risk_override_block,
        check_tier_ordering,
    )

    data_root = Path(config.data_root)

    check_tier_ordering(
        config.experiment_id, config.target_tier, data_root=data_root,
    )
    context.log.info("Gate passed: tier ordering")

    check_risk_override_block(
        config.experiment_id, config.target_tier, data_root=data_root,
    )
    context.log.info("Gate passed: risk override block")

    check_baseline_integrity(
        config.experiment_id, config.target_tier,
        qualification_report=qualification_report,
    )
    context.log.info("Gate passed: baseline integrity")

    check_data_drift(
        config.experiment_id, config.target_tier,
        qualification_report=qualification_report,
        managing_partner_ack=config.managing_partner_ack,
    )
    context.log.info("Gate passed: data drift")

    check_production_approval(
        config.target_tier,
        managing_partner_ack=config.managing_partner_ack,
    )
    context.log.info("Gate passed: production approval")

    context.log.info(
        "All gates passed for %s → %s",
        config.experiment_id, config.target_tier,
    )

    return {
        "qualification_report": qualification_report,
        "gates_passed": True,
    }


@op(ins={"gate_result": In(dict)}, out=Out(dict))
def write_promotion(
    context: OpExecutionContext,
    config: PromoteConfig,
    gate_result: dict,
) -> dict:
    """Write immutable promotion record and update QuestDB."""
    from research.promotion.promote import promote

    data_root = Path(config.data_root)
    dagster_run_id = context.run_id if hasattr(context, "run_id") else ""

    record = promote(
        config.experiment_id,
        config.target_tier,
        promotion_reason=config.promotion_reason,
        promoted_by=config.promoted_by,
        data_root=data_root,
        dagster_run_id=dagster_run_id,
        managing_partner_ack=config.managing_partner_ack,
    )

    context.log.info(
        "Promotion written: %s → %s (record=%s)",
        config.experiment_id,
        config.target_tier,
        record.promoted_at,
    )

    return record.to_dict()


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------


@job
def promote_job():
    """Promotion pipeline: verify qualification -> check gates -> write record."""
    qualification_report = verify_qualification()
    gate_result = check_promotion_gates(qualification_report)
    write_promotion(gate_result)
