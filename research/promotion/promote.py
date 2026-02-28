"""Tiered promotion pipeline — research → candidate → production.

Implements immutable promotion records, hard gates (risk override block,
baseline integrity, tier ordering), and QuestDB/filesystem writes.

PRD §11 (lines 839-868), §24 (lines 2179-2226).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TIERS = ("research", "candidate", "production")
_TIER_INDEX = {t: i for i, t in enumerate(TIERS)}


# ---------------------------------------------------------------------------
# Promotion record
# ---------------------------------------------------------------------------


@dataclass
class PromotionRecord:
    """Immutable promotion record — written once per experiment per tier."""

    experiment_id: str
    tier: str
    qualification_report_path: str
    spec_path: str
    metrics_path: str
    promotion_reason: str
    promoted_at: str
    promoted_by: str
    dagster_run_id: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "experiment_id": self.experiment_id,
            "tier": self.tier,
            "qualification_report_path": self.qualification_report_path,
            "spec_path": self.spec_path,
            "metrics_path": self.metrics_path,
            "promotion_reason": self.promotion_reason,
            "promoted_at": self.promoted_at,
            "promoted_by": self.promoted_by,
            "dagster_run_id": self.dagster_run_id,
        }


# ---------------------------------------------------------------------------
# Gate checks
# ---------------------------------------------------------------------------


class PromotionError(Exception):
    """Raised when a promotion hard gate fails."""


def _current_tier(
    experiment_id: str,
    *,
    data_root: Path,
) -> str | None:
    """Return the highest tier an experiment has been promoted to, or None."""
    promotions_root = data_root / "promotions"
    for tier in reversed(TIERS):
        record_path = promotions_root / tier / f"{experiment_id}.json"
        if record_path.exists():
            return tier
    return None


def check_tier_ordering(
    experiment_id: str,
    target_tier: str,
    *,
    data_root: Path,
) -> None:
    """Verify tier ordering — can't skip tiers.

    Raises PromotionError if ordering is violated.
    """
    if target_tier not in _TIER_INDEX:
        raise PromotionError(f"Invalid tier: {target_tier!r}. Must be one of {TIERS}")

    target_idx = _TIER_INDEX[target_tier]

    if target_idx == 0:
        # research tier: always allowed
        return

    # Must have the previous tier
    required_tier = TIERS[target_idx - 1]
    record_path = data_root / "promotions" / required_tier / f"{experiment_id}.json"
    if not record_path.exists():
        raise PromotionError(
            f"Cannot promote {experiment_id} to {target_tier}: "
            f"missing {required_tier} promotion record. "
            f"Tier ordering: {' → '.join(TIERS)}"
        )


def check_qualification_passed(
    experiment_id: str,
    *,
    data_root: Path,
) -> dict[str, Any]:
    """Verify qualification passed. Returns the qualification report dict.

    Raises PromotionError if qualification not found or failed.
    """
    report_path = (
        data_root / "experiments" / experiment_id
        / "promotion" / "qualification_report.json"
    )
    if not report_path.exists():
        raise PromotionError(
            f"No qualification report for {experiment_id}. "
            f"Run qualify pipeline first."
        )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    if not report.get("passed", False):
        raise PromotionError(
            f"Qualification FAILED for {experiment_id}. "
            f"Cannot promote."
        )
    return report


def check_risk_override_block(
    experiment_id: str,
    target_tier: str,
    *,
    data_root: Path,
) -> None:
    """Risk override experiments CANNOT be promoted beyond research.

    PRD §24.1 (lines 2179-2195).
    """
    if target_tier == "research":
        return

    spec_path = (
        data_root / "experiments" / experiment_id
        / "spec" / "experiment_spec.json"
    )
    if not spec_path.exists():
        raise PromotionError(f"Experiment spec not found: {experiment_id}")

    spec_data = json.loads(spec_path.read_text(encoding="utf-8"))
    risk_overrides = spec_data.get("risk_overrides")
    if risk_overrides is not None:
        raise PromotionError(
            f"Cannot promote {experiment_id} to {target_tier}: "
            f"experiment has risk_overrides. Must retrain under "
            f"production risk config (configs/risk.yml) and re-qualify."
        )


def check_baseline_integrity(
    experiment_id: str,
    target_tier: str,
    *,
    qualification_report: dict[str, Any],
) -> None:
    """baseline_id == candidate_id → limited to research tier.

    PRD §24.2 (lines 2196-2203).
    """
    if target_tier == "research":
        return

    baseline_id = qualification_report.get("baseline_id", "")
    if baseline_id == experiment_id:
        raise PromotionError(
            f"Cannot promote {experiment_id} to {target_tier}: "
            f"baseline_id == candidate_id. "
            f"Self-baseline experiments limited to research tier."
        )


def check_data_drift(
    experiment_id: str,
    target_tier: str,
    *,
    qualification_report: dict[str, Any],
    managing_partner_ack: bool = False,
) -> None:
    """Data drift + production requires managing_partner acknowledgment.

    PRD §24.3 (lines 2205-2213).
    """
    if target_tier != "production":
        return

    warnings = qualification_report.get("warnings", [])
    if "data_drift" in warnings and not managing_partner_ack:
        raise PromotionError(
            f"Cannot promote {experiment_id} to production: "
            f"data_drift detected and managing_partner acknowledgment "
            f"not provided."
        )


def check_production_approval(
    target_tier: str,
    *,
    managing_partner_ack: bool = False,
) -> None:
    """Production tier requires managing_partner approval.

    PRD §11.2: production tier requires human approval via MCP tool
    with requires_approval flag.
    """
    if target_tier != "production":
        return

    if not managing_partner_ack:
        raise PromotionError(
            "Cannot promote to production: managing_partner approval required. "
            "Use the promote.to_production MCP tool with requires_approval flag."
        )


def check_staleness(
    experiment_id: str,
    *,
    questdb_resource: Any | None = None,
) -> None:
    """Stale experiments CANNOT be promoted at any tier.

    PRD §24.5 (line 2263): stale experiments are hard-blocked from promotion.
    Refresh by re-running evaluation against updated canonical data.
    """
    from research.data.canonical_integrity import is_experiment_stale

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
        if is_experiment_stale(conn, experiment_id):
            raise PromotionError(
                f"Cannot promote {experiment_id}: experiment is STALE. "
                f"Canonical data has changed since evaluation. "
                f"Re-run evaluation against updated canonical data to clear "
                f"the stale flag."
            )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Immutability check
# ---------------------------------------------------------------------------


def check_immutability(
    record: PromotionRecord,
    *,
    data_root: Path,
) -> None:
    """If a promotion record exists with different content → error."""
    record_path = (
        data_root / "promotions" / record.tier / f"{record.experiment_id}.json"
    )
    if not record_path.exists():
        return

    existing = json.loads(record_path.read_text(encoding="utf-8"))
    new = record.to_dict()

    # Compare content fields (excluding promoted_at and dagster_run_id
    # which are expected to differ between attempts)
    content_keys = (
        "experiment_id", "tier", "qualification_report_path",
        "spec_path", "metrics_path",
    )
    for key in content_keys:
        if existing.get(key) != new.get(key):
            raise PromotionError(
                f"Immutability violation: promotion record already exists "
                f"for {record.experiment_id} at tier {record.tier} "
                f"with different content (field: {key}). "
                f"No overwrites allowed."
            )

    logger.info(
        "Promotion record already exists for %s at %s with same content — idempotent",
        record.experiment_id, record.tier,
    )


# ---------------------------------------------------------------------------
# Write promotion record
# ---------------------------------------------------------------------------


def write_promotion_record(
    record: PromotionRecord,
    *,
    data_root: Path,
) -> Path:
    """Write promotion record to filesystem. Returns the record path."""
    record_path = (
        data_root / "promotions" / record.tier / f"{record.experiment_id}.json"
    )
    record_path.parent.mkdir(parents=True, exist_ok=True)

    record_path.write_text(
        json.dumps(record.to_dict(), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    logger.info(
        "Wrote promotion record: %s → %s at %s",
        record.experiment_id, record.tier, record_path,
    )
    return record_path


# ---------------------------------------------------------------------------
# QuestDB promotions table write
# ---------------------------------------------------------------------------


def write_promotions_row(
    record: PromotionRecord,
    *,
    qualification_passed: bool,
    sharpe: float = 0.0,
    max_drawdown: float = 0.0,
    questdb_resource: Any | None = None,
) -> None:
    """Write a row to the QuestDB promotions table via ILP."""
    from questdb.ingress import Protocol, Sender, TimestampNanos

    if questdb_resource is None:
        from pipelines.yats_pipelines.resources.questdb import QuestDBResource
        questdb_resource = QuestDBResource()

    now = datetime.now(timezone.utc)
    ts_nanos = TimestampNanos(int(now.timestamp() * 1_000_000_000))

    with Sender(Protocol.Tcp, questdb_resource.ilp_host, questdb_resource.ilp_port) as sender:
        sender.row(
            "promotions",
            symbols={
                "experiment_id": record.experiment_id,
                "tier": record.tier,
                "promoted_by": record.promoted_by,
            },
            columns={
                "qualification_passed": qualification_passed,
                "sharpe": sharpe,
                "max_drawdown": max_drawdown,
                "dagster_run_id": record.dagster_run_id,
            },
            at=ts_nanos,
        )
        sender.flush()

    logger.info(
        "Wrote promotions row: %s tier=%s sharpe=%.4f",
        record.experiment_id, record.tier, sharpe,
    )


# ---------------------------------------------------------------------------
# Update experiment_index promotion_tier
# ---------------------------------------------------------------------------


def update_experiment_index_tier(
    experiment_id: str,
    tier: str,
    *,
    data_root: Path,
    dagster_run_id: str = "",
    questdb_resource: Any | None = None,
) -> None:
    """Update experiment_index with new promotion_tier."""
    from research.experiments.registry import get, write_index_row

    from pipelines.yats_pipelines.jobs.shadow_run import _reconstruct_spec

    exp = get(experiment_id, data_root=data_root)
    spec = _reconstruct_spec(exp["spec"])

    # Load metrics if available
    metrics: dict[str, Any] | None = None
    metrics_path = data_root / "experiments" / experiment_id / "evaluation" / "metrics.json"
    if metrics_path.exists():
        full_metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
        metrics = full_metrics.get("performance", {})
        metrics.update(full_metrics.get("trading", {}))

    write_index_row(
        spec,
        metrics=metrics,
        dagster_run_id=dagster_run_id,
        promotion_tier=tier,
        data_root=data_root,
        questdb_resource=questdb_resource,
    )

    logger.info(
        "Updated experiment_index: %s promotion_tier=%s",
        experiment_id, tier,
    )


# ---------------------------------------------------------------------------
# Main promote function
# ---------------------------------------------------------------------------


def promote(
    experiment_id: str,
    target_tier: str,
    *,
    promotion_reason: str = "",
    promoted_by: str = "dagster",
    data_root: Path | None = None,
    dagster_run_id: str = "",
    managing_partner_ack: bool = False,
    questdb_resource: Any | None = None,
) -> PromotionRecord:
    """Run the full promotion pipeline for an experiment.

    Checks all gates, writes immutable promotion record, updates QuestDB
    promotions table and experiment_index.

    Args:
        experiment_id: Experiment to promote.
        target_tier: Target tier (research, candidate, production).
        promotion_reason: Free-text reason for promotion.
        promoted_by: Who triggered the promotion.
        data_root: Path to .yats_data root.
        dagster_run_id: Dagster run ID for traceability.
        managing_partner_ack: Required for production promotions and
            data_drift production promotions.
        questdb_resource: QuestDB resource for writes.

    Returns:
        PromotionRecord that was written.

    Raises:
        PromotionError: If any hard gate fails.
    """
    root = data_root or Path(".yats_data")

    logger.info("Starting promotion: %s → %s", experiment_id, target_tier)

    # --- Gate 0: Staleness check (PRD §24.5 line 2263) ---
    check_staleness(
        experiment_id, questdb_resource=questdb_resource,
    )

    # --- Gate 1: Qualification passed ---
    qualification_report = check_qualification_passed(
        experiment_id, data_root=root,
    )

    # --- Gate 2: Tier ordering ---
    check_tier_ordering(experiment_id, target_tier, data_root=root)

    # --- Gate 3: Risk override block ---
    check_risk_override_block(experiment_id, target_tier, data_root=root)

    # --- Gate 4: Baseline integrity ---
    check_baseline_integrity(
        experiment_id, target_tier,
        qualification_report=qualification_report,
    )

    # --- Gate 5: Data drift ---
    check_data_drift(
        experiment_id, target_tier,
        qualification_report=qualification_report,
        managing_partner_ack=managing_partner_ack,
    )

    # --- Gate 6: Production approval ---
    check_production_approval(
        target_tier, managing_partner_ack=managing_partner_ack,
    )

    # --- Build record ---
    exp_root = root / "experiments" / experiment_id
    qual_report_path = str(
        exp_root / "promotion" / "qualification_report.json"
    )
    spec_path = str(exp_root / "spec" / "experiment_spec.json")
    metrics_path = str(exp_root / "evaluation" / "metrics.json")

    record = PromotionRecord(
        experiment_id=experiment_id,
        tier=target_tier,
        qualification_report_path=qual_report_path,
        spec_path=spec_path,
        metrics_path=metrics_path,
        promotion_reason=promotion_reason,
        promoted_at=datetime.now(timezone.utc).isoformat(),
        promoted_by=promoted_by,
        dagster_run_id=dagster_run_id,
    )

    # --- Immutability check ---
    check_immutability(record, data_root=root)

    # --- Write filesystem record ---
    write_promotion_record(record, data_root=root)

    # --- Write QuestDB promotions table ---
    # Extract metrics for the row
    sharpe = 0.0
    max_drawdown = 0.0
    try:
        from research.promotion.qualify import load_experiment_metrics
        metrics = load_experiment_metrics(experiment_id, data_root=root)
        perf = metrics.get("performance", {})
        sharpe = float(perf.get("sharpe", 0.0))
        max_drawdown = float(perf.get("max_drawdown", 0.0))
    except FileNotFoundError:
        logger.warning("No metrics for %s, writing promotions row with zeros", experiment_id)

    write_promotions_row(
        record,
        qualification_passed=True,
        sharpe=sharpe,
        max_drawdown=max_drawdown,
        questdb_resource=questdb_resource,
    )

    # --- Update experiment_index ---
    update_experiment_index_tier(
        experiment_id,
        target_tier,
        data_root=root,
        dagster_run_id=dagster_run_id,
        questdb_resource=questdb_resource,
    )

    logger.info(
        "Promotion complete: %s → %s (by=%s, reason=%s)",
        experiment_id, target_tier, promoted_by, promotion_reason,
    )

    return record
