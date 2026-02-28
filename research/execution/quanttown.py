"""QuantTown MEOW integration — molecule-to-YATS tool invocation mapping.

Implements PRD Phase 5 (lines 1774-1787):
- Molecule → YATS tool invocation mapping
- Gate evaluation integration (qualification gates as molecule gates)
- Audit trail linkage (quanttown_molecule_id, quanttown_bead_id columns)
- End-to-end formula: cook → pour → sling → execute through all beads

QuantTown agents sling molecules that invoke YATS tools. This module
provides the bridge: it maps molecule bead steps to YATS tool invocations
and threads the QuantTown provenance through the audit trail.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from questdb.ingress import Protocol, Sender, TimestampNanos

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# QuantTown context — threaded through all YATS tool invocations
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class QuantTownContext:
    """Provenance context from a QuantTown molecule invocation.

    When a QuantTown agent invokes a YATS tool as part of a molecule,
    this context threads the molecule_id and bead_id through to the
    audit trail for end-to-end traceability.
    """

    molecule_id: str = ""
    bead_id: str = ""

    @property
    def is_quanttown_invocation(self) -> bool:
        return bool(self.molecule_id)


# ---------------------------------------------------------------------------
# Audit trail writer with QuantTown linkage
# ---------------------------------------------------------------------------


def write_audit_entry(
    *,
    tool_name: str,
    invoker: str,
    experiment_id: str,
    mode: str,
    result_status: str,
    parameters: str = "{}",
    result_summary: str = "",
    duration_ms: int = 0,
    dagster_run_id: str = "",
    quanttown_ctx: QuantTownContext | None = None,
    ilp_host: str = "localhost",
    ilp_port: int = 9009,
) -> None:
    """Write an audit trail entry with optional QuantTown linkage.

    This is the canonical way to write audit_trail rows that include
    quanttown_molecule_id and quanttown_bead_id columns.
    """
    now = datetime.now(timezone.utc)
    ts = TimestampNanos(int(now.timestamp() * 1_000_000_000))

    columns: dict[str, Any] = {
        "parameters": parameters,
        "result_summary": result_summary,
        "duration_ms": duration_ms,
    }

    if dagster_run_id:
        columns["dagster_run_id"] = dagster_run_id

    # Thread QuantTown provenance into audit trail
    if quanttown_ctx and quanttown_ctx.is_quanttown_invocation:
        columns["quanttown_molecule_id"] = quanttown_ctx.molecule_id
        columns["quanttown_bead_id"] = quanttown_ctx.bead_id

    with Sender(Protocol.Tcp, ilp_host, ilp_port) as sender:
        sender.row(
            "audit_trail",
            symbols={
                "tool_name": tool_name,
                "invoker": invoker,
                "experiment_id": experiment_id,
                "mode": mode,
                "result_status": result_status,
            },
            columns=columns,
            at=ts,
        )
        sender.flush()


# ---------------------------------------------------------------------------
# Molecule → YATS tool mapping
# ---------------------------------------------------------------------------

# Maps QuantTown molecule bead types to YATS MCP tool names.
# When a QuantTown agent slings a molecule, each bead step maps
# to a specific YATS tool invocation.

MEOW_TOOL_MAPPING: dict[str, str] = {
    # Research phase
    "cook_hypothesis": "experiment.create",
    "cook_features": "features.compute",
    "cook_backtest": "experiment.run",

    # Qualification phase
    "pour_qualify": "qualify.run",
    "pour_review": "qualify.report",

    # Promotion phase
    "sling_promote_candidate": "promote.to_candidate",
    "sling_promote_production": "promote.to_production",

    # Execution phase
    "execute_paper": "execution.start_paper",
    "execute_paper_status": "execution.paper_status",
    "execute_stop_paper": "execution.stop_paper",
    "execute_promote_live": "execution.promote_live",

    # Monitoring phase
    "monitor_positions": "execution.positions",
    "monitor_orders": "execution.orders",
    "monitor_nav": "execution.nav",
    "monitor_health": "monitor.health",
}


def resolve_tool_for_bead(bead_type: str) -> str | None:
    """Resolve a QuantTown molecule bead type to a YATS MCP tool name.

    Returns None if the bead type has no YATS tool mapping.
    """
    return MEOW_TOOL_MAPPING.get(bead_type)


# ---------------------------------------------------------------------------
# Gate evaluation — qualification gates as molecule gates
# ---------------------------------------------------------------------------


@dataclass
class GateResult:
    """Result of evaluating a molecule gate against YATS criteria."""

    passed: bool
    gate_name: str
    details: str = ""
    blocking_reason: str = ""


def evaluate_qualification_gate(
    experiment_id: str,
    *,
    pg_host: str = "localhost",
    pg_port: int = 8812,
) -> GateResult:
    """Evaluate whether an experiment passes the qualification gate.

    Used by QuantTown molecules to check if a bead can proceed past
    the qualification step.
    """
    import psycopg2

    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        user="admin",
        password="quest",
        database="qdb",
    )

    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT passed, sharpe, max_drawdown
            FROM qualification_reports
            WHERE experiment_id = %s
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (experiment_id,),
        )
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()

    if row is None:
        return GateResult(
            passed=False,
            gate_name="qualification",
            blocking_reason=f"No qualification report found for {experiment_id}",
        )

    passed, sharpe, max_drawdown = row

    if not passed:
        return GateResult(
            passed=False,
            gate_name="qualification",
            details=f"sharpe={sharpe:.4f}, max_drawdown={max_drawdown:.4f}",
            blocking_reason="Qualification report indicates failure",
        )

    return GateResult(
        passed=True,
        gate_name="qualification",
        details=f"sharpe={sharpe:.4f}, max_drawdown={max_drawdown:.4f}",
    )


def evaluate_promotion_gate(
    experiment_id: str,
    target_tier: str,
    *,
    managing_partner_ack: bool = False,
    pg_host: str = "localhost",
    pg_port: int = 8812,
) -> GateResult:
    """Evaluate whether an experiment can be promoted to a target tier.

    Used by QuantTown molecules to gate promotion beads.
    """
    import psycopg2

    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        user="admin",
        password="quest",
        database="qdb",
    )

    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT tier
            FROM promotions
            WHERE experiment_id = %s
            ORDER BY promoted_at DESC
            LIMIT 1
            """,
            (experiment_id,),
        )
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()

    current_tier = row[0] if row else "research"

    # Tier ordering: research → candidate → production
    tier_order = {"research": 0, "candidate": 1, "production": 2}
    current_level = tier_order.get(current_tier, 0)
    target_level = tier_order.get(target_tier, 0)

    if target_level <= current_level:
        return GateResult(
            passed=False,
            gate_name="promotion",
            details=f"current={current_tier}, target={target_tier}",
            blocking_reason=f"Cannot promote from '{current_tier}' to '{target_tier}' (must be forward)",
        )

    if target_level > current_level + 1:
        return GateResult(
            passed=False,
            gate_name="promotion",
            details=f"current={current_tier}, target={target_tier}",
            blocking_reason=f"Cannot skip tiers: {current_tier} → {target_tier}",
        )

    # Production requires managing_partner_ack
    if target_tier == "production" and not managing_partner_ack:
        return GateResult(
            passed=False,
            gate_name="promotion",
            details=f"current={current_tier}, target={target_tier}",
            blocking_reason="Production promotion requires managing_partner_ack=True",
        )

    return GateResult(
        passed=True,
        gate_name="promotion",
        details=f"current={current_tier}, target={target_tier}",
    )


def evaluate_execution_gate(
    experiment_id: str,
    execution_type: str,
    *,
    managing_partner_ack: bool = False,
    pg_host: str = "localhost",
    pg_port: int = 8812,
) -> GateResult:
    """Evaluate whether an experiment can proceed to paper or live execution.

    Used by QuantTown molecules to gate execution beads.
    """
    import psycopg2

    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        user="admin",
        password="quest",
        database="qdb",
    )

    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT tier
            FROM promotions
            WHERE experiment_id = %s
            ORDER BY promoted_at DESC
            LIMIT 1
            """,
            (experiment_id,),
        )
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()

    current_tier = row[0] if row else None

    if execution_type == "paper":
        if current_tier not in ("candidate", "production"):
            return GateResult(
                passed=False,
                gate_name="execution_paper",
                blocking_reason=(
                    f"Paper trading requires 'candidate' or 'production' tier, "
                    f"currently at '{current_tier or 'none'}'"
                ),
            )
        return GateResult(
            passed=True,
            gate_name="execution_paper",
            details=f"tier={current_tier}",
        )

    elif execution_type == "live":
        if current_tier != "production":
            return GateResult(
                passed=False,
                gate_name="execution_live",
                blocking_reason=(
                    f"Live trading requires 'production' tier, "
                    f"currently at '{current_tier or 'none'}'"
                ),
            )

        if not managing_partner_ack:
            return GateResult(
                passed=False,
                gate_name="execution_live",
                blocking_reason="Live trading requires managing_partner_ack=True",
            )

        return GateResult(
            passed=True,
            gate_name="execution_live",
            details=f"tier={current_tier}, managing_partner_ack=True",
        )

    return GateResult(
        passed=False,
        gate_name=f"execution_{execution_type}",
        blocking_reason=f"Unknown execution type: {execution_type}",
    )
