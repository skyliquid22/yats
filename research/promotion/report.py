"""Qualification report generation.

Produces qualification_report.json matching PRD §10.3 schema.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from research.promotion.criteria import GateResult


@dataclass
class QualificationReport:
    """Full qualification report matching PRD §10.3."""

    experiment_id: str
    baseline_id: str
    passed: bool
    hard_gates: list[dict[str, Any]]
    soft_gates: list[dict[str, Any]]
    execution_gates: list[dict[str, Any]]
    regime_gates: list[dict[str, Any]]
    warnings: list[str]
    timestamp: str
    dagster_run_id: str = ""


def gate_result_to_dict(gate: GateResult) -> dict[str, Any]:
    """Convert a GateResult to the report dict format."""
    return {
        "name": gate.name,
        "passed": gate.passed,
        "value": gate.value,
        "baseline": gate.baseline,
        "threshold": gate.threshold,
        "detail": gate.detail,
    }


def build_report(
    *,
    experiment_id: str,
    baseline_id: str,
    all_gates: list[GateResult],
    warnings: list[str],
    dagster_run_id: str = "",
) -> QualificationReport:
    """Build a QualificationReport from gate results.

    Separates gates into hard/soft/execution/regime categories.
    """
    hard_gates = []
    soft_gates = []
    execution_gates = []
    regime_gates = []

    execution_gate_names = {
        "fill_rate", "reject_rate", "p95_slippage", "execution_halts",
        "avg_slippage_delta", "total_fees_delta", "turnover_drift",
        "execution_evidence",
    }
    regime_gate_names = {
        "highvol_drawdown_regression", "highvol_exposure_cap",
        "execution_drawdown_regression", "regime_sharpe_degradation",
        "highvol_turnover_increase", "mode_transition_fraction",
        "defensive_exposure_cap",
    }

    for gate in all_gates:
        d = gate_result_to_dict(gate)
        if gate.name in execution_gate_names:
            execution_gates.append(d)
        elif gate.name in regime_gate_names:
            regime_gates.append(d)
        elif gate.gate_type == "hard":
            hard_gates.append(d)
        else:
            soft_gates.append(d)

    # Overall pass: all hard gates must pass
    hard_all = [g for g in all_gates if g.gate_type == "hard"]
    passed = all(g.passed for g in hard_all)

    return QualificationReport(
        experiment_id=experiment_id,
        baseline_id=baseline_id,
        passed=passed,
        hard_gates=hard_gates,
        soft_gates=soft_gates,
        execution_gates=execution_gates,
        regime_gates=regime_gates,
        warnings=warnings,
        timestamp=datetime.now(timezone.utc).isoformat(),
        dagster_run_id=dagster_run_id,
    )


def write_report(
    report: QualificationReport,
    output_path: Path | str,
) -> None:
    """Write qualification_report.json to disk."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    report_dict = {
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

    output_path.write_text(
        json.dumps(report_dict, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
