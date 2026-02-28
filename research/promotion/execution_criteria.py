"""Execution-specific qualification gates.

Implements gates 8-9 from PRD Appendix D.1 evaluation order:
  8. Execution hard gates (fill rate, reject rate, slippage, halts)
  9. Execution soft gates (deltas vs baseline)
"""

from __future__ import annotations

from typing import Any

from research.promotion.criteria import GateResult


# ---------------------------------------------------------------------------
# Execution gate thresholds (PRD Appendix D.3)
# ---------------------------------------------------------------------------

EXECUTION_HARD_THRESHOLDS: dict[str, Any] = {
    "fill_rate": {"min": 0.99, "name": "fill_rate"},
    "reject_rate": {"max": 0.01, "name": "reject_rate"},
    "p95_slippage_bps": {"max": 25.0, "name": "p95_slippage"},
    "execution_halts": {"max": 0, "name": "execution_halts"},
}

EXECUTION_SOFT_DELTAS: dict[str, Any] = {
    "avg_slippage_bps": {"max_delta": 5.0, "name": "avg_slippage_delta"},
    "total_fees": {"max_delta_pct": 10.0, "name": "total_fees_delta"},
    "total_turnover": {"max_delta_pct": 10.0, "name": "turnover_drift"},
}


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------


def evaluate_execution_hard_gates(
    execution_metrics: dict[str, Any] | None,
) -> list[GateResult]:
    """Evaluate execution hard gates (step 8 in gate order).

    Args:
        execution_metrics: Row from execution_metrics table (QuestDB).
    """
    results: list[GateResult] = []

    if execution_metrics is None:
        # No execution evidence — this is handled by the execution evidence gate
        # (step 7). If we get here with None, gates fail.
        for key, gate in EXECUTION_HARD_THRESHOLDS.items():
            results.append(GateResult(
                name=gate["name"], passed=False, value=None,
                baseline=None, threshold=None, gate_type="hard",
                detail="No execution metrics available",
            ))
        return results

    # Fill rate >= 99%
    fill_rate = execution_metrics.get("fill_rate")
    if fill_rate is not None:
        passed = fill_rate >= 0.99
        results.append(GateResult(
            name="fill_rate", passed=passed,
            value=fill_rate, baseline=None, threshold=0.99,
            gate_type="hard",
        ))
    else:
        results.append(GateResult(
            name="fill_rate", passed=False, value=None,
            baseline=None, threshold=0.99, gate_type="hard",
            detail="Missing fill_rate metric",
        ))

    # Reject rate <= 1%
    reject_rate = execution_metrics.get("reject_rate")
    if reject_rate is not None:
        passed = reject_rate <= 0.01
        results.append(GateResult(
            name="reject_rate", passed=passed,
            value=reject_rate, baseline=None, threshold=0.01,
            gate_type="hard",
        ))
    else:
        results.append(GateResult(
            name="reject_rate", passed=False, value=None,
            baseline=None, threshold=0.01, gate_type="hard",
            detail="Missing reject_rate metric",
        ))

    # P95 slippage <= 25 bps
    p95_slippage = execution_metrics.get("p95_slippage_bps")
    if p95_slippage is not None:
        passed = p95_slippage <= 25.0
        results.append(GateResult(
            name="p95_slippage", passed=passed,
            value=p95_slippage, baseline=None, threshold=25.0,
            gate_type="hard",
        ))
    else:
        results.append(GateResult(
            name="p95_slippage", passed=False, value=None,
            baseline=None, threshold=25.0, gate_type="hard",
            detail="Missing p95_slippage_bps metric",
        ))

    # Execution halts == 0
    halts = execution_metrics.get("execution_halts")
    if halts is not None:
        passed = halts == 0
        results.append(GateResult(
            name="execution_halts", passed=passed,
            value=float(halts), baseline=None, threshold=0.0,
            gate_type="hard",
        ))
    else:
        results.append(GateResult(
            name="execution_halts", passed=False, value=None,
            baseline=None, threshold=0.0, gate_type="hard",
            detail="Missing execution_halts metric",
        ))

    return results


def evaluate_execution_soft_gates(
    candidate_execution: dict[str, Any] | None,
    baseline_execution: dict[str, Any] | None,
    *,
    skip_delta_checks: bool = False,
) -> list[GateResult]:
    """Evaluate execution soft gates (step 9 in gate order).

    Args:
        candidate_execution: execution_metrics row for candidate.
        baseline_execution: execution_metrics row for baseline.
        skip_delta_checks: True when baseline == candidate.
    """
    results: list[GateResult] = []

    if candidate_execution is None or baseline_execution is None or skip_delta_checks:
        detail = "Skipped (baseline==candidate)" if skip_delta_checks else "No execution data"
        for gate in EXECUTION_SOFT_DELTAS.values():
            results.append(GateResult(
                name=gate["name"], passed=True, value=None,
                baseline=None, threshold=None, gate_type="soft",
                detail=detail,
            ))
        return results

    # Avg slippage delta (absolute, max 5 bps)
    cand_slip = candidate_execution.get("avg_slippage_bps", 0.0)
    base_slip = baseline_execution.get("avg_slippage_bps", 0.0)
    delta = abs(cand_slip - base_slip)
    results.append(GateResult(
        name="avg_slippage_delta", passed=delta <= 5.0,
        value=cand_slip, baseline=base_slip, threshold=5.0,
        gate_type="soft",
        detail=f"delta={delta:.2f} bps",
    ))

    # Total fees delta (percentage, max 10%)
    cand_fees = candidate_execution.get("total_fees", 0.0)
    base_fees = baseline_execution.get("total_fees", 0.0)
    if abs(base_fees) > 1e-9:
        fees_delta_pct = abs((cand_fees - base_fees) / base_fees) * 100
    else:
        fees_delta_pct = 0.0
    results.append(GateResult(
        name="total_fees_delta", passed=fees_delta_pct <= 10.0,
        value=cand_fees, baseline=base_fees, threshold=10.0,
        gate_type="soft",
        detail=f"delta_pct={fees_delta_pct:.2f}%",
    ))

    # Turnover drift (percentage, max 10%)
    cand_turnover = candidate_execution.get("total_turnover", 0.0)
    base_turnover = baseline_execution.get("total_turnover", 0.0)
    if abs(base_turnover) > 1e-9:
        turnover_delta_pct = abs((cand_turnover - base_turnover) / base_turnover) * 100
    else:
        turnover_delta_pct = 0.0
    results.append(GateResult(
        name="turnover_drift", passed=turnover_delta_pct <= 10.0,
        value=cand_turnover, baseline=base_turnover, threshold=10.0,
        gate_type="soft",
        detail=f"delta_pct={turnover_delta_pct:.2f}%",
    ))

    return results
