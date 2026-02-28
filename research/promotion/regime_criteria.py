"""Regime-specific qualification gates.

Implements gates 3-4 from PRD Appendix D.1 evaluation order:
  3. Regime hard gates (high-vol drawdown/exposure)
  4. Regime soft gates (Sharpe degradation, stability)
"""

from __future__ import annotations

from typing import Any

from research.promotion.criteria import GateResult, _extract_metric


# ---------------------------------------------------------------------------
# Regime gate thresholds (PRD Appendix D.3)
# ---------------------------------------------------------------------------

REGIME_HARD_GATES: list[dict[str, Any]] = [
    {
        "name": "highvol_drawdown_regression",
        "regime": "high_vol",
        "metric": "performance.max_drawdown",
        "threshold_pct": 10.0,
        "gate_type": "hard",
    },
    {
        "name": "highvol_exposure_cap",
        "threshold": 0.40,
        "gate_type": "hard",
    },
    {
        "name": "execution_drawdown_regression",
        "threshold_pct": 2.0,
        "gate_type": "hard",
    },
]

REGIME_SOFT_GATES: list[dict[str, Any]] = [
    {
        "name": "regime_sharpe_degradation",
        "regime": "high_vol",
        "metric": "performance.sharpe",
        "threshold_pct": 15.0,
        "gate_type": "soft",
    },
    {
        "name": "highvol_turnover_increase",
        "regime": "high_vol",
        "metric": "trading.turnover_1d_mean",
        "threshold_pct": 20.0,
        "gate_type": "soft",
    },
    {
        "name": "mode_transition_fraction",
        "threshold": 0.10,
        "gate_type": "soft",
    },
    {
        "name": "defensive_exposure_cap",
        "gate_type": "soft",
        "requires_hierarchy": True,
    },
]


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------


def evaluate_regime_hard_gates(
    candidate_metrics: dict[str, Any],
    baseline_metrics: dict[str, Any],
    *,
    candidate_execution: dict[str, Any] | None = None,
    baseline_execution: dict[str, Any] | None = None,
    skip_delta_checks: bool = False,
) -> list[GateResult]:
    """Evaluate regime hard gates (step 3 in gate order).

    Args:
        candidate_metrics: evaluation/metrics.json for candidate.
        baseline_metrics: evaluation/metrics.json for baseline.
        candidate_execution: execution_metrics row for candidate.
        baseline_execution: execution_metrics row for baseline.
        skip_delta_checks: True when baseline == candidate.
    """
    results: list[GateResult] = []

    # --- High-vol drawdown regression (10%) ---
    cand_regime = candidate_metrics.get("performance_by_regime", {})
    base_regime = baseline_metrics.get("performance_by_regime", {})

    cand_hv = cand_regime.get("high_vol", {})
    base_hv = base_regime.get("high_vol", {})

    cand_hv_dd = cand_hv.get("max_drawdown")
    base_hv_dd = base_hv.get("max_drawdown")

    if cand_hv_dd is not None and base_hv_dd is not None and not skip_delta_checks:
        # max_drawdown is negative; lower_is_better means more negative is worse
        degradation = max(0.0, cand_hv_dd - base_hv_dd)  # worse if candidate more negative
        # Actually for drawdown (negative values), worsening means candidate < baseline
        degradation = max(0.0, base_hv_dd - cand_hv_dd) if cand_hv_dd < base_hv_dd else 0.0
        threshold = abs(base_hv_dd) * 0.10 if abs(base_hv_dd) > 1e-9 else 0.0
        violation = degradation > threshold
        results.append(GateResult(
            name="highvol_drawdown_regression", passed=not violation,
            value=cand_hv_dd, baseline=base_hv_dd, threshold=10.0,
            gate_type="hard",
        ))
    else:
        results.append(GateResult(
            name="highvol_drawdown_regression", passed=True,
            value=cand_hv_dd, baseline=base_hv_dd, threshold=10.0,
            gate_type="hard",
            detail="Skipped (no regime data or baseline==candidate)",
        ))

    # --- High-vol exposure cap (0.40) ---
    cand_hv_exposure = cand_hv.get("gross_exposure_mean")
    if cand_hv_exposure is not None:
        passed = cand_hv_exposure <= 0.40
        results.append(GateResult(
            name="highvol_exposure_cap", passed=passed,
            value=cand_hv_exposure, baseline=None, threshold=0.40,
            gate_type="hard",
        ))
    else:
        results.append(GateResult(
            name="highvol_exposure_cap", passed=True,
            value=None, baseline=None, threshold=0.40,
            gate_type="hard",
            detail="Skipped (no high-vol exposure data)",
        ))

    # --- Execution drawdown regression (2%) ---
    if candidate_execution and baseline_execution and not skip_delta_checks:
        cand_exec_dd = candidate_execution.get("max_drawdown", 0.0)
        base_exec_dd = baseline_execution.get("max_drawdown", 0.0)
        degradation = max(0.0, base_exec_dd - cand_exec_dd) if cand_exec_dd < base_exec_dd else 0.0
        threshold = abs(base_exec_dd) * 0.02 if abs(base_exec_dd) > 1e-9 else 0.0
        violation = degradation > threshold
        results.append(GateResult(
            name="execution_drawdown_regression", passed=not violation,
            value=cand_exec_dd, baseline=base_exec_dd, threshold=2.0,
            gate_type="hard",
        ))
    else:
        results.append(GateResult(
            name="execution_drawdown_regression", passed=True,
            value=None, baseline=None, threshold=2.0,
            gate_type="hard",
            detail="Skipped (no execution data or baseline==candidate)",
        ))

    return results


def evaluate_regime_soft_gates(
    candidate_metrics: dict[str, Any],
    baseline_metrics: dict[str, Any],
    *,
    hierarchy_enabled: bool = False,
    skip_delta_checks: bool = False,
) -> list[GateResult]:
    """Evaluate regime soft gates (step 4 in gate order).

    Args:
        candidate_metrics: evaluation/metrics.json for candidate.
        baseline_metrics: evaluation/metrics.json for baseline.
        hierarchy_enabled: True if hierarchy is enabled for this experiment.
        skip_delta_checks: True when baseline == candidate.
    """
    results: list[GateResult] = []

    cand_regime = candidate_metrics.get("performance_by_regime", {})
    base_regime = baseline_metrics.get("performance_by_regime", {})

    cand_hv = cand_regime.get("high_vol", {})
    base_hv = base_regime.get("high_vol", {})

    # --- Sharpe degradation in high-vol (15%) ---
    cand_hv_sharpe = cand_hv.get("sharpe")
    base_hv_sharpe = base_hv.get("sharpe")

    if cand_hv_sharpe is not None and base_hv_sharpe is not None and not skip_delta_checks:
        degradation = max(0.0, base_hv_sharpe - cand_hv_sharpe)
        threshold = abs(base_hv_sharpe) * 0.15 if abs(base_hv_sharpe) > 1e-9 else 0.0
        violation = degradation > threshold
        results.append(GateResult(
            name="regime_sharpe_degradation", passed=not violation,
            value=cand_hv_sharpe, baseline=base_hv_sharpe, threshold=15.0,
            gate_type="soft",
        ))
    else:
        results.append(GateResult(
            name="regime_sharpe_degradation", passed=True,
            value=cand_hv_sharpe, baseline=base_hv_sharpe, threshold=15.0,
            gate_type="soft",
            detail="Skipped (no regime data or baseline==candidate)",
        ))

    # --- Turnover increase in high-vol (20%) ---
    cand_hv_turnover = cand_hv.get("turnover_1d_mean")
    base_hv_turnover = base_hv.get("turnover_1d_mean")

    if cand_hv_turnover is not None and base_hv_turnover is not None and not skip_delta_checks:
        degradation = max(0.0, cand_hv_turnover - base_hv_turnover)
        threshold = abs(base_hv_turnover) * 0.20 if abs(base_hv_turnover) > 1e-9 else 0.0
        violation = degradation > threshold
        results.append(GateResult(
            name="highvol_turnover_increase", passed=not violation,
            value=cand_hv_turnover, baseline=base_hv_turnover, threshold=20.0,
            gate_type="soft",
        ))
    else:
        results.append(GateResult(
            name="highvol_turnover_increase", passed=True,
            value=cand_hv_turnover, baseline=base_hv_turnover, threshold=20.0,
            gate_type="soft",
            detail="Skipped (no regime data or baseline==candidate)",
        ))

    # --- Mode transition fraction (0.10) ---
    regime_info = candidate_metrics.get("regime", {})
    mode_transition_frac = regime_info.get("mode_transition_fraction")
    if mode_transition_frac is not None:
        passed = mode_transition_frac <= 0.10
        results.append(GateResult(
            name="mode_transition_fraction", passed=passed,
            value=mode_transition_frac, baseline=None, threshold=0.10,
            gate_type="soft",
        ))
    else:
        results.append(GateResult(
            name="mode_transition_fraction", passed=True,
            value=None, baseline=None, threshold=0.10,
            gate_type="soft",
            detail="Skipped (no mode transition data)",
        ))

    # --- Defensive exposure cap (if hierarchy enabled) ---
    if hierarchy_enabled:
        results.append(GateResult(
            name="defensive_exposure_cap", passed=True,
            value=None, baseline=None, threshold=None,
            gate_type="soft",
            detail="Hierarchy enabled; defensive exposure not yet implemented",
        ))

    return results
