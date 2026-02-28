"""Hard/soft gate definitions with thresholds for qualification.

Implements regression comparison methodology from PRD Appendix D.3 and H.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class GateResult:
    """Result of evaluating a single qualification gate."""

    name: str
    passed: bool
    value: float | None
    baseline: float | None
    threshold: float | None
    gate_type: str  # "hard" or "soft"
    detail: str = ""


# ---------------------------------------------------------------------------
# Metric direction definitions (PRD Appendix H.1)
# ---------------------------------------------------------------------------

HIGHER_IS_BETTER = "higher_is_better"
LOWER_IS_BETTER = "lower_is_better"
EQUALS_ZERO = "equals_zero"

METRIC_DIRECTIONS: dict[str, str] = {
    "performance.sharpe": HIGHER_IS_BETTER,
    "performance.calmar": HIGHER_IS_BETTER,
    "performance.sortino": HIGHER_IS_BETTER,
    "performance.total_return": HIGHER_IS_BETTER,
    "performance.annualized_return": HIGHER_IS_BETTER,
    "performance.max_drawdown": LOWER_IS_BETTER,
    "performance.max_drawdown_duration": LOWER_IS_BETTER,
    "trading.turnover_1d_mean": LOWER_IS_BETTER,
    "trading.turnover_1d_std": LOWER_IS_BETTER,
    "trading.win_rate": HIGHER_IS_BETTER,
    "trading.profit_factor": HIGHER_IS_BETTER,
    "safety.nan_inf_violations": EQUALS_ZERO,
}


# ---------------------------------------------------------------------------
# Threshold definitions (PRD Appendix D.3)
# ---------------------------------------------------------------------------

REGRESSION_GATES: list[dict[str, Any]] = [
    {
        "metric": "performance.sharpe",
        "threshold_pct": 5.0,
        "gate_type": "hard",
        "name": "sharpe_regression",
    },
    {
        "metric": "performance.max_drawdown",
        "threshold_pct": 10.0,
        "gate_type": "soft",
        "name": "max_drawdown_regression",
    },
    {
        "metric": "trading.turnover_1d_mean",
        "threshold_pct": 15.0,
        "gate_type": "soft",
        "name": "turnover_regression",
    },
    {
        "metric": "safety.nan_inf_violations",
        "threshold_pct": None,  # equals_zero check, no pct
        "gate_type": "hard",
        "name": "nan_inf_violations",
    },
]

CONSTRAINT_GATES: list[dict[str, Any]] = [
    {
        "metric": "safety.constraint_violations",
        "threshold": 0,
        "gate_type": "hard",
        "name": "constraint_violations",
    },
    {
        "metric": "performance.max_drawdown",
        "config_key": "max_drawdown",
        "default_threshold": -1.0,
        "gate_type": "hard",
        "name": "max_drawdown_constraint",
    },
    {
        "metric": "trading.turnover_1d_mean",
        "config_key": "max_turnover",
        "default_threshold": 1.0,
        "gate_type": "hard",
        "name": "turnover_constraint",
    },
]


# ---------------------------------------------------------------------------
# Metric extraction helper
# ---------------------------------------------------------------------------


def _extract_metric(metrics: dict[str, Any], path: str) -> float | None:
    """Extract a metric value by dot-path (e.g. 'performance.sharpe')."""
    parts = path.split(".")
    current: Any = metrics
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
        if current is None:
            return None
    if isinstance(current, (int, float)):
        return float(current)
    return None


# ---------------------------------------------------------------------------
# Regression comparison (PRD Appendix H.2)
# ---------------------------------------------------------------------------


def compare_metric(
    candidate_value: float,
    baseline_value: float,
    direction: str,
    threshold_pct: float | None,
) -> tuple[float, float | None, bool]:
    """Compare candidate vs baseline for a single metric.

    Returns (degradation, delta_pct, is_violation).
    """
    delta = candidate_value - baseline_value

    if abs(baseline_value) < 1e-9:
        delta_pct = None
    else:
        delta_pct = (delta / baseline_value) * 100

    if direction == EQUALS_ZERO:
        degradation = abs(candidate_value)
        violation = candidate_value != 0
        return degradation, delta_pct, violation

    if direction == HIGHER_IS_BETTER:
        degradation = max(0.0, baseline_value - candidate_value)
    else:  # lower_is_better
        degradation = max(0.0, candidate_value - baseline_value)

    if threshold_pct is None:
        violation = degradation > 0
    else:
        violation = degradation > abs(baseline_value) * (threshold_pct / 100)

    return degradation, delta_pct, violation


# ---------------------------------------------------------------------------
# Gate evaluation
# ---------------------------------------------------------------------------


def evaluate_regression_gates(
    candidate_metrics: dict[str, Any],
    baseline_metrics: dict[str, Any],
    *,
    skip_delta_checks: bool = False,
) -> list[GateResult]:
    """Evaluate regression gates (step 1 in gate order).

    PRD Appendix D.1 step 1 + H.2.
    """
    results: list[GateResult] = []

    for gate_def in REGRESSION_GATES:
        metric_path: str = gate_def["metric"]
        direction = METRIC_DIRECTIONS.get(metric_path, HIGHER_IS_BETTER)
        threshold_pct: float | None = gate_def["threshold_pct"]
        gate_type: str = gate_def["gate_type"]
        name: str = gate_def["name"]

        candidate_val = _extract_metric(candidate_metrics, metric_path)
        baseline_val = _extract_metric(baseline_metrics, metric_path)

        if candidate_val is None:
            results.append(GateResult(
                name=name, passed=False, value=None,
                baseline=baseline_val, threshold=threshold_pct,
                gate_type=gate_type,
                detail=f"Candidate missing metric: {metric_path}",
            ))
            continue

        if baseline_val is None:
            results.append(GateResult(
                name=name, passed=True, value=candidate_val,
                baseline=None, threshold=threshold_pct,
                gate_type=gate_type,
                detail=f"Baseline missing metric: {metric_path}, skipped",
            ))
            continue

        if skip_delta_checks and direction != EQUALS_ZERO:
            results.append(GateResult(
                name=name, passed=True, value=candidate_val,
                baseline=baseline_val, threshold=threshold_pct,
                gate_type=gate_type,
                detail="Skipped (baseline == candidate)",
            ))
            continue

        _degradation, delta_pct, violation = compare_metric(
            candidate_val, baseline_val, direction, threshold_pct,
        )

        results.append(GateResult(
            name=name, passed=not violation, value=candidate_val,
            baseline=baseline_val, threshold=threshold_pct,
            gate_type=gate_type,
            detail=f"delta_pct={delta_pct:.2f}%" if delta_pct is not None else "",
        ))

    return results


def evaluate_constraint_gates(
    candidate_metrics: dict[str, Any],
    *,
    config: dict[str, Any] | None = None,
) -> list[GateResult]:
    """Evaluate constraint violation gates (step 2 in gate order).

    PRD Appendix D.1 step 2.
    """
    config = config or {}
    results: list[GateResult] = []

    for gate_def in CONSTRAINT_GATES:
        metric_path: str = gate_def["metric"]
        name: str = gate_def["name"]
        gate_type: str = gate_def["gate_type"]

        candidate_val = _extract_metric(candidate_metrics, metric_path)
        if candidate_val is None:
            results.append(GateResult(
                name=name, passed=False, value=None,
                baseline=None, threshold=None,
                gate_type=gate_type,
                detail=f"Candidate missing metric: {metric_path}",
            ))
            continue

        if "threshold" in gate_def:
            threshold = gate_def["threshold"]
            passed = candidate_val <= threshold
            results.append(GateResult(
                name=name, passed=passed, value=candidate_val,
                baseline=None, threshold=float(threshold),
                gate_type=gate_type,
            ))
        elif "config_key" in gate_def:
            config_key: str = gate_def["config_key"]
            default: float = gate_def["default_threshold"]
            threshold = config.get(config_key, default)
            # For max_drawdown, value is negative; threshold should be negative
            if metric_path == "performance.max_drawdown":
                passed = candidate_val >= threshold  # e.g. -0.05 >= -1.0
            else:
                passed = candidate_val <= threshold
            results.append(GateResult(
                name=name, passed=passed, value=candidate_val,
                baseline=None, threshold=float(threshold),
                gate_type=gate_type,
            ))

    return results


def evaluate_artifact_gates(
    experiment_path: str | None,
) -> list[GateResult]:
    """Evaluate artifact presence gates (step 5 in gate order).

    PRD Appendix D.1 step 5.
    """
    from pathlib import Path

    results: list[GateResult] = []
    required_artifacts = ["spec/experiment_spec.json", "evaluation/metrics.json"]

    if experiment_path is None:
        results.append(GateResult(
            name="artifact_presence", passed=False, value=None,
            baseline=None, threshold=None, gate_type="hard",
            detail="No experiment path provided",
        ))
        return results

    exp_dir = Path(experiment_path)
    missing = []
    for artifact in required_artifacts:
        if not (exp_dir / artifact).exists():
            missing.append(artifact)

    passed = len(missing) == 0
    detail = f"Missing: {', '.join(missing)}" if missing else ""
    results.append(GateResult(
        name="artifact_presence", passed=passed, value=None,
        baseline=None, threshold=None, gate_type="hard",
        detail=detail,
    ))

    return results
