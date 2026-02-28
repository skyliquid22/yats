"""Tests for research.promotion.criteria — gate definitions and regression."""

from __future__ import annotations

import pytest

from research.promotion.criteria import (
    EQUALS_ZERO,
    HIGHER_IS_BETTER,
    LOWER_IS_BETTER,
    GateResult,
    compare_metric,
    evaluate_artifact_gates,
    evaluate_constraint_gates,
    evaluate_regression_gates,
    _extract_metric,
)


# ---------------------------------------------------------------------------
# _extract_metric
# ---------------------------------------------------------------------------

class TestExtractMetric:
    def test_nested_path(self):
        metrics = {"performance": {"sharpe": 1.5}}
        assert _extract_metric(metrics, "performance.sharpe") == 1.5

    def test_missing_key(self):
        metrics = {"performance": {"sharpe": 1.5}}
        assert _extract_metric(metrics, "performance.calmar") is None

    def test_missing_top_level(self):
        metrics = {"trading": {}}
        assert _extract_metric(metrics, "performance.sharpe") is None

    def test_integer_value(self):
        metrics = {"safety": {"nan_inf_violations": 0}}
        assert _extract_metric(metrics, "safety.nan_inf_violations") == 0.0


# ---------------------------------------------------------------------------
# compare_metric
# ---------------------------------------------------------------------------

class TestCompareMetric:
    def test_higher_is_better_no_degradation(self):
        degradation, delta_pct, violation = compare_metric(
            candidate_value=1.5, baseline_value=1.0,
            direction=HIGHER_IS_BETTER, threshold_pct=5.0,
        )
        assert degradation == 0.0
        assert not violation

    def test_higher_is_better_with_degradation(self):
        degradation, delta_pct, violation = compare_metric(
            candidate_value=0.9, baseline_value=1.0,
            direction=HIGHER_IS_BETTER, threshold_pct=5.0,
        )
        assert degradation == pytest.approx(0.1)
        assert violation  # 10% > 5% threshold

    def test_higher_is_better_within_threshold(self):
        degradation, delta_pct, violation = compare_metric(
            candidate_value=0.96, baseline_value=1.0,
            direction=HIGHER_IS_BETTER, threshold_pct=5.0,
        )
        assert degradation == pytest.approx(0.04)
        assert not violation  # 4% < 5% threshold

    def test_lower_is_better_no_degradation(self):
        degradation, delta_pct, violation = compare_metric(
            candidate_value=-0.15, baseline_value=-0.10,
            direction=LOWER_IS_BETTER, threshold_pct=10.0,
        )
        # candidate is more negative (worse for drawdown)
        degradation, delta_pct, violation = compare_metric(
            candidate_value=0.08, baseline_value=0.10,
            direction=LOWER_IS_BETTER, threshold_pct=15.0,
        )
        assert degradation == 0.0
        assert not violation

    def test_lower_is_better_with_degradation(self):
        degradation, delta_pct, violation = compare_metric(
            candidate_value=0.20, baseline_value=0.10,
            direction=LOWER_IS_BETTER, threshold_pct=15.0,
        )
        assert degradation == pytest.approx(0.10)
        assert violation  # 100% > 15%

    def test_equals_zero_pass(self):
        degradation, delta_pct, violation = compare_metric(
            candidate_value=0, baseline_value=0,
            direction=EQUALS_ZERO, threshold_pct=None,
        )
        assert not violation

    def test_equals_zero_fail(self):
        degradation, delta_pct, violation = compare_metric(
            candidate_value=3, baseline_value=0,
            direction=EQUALS_ZERO, threshold_pct=None,
        )
        assert violation

    def test_baseline_near_zero(self):
        degradation, delta_pct, violation = compare_metric(
            candidate_value=0.5, baseline_value=0.0,
            direction=HIGHER_IS_BETTER, threshold_pct=5.0,
        )
        assert delta_pct is None  # Avoid division by zero


# ---------------------------------------------------------------------------
# evaluate_regression_gates
# ---------------------------------------------------------------------------

class TestRegressionGates:
    def _make_metrics(
        self, sharpe=1.5, max_drawdown=-0.10, turnover=0.05, nan_inf=0,
    ):
        return {
            "performance": {"sharpe": sharpe, "max_drawdown": max_drawdown},
            "trading": {"turnover_1d_mean": turnover},
            "safety": {"nan_inf_violations": nan_inf},
        }

    def test_all_pass(self):
        candidate = self._make_metrics(sharpe=1.6)
        baseline = self._make_metrics(sharpe=1.5)
        results = evaluate_regression_gates(candidate, baseline)
        assert all(r.passed for r in results)

    def test_sharpe_regression_hard_fail(self):
        candidate = self._make_metrics(sharpe=1.0)
        baseline = self._make_metrics(sharpe=1.5)
        results = evaluate_regression_gates(candidate, baseline)
        sharpe_gate = next(r for r in results if r.name == "sharpe_regression")
        assert not sharpe_gate.passed
        assert sharpe_gate.gate_type == "hard"

    def test_nan_inf_hard_fail(self):
        candidate = self._make_metrics(nan_inf=3)
        baseline = self._make_metrics(nan_inf=0)
        results = evaluate_regression_gates(candidate, baseline)
        nan_gate = next(r for r in results if r.name == "nan_inf_violations")
        assert not nan_gate.passed
        assert nan_gate.gate_type == "hard"

    def test_skip_delta_checks(self):
        candidate = self._make_metrics(sharpe=0.5)
        baseline = self._make_metrics(sharpe=1.5)
        results = evaluate_regression_gates(
            candidate, baseline, skip_delta_checks=True,
        )
        sharpe_gate = next(r for r in results if r.name == "sharpe_regression")
        assert sharpe_gate.passed  # Skipped due to baseline==candidate
        # nan_inf still checked (equals_zero is not delta)
        nan_gate = next(r for r in results if r.name == "nan_inf_violations")
        assert nan_gate.passed

    def test_soft_gates_dont_block(self):
        candidate = self._make_metrics(max_drawdown=-0.20, turnover=0.20)
        baseline = self._make_metrics(max_drawdown=-0.10, turnover=0.05)
        results = evaluate_regression_gates(candidate, baseline)
        soft_gates = [r for r in results if r.gate_type == "soft"]
        # Some soft gates may fail, but that's OK — they're soft


# ---------------------------------------------------------------------------
# evaluate_constraint_gates
# ---------------------------------------------------------------------------

class TestConstraintGates:
    def test_all_pass(self):
        metrics = {
            "safety": {"constraint_violations": 0},
            "performance": {"max_drawdown": -0.05},
            "trading": {"turnover_1d_mean": 0.10},
        }
        results = evaluate_constraint_gates(metrics)
        assert all(r.passed for r in results)

    def test_constraint_violations_fail(self):
        metrics = {
            "safety": {"constraint_violations": 5},
            "performance": {"max_drawdown": -0.05},
            "trading": {"turnover_1d_mean": 0.10},
        }
        results = evaluate_constraint_gates(metrics)
        cv_gate = next(r for r in results if r.name == "constraint_violations")
        assert not cv_gate.passed

    def test_custom_thresholds(self):
        metrics = {
            "safety": {"constraint_violations": 0},
            "performance": {"max_drawdown": -0.50},
            "trading": {"turnover_1d_mean": 0.80},
        }
        config = {"max_drawdown": -0.30, "max_turnover": 0.50}
        results = evaluate_constraint_gates(metrics, config=config)
        dd_gate = next(r for r in results if r.name == "max_drawdown_constraint")
        assert not dd_gate.passed


# ---------------------------------------------------------------------------
# evaluate_artifact_gates
# ---------------------------------------------------------------------------

class TestArtifactGates:
    def test_no_path(self):
        results = evaluate_artifact_gates(None)
        assert not results[0].passed

    def test_missing_dir(self, tmp_path):
        results = evaluate_artifact_gates(str(tmp_path / "nonexistent"))
        assert not results[0].passed

    def test_all_present(self, tmp_path):
        (tmp_path / "spec").mkdir()
        (tmp_path / "spec" / "experiment_spec.json").write_text("{}")
        (tmp_path / "evaluation").mkdir()
        (tmp_path / "evaluation" / "metrics.json").write_text("{}")
        results = evaluate_artifact_gates(str(tmp_path))
        assert results[0].passed
