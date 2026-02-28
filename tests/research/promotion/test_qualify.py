"""Tests for research.promotion.qualify — main qualification runner."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from research.promotion.qualify import (
    load_experiment_metrics,
    run_qualification,
)
from research.promotion.report import QualificationReport


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _write_metrics(exp_dir: Path, metrics: dict) -> None:
    """Write metrics.json for a test experiment."""
    eval_dir = exp_dir / "evaluation"
    eval_dir.mkdir(parents=True, exist_ok=True)
    (eval_dir / "metrics.json").write_text(json.dumps(metrics))

    spec_dir = exp_dir / "spec"
    spec_dir.mkdir(parents=True, exist_ok=True)
    (spec_dir / "experiment_spec.json").write_text(json.dumps({
        "experiment_name": "test",
        "symbols": ["AAPL"],
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "feature_set": "core_v1",
        "policy": "equal_weight",
        "hierarchy_enabled": False,
    }))


def _good_metrics(sharpe=1.5, max_drawdown=-0.05, turnover=0.05):
    return {
        "performance": {
            "sharpe": sharpe,
            "calmar": 2.0,
            "sortino": 2.5,
            "total_return": 0.15,
            "annualized_return": 0.15,
            "max_drawdown": max_drawdown,
            "max_drawdown_duration": 10,
        },
        "trading": {
            "turnover_1d_mean": turnover,
            "turnover_1d_std": 0.01,
            "avg_holding_period": 20.0,
            "win_rate": 0.55,
            "profit_factor": 1.5,
        },
        "safety": {
            "nan_inf_violations": 0,
            "constraint_violations": 0,
        },
        "performance_by_regime": {},
        "regime": {},
    }


def _good_execution():
    return {
        "fill_rate": 0.995,
        "reject_rate": 0.005,
        "avg_slippage_bps": 8.0,
        "p95_slippage_bps": 15.0,
        "total_fees": 1000.0,
        "total_turnover": 5.0,
        "execution_halts": 0,
        "max_drawdown": -0.03,
    }


# ---------------------------------------------------------------------------
# load_experiment_metrics
# ---------------------------------------------------------------------------


class TestLoadMetrics:
    def test_load_success(self, tmp_path):
        exp_dir = tmp_path / "experiments" / "exp_123"
        _write_metrics(exp_dir, _good_metrics())
        metrics = load_experiment_metrics("exp_123", data_root=tmp_path)
        assert metrics["performance"]["sharpe"] == 1.5

    def test_load_missing(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_experiment_metrics("nonexistent", data_root=tmp_path)


# ---------------------------------------------------------------------------
# run_qualification
# ---------------------------------------------------------------------------


class TestRunQualification:
    @patch("research.promotion.qualify.load_execution_metrics")
    def test_pass_scenario(self, mock_exec, tmp_path):
        """All gates pass with good metrics."""
        mock_exec.return_value = _good_execution()

        candidate_dir = tmp_path / "experiments" / "candidate_1"
        baseline_dir = tmp_path / "experiments" / "baseline_1"
        _write_metrics(candidate_dir, _good_metrics(sharpe=1.6))
        _write_metrics(baseline_dir, _good_metrics(sharpe=1.5))

        report = run_qualification(
            "candidate_1", "baseline_1",
            data_root=tmp_path,
            dagster_run_id="test-run-1",
        )

        assert isinstance(report, QualificationReport)
        assert report.passed
        assert report.experiment_id == "candidate_1"
        assert report.baseline_id == "baseline_1"

        # Report file should exist
        report_path = (
            tmp_path / "experiments" / "candidate_1"
            / "promotion" / "qualification_report.json"
        )
        assert report_path.exists()

    @patch("research.promotion.qualify.load_execution_metrics")
    def test_fail_sharpe_regression(self, mock_exec, tmp_path):
        """Sharpe regression exceeds 5% threshold."""
        mock_exec.return_value = _good_execution()

        candidate_dir = tmp_path / "experiments" / "candidate_2"
        baseline_dir = tmp_path / "experiments" / "baseline_2"
        _write_metrics(candidate_dir, _good_metrics(sharpe=0.5))
        _write_metrics(baseline_dir, _good_metrics(sharpe=1.5))

        report = run_qualification(
            "candidate_2", "baseline_2",
            data_root=tmp_path,
        )

        assert not report.passed

    @patch("research.promotion.qualify.load_execution_metrics")
    def test_baseline_equals_candidate(self, mock_exec, tmp_path):
        """baseline==candidate: delta checks skipped, warning added."""
        mock_exec.return_value = _good_execution()

        exp_dir = tmp_path / "experiments" / "same_exp"
        _write_metrics(exp_dir, _good_metrics())

        report = run_qualification(
            "same_exp", "same_exp",
            data_root=tmp_path,
        )

        assert report.passed
        assert "baseline_is_candidate" in report.warnings

    @patch("research.promotion.qualify.load_execution_metrics")
    @patch("research.promotion.qualify.trigger_qualification_replay")
    def test_no_execution_triggers_replay(self, mock_replay, mock_exec, tmp_path):
        """No execution evidence triggers qualification replay."""
        mock_exec.return_value = None  # No execution metrics
        mock_replay.return_value = _good_execution()

        candidate_dir = tmp_path / "experiments" / "candidate_3"
        baseline_dir = tmp_path / "experiments" / "baseline_3"
        _write_metrics(candidate_dir, _good_metrics())
        _write_metrics(baseline_dir, _good_metrics())

        report = run_qualification(
            "candidate_3", "baseline_3",
            data_root=tmp_path,
        )

        mock_replay.assert_called_once()
        assert "qualification_replay_triggered" in report.warnings

    @patch("research.promotion.qualify.load_execution_metrics")
    def test_nan_inf_hard_fail(self, mock_exec, tmp_path):
        """NaN/Inf violations cause hard failure."""
        mock_exec.return_value = _good_execution()

        bad_metrics = _good_metrics()
        bad_metrics["safety"]["nan_inf_violations"] = 5
        candidate_dir = tmp_path / "experiments" / "candidate_4"
        baseline_dir = tmp_path / "experiments" / "baseline_4"
        _write_metrics(candidate_dir, bad_metrics)
        _write_metrics(baseline_dir, _good_metrics())

        report = run_qualification(
            "candidate_4", "baseline_4",
            data_root=tmp_path,
        )

        assert not report.passed

    @patch("research.promotion.qualify.load_execution_metrics")
    def test_execution_halt_hard_fail(self, mock_exec, tmp_path):
        """Execution halt causes hard failure."""
        bad_exec = _good_execution()
        bad_exec["execution_halts"] = 1
        mock_exec.return_value = bad_exec

        candidate_dir = tmp_path / "experiments" / "candidate_5"
        baseline_dir = tmp_path / "experiments" / "baseline_5"
        _write_metrics(candidate_dir, _good_metrics())
        _write_metrics(baseline_dir, _good_metrics())

        report = run_qualification(
            "candidate_5", "baseline_5",
            data_root=tmp_path,
        )

        assert not report.passed
