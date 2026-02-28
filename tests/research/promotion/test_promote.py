"""Tests for research.promotion.promote — tiered promotion pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from research.promotion.promote import (
    PromotionError,
    PromotionRecord,
    check_baseline_integrity,
    check_data_drift,
    check_immutability,
    check_production_approval,
    check_qualification_passed,
    check_risk_override_block,
    check_tier_ordering,
    promote,
    write_promotion_record,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _setup_experiment(data_root: Path, experiment_id: str, **spec_overrides) -> Path:
    """Create a minimal experiment directory with spec and metrics."""
    exp_dir = data_root / "experiments" / experiment_id
    spec_dir = exp_dir / "spec"
    spec_dir.mkdir(parents=True, exist_ok=True)

    spec = {
        "experiment_name": "test",
        "symbols": ["AAPL"],
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "feature_set": "core_v1",
        "policy": "equal_weight",
        "policy_params": {},
        "cost_config": {"transaction_cost_bp": 5.0},
        "seed": 42,
        "interval": "daily",
        **spec_overrides,
    }
    (spec_dir / "experiment_spec.json").write_text(json.dumps(spec))

    eval_dir = exp_dir / "evaluation"
    eval_dir.mkdir(parents=True, exist_ok=True)
    metrics = {
        "performance": {"sharpe": 1.5, "max_drawdown": -0.05},
        "trading": {"turnover_1d_mean": 0.05},
    }
    (eval_dir / "metrics.json").write_text(json.dumps(metrics))

    return exp_dir


def _write_qual_report(
    data_root: Path, experiment_id: str,
    passed: bool = True,
    baseline_id: str = "baseline_1",
    warnings: list | None = None,
) -> Path:
    """Write a qualification report."""
    report_path = (
        data_root / "experiments" / experiment_id
        / "promotion" / "qualification_report.json"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "experiment_id": experiment_id,
        "baseline_id": baseline_id,
        "passed": passed,
        "hard_gates": [],
        "soft_gates": [],
        "execution_gates": [],
        "regime_gates": [],
        "warnings": warnings or [],
        "timestamp": "2024-01-01T00:00:00+00:00",
    }
    report_path.write_text(json.dumps(report))
    return report_path


def _write_promotion_record(
    data_root: Path, experiment_id: str, tier: str,
) -> Path:
    """Write a minimal promotion record."""
    record_path = data_root / "promotions" / tier / f"{experiment_id}.json"
    record_path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "experiment_id": experiment_id,
        "tier": tier,
        "qualification_report_path": f"experiments/{experiment_id}/promotion/qualification_report.json",
        "spec_path": f"experiments/{experiment_id}/spec/experiment_spec.json",
        "metrics_path": f"experiments/{experiment_id}/evaluation/metrics.json",
        "promotion_reason": "test",
        "promoted_at": "2024-01-01T00:00:00+00:00",
        "promoted_by": "test",
        "dagster_run_id": "test-run",
    }
    record_path.write_text(json.dumps(record))
    return record_path


# ---------------------------------------------------------------------------
# check_qualification_passed
# ---------------------------------------------------------------------------


class TestCheckQualificationPassed:
    def test_passed(self, tmp_path):
        _setup_experiment(tmp_path, "exp1")
        _write_qual_report(tmp_path, "exp1", passed=True)
        report = check_qualification_passed("exp1", data_root=tmp_path)
        assert report["passed"] is True

    def test_failed(self, tmp_path):
        _setup_experiment(tmp_path, "exp1")
        _write_qual_report(tmp_path, "exp1", passed=False)
        with pytest.raises(PromotionError, match="Qualification FAILED"):
            check_qualification_passed("exp1", data_root=tmp_path)

    def test_missing_report(self, tmp_path):
        with pytest.raises(PromotionError, match="No qualification report"):
            check_qualification_passed("no_exp", data_root=tmp_path)


# ---------------------------------------------------------------------------
# check_tier_ordering
# ---------------------------------------------------------------------------


class TestCheckTierOrdering:
    def test_research_always_allowed(self, tmp_path):
        check_tier_ordering("exp1", "research", data_root=tmp_path)

    def test_candidate_requires_research(self, tmp_path):
        with pytest.raises(PromotionError, match="missing research"):
            check_tier_ordering("exp1", "candidate", data_root=tmp_path)

    def test_candidate_with_research(self, tmp_path):
        _write_promotion_record(tmp_path, "exp1", "research")
        check_tier_ordering("exp1", "candidate", data_root=tmp_path)

    def test_production_requires_candidate(self, tmp_path):
        _write_promotion_record(tmp_path, "exp1", "research")
        with pytest.raises(PromotionError, match="missing candidate"):
            check_tier_ordering("exp1", "production", data_root=tmp_path)

    def test_production_with_candidate(self, tmp_path):
        _write_promotion_record(tmp_path, "exp1", "research")
        _write_promotion_record(tmp_path, "exp1", "candidate")
        check_tier_ordering("exp1", "production", data_root=tmp_path)

    def test_invalid_tier(self, tmp_path):
        with pytest.raises(PromotionError, match="Invalid tier"):
            check_tier_ordering("exp1", "invalid", data_root=tmp_path)


# ---------------------------------------------------------------------------
# check_risk_override_block
# ---------------------------------------------------------------------------


class TestCheckRiskOverrideBlock:
    def test_research_allowed_with_overrides(self, tmp_path):
        _setup_experiment(tmp_path, "exp1", risk_overrides={"max_drawdown": -0.10})
        check_risk_override_block("exp1", "research", data_root=tmp_path)

    def test_candidate_blocked_with_overrides(self, tmp_path):
        _setup_experiment(tmp_path, "exp1", risk_overrides={"max_drawdown": -0.10})
        with pytest.raises(PromotionError, match="risk_overrides"):
            check_risk_override_block("exp1", "candidate", data_root=tmp_path)

    def test_production_blocked_with_overrides(self, tmp_path):
        _setup_experiment(tmp_path, "exp1", risk_overrides={"max_drawdown": -0.10})
        with pytest.raises(PromotionError, match="risk_overrides"):
            check_risk_override_block("exp1", "production", data_root=tmp_path)

    def test_no_overrides_allowed(self, tmp_path):
        _setup_experiment(tmp_path, "exp1")
        check_risk_override_block("exp1", "candidate", data_root=tmp_path)


# ---------------------------------------------------------------------------
# check_baseline_integrity
# ---------------------------------------------------------------------------


class TestCheckBaselineIntegrity:
    def test_research_always_allowed(self):
        report = {"baseline_id": "exp1"}
        check_baseline_integrity("exp1", "research", qualification_report=report)

    def test_candidate_blocked_self_baseline(self):
        report = {"baseline_id": "exp1"}
        with pytest.raises(PromotionError, match="baseline_id == candidate_id"):
            check_baseline_integrity("exp1", "candidate", qualification_report=report)

    def test_candidate_allowed_different_baseline(self):
        report = {"baseline_id": "baseline_1"}
        check_baseline_integrity("exp1", "candidate", qualification_report=report)


# ---------------------------------------------------------------------------
# check_data_drift
# ---------------------------------------------------------------------------


class TestCheckDataDrift:
    def test_no_drift_allowed(self):
        report = {"warnings": []}
        check_data_drift("exp1", "production", qualification_report=report)

    def test_drift_without_ack_blocked(self):
        report = {"warnings": ["data_drift"]}
        with pytest.raises(PromotionError, match="data_drift"):
            check_data_drift(
                "exp1", "production",
                qualification_report=report,
                managing_partner_ack=False,
            )

    def test_drift_with_ack_allowed(self):
        report = {"warnings": ["data_drift"]}
        check_data_drift(
            "exp1", "production",
            qualification_report=report,
            managing_partner_ack=True,
        )

    def test_non_production_ignores_drift(self):
        report = {"warnings": ["data_drift"]}
        check_data_drift("exp1", "candidate", qualification_report=report)


# ---------------------------------------------------------------------------
# check_production_approval
# ---------------------------------------------------------------------------


class TestCheckProductionApproval:
    def test_production_requires_ack(self):
        with pytest.raises(PromotionError, match="managing_partner approval"):
            check_production_approval("production", managing_partner_ack=False)

    def test_production_with_ack(self):
        check_production_approval("production", managing_partner_ack=True)

    def test_non_production_no_ack_needed(self):
        check_production_approval("research", managing_partner_ack=False)
        check_production_approval("candidate", managing_partner_ack=False)


# ---------------------------------------------------------------------------
# check_immutability
# ---------------------------------------------------------------------------


class TestCheckImmutability:
    def test_no_existing_record(self, tmp_path):
        record = PromotionRecord(
            experiment_id="exp1", tier="research",
            qualification_report_path="qr.json",
            spec_path="spec.json", metrics_path="metrics.json",
            promotion_reason="test", promoted_at="now",
            promoted_by="test", dagster_run_id="run1",
        )
        check_immutability(record, data_root=tmp_path)

    def test_same_content_idempotent(self, tmp_path):
        exp_root = tmp_path / "experiments" / "exp1"
        record = PromotionRecord(
            experiment_id="exp1", tier="research",
            qualification_report_path=str(exp_root / "promotion" / "qualification_report.json"),
            spec_path=str(exp_root / "spec" / "experiment_spec.json"),
            metrics_path=str(exp_root / "evaluation" / "metrics.json"),
            promotion_reason="test", promoted_at="now",
            promoted_by="test", dagster_run_id="run1",
        )
        write_promotion_record(record, data_root=tmp_path)
        # Same content should not raise
        check_immutability(record, data_root=tmp_path)

    def test_different_content_errors(self, tmp_path):
        exp_root = tmp_path / "experiments" / "exp1"
        record1 = PromotionRecord(
            experiment_id="exp1", tier="research",
            qualification_report_path=str(exp_root / "promotion" / "qualification_report.json"),
            spec_path=str(exp_root / "spec" / "experiment_spec.json"),
            metrics_path=str(exp_root / "evaluation" / "metrics.json"),
            promotion_reason="test", promoted_at="now",
            promoted_by="test", dagster_run_id="run1",
        )
        write_promotion_record(record1, data_root=tmp_path)

        record2 = PromotionRecord(
            experiment_id="exp1", tier="research",
            qualification_report_path="different_path.json",
            spec_path=str(exp_root / "spec" / "experiment_spec.json"),
            metrics_path=str(exp_root / "evaluation" / "metrics.json"),
            promotion_reason="test", promoted_at="later",
            promoted_by="test", dagster_run_id="run2",
        )
        with pytest.raises(PromotionError, match="Immutability violation"):
            check_immutability(record2, data_root=tmp_path)


# ---------------------------------------------------------------------------
# promote (integration)
# ---------------------------------------------------------------------------


class TestPromoteIntegration:
    @patch("research.promotion.promote.write_promotions_row")
    @patch("research.promotion.promote.update_experiment_index_tier")
    def test_research_promotion(self, mock_index, mock_qdb, tmp_path):
        """Full research promotion with all gates passing."""
        _setup_experiment(tmp_path, "exp1")
        _write_qual_report(tmp_path, "exp1", passed=True, baseline_id="baseline_1")
        _setup_experiment(tmp_path, "baseline_1")

        record = promote(
            "exp1", "research",
            promotion_reason="good sharpe",
            data_root=tmp_path,
            dagster_run_id="test-run",
        )

        assert record.experiment_id == "exp1"
        assert record.tier == "research"
        assert record.promotion_reason == "good sharpe"

        # Filesystem record should exist
        record_path = tmp_path / "promotions" / "research" / "exp1.json"
        assert record_path.exists()

        # QuestDB and index should have been called
        mock_qdb.assert_called_once()
        mock_index.assert_called_once()

    @patch("research.promotion.promote.write_promotions_row")
    @patch("research.promotion.promote.update_experiment_index_tier")
    def test_candidate_promotion(self, mock_index, mock_qdb, tmp_path):
        """Candidate promotion requires prior research promotion."""
        _setup_experiment(tmp_path, "exp1")
        _write_qual_report(tmp_path, "exp1", passed=True, baseline_id="baseline_1")
        _write_promotion_record(tmp_path, "exp1", "research")

        record = promote(
            "exp1", "candidate",
            data_root=tmp_path,
        )

        assert record.tier == "candidate"

    @patch("research.promotion.promote.write_promotions_row")
    @patch("research.promotion.promote.update_experiment_index_tier")
    def test_production_requires_ack(self, mock_index, mock_qdb, tmp_path):
        """Production promotion requires managing_partner_ack."""
        _setup_experiment(tmp_path, "exp1")
        _write_qual_report(tmp_path, "exp1", passed=True, baseline_id="baseline_1")
        _write_promotion_record(tmp_path, "exp1", "research")
        _write_promotion_record(tmp_path, "exp1", "candidate")

        with pytest.raises(PromotionError, match="managing_partner approval"):
            promote("exp1", "production", data_root=tmp_path)

    @patch("research.promotion.promote.write_promotions_row")
    @patch("research.promotion.promote.update_experiment_index_tier")
    def test_production_with_ack(self, mock_index, mock_qdb, tmp_path):
        """Production promotion succeeds with ack."""
        _setup_experiment(tmp_path, "exp1")
        _write_qual_report(tmp_path, "exp1", passed=True, baseline_id="baseline_1")
        _write_promotion_record(tmp_path, "exp1", "research")
        _write_promotion_record(tmp_path, "exp1", "candidate")

        record = promote(
            "exp1", "production",
            data_root=tmp_path,
            managing_partner_ack=True,
        )

        assert record.tier == "production"

    def test_skip_tier_blocked(self, tmp_path):
        """Cannot skip from research to production."""
        _setup_experiment(tmp_path, "exp1")
        _write_qual_report(tmp_path, "exp1", passed=True, baseline_id="baseline_1")
        _write_promotion_record(tmp_path, "exp1", "research")

        with pytest.raises(PromotionError, match="missing candidate"):
            promote(
                "exp1", "production",
                data_root=tmp_path,
                managing_partner_ack=True,
            )

    def test_risk_override_blocked(self, tmp_path):
        """Experiments with risk_overrides blocked from candidate."""
        _setup_experiment(
            tmp_path, "exp1",
            risk_overrides={"max_drawdown": -0.10},
        )
        _write_qual_report(tmp_path, "exp1", passed=True, baseline_id="baseline_1")
        _write_promotion_record(tmp_path, "exp1", "research")

        with pytest.raises(PromotionError, match="risk_overrides"):
            promote("exp1", "candidate", data_root=tmp_path)

    @patch("research.promotion.promote.write_promotions_row")
    @patch("research.promotion.promote.update_experiment_index_tier")
    def test_immutability_idempotent(self, mock_index, mock_qdb, tmp_path):
        """Re-promoting same tier with same content is idempotent."""
        _setup_experiment(tmp_path, "exp1")
        _write_qual_report(tmp_path, "exp1", passed=True, baseline_id="baseline_1")

        promote("exp1", "research", data_root=tmp_path)
        # Second promote should succeed (idempotent)
        promote("exp1", "research", data_root=tmp_path)
