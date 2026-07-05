"""Tests for shadow_run Dagster job — promotion gate."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from dagster import build_op_context

from pipelines.yats_pipelines.jobs.shadow_run import (
    ShadowRunConfig,
    _is_promoted_or_allowlisted,
    load_shadow_spec,
)


# ---------------------------------------------------------------------------
# _is_promoted_or_allowlisted
# ---------------------------------------------------------------------------


class TestIsPromotedOrAllowlisted:
    def test_unpromoted_no_allowlist(self, tmp_path):
        assert not _is_promoted_or_allowlisted("exp_xyz", tmp_path)

    def test_candidate_tier_passes(self, tmp_path):
        (tmp_path / "promotions" / "candidate").mkdir(parents=True)
        (tmp_path / "promotions" / "candidate" / "exp_xyz.json").write_text("{}")
        assert _is_promoted_or_allowlisted("exp_xyz", tmp_path)

    def test_production_tier_passes(self, tmp_path):
        (tmp_path / "promotions" / "production").mkdir(parents=True)
        (tmp_path / "promotions" / "production" / "exp_xyz.json").write_text("{}")
        assert _is_promoted_or_allowlisted("exp_xyz", tmp_path)

    def test_research_tier_rejected(self, tmp_path):
        (tmp_path / "promotions" / "research").mkdir(parents=True)
        (tmp_path / "promotions" / "research" / "exp_xyz.json").write_text("{}")
        assert not _is_promoted_or_allowlisted("exp_xyz", tmp_path)

    def test_allowlist_passes(self, tmp_path, monkeypatch):
        allowlist_path = tmp_path / "shadow_allowlist.yml"
        allowlist_path.write_text("allowlist:\n  - exp_xyz\n")
        monkeypatch.chdir(tmp_path)
        # configs/ subdir expected by the function
        (tmp_path / "configs").mkdir()
        (tmp_path / "configs" / "shadow_allowlist.yml").write_text(
            "allowlist:\n  - exp_xyz\n"
        )
        assert _is_promoted_or_allowlisted("exp_xyz", tmp_path)

    def test_allowlist_other_experiment_rejected(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "configs").mkdir()
        (tmp_path / "configs" / "shadow_allowlist.yml").write_text(
            "allowlist:\n  - other_exp\n"
        )
        assert not _is_promoted_or_allowlisted("exp_xyz", tmp_path)


# ---------------------------------------------------------------------------
# load_shadow_spec — gate enforcement
# ---------------------------------------------------------------------------


def _fake_get(experiment_id, *, data_root):
    return {"spec": {"policy": "equal_weight", "experiment_name": experiment_id}}


class TestLoadShadowSpecGate:
    def test_gate_rejects_unpromoted(self, tmp_path):
        ctx = build_op_context()
        config = ShadowRunConfig(
            experiment_id="exp_xyz",
            data_root=str(tmp_path),
            qualification_replay=False,
        )
        with patch("pipelines.yats_pipelines.jobs.shadow_run._is_promoted_or_allowlisted", return_value=False):
            with pytest.raises(ValueError, match="Shadow run blocked"):
                load_shadow_spec(ctx, config)

    def test_gate_passes_promoted(self, tmp_path):
        ctx = build_op_context()
        config = ShadowRunConfig(
            experiment_id="exp_xyz",
            data_root=str(tmp_path),
            qualification_replay=False,
        )
        with patch("pipelines.yats_pipelines.jobs.shadow_run._is_promoted_or_allowlisted", return_value=True):
            with patch("research.experiments.registry.get", _fake_get):
                result = load_shadow_spec(ctx, config)
        assert result["policy"] == "equal_weight"

    def test_qualification_replay_bypasses_gate(self, tmp_path):
        ctx = build_op_context()
        config = ShadowRunConfig(
            experiment_id="exp_xyz",
            data_root=str(tmp_path),
            qualification_replay=True,
        )
        # No promotion record exists — gate should be bypassed
        with patch("research.experiments.registry.get", _fake_get):
            result = load_shadow_spec(ctx, config)
        assert result["policy"] == "equal_weight"
