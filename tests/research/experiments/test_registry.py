"""Tests for research.experiments.registry — filesystem registry + QuestDB index."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from research.experiments.spec import CostConfig, ExperimentSpec
from research.experiments.registry import (
    _derive_universe,
    _derive_universe_from_spec,
    create,
    exists,
    get,
    get_artifacts_path,
    list_experiments,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_spec(**overrides) -> ExperimentSpec:
    defaults = dict(
        experiment_name="test_exp",
        symbols=("AAPL", "MSFT"),
        start_date=date(2023, 1, 1),
        end_date=date(2024, 1, 1),
        interval="daily",
        feature_set="core_v1",
        policy="ppo",
        policy_params={"learning_rate": 0.001},
        cost_config=CostConfig(transaction_cost_bp=5.0),
        seed=42,
    )
    defaults.update(overrides)
    return ExperimentSpec(**defaults)


# ===================================================================
# create
# ===================================================================


class TestCreate:
    def test_creates_directory_structure(self, tmp_path: Path):
        spec = _make_spec()
        eid = create(spec, data_root=tmp_path)
        exp_dir = tmp_path / "experiments" / eid
        assert exp_dir.is_dir()
        for subdir in ("spec", "runs", "evaluation", "logs", "promotion"):
            assert (exp_dir / subdir).is_dir()

    def test_writes_spec_json(self, tmp_path: Path):
        spec = _make_spec()
        eid = create(spec, data_root=tmp_path)
        spec_path = tmp_path / "experiments" / eid / "spec" / "experiment_spec.json"
        assert spec_path.exists()
        data = json.loads(spec_path.read_text())
        assert data["experiment_name"] == "test_exp"
        assert data["experiment_id"] == eid
        assert data["feature_set"] == "core_v1"

    def test_returns_experiment_id(self, tmp_path: Path):
        spec = _make_spec()
        eid = create(spec, data_root=tmp_path)
        assert eid == spec.experiment_id
        assert len(eid) == 64

    def test_idempotent(self, tmp_path: Path):
        spec = _make_spec()
        eid1 = create(spec, data_root=tmp_path)
        eid2 = create(spec, data_root=tmp_path)
        assert eid1 == eid2

    def test_different_specs_different_dirs(self, tmp_path: Path):
        spec1 = _make_spec(seed=42)
        spec2 = _make_spec(seed=99)
        eid1 = create(spec1, data_root=tmp_path)
        eid2 = create(spec2, data_root=tmp_path)
        assert eid1 != eid2
        assert (tmp_path / "experiments" / eid1).is_dir()
        assert (tmp_path / "experiments" / eid2).is_dir()

    def test_spec_json_is_valid_json(self, tmp_path: Path):
        spec = _make_spec()
        eid = create(spec, data_root=tmp_path)
        spec_path = tmp_path / "experiments" / eid / "spec" / "experiment_spec.json"
        data = json.loads(spec_path.read_text())
        # Keys are sorted (we use sort_keys=True)
        keys = list(data.keys())
        assert keys == sorted(keys)


# ===================================================================
# get
# ===================================================================


class TestGet:
    def test_get_existing(self, tmp_path: Path):
        spec = _make_spec()
        eid = create(spec, data_root=tmp_path)
        result = get(eid, data_root=tmp_path)
        assert result["spec"]["experiment_name"] == "test_exp"
        assert result["spec"]["experiment_id"] == eid
        assert "spec" in result["artifacts"]
        assert result["path"] == tmp_path / "experiments" / eid

    def test_get_nonexistent(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError, match="Experiment not found"):
            get("nonexistent_id", data_root=tmp_path)

    def test_artifacts_contain_all_subdirs(self, tmp_path: Path):
        spec = _make_spec()
        eid = create(spec, data_root=tmp_path)
        result = get(eid, data_root=tmp_path)
        for subdir in ("spec", "runs", "evaluation", "logs", "promotion"):
            assert subdir in result["artifacts"]


# ===================================================================
# exists
# ===================================================================


class TestExists:
    def test_exists_true(self, tmp_path: Path):
        spec = _make_spec()
        eid = create(spec, data_root=tmp_path)
        assert exists(eid, data_root=tmp_path) is True

    def test_exists_false(self, tmp_path: Path):
        assert exists("nonexistent_id", data_root=tmp_path) is False


# ===================================================================
# get_artifacts_path
# ===================================================================


class TestGetArtifactsPath:
    def test_returns_path(self, tmp_path: Path):
        spec = _make_spec()
        eid = create(spec, data_root=tmp_path)
        path = get_artifacts_path(eid, data_root=tmp_path)
        assert path == tmp_path / "experiments" / eid

    def test_nonexistent_raises(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError, match="Experiment not found"):
            get_artifacts_path("nonexistent_id", data_root=tmp_path)


# ===================================================================
# list_experiments (filesystem)
# ===================================================================


class TestListExperiments:
    def test_empty(self, tmp_path: Path):
        result = list_experiments(data_root=tmp_path)
        assert result == []

    def test_lists_all(self, tmp_path: Path):
        spec1 = _make_spec(seed=1)
        spec2 = _make_spec(seed=2)
        create(spec1, data_root=tmp_path)
        create(spec2, data_root=tmp_path)
        result = list_experiments(data_root=tmp_path)
        assert len(result) == 2

    def test_filter_by_feature_set(self, tmp_path: Path):
        spec1 = _make_spec(seed=1, feature_set="core_v1")
        spec2 = _make_spec(seed=2, feature_set="core_v2")
        create(spec1, data_root=tmp_path)
        create(spec2, data_root=tmp_path)
        result = list_experiments(data_root=tmp_path, feature_set="core_v1")
        assert len(result) == 1
        assert result[0]["feature_set"] == "core_v1"

    def test_filter_by_policy_type(self, tmp_path: Path):
        spec1 = _make_spec(seed=1, policy="ppo")
        spec2 = _make_spec(seed=2, policy="sac")
        create(spec1, data_root=tmp_path)
        create(spec2, data_root=tmp_path)
        result = list_experiments(data_root=tmp_path, policy_type="sac")
        assert len(result) == 1
        assert result[0]["policy"] == "sac"

    def test_list_returns_summary_fields(self, tmp_path: Path):
        spec = _make_spec()
        create(spec, data_root=tmp_path)
        result = list_experiments(data_root=tmp_path)
        assert len(result) == 1
        entry = result[0]
        assert "experiment_id" in entry
        assert "experiment_name" in entry
        assert "feature_set" in entry
        assert "policy" in entry
        assert "universe" in entry
        assert "path" in entry


# ===================================================================
# Universe derivation
# ===================================================================


class TestDeriveUniverse:
    def test_small_symbol_list(self):
        assert _derive_universe({"symbols": ["AAPL", "MSFT"]}) == "AAPL,MSFT"

    def test_large_symbol_list(self):
        symbols = [f"SYM{i}" for i in range(20)]
        assert _derive_universe({"symbols": symbols}) == "custom_20"

    def test_from_spec(self):
        spec = _make_spec(symbols=("AAPL", "GOOG", "MSFT"))
        result = _derive_universe_from_spec(spec)
        assert result == "AAPL,GOOG,MSFT"

    def test_from_spec_large(self):
        symbols = tuple(f"SYM{i:03d}" for i in range(50))
        spec = _make_spec(symbols=symbols)
        result = _derive_universe_from_spec(spec)
        assert result == "custom_50"
