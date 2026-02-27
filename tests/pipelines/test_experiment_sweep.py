"""Tests for experiment_sweep Dagster job ops."""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import numpy as np
import pytest


# ---------------------------------------------------------------------------
# Mocks
# ---------------------------------------------------------------------------

def _install_mocks():
    if "gymnasium" not in sys.modules:
        gym_mod = ModuleType("gymnasium")
        spaces_mod = ModuleType("gymnasium.spaces")
        class MockBox:
            def __init__(self, low, high, shape, dtype=np.float32):
                self.shape = shape
                self.dtype = dtype
            def sample(self):
                return np.zeros(self.shape, dtype=self.dtype)
            def contains(self, x):
                return True
        spaces_mod.Box = MockBox
        gym_mod.spaces = spaces_mod
        sys.modules["gymnasium"] = gym_mod
        sys.modules["gymnasium.spaces"] = spaces_mod

    if "stable_baselines3" not in sys.modules:
        sb3_mod = ModuleType("stable_baselines3")
        sb3_common = ModuleType("stable_baselines3.common")
        sb3_callbacks = ModuleType("stable_baselines3.common.callbacks")
        class MockModel:
            def __init__(self, policy, env, **kwargs): pass
            def learn(self, total_timesteps, **kwargs): return self
            def save(self, path): Path(f"{path}.zip").parent.mkdir(parents=True, exist_ok=True); Path(f"{path}.zip").write_text("m")
            @classmethod
            def load(cls, path, env=None): return cls("MlpPolicy", env)
            def predict(self, obs, deterministic=False): return np.array([0.5]), None
        sb3_mod.PPO = MockModel
        sb3_mod.SAC = MockModel
        sb3_callbacks.BaseCallback = object
        sb3_common.callbacks = sb3_callbacks
        sb3_mod.common = sb3_common
        sys.modules["stable_baselines3"] = sb3_mod
        sys.modules["stable_baselines3.common"] = sb3_common
        sys.modules["stable_baselines3.common.callbacks"] = sb3_callbacks

_install_mocks()

from pipelines.yats_pipelines.jobs.experiment_sweep import (
    _expand_grid,
    _set_nested,
)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestExpandGrid:
    def test_single_param(self):
        base = {
            "experiment_name": "sweep_test",
            "policy_params": {"learning_rate": 0.001},
        }
        grid = {"policy_params.learning_rate": [1e-4, 3e-4, 1e-3]}

        specs = _expand_grid(base, grid)

        assert len(specs) == 3
        lrs = [s["policy_params"]["learning_rate"] for s in specs]
        assert sorted(lrs) == sorted([1e-4, 3e-4, 1e-3])

    def test_two_params_cartesian(self):
        base = {
            "experiment_name": "sweep",
            "policy_params": {"learning_rate": 0.001},
            "seed": 42,
        }
        grid = {
            "policy_params.learning_rate": [1e-4, 1e-3],
            "seed": [42, 123],
        }

        specs = _expand_grid(base, grid)

        assert len(specs) == 4  # 2 * 2

    def test_empty_grid(self):
        base = {"experiment_name": "no_sweep"}
        specs = _expand_grid(base, {})

        assert len(specs) == 1
        assert specs[0]["experiment_name"] == "no_sweep"

    def test_unique_experiment_names(self):
        base = {"experiment_name": "sweep"}
        grid = {"seed": [1, 2, 3]}

        specs = _expand_grid(base, grid)

        names = [s["experiment_name"] for s in specs]
        assert len(names) == len(set(names))  # All unique

    def test_nested_key_override(self):
        base = {
            "experiment_name": "test",
            "policy_params": {
                "learning_rate": 0.001,
                "clip_range": 0.2,
            },
        }
        grid = {"policy_params.clip_range": [0.1, 0.2, 0.3]}

        specs = _expand_grid(base, grid)

        assert len(specs) == 3
        # learning_rate should be preserved
        for s in specs:
            assert s["policy_params"]["learning_rate"] == 0.001

    def test_three_params_cartesian(self):
        base = {"experiment_name": "s"}
        grid = {
            "seed": [1, 2],
            "policy_params.lr": [0.01, 0.001],
            "policy_params.batch": [32, 64],
        }

        specs = _expand_grid(base, grid)
        assert len(specs) == 8  # 2 * 2 * 2


class TestSetNested:
    def test_top_level(self):
        d = {"a": 1}
        _set_nested(d, "a", 2)
        assert d["a"] == 2

    def test_nested_one_level(self):
        d = {"a": {"b": 1}}
        _set_nested(d, "a.b", 2)
        assert d["a"]["b"] == 2

    def test_nested_creates_intermediate(self):
        d = {}
        _set_nested(d, "a.b.c", 42)
        assert d["a"]["b"]["c"] == 42

    def test_preserves_siblings(self):
        d = {"a": {"b": 1, "c": 2}}
        _set_nested(d, "a.b", 99)
        assert d["a"]["b"] == 99
        assert d["a"]["c"] == 2


class TestSweepConfigParsing:
    def test_json_sweep_config(self, tmp_path):
        """Test that sweep config can be loaded from JSON."""
        sweep = {
            "base_spec": {
                "experiment_name": "json_sweep",
                "symbols": ["AAPL"],
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
                "interval": "daily",
                "feature_set": "core_v1",
                "policy": "ppo",
                "policy_params": {"reward_version": "v1"},
                "cost_config": {"transaction_cost_bp": 5.0},
                "seed": 42,
            },
            "grid": {
                "seed": [42, 123],
                "policy_params.learning_rate": [1e-4, 3e-4],
            },
        }

        path = tmp_path / "sweep.json"
        path.write_text(json.dumps(sweep))

        loaded = json.loads(path.read_text())
        specs = _expand_grid(loaded["base_spec"], loaded["grid"])

        assert len(specs) == 4

    def test_yaml_sweep_config(self, tmp_path):
        """Test that sweep config can be loaded from YAML."""
        import yaml

        sweep = {
            "base_spec": {
                "experiment_name": "yaml_sweep",
                "seed": 42,
            },
            "grid": {
                "seed": [42, 123, 456],
            },
        }

        path = tmp_path / "sweep.yml"
        path.write_text(yaml.dump(sweep))

        loaded = yaml.safe_load(path.read_text())
        specs = _expand_grid(loaded["base_spec"], loaded["grid"])

        assert len(specs) == 3
