"""Tests for SAC trainer."""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from types import ModuleType

import numpy as np
import pytest

from research.envs.signal_weight_env import SignalWeightEnv, SignalWeightEnvConfig
from research.experiments.spec import CostConfig, ExperimentSpec


# ---------------------------------------------------------------------------
# Mock SB3 + gymnasium
# ---------------------------------------------------------------------------

def _install_mocks():
    """Install mock gymnasium and stable_baselines3 modules."""
    if "gymnasium" not in sys.modules:
        gym_mod = ModuleType("gymnasium")
        spaces_mod = ModuleType("gymnasium.spaces")

        class MockBox:
            def __init__(self, low, high, shape, dtype=np.float32):
                self.low = np.full(shape, low, dtype=dtype) if np.isscalar(low) else low
                self.high = np.full(shape, high, dtype=dtype) if np.isscalar(high) else high
                self.shape = shape
                self.dtype = dtype

            def sample(self):
                return np.random.uniform(self.low, self.high, size=self.shape).astype(self.dtype)

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

        class MockSAC:
            def __init__(self, policy, env, **kwargs):
                self.env = env
                self.kwargs = kwargs
                self._trained = False

            def learn(self, total_timesteps, **kwargs):
                self._trained = True
                return self

            def save(self, path):
                Path(path).parent.mkdir(parents=True, exist_ok=True)
                Path(f"{path}.zip").write_text("mock_sac_model")

            @classmethod
            def load(cls, path, env=None):
                mock = cls("MlpPolicy", env)
                mock._trained = True
                return mock

            def predict(self, obs, deterministic=False):
                n = self.env.action_space.shape[0]
                action = np.full(n, 1.0 / n, dtype=np.float32)
                return action, None

        sb3_mod.SAC = MockSAC
        sb3_callbacks.BaseCallback = object
        sb3_common.callbacks = sb3_callbacks
        sb3_mod.common = sb3_common
        sys.modules["stable_baselines3"] = sb3_mod
        sys.modules["stable_baselines3.common"] = sb3_common
        sys.modules["stable_baselines3.common.callbacks"] = sb3_callbacks
    else:
        # SB3 module exists but might not have SAC — add it
        sb3_mod = sys.modules["stable_baselines3"]
        if not hasattr(sb3_mod, "SAC"):
            class MockSAC:
                def __init__(self, policy, env, **kwargs):
                    self.env = env
                    self.kwargs = kwargs
                    self._trained = False

                def learn(self, total_timesteps, **kwargs):
                    self._trained = True
                    return self

                def save(self, path):
                    Path(path).parent.mkdir(parents=True, exist_ok=True)
                    Path(f"{path}.zip").write_text("mock_sac_model")

                @classmethod
                def load(cls, path, env=None):
                    mock = cls("MlpPolicy", env)
                    mock._trained = True
                    return mock

                def predict(self, obs, deterministic=False):
                    n = self.env.action_space.shape[0]
                    action = np.full(n, 1.0 / n, dtype=np.float32)
                    return action, None

            sb3_mod.SAC = MockSAC


_install_mocks()

from research.training.sac_trainer import SACTrainResult, train_sac, rollout_sac  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_row(symbols, close, timestamp="2024-01-01"):
    return {
        "timestamp": timestamp,
        "close": close,
        "open": close,
        "high": {s: v * 1.01 for s, v in close.items()},
        "low": {s: v * 0.99 for s, v in close.items()},
        "volume": {s: 1_000_000.0 for s in symbols},
    }


def _make_data(n_rows=20):
    symbols = ["AAPL", "MSFT"]
    return [
        _make_row(
            symbols,
            {"AAPL": 100.0 + i * 0.5, "MSFT": 200.0 + i},
            timestamp=f"2024-01-{i+1:02d}",
        )
        for i in range(n_rows)
    ]


def _make_spec(**overrides):
    defaults = dict(
        experiment_name="test_sac",
        symbols=("AAPL", "MSFT"),
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
        interval="daily",
        feature_set="core_v1",
        policy="sac",
        policy_params={
            "reward_version": "v1",
            "learning_rate": 3e-4,
            "batch_size": 256,
            "total_timesteps": 100,
        },
        cost_config=CostConfig(transaction_cost_bp=5.0),
        seed=42,
    )
    defaults.update(overrides)
    return ExperimentSpec(**defaults)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSACTrainer:
    def test_train_sac_returns_result(self, tmp_path):
        spec = _make_spec()
        data = _make_data()
        result = train_sac(spec, data, tmp_path / "runs", observation_columns=["close"])

        assert isinstance(result, SACTrainResult)
        assert result.total_timesteps == 100
        assert result.checkpoint_path == tmp_path / "runs" / "sac_checkpoint"

    def test_train_sac_saves_checkpoint(self, tmp_path):
        spec = _make_spec()
        data = _make_data()
        train_sac(spec, data, tmp_path / "runs", observation_columns=["close"])

        assert (tmp_path / "runs" / "sac_checkpoint.zip").exists()

    def test_train_sac_saves_metadata(self, tmp_path):
        spec = _make_spec()
        data = _make_data()
        train_sac(spec, data, tmp_path / "runs", observation_columns=["close"])

        meta_path = tmp_path / "runs" / "training_meta.json"
        assert meta_path.exists()

        meta = json.loads(meta_path.read_text())
        assert meta["algorithm"] == "SAC"
        assert meta["total_timesteps"] == 100
        assert meta["seed"] == 42

    def test_train_sac_with_v2_reward(self, tmp_path):
        spec = _make_spec(policy_params={
            "reward_version": "v2",
            "learning_rate": 3e-4,
            "total_timesteps": 100,
            "turnover_scale": 0.1,
        })
        data = _make_data()
        result = train_sac(spec, data, tmp_path / "runs", observation_columns=["close"])

        meta = json.loads((tmp_path / "runs" / "training_meta.json").read_text())
        assert meta["reward_version"] == "v2"

    def test_train_sac_hyperparams(self, tmp_path):
        spec = _make_spec(policy_params={
            "reward_version": "v1",
            "learning_rate": 1e-3,
            "batch_size": 512,
            "gamma": 0.95,
            "tau": 0.01,
            "total_timesteps": 50,
        })
        data = _make_data()
        train_sac(spec, data, tmp_path / "runs", observation_columns=["close"])

        meta = json.loads((tmp_path / "runs" / "training_meta.json").read_text())
        assert meta["hyperparameters"]["learning_rate"] == 1e-3
        assert meta["hyperparameters"]["batch_size"] == 512
        assert meta["hyperparameters"]["gamma"] == 0.95
        assert meta["hyperparameters"]["tau"] == 0.01

    def test_rollout_sac(self, tmp_path):
        spec = _make_spec()
        data = _make_data()

        train_sac(spec, data, tmp_path / "runs", observation_columns=["close"])
        checkpoint = tmp_path / "runs" / "sac_checkpoint"

        weights, rewards, infos = rollout_sac(
            spec, data, checkpoint, observation_columns=["close"],
        )

        assert len(weights) > 0
        assert len(rewards) == len(weights)
        assert all(isinstance(w, np.ndarray) for w in weights)

    def test_sac_variant_policy(self, tmp_path):
        """Test that sac_* policy variants work."""
        spec = _make_spec(policy="sac_entropy_tuned", policy_params={
            "reward_version": "v1",
            "learning_rate": 3e-4,
            "total_timesteps": 100,
            "ent_coef": "auto",
        })
        data = _make_data()
        result = train_sac(spec, data, tmp_path / "runs", observation_columns=["close"])

        assert isinstance(result, SACTrainResult)

    def test_training_info_in_result(self, tmp_path):
        spec = _make_spec()
        data = _make_data()
        result = train_sac(spec, data, tmp_path / "runs", observation_columns=["close"])

        assert result.training_info["algorithm"] == "SAC"
        assert "hyperparameters" in result.training_info
