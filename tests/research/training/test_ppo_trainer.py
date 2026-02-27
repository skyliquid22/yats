"""Tests for PPO trainer."""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from research.envs.signal_weight_env import SignalWeightEnv, SignalWeightEnvConfig
from research.experiments.spec import CostConfig, ExperimentSpec


# ---------------------------------------------------------------------------
# Mock SB3 + gymnasium
# ---------------------------------------------------------------------------

def _install_mocks():
    """Install mock gymnasium and stable_baselines3 modules."""
    # Gymnasium mock
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

    # SB3 mock
    sb3_mod = ModuleType("stable_baselines3")
    sb3_common = ModuleType("stable_baselines3.common")
    sb3_callbacks = ModuleType("stable_baselines3.common.callbacks")

    class MockPPO:
        def __init__(self, policy, env, **kwargs):
            self.env = env
            self.kwargs = kwargs
            self._trained = False

        def learn(self, total_timesteps, **kwargs):
            self._trained = True
            return self

        def save(self, path):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(f"{path}.zip").write_text("mock_ppo_model")

        @classmethod
        def load(cls, path, env=None):
            mock = cls("MlpPolicy", env)
            mock._trained = True
            return mock

        def predict(self, obs, deterministic=False):
            n = self.env.action_space.shape[0]
            action = np.full(n, 1.0 / n, dtype=np.float32)
            return action, None

    sb3_mod.PPO = MockPPO
    sb3_callbacks.BaseCallback = object
    sb3_common.callbacks = sb3_callbacks
    sb3_mod.common = sb3_common
    sys.modules["stable_baselines3"] = sb3_mod
    sys.modules["stable_baselines3.common"] = sb3_common
    sys.modules["stable_baselines3.common.callbacks"] = sb3_callbacks


_install_mocks()

from research.training.ppo_trainer import PPOTrainResult, train_ppo, rollout_ppo, _infer_observation_columns  # noqa: E402


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
        experiment_name="test_ppo",
        symbols=("AAPL", "MSFT"),
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
        interval="daily",
        feature_set="core_v1",
        policy="ppo",
        policy_params={
            "reward_version": "v1",
            "learning_rate": 3e-4,
            "batch_size": 64,
            "n_epochs": 10,
            "clip_range": 0.2,
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


class TestPPOTrainer:
    def test_train_ppo_returns_result(self, tmp_path):
        spec = _make_spec()
        data = _make_data()
        result = train_ppo(spec, data, tmp_path / "runs", observation_columns=["close"])

        assert isinstance(result, PPOTrainResult)
        assert result.total_timesteps == 100
        assert result.checkpoint_path == tmp_path / "runs" / "ppo_checkpoint"

    def test_train_ppo_saves_checkpoint(self, tmp_path):
        spec = _make_spec()
        data = _make_data()
        result = train_ppo(spec, data, tmp_path / "runs", observation_columns=["close"])

        # Mock save creates a .zip file
        assert (tmp_path / "runs" / "ppo_checkpoint.zip").exists()

    def test_train_ppo_saves_metadata(self, tmp_path):
        spec = _make_spec()
        data = _make_data()
        train_ppo(spec, data, tmp_path / "runs", observation_columns=["close"])

        meta_path = tmp_path / "runs" / "training_meta.json"
        assert meta_path.exists()

        meta = json.loads(meta_path.read_text())
        assert meta["algorithm"] == "PPO"
        assert meta["total_timesteps"] == 100
        assert meta["seed"] == 42
        assert meta["reward_version"] == "v1"

    def test_train_ppo_with_v2_reward(self, tmp_path):
        spec = _make_spec(policy_params={
            "reward_version": "v2",
            "learning_rate": 3e-4,
            "total_timesteps": 100,
            "turnover_scale": 0.1,
        })
        data = _make_data()
        result = train_ppo(spec, data, tmp_path / "runs", observation_columns=["close"])

        meta = json.loads((tmp_path / "runs" / "training_meta.json").read_text())
        assert meta["reward_version"] == "v2"

    def test_train_ppo_hyperparams_passed(self, tmp_path):
        spec = _make_spec(policy_params={
            "reward_version": "v1",
            "learning_rate": 1e-3,
            "batch_size": 128,
            "n_epochs": 5,
            "clip_range": 0.1,
            "total_timesteps": 50,
        })
        data = _make_data()
        train_ppo(spec, data, tmp_path / "runs", observation_columns=["close"])

        meta = json.loads((tmp_path / "runs" / "training_meta.json").read_text())
        assert meta["hyperparameters"]["learning_rate"] == 1e-3
        assert meta["hyperparameters"]["batch_size"] == 128
        assert meta["hyperparameters"]["n_epochs"] == 5
        assert meta["hyperparameters"]["clip_range"] == 0.1

    def test_train_ppo_creates_output_dir(self, tmp_path):
        spec = _make_spec()
        data = _make_data()
        output_dir = tmp_path / "deep" / "nested" / "runs"
        train_ppo(spec, data, output_dir, observation_columns=["close"])

        assert output_dir.exists()

    def test_rollout_ppo(self, tmp_path):
        spec = _make_spec()
        data = _make_data()

        # Train first
        train_ppo(spec, data, tmp_path / "runs", observation_columns=["close"])
        checkpoint = tmp_path / "runs" / "ppo_checkpoint"

        # Rollout
        weights, rewards, infos = rollout_ppo(
            spec, data, checkpoint, observation_columns=["close"],
        )

        assert len(weights) > 0
        assert len(rewards) == len(weights)
        assert len(infos) == len(weights)
        assert all(isinstance(w, np.ndarray) for w in weights)

    def test_training_info_in_result(self, tmp_path):
        spec = _make_spec()
        data = _make_data()
        result = train_ppo(spec, data, tmp_path / "runs", observation_columns=["close"])

        assert result.training_info["algorithm"] == "PPO"
        assert "hyperparameters" in result.training_info


class TestInferObservationColumns:
    def test_infers_dict_columns(self):
        data = [
            {
                "timestamp": "2024-01-01",
                "close": {"AAPL": 100.0},
                "volume": {"AAPL": 1000.0},
                "market_vol": 0.15,  # scalar — not included
            }
        ]
        cols = _infer_observation_columns(data)
        assert "close" in cols
        assert "volume" in cols
        assert "market_vol" not in cols

    def test_empty_data_returns_close(self):
        assert _infer_observation_columns([]) == ["close"]

    def test_excludes_timestamp(self):
        data = [{"timestamp": "2024-01-01", "close": {"AAPL": 100.0}}]
        cols = _infer_observation_columns(data)
        assert "timestamp" not in cols
