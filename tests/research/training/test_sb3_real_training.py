"""Real (non-mocked) integration tests for SB3 training.

These tests use the actual stable_baselines3 and gymnasium libraries — no mocks.
They validate that SB3EnvWrapper is accepted by SB3 as a proper gymnasium.Env
subclass, which is the class of bug that mocked tests cannot catch.

The test clears any gymnasium/SB3 mocks that earlier test modules may have
installed in sys.modules, then reimports with real libraries.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import numpy as np
import pytest


def _make_data(n_rows: int = 30) -> list[dict[str, Any]]:
    symbols = ["AAPL", "MSFT"]
    return [
        {
            "timestamp": f"2024-01-{i + 1:02d}",
            "close": {"AAPL": 100.0 + i, "MSFT": 200.0 + i * 2},
            "open": {"AAPL": 100.0 + i, "MSFT": 200.0 + i * 2},
            "high": {"AAPL": 101.0 + i, "MSFT": 202.0 + i * 2},
            "low": {"AAPL": 99.0 + i, "MSFT": 198.0 + i * 2},
            "volume": {"AAPL": 1_000_000.0, "MSFT": 2_000_000.0},
        }
        for i in range(n_rows)
    ]


def _clear_mocks_and_import():
    """Clear any mocked gymnasium/SB3 modules, return real imports."""
    for key in list(sys.modules):
        if key.startswith(("gymnasium", "stable_baselines3", "research.training.sb3_wrapper")):
            del sys.modules[key]

    import gymnasium  # noqa: F401  — ensures real gymnasium is now in sys.modules
    from stable_baselines3 import PPO, SAC
    from research.training.sb3_wrapper import SB3EnvWrapper
    from research.envs.signal_weight_env import SignalWeightEnv, SignalWeightEnvConfig
    from research.training.reward_registry import RewardAdapterEnv

    return gymnasium, PPO, SAC, SB3EnvWrapper, SignalWeightEnv, SignalWeightEnvConfig, RewardAdapterEnv


class TestRealSB3Training:
    """Real SB3 integration tests — validate SB3EnvWrapper is a proper gymnasium.Env."""

    def test_ppo_smoke_training(self, tmp_path):
        """PPO with real SB3: constructs env, trains 64 steps, writes checkpoint."""
        gymnasium, PPO, SAC, SB3EnvWrapper, SignalWeightEnv, SignalWeightEnvConfig, RewardAdapterEnv = (
            _clear_mocks_and_import()
        )

        data = _make_data(30)
        config = SignalWeightEnvConfig(
            symbols=["AAPL", "MSFT"],
            observation_columns=["close"],
            transaction_cost_bp=5.0,
        )
        base_env = SignalWeightEnv(config)
        reward_env = RewardAdapterEnv(base_env, {"reward_version": "v1"})
        gym_env = SB3EnvWrapper(reward_env, 2, data)

        assert isinstance(gym_env, gymnasium.Env), (
            "SB3EnvWrapper must be a gymnasium.Env subclass for SB3 to accept it"
        )

        model = PPO(
            "MlpPolicy",
            gym_env,
            n_steps=32,
            batch_size=16,
            verbose=0,
        )
        model.learn(total_timesteps=64)

        checkpoint = tmp_path / "ppo_smoke"
        model.save(str(checkpoint))

        assert (tmp_path / "ppo_smoke.zip").exists(), "Checkpoint file must be written"

    def test_sac_smoke_training(self, tmp_path):
        """SAC with real SB3: constructs env, trains 64 steps, writes checkpoint."""
        gymnasium, PPO, SAC, SB3EnvWrapper, SignalWeightEnv, SignalWeightEnvConfig, RewardAdapterEnv = (
            _clear_mocks_and_import()
        )

        data = _make_data(30)
        config = SignalWeightEnvConfig(
            symbols=["AAPL", "MSFT"],
            observation_columns=["close"],
            transaction_cost_bp=5.0,
        )
        base_env = SignalWeightEnv(config)
        reward_env = RewardAdapterEnv(base_env, {"reward_version": "v1"})
        gym_env = SB3EnvWrapper(reward_env, 2, data)

        model = SAC(
            "MlpPolicy",
            gym_env,
            learning_starts=32,
            buffer_size=100,
            batch_size=16,
            verbose=0,
        )
        model.learn(total_timesteps=64)

        checkpoint = tmp_path / "sac_smoke"
        model.save(str(checkpoint))

        assert (tmp_path / "sac_smoke.zip").exists(), "Checkpoint file must be written"
