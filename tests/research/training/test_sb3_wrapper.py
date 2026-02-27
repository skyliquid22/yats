"""Tests for SB3EnvWrapper — Gymnasium adapter for RewardAdapterEnv."""

from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import MagicMock

import numpy as np
import pytest

from research.envs.signal_weight_env import SignalWeightEnv, SignalWeightEnvConfig
from research.training.reward_registry import RewardAdapterEnv


# ---------------------------------------------------------------------------
# Gymnasium mock (SB3/gymnasium not installed in test env)
# ---------------------------------------------------------------------------

def _install_gymnasium_mock():
    """Install a mock gymnasium module with spaces.Box."""
    gym_mod = ModuleType("gymnasium")
    spaces_mod = ModuleType("gymnasium.spaces")

    class MockBox:
        def __init__(self, low, high, shape, dtype=np.float32):
            self.low = np.full(shape, low, dtype=dtype) if np.isscalar(low) else low
            self.high = np.full(shape, high, dtype=dtype) if np.isscalar(high) else high
            self.shape = shape
            self.dtype = dtype

        def sample(self):
            return np.random.uniform(
                self.low, self.high, size=self.shape
            ).astype(self.dtype)

        def contains(self, x):
            return True

    spaces_mod.Box = MockBox
    gym_mod.spaces = spaces_mod
    sys.modules["gymnasium"] = gym_mod
    sys.modules["gymnasium.spaces"] = spaces_mod


_install_gymnasium_mock()

from research.training.sb3_wrapper import SB3EnvWrapper  # noqa: E402


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


def _make_data(n_rows=10):
    symbols = ["AAPL", "MSFT"]
    return [
        _make_row(
            symbols,
            {"AAPL": 100.0 + i, "MSFT": 200.0 + i * 2},
            timestamp=f"2024-01-{i+1:02d}",
        )
        for i in range(n_rows)
    ]


def _base_config(**overrides):
    defaults = dict(
        symbols=["AAPL", "MSFT"],
        observation_columns=["close"],
        transaction_cost_bp=5.0,
    )
    defaults.update(overrides)
    return SignalWeightEnvConfig(**defaults)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSB3EnvWrapper:
    def test_observation_space_shape(self):
        env = SignalWeightEnv(_base_config())
        adapter = RewardAdapterEnv(env, {"reward_version": "v1"})
        wrapper = SB3EnvWrapper(adapter, 2, _make_data())

        assert wrapper.observation_space.shape == (env.observation_length,)

    def test_action_space_shape(self):
        env = SignalWeightEnv(_base_config())
        adapter = RewardAdapterEnv(env, {"reward_version": "v1"})
        wrapper = SB3EnvWrapper(adapter, 2, _make_data())

        assert wrapper.action_space.shape == (2,)

    def test_reset_returns_ndarray_and_info(self):
        env = SignalWeightEnv(_base_config())
        adapter = RewardAdapterEnv(env, {"reward_version": "v1"})
        wrapper = SB3EnvWrapper(adapter, 2, _make_data())

        obs, info = wrapper.reset()

        assert isinstance(obs, np.ndarray)
        assert obs.dtype == np.float32
        assert obs.shape == (env.observation_length,)
        assert isinstance(info, dict)

    def test_step_returns_five_tuple(self):
        env = SignalWeightEnv(_base_config())
        adapter = RewardAdapterEnv(env, {"reward_version": "v1"})
        wrapper = SB3EnvWrapper(adapter, 2, _make_data())

        wrapper.reset()
        obs, reward, terminated, truncated, info = wrapper.step(np.array([0.5, 0.5]))

        assert isinstance(obs, np.ndarray)
        assert obs.dtype == np.float32
        assert isinstance(reward, float)
        assert isinstance(terminated, bool)
        assert truncated is False  # We never truncate
        assert isinstance(info, dict)

    def test_full_episode_runs_to_completion(self):
        data = _make_data(10)
        env = SignalWeightEnv(_base_config())
        adapter = RewardAdapterEnv(env, {"reward_version": "v1"})
        wrapper = SB3EnvWrapper(adapter, 2, data)

        obs, _ = wrapper.reset()
        steps = 0
        done = False

        while not done:
            action = np.array([0.5, 0.5], dtype=np.float32)
            obs, reward, done, truncated, info = wrapper.step(action)
            steps += 1

        assert steps == len(data) - 1  # done at step_idx >= len(data) - 1
        assert done is True

    def test_render_and_close_are_no_ops(self):
        env = SignalWeightEnv(_base_config())
        adapter = RewardAdapterEnv(env, {"reward_version": "v1"})
        wrapper = SB3EnvWrapper(adapter, 2, _make_data())

        wrapper.render()
        wrapper.close()

    def test_reset_with_seed(self):
        env = SignalWeightEnv(_base_config())
        adapter = RewardAdapterEnv(env, {"reward_version": "v1"})
        wrapper = SB3EnvWrapper(adapter, 2, _make_data())

        obs, info = wrapper.reset(seed=42)
        assert isinstance(obs, np.ndarray)

    def test_v2_reward_through_wrapper(self):
        env = SignalWeightEnv(_base_config())
        adapter = RewardAdapterEnv(env, {"reward_version": "v2"})
        wrapper = SB3EnvWrapper(adapter, 2, _make_data())

        wrapper.reset()
        _, _, _, _, info = wrapper.step(np.array([0.5, 0.5]))

        assert "reward_components" in info

    def test_obs_values_match_base_env(self):
        data = _make_data()
        env = SignalWeightEnv(_base_config())
        adapter = RewardAdapterEnv(env, {"reward_version": "v1"})
        wrapper = SB3EnvWrapper(adapter, 2, data)

        # Reset wrapper
        wrapped_obs, _ = wrapper.reset()

        # Reset base env directly
        env2 = SignalWeightEnv(_base_config())
        base_obs = env2.reset(data=data)

        np.testing.assert_allclose(wrapped_obs, np.array(base_obs, dtype=np.float32))
