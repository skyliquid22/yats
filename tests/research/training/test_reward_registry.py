"""Tests for RewardAdapterEnv and reward functions."""

from __future__ import annotations

import math

import numpy as np
import pytest

from research.envs.signal_weight_env import SignalWeightEnv, SignalWeightEnvConfig
from research.training.reward_registry import RewardAdapterEnv
from research.training.rewards.reward_v1 import reward_v1
from research.training.rewards.reward_v2 import (
    RewardV2Config,
    RewardV2State,
    reward_v2,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_row(
    symbols: list[str],
    close: dict[str, float],
    timestamp: str = "2024-01-01",
) -> dict:
    row: dict = {
        "timestamp": timestamp,
        "close": close,
        "open": close,
        "high": {s: v * 1.01 for s, v in close.items()},
        "low": {s: v * 0.99 for s, v in close.items()},
        "volume": {s: 1_000_000.0 for s in symbols},
    }
    return row


def _make_data(n_rows: int = 5) -> list[dict]:
    symbols = ["AAPL", "MSFT"]
    return [
        _make_row(
            symbols,
            {"AAPL": 100.0 + i, "MSFT": 200.0 + i * 2},
            timestamp=f"2024-01-{i+1:02d}",
        )
        for i in range(n_rows)
    ]


def _base_config(**overrides) -> SignalWeightEnvConfig:
    defaults = dict(
        symbols=["AAPL", "MSFT"],
        observation_columns=["close"],
        transaction_cost_bp=5.0,
    )
    defaults.update(overrides)
    return SignalWeightEnvConfig(**defaults)


def _make_adapter(reward_version: str = "v1", **extra_params) -> RewardAdapterEnv:
    env = SignalWeightEnv(_base_config())
    policy_params = {"reward_version": reward_version, **extra_params}
    return RewardAdapterEnv(env, policy_params)


# ---------------------------------------------------------------------------
# reward_v1 unit tests
# ---------------------------------------------------------------------------

class TestRewardV1:
    def test_passthrough(self):
        info = {"some_key": "value"}
        reward, out_info = reward_v1(0.05, info)
        assert reward == 0.05
        assert out_info is info

    def test_negative_reward(self):
        reward, _ = reward_v1(-0.03, {})
        assert reward == -0.03

    def test_zero_reward(self):
        reward, _ = reward_v1(0.0, {})
        assert reward == 0.0


# ---------------------------------------------------------------------------
# reward_v2 unit tests
# ---------------------------------------------------------------------------

class TestRewardV2:
    def test_no_penalties_when_static(self):
        """No weight change, no drawdown, no cost → reward unchanged."""
        config = RewardV2Config()
        state = RewardV2State()
        prev_w = np.array([0.5, 0.5])
        info = {
            "weight_realized": np.array([0.5, 0.5]),
            "portfolio_value": 1.01,
            "cost_paid": 0.0,
        }
        reward, out_info = reward_v2(0.01, info, config, state, prev_w)
        # No turnover, portfolio rising → no drawdown, no cost
        assert reward == pytest.approx(0.01, abs=1e-10)
        assert "reward_components" in out_info

    def test_turnover_penalty(self):
        config = RewardV2Config(turnover_scale=0.10, drawdown_scale=0.0, cost_scale=0.0)
        state = RewardV2State()
        prev_w = np.array([0.0, 0.0])
        info = {
            "weight_realized": np.array([0.5, 0.5]),
            "portfolio_value": 1.0,
            "cost_paid": 0.0,
        }
        reward, out_info = reward_v2(0.01, info, config, state, prev_w)
        # turnover = 0.5 * (0.5 + 0.5) = 0.5
        # penalty = 0.5 * 0.10 = 0.05
        expected = 0.01 - 0.05
        assert reward == pytest.approx(expected, abs=1e-10)
        assert out_info["reward_components"]["turnover_penalty"] == pytest.approx(
            0.05, abs=1e-10
        )

    def test_drawdown_penalty(self):
        config = RewardV2Config(turnover_scale=0.0, drawdown_scale=0.50, cost_scale=0.0)
        state = RewardV2State()
        prev_w = np.array([0.5, 0.5])

        # Step 1: portfolio rises to 1.1 → no drawdown
        info1 = {
            "weight_realized": np.array([0.5, 0.5]),
            "portfolio_value": 1.1,
            "cost_paid": 0.0,
        }
        reward1, _ = reward_v2(0.05, info1, config, state, prev_w)
        assert reward1 == pytest.approx(0.05, abs=1e-10)

        # Step 2: portfolio drops to 1.0 → drawdown increase
        info2 = {
            "weight_realized": np.array([0.5, 0.5]),
            "portfolio_value": 1.0,
            "cost_paid": 0.0,
        }
        reward2, out_info = reward_v2(-0.05, info2, config, state, prev_w)
        # Peak was 1.1, now 1.0 → drawdown = (1.1-1.0)/1.1 ≈ 0.0909
        # Previous drawdown was 0 → increase = 0.0909
        # Penalty = 0.0909 * 0.50 = 0.04545
        dd_increase = (1.1 - 1.0) / 1.1
        expected = -0.05 - dd_increase * 0.50
        assert reward2 == pytest.approx(expected, abs=1e-6)
        assert out_info["reward_components"]["drawdown_penalty"] == pytest.approx(
            dd_increase * 0.50, abs=1e-6
        )

    def test_cost_penalty(self):
        config = RewardV2Config(turnover_scale=0.0, drawdown_scale=0.0, cost_scale=1.0)
        state = RewardV2State()
        prev_w = np.array([0.5, 0.5])
        info = {
            "weight_realized": np.array([0.5, 0.5]),
            "portfolio_value": 1.0,
            "cost_paid": 0.001,  # cost amount
        }
        reward, out_info = reward_v2(0.0, info, config, state, prev_w)
        # prev_portfolio_value = 1.0 / exp(0.0) = 1.0
        # cost_penalty = (0.001 / 1.0) * 1.0 = 0.001
        assert reward == pytest.approx(-0.001, abs=1e-6)
        assert out_info["reward_components"]["cost_penalty"] == pytest.approx(
            0.001, abs=1e-6
        )

    def test_clip_reward(self):
        config = RewardV2Config(clip_reward=(-0.01, 0.01))
        state = RewardV2State()
        prev_w = np.array([0.0, 0.0])
        info = {
            "weight_realized": np.array([1.0, 0.0]),
            "portfolio_value": 1.0,
            "cost_paid": 0.0,
        }
        # Large base reward should get clipped
        reward, _ = reward_v2(0.10, info, config, state, prev_w)
        assert reward <= 0.01

    def test_reward_components_keys(self):
        config = RewardV2Config()
        state = RewardV2State()
        prev_w = np.array([0.5, 0.5])
        info = {
            "weight_realized": np.array([0.5, 0.5]),
            "portfolio_value": 1.0,
            "cost_paid": 0.0,
        }
        _, out_info = reward_v2(0.01, info, config, state, prev_w)
        rc = out_info["reward_components"]
        assert set(rc.keys()) == {"base", "turnover_penalty", "drawdown_penalty", "cost_penalty", "final"}

    def test_drawdown_tracks_across_steps(self):
        """Drawdown state persists across multiple reward_v2 calls."""
        config = RewardV2Config(turnover_scale=0.0, drawdown_scale=1.0, cost_scale=0.0)
        state = RewardV2State()
        prev_w = np.array([0.5, 0.5])

        # Rising portfolio → no drawdown penalty
        for pv in [1.0, 1.1, 1.2]:
            info = {
                "weight_realized": np.array([0.5, 0.5]),
                "portfolio_value": pv,
                "cost_paid": 0.0,
            }
            reward, _ = reward_v2(0.01, info, config, state, prev_w)
            assert reward == pytest.approx(0.01, abs=1e-10)

        # Now drop → drawdown penalty applies
        info = {
            "weight_realized": np.array([0.5, 0.5]),
            "portfolio_value": 1.1,
            "cost_paid": 0.0,
        }
        reward, out_info = reward_v2(-0.05, info, config, state, prev_w)
        assert reward < -0.05  # penalty subtracted
        assert out_info["reward_components"]["drawdown_penalty"] > 0


# ---------------------------------------------------------------------------
# RewardAdapterEnv
# ---------------------------------------------------------------------------

class TestRewardAdapterEnv:
    def test_v1_passthrough(self):
        adapter = _make_adapter("v1")
        adapter.reset(data=_make_data(10))
        obs, reward, done, info = adapter.step([0.3, 0.3])
        assert isinstance(obs, tuple)
        assert isinstance(reward, float)
        assert not done
        # v1 should not add reward_components
        assert "reward_components" not in info

    def test_v2_adds_reward_components(self):
        adapter = _make_adapter("v2")
        adapter.reset(data=_make_data(10))
        _, _, _, info = adapter.step([0.3, 0.3])
        assert "reward_components" in info
        rc = info["reward_components"]
        assert "base" in rc
        assert "final" in rc

    def test_v2_applies_turnover_penalty(self):
        adapter = _make_adapter("v2", turnover_scale=0.10)
        adapter.reset(data=_make_data(10))
        # First step: from zero weights to [0.5, 0.5] → turnover penalty
        _, reward, _, info = adapter.step([0.5, 0.5])
        base = info["reward_components"]["base"]
        assert reward < base  # penalty applied

    def test_invalid_version_raises(self):
        env = SignalWeightEnv(_base_config())
        with pytest.raises(ValueError, match="Unknown reward_version"):
            RewardAdapterEnv(env, {"reward_version": "v999"})

    def test_default_version_is_v1(self):
        env = SignalWeightEnv(_base_config())
        adapter = RewardAdapterEnv(env, {})
        assert adapter.reward_version == "v1"

    def test_observation_length_delegated(self):
        adapter = _make_adapter("v1")
        assert adapter.observation_length == 4  # 2 symbols * 1 feat + 2 weights

    def test_full_episode_v1(self):
        adapter = _make_adapter("v1")
        adapter.reset(data=_make_data(10))
        done = False
        steps = 0
        while not done:
            _, reward, done, info = adapter.step([0.3, 0.3])
            steps += 1
        assert steps == 9

    def test_full_episode_v2(self):
        adapter = _make_adapter("v2")
        adapter.reset(data=_make_data(10))
        done = False
        steps = 0
        while not done:
            _, reward, done, info = adapter.step([0.3, 0.3])
            assert "reward_components" in info
            steps += 1
        assert steps == 9

    def test_reset_clears_state(self):
        """After reset, drawdown and weight state should be cleared."""
        adapter = _make_adapter("v2", turnover_scale=0.10)
        data = _make_data(10)

        # Run a few steps
        adapter.reset(data=data)
        adapter.step([0.5, 0.5])
        adapter.step([0.3, 0.7])

        # Reset and verify clean state (first step turnover from zero weights)
        adapter.reset(data=data)
        _, _, _, info = adapter.step([0.5, 0.5])
        rc = info["reward_components"]
        # Turnover from [0,0] to [0.5,0.5] = 0.5 * 1.0 = 0.5
        assert rc["turnover_penalty"] == pytest.approx(0.5 * 0.10, abs=1e-6)

    def test_v2_clip_reward_config(self):
        adapter = _make_adapter("v2", clip_reward=(-0.001, 0.001))
        adapter.reset(data=_make_data(10))
        _, reward, _, _ = adapter.step([0.5, 0.5])
        assert -0.001 <= reward <= 0.001

    def test_v2_drawdown_penalty_across_steps(self):
        """Drawdown tracking works correctly in the adapter across steps."""
        # Use zero transaction costs so price movements are the only factor
        symbols = ["AAPL", "MSFT"]
        data = [
            _make_row(symbols, {"AAPL": 100.0, "MSFT": 200.0}, "2024-01-01"),
            _make_row(symbols, {"AAPL": 101.0, "MSFT": 202.0}, "2024-01-02"),
            _make_row(symbols, {"AAPL": 99.0, "MSFT": 198.0}, "2024-01-03"),
            _make_row(symbols, {"AAPL": 97.0, "MSFT": 194.0}, "2024-01-04"),
            _make_row(symbols, {"AAPL": 100.0, "MSFT": 200.0}, "2024-01-05"),
        ]
        env = SignalWeightEnv(_base_config(transaction_cost_bp=0.0))
        policy_params = {"reward_version": "v2", "drawdown_scale": 1.0, "turnover_scale": 0.0, "cost_scale": 0.0}
        adapter = RewardAdapterEnv(env, policy_params)
        adapter.reset(data=data)

        # Step 1: prices rise → no drawdown
        _, _, _, info1 = adapter.step([0.5, 0.5])
        assert info1["reward_components"]["drawdown_penalty"] == pytest.approx(0.0, abs=1e-10)

        # Step 2: prices drop → drawdown penalty
        _, _, _, info2 = adapter.step([0.5, 0.5])
        assert info2["reward_components"]["drawdown_penalty"] > 0

        # Step 3: prices drop more → more drawdown penalty
        _, _, _, info3 = adapter.step([0.5, 0.5])
        assert info3["reward_components"]["drawdown_penalty"] > 0
