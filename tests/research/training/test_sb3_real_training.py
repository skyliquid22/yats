"""Real (non-mocked) integration tests for SB3 training.

These tests use the actual stable_baselines3 and gymnasium libraries — no mocks.
They validate that SB3EnvWrapper is accepted by SB3 as a proper gymnasium.Env
subclass, which is the class of bug that mocked tests cannot catch.

The test clears any gymnasium/SB3 mocks that earlier test modules may have
installed in sys.modules, then reimports with real libraries.
"""

from __future__ import annotations

import sys
from datetime import date, datetime
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


def _make_data_with_regime(
    n_rows: int = 30,
    symbols: list[str] | None = None,
    regime_cols: tuple[str, ...] = (
        "market_vol_20d", "market_trend_20d", "dispersion_20d", "corr_mean_20d"
    ),
) -> list[dict[str, Any]]:
    if symbols is None:
        symbols = ["AAPL", "MSFT"]
    rows = []
    for i in range(n_rows):
        row: dict[str, Any] = {
            "timestamp": f"2024-01-{(i % 28) + 1:02d}",
            "close": {sym: 100.0 + i for sym in symbols},
            "ret_1d": {sym: 0.01 for sym in symbols},
        }
        for j, col in enumerate(regime_cols):
            row[col] = 0.1 * (j + 1)
        rows.append(row)
    return rows


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


class TestShadowReplayShapeParity:
    """Regression tests for ya-jl1k9: shadow replay obs shape == training obs shape."""

    SYMBOLS = ["AAPL", "MSFT"]
    OBS_COLS = ["close", "ret_1d"]
    REGIME_COLS = (
        "market_vol_20d", "market_trend_20d", "dispersion_20d", "corr_mean_20d"
    )

    def test_ppo_replay_no_shape_error(self, tmp_path):
        """Train PPO with regime features, replay through ShadowEngine — no shape error.

        Regression test for: 'ValueError: Unexpected observation shape (200,)
        for Box environment, please use (204,)'
        """
        gymnasium, PPO, SAC, SB3EnvWrapper, SignalWeightEnv, SignalWeightEnvConfig, RewardAdapterEnv = (
            _clear_mocks_and_import()
        )

        from research.experiments.spec import CostConfig, ExperimentSpec, RiskConfig
        from research.policies.rl_policy_wrapper import RLPolicyWrapper
        from research.shadow.data_source import Snapshot
        from research.shadow.engine import ShadowEngine, ShadowRunConfig

        # --- Train PPO with regime features ---
        train_data = _make_data_with_regime(30, self.SYMBOLS, self.REGIME_COLS)
        env_config = SignalWeightEnvConfig(
            symbols=self.SYMBOLS,
            observation_columns=self.OBS_COLS,
            regime_feature_names=list(self.REGIME_COLS),
        )
        base_env = SignalWeightEnv(env_config)
        reward_env = RewardAdapterEnv(base_env, {"reward_version": "v1"})
        gym_env = SB3EnvWrapper(reward_env, len(self.SYMBOLS), train_data)

        model = PPO("MlpPolicy", gym_env, n_steps=16, batch_size=8, verbose=0)
        model.learn(total_timesteps=64)

        checkpoint = tmp_path / "ppo_regime"
        model.save(str(checkpoint))
        assert (tmp_path / "ppo_regime.zip").exists()

        # --- Replay through ShadowEngine with the same obs contract ---
        loaded_model = PPO.load(str(checkpoint) + ".zip")
        rl_policy = RLPolicyWrapper(model=loaded_model, n_symbols=len(self.SYMBOLS))

        spec = ExperimentSpec(
            experiment_name="parity_test",
            symbols=tuple(sorted(self.SYMBOLS)),
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            interval="daily",
            feature_set="core_v1",
            policy="ppo",
            policy_params={},
            cost_config=CostConfig(transaction_cost_bp=0.0),
            seed=42,
        )

        snapshots = [
            Snapshot(
                as_of=datetime(2024, 1, 3 + i),
                symbols=tuple(sorted(self.SYMBOLS)),
                panel={sym: {"close": 100.0 + i, "ret_1d": 0.01} for sym in self.SYMBOLS},
                regime_features=tuple(0.1 * (j + 1) for j in range(len(self.REGIME_COLS))),
                regime_feature_names=self.REGIME_COLS,
                observation_columns=tuple(self.OBS_COLS),
            )
            for i in range(5)
        ]

        config = ShadowRunConfig(
            experiment_id="parity_replay",
            run_id="r1",
            output_dir=tmp_path / "parity_replay" / "r1",
        )
        engine = ShadowEngine(spec, rl_policy, snapshots, config)

        # Must not raise ValueError about observation shape mismatch
        summary = engine.run()
        assert summary["steps_executed"] > 0
