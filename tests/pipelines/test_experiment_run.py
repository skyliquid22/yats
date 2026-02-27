"""Tests for experiment_run Dagster job ops."""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from research.experiments.spec import CostConfig, ExperimentSpec


# ---------------------------------------------------------------------------
# Mock SB3 + gymnasium (needed for trainer imports)
# ---------------------------------------------------------------------------

def _install_mocks():
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

        class MockModel:
            def __init__(self, policy, env, **kwargs):
                self.env = env
                self._trained = False
            def learn(self, total_timesteps, **kwargs):
                self._trained = True
                return self
            def save(self, path):
                Path(path).parent.mkdir(parents=True, exist_ok=True)
                Path(f"{path}.zip").write_text("mock_model")
            @classmethod
            def load(cls, path, env=None):
                m = cls("MlpPolicy", env)
                m._trained = True
                return m
            def predict(self, obs, deterministic=False):
                n = self.env.action_space.shape[0]
                return np.full(n, 1.0 / n, dtype=np.float32), None

        sb3_mod.PPO = MockModel
        sb3_mod.SAC = MockModel
        sb3_callbacks.BaseCallback = object
        sb3_common.callbacks = sb3_callbacks
        sb3_mod.common = sb3_common
        sys.modules["stable_baselines3"] = sb3_mod
        sys.modules["stable_baselines3.common"] = sb3_common
        sys.modules["stable_baselines3.common.callbacks"] = sb3_callbacks


_install_mocks()

from pipelines.yats_pipelines.jobs.experiment_run import (
    _build_returns_df,
    _reconstruct_spec,
    _dataframe_to_env_rows,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_spec_dict(**overrides):
    defaults = {
        "experiment_name": "test_exp",
        "symbols": ["AAPL", "MSFT"],
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "interval": "daily",
        "feature_set": "core_v1",
        "policy": "ppo",
        "policy_params": {"reward_version": "v1", "total_timesteps": 100},
        "cost_config": {"transaction_cost_bp": 5.0, "slippage_bp": 0.0},
        "seed": 42,
    }
    defaults.update(overrides)
    return defaults


def _make_data_rows(n=10):
    symbols = ["AAPL", "MSFT"]
    return [
        {
            "timestamp": f"2024-01-{i+1:02d}",
            "close": {"AAPL": 100.0 + i, "MSFT": 200.0 + i * 2},
            "open": {"AAPL": 100.0 + i, "MSFT": 200.0 + i * 2},
            "high": {"AAPL": 101.0 + i, "MSFT": 202.0 + i * 2},
            "low": {"AAPL": 99.0 + i, "MSFT": 198.0 + i * 2},
            "volume": {"AAPL": 1_000_000.0, "MSFT": 800_000.0},
        }
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestReconstructSpec:
    def test_basic_reconstruction(self):
        spec_data = _make_spec_dict()
        spec = _reconstruct_spec(spec_data)

        assert isinstance(spec, ExperimentSpec)
        assert spec.experiment_name == "test_exp"
        assert spec.policy == "ppo"
        assert spec.symbols == ("AAPL", "MSFT")
        assert spec.start_date == date(2024, 1, 1)
        assert spec.end_date == date(2024, 12, 31)

    def test_reconstruction_with_risk_config(self):
        spec_data = _make_spec_dict()
        spec_data["risk_config"] = {
            "max_gross_exposure": 0.9,
            "max_symbol_weight": 0.3,
        }
        spec = _reconstruct_spec(spec_data)

        assert spec.risk_config.max_gross_exposure == 0.9
        assert spec.risk_config.max_symbol_weight == 0.3

    def test_reconstruction_with_execution_sim(self):
        spec_data = _make_spec_dict()
        spec_data["execution_sim"] = {
            "enabled": True,
            "slippage_model": "flat",
            "slippage_bp": 3.0,
            "fill_probability": 0.98,
            "range_shrink": 0.5,
        }
        spec = _reconstruct_spec(spec_data)

        assert spec.execution_sim is not None
        assert spec.execution_sim.enabled is True
        assert spec.execution_sim.slippage_bp == 3.0

    def test_reconstruction_with_eval_split(self):
        spec_data = _make_spec_dict()
        spec_data["evaluation_split"] = {
            "train_ratio": 0.8,
            "test_ratio": 0.2,
        }
        spec = _reconstruct_spec(spec_data)

        assert spec.evaluation_split is not None
        assert spec.evaluation_split.train_ratio == 0.8

    def test_reconstruction_sac_policy(self):
        spec_data = _make_spec_dict(policy="sac")
        spec = _reconstruct_spec(spec_data)
        assert spec.policy == "sac"

    def test_reconstruction_date_objects(self):
        spec_data = _make_spec_dict()
        spec_data["start_date"] = date(2024, 1, 1)
        spec_data["end_date"] = date(2024, 12, 31)
        spec = _reconstruct_spec(spec_data)

        assert spec.start_date == date(2024, 1, 1)


class TestBuildReturnsDf:
    def test_returns_shape(self):
        data = _make_data_rows(10)
        df = _build_returns_df(data, ["AAPL", "MSFT"])

        assert df.shape == (9, 2)  # n-1 rows due to pct_change
        assert "AAPL" in df.columns
        assert "MSFT" in df.columns

    def test_returns_values(self):
        data = _make_data_rows(3)
        df = _build_returns_df(data, ["AAPL", "MSFT"])

        # AAPL: 100, 101, 102 → returns: 0.01, ~0.0099
        assert df.iloc[0]["AAPL"] == pytest.approx(1.0 / 100.0)

    def test_empty_data(self):
        df = _build_returns_df([], ["AAPL"])
        assert len(df) == 0


class TestDataframeToEnvRows:
    def test_basic_conversion(self):
        # Simulated QuestDB output
        df = pd.DataFrame([
            {"timestamp": "2024-01-01", "symbol": "AAPL", "close": 100.0, "volume": 1e6},
            {"timestamp": "2024-01-01", "symbol": "MSFT", "close": 200.0, "volume": 8e5},
            {"timestamp": "2024-01-02", "symbol": "AAPL", "close": 101.0, "volume": 1e6},
            {"timestamp": "2024-01-02", "symbol": "MSFT", "close": 202.0, "volume": 8e5},
        ])

        rows = _dataframe_to_env_rows(
            df, ["AAPL", "MSFT"], ["close", "volume"], [],
        )

        assert len(rows) == 2
        assert rows[0]["close"]["AAPL"] == 100.0
        assert rows[0]["close"]["MSFT"] == 200.0
        assert rows[1]["close"]["AAPL"] == 101.0

    def test_empty_df(self):
        df = pd.DataFrame()
        rows = _dataframe_to_env_rows(df, ["AAPL"], ["close"], [])
        assert rows == []

    def test_regime_columns(self):
        df = pd.DataFrame([
            {"timestamp": "2024-01-01", "symbol": "AAPL", "close": 100.0, "market_vol_20d": 0.15},
            {"timestamp": "2024-01-01", "symbol": "MSFT", "close": 200.0, "market_vol_20d": 0.15},
        ])

        rows = _dataframe_to_env_rows(
            df, ["AAPL", "MSFT"], ["close"], ["market_vol_20d"],
        )

        assert len(rows) == 1
        assert rows[0]["market_vol_20d"] == 0.15
