"""Tests for SignalWeightEnv — reset/step cycle, observation construction, info dict."""

from __future__ import annotations

import math

import numpy as np
import pytest

from research.envs.signal_weight_env import SignalWeightEnv, SignalWeightEnvConfig
from research.experiments.spec import ExecutionSimConfig, RiskConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_row(
    symbols: list[str],
    close: dict[str, float],
    timestamp: str = "2024-01-01",
    extra_cols: dict | None = None,
) -> dict:
    """Build a feature row dict for testing."""
    row: dict = {
        "timestamp": timestamp,
        "close": close,
        "open": close,
        "high": {s: v * 1.01 for s, v in close.items()},
        "low": {s: v * 0.99 for s, v in close.items()},
        "volume": {s: 1_000_000.0 for s in symbols},
    }
    if extra_cols:
        row.update(extra_cols)
    return row


def _make_data(n_rows: int = 5) -> list[dict]:
    """Generate n_rows of feature data for AAPL + MSFT with rising prices."""
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
    """Minimal valid config."""
    defaults = dict(
        symbols=["AAPL", "MSFT"],
        observation_columns=["close"],
        transaction_cost_bp=5.0,
    )
    defaults.update(overrides)
    return SignalWeightEnvConfig(**defaults)


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

class TestSignalWeightEnvConfig:
    def test_requires_close(self):
        with pytest.raises(ValueError, match="close"):
            SignalWeightEnvConfig(
                symbols=["AAPL"],
                observation_columns=["open", "high"],
            )

    def test_requires_symbols(self):
        with pytest.raises(ValueError, match="symbols"):
            SignalWeightEnvConfig(
                symbols=[],
                observation_columns=["close"],
            )

    def test_valid_config(self):
        cfg = _base_config()
        assert cfg.symbols == ["AAPL", "MSFT"]
        assert cfg.action_clip == (0.0, 1.0)


# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------

class TestReset:
    def test_reset_returns_tuple(self):
        env = SignalWeightEnv(_base_config())
        obs = env.reset(data=_make_data())
        assert isinstance(obs, tuple)

    def test_reset_observation_length(self):
        env = SignalWeightEnv(_base_config())
        obs = env.reset(data=_make_data())
        # 2 symbols * 1 feature + 0 regime + 2 prev_weights = 4
        assert len(obs) == 4
        assert len(obs) == env.observation_length

    def test_reset_no_data_raises(self):
        env = SignalWeightEnv(_base_config())
        with pytest.raises(ValueError, match="No data"):
            env.reset()

    def test_reset_initial_weights_zero(self):
        env = SignalWeightEnv(_base_config())
        obs = env.reset(data=_make_data())
        # Last 2 values are previous weights, should be 0
        assert obs[-1] == 0.0
        assert obs[-2] == 0.0


# ---------------------------------------------------------------------------
# Observation vector construction
# ---------------------------------------------------------------------------

class TestObservation:
    def test_symbol_major_order(self):
        """Symbols sorted alphabetically, each symbol's features in order."""
        cfg = _base_config(
            symbols=["MSFT", "AAPL"],  # unsorted
            observation_columns=["close", "open"],
        )
        env = SignalWeightEnv(cfg)
        data = _make_data()
        obs = env.reset(data=data)

        # Sorted: AAPL, MSFT
        # obs = [AAPL_close, AAPL_open, MSFT_close, MSFT_open, w_AAPL, w_MSFT]
        assert len(obs) == 6  # 2 symbols * 2 features + 2 weights
        assert obs[0] == 100.0  # AAPL close
        assert obs[1] == 100.0  # AAPL open (same as close in test data)
        assert obs[2] == 200.0  # MSFT close
        assert obs[3] == 200.0  # MSFT open

    def test_regime_features_appended(self):
        cfg = _base_config(
            regime_feature_names=["market_vol_20d", "market_trend_20d"],
        )
        env = SignalWeightEnv(cfg)

        data = _make_data()
        for row in data:
            row["market_vol_20d"] = 0.15
            row["market_trend_20d"] = 0.02

        obs = env.reset(data=data)
        # 2 * 1 feature + 2 regime + 2 weights = 6
        assert len(obs) == 6
        assert obs[2] == 0.15  # market_vol_20d
        assert obs[3] == 0.02  # market_trend_20d

    def test_prev_weights_updated_after_step(self):
        env = SignalWeightEnv(_base_config())
        env.reset(data=_make_data(10))
        action = [0.5, 0.3]
        obs, _, _, _ = env.step(action)
        # Last 2 obs values should be the realized weights
        assert obs[-2] == pytest.approx(0.5, abs=0.01)
        assert obs[-1] == pytest.approx(0.3, abs=0.01)


# ---------------------------------------------------------------------------
# Step execution
# ---------------------------------------------------------------------------

class TestStep:
    def test_step_after_done_raises(self):
        env = SignalWeightEnv(_base_config())
        env.reset(data=_make_data(2))  # only 2 rows, step once = done
        env.step([0.5, 0.5])
        with pytest.raises(RuntimeError, match="done"):
            env.step([0.5, 0.5])

    def test_step_returns_four_tuple(self):
        env = SignalWeightEnv(_base_config())
        env.reset(data=_make_data())
        result = env.step([0.5, 0.5])
        assert len(result) == 4
        obs, reward, done, info = result
        assert isinstance(obs, tuple)
        assert isinstance(reward, float)
        assert isinstance(done, bool)
        assert isinstance(info, dict)

    def test_done_on_last_row(self):
        env = SignalWeightEnv(_base_config())
        env.reset(data=_make_data(3))
        _, _, done1, _ = env.step([0.5, 0.5])
        assert not done1
        _, _, done2, _ = env.step([0.5, 0.5])
        assert done2

    def test_action_clipping(self):
        cfg = _base_config(action_clip=(0.0, 0.5))
        env = SignalWeightEnv(cfg)
        env.reset(data=_make_data())
        _, _, _, info = env.step([0.8, -0.1])
        # Weights should be clipped to [0, 0.5]
        assert info["weights"][0] <= 0.5
        assert info["weights"][1] >= 0.0

    def test_transaction_cost_reduces_value(self):
        env = SignalWeightEnv(_base_config(transaction_cost_bp=100.0))
        env.reset(data=_make_data(10))
        # Start with zero weights, move to 50/50 — full turnover
        _, _, _, info = env.step([0.5, 0.5])
        # Portfolio value should be < 1.0 due to costs
        assert info["portfolio_value"] < 1.0

    def test_zero_cost_preserves_value_on_flat_prices(self):
        cfg = _base_config(transaction_cost_bp=0.0)
        env = SignalWeightEnv(cfg)
        # All same prices
        data = [
            _make_row(["AAPL", "MSFT"], {"AAPL": 100.0, "MSFT": 200.0},
                      timestamp=f"2024-01-{i+1:02d}")
            for i in range(5)
        ]
        env.reset(data=data)
        _, _, _, info = env.step([0.0, 0.0])  # no allocation, no cost
        assert info["portfolio_value"] == pytest.approx(1.0, abs=1e-10)

    def test_reward_is_log_return(self):
        env = SignalWeightEnv(_base_config(transaction_cost_bp=0.0))
        data = [
            _make_row(["AAPL", "MSFT"], {"AAPL": 100.0, "MSFT": 200.0}),
            _make_row(["AAPL", "MSFT"], {"AAPL": 100.0, "MSFT": 200.0}),
            _make_row(["AAPL", "MSFT"], {"AAPL": 100.0, "MSFT": 200.0}),
        ]
        env.reset(data=data)
        # Zero weights, zero return
        _, reward, _, _ = env.step([0.0, 0.0])
        assert reward == pytest.approx(0.0, abs=1e-10)


# ---------------------------------------------------------------------------
# Info dict
# ---------------------------------------------------------------------------

class TestInfoDict:
    def test_base_keys_present(self):
        env = SignalWeightEnv(_base_config())
        env.reset(data=_make_data())
        _, _, _, info = env.step([0.5, 0.5])

        expected_keys = {
            "timestamp", "price_close", "raw_action", "weights",
            "weight_target", "weight_realized", "portfolio_value",
            "cost_paid", "reward",
        }
        assert expected_keys.issubset(info.keys())

    def test_regime_keys_present(self):
        cfg = _base_config(regime_feature_names=["market_vol_20d"])
        env = SignalWeightEnv(cfg)
        data = _make_data()
        for row in data:
            row["market_vol_20d"] = 0.15
        env.reset(data=data)
        _, _, _, info = env.step([0.5, 0.5])
        assert "regime_features" in info
        assert "regime_state" in info

    def test_execution_sim_keys(self):
        cfg = _base_config(
            execution_sim=ExecutionSimConfig(enabled=True, slippage_bp=5.0),
        )
        env = SignalWeightEnv(cfg)
        env.reset(data=_make_data())
        _, _, _, info = env.step([0.5, 0.5])
        assert "execution_slippage_bps" in info
        assert "missed_fill_ratio" in info
        assert "unfilled_notional" in info
        assert "order_type_counts" in info

    def test_mode_key_from_kwargs(self):
        env = SignalWeightEnv(_base_config())
        env.reset(data=_make_data())
        _, _, _, info = env.step([0.5, 0.5], mode="risk_on")
        assert info["mode"] == "risk_on"

    def test_price_close_is_dict(self):
        env = SignalWeightEnv(_base_config())
        env.reset(data=_make_data())
        _, _, _, info = env.step([0.5, 0.5])
        assert isinstance(info["price_close"], dict)
        assert "AAPL" in info["price_close"]
        assert "MSFT" in info["price_close"]


# ---------------------------------------------------------------------------
# Risk constraints (project_weights integration)
# ---------------------------------------------------------------------------

class TestRiskConstraints:
    def test_exposure_cap(self):
        risk = RiskConfig(max_gross_exposure=0.5)
        cfg = _base_config(risk_config=risk)
        env = SignalWeightEnv(cfg)
        env.reset(data=_make_data())
        _, _, _, info = env.step([0.8, 0.8])
        assert info["weights"].sum() <= 0.5 + 1e-10

    def test_per_symbol_max(self):
        risk = RiskConfig(max_symbol_weight=0.3)
        cfg = _base_config(risk_config=risk)
        env = SignalWeightEnv(cfg)
        env.reset(data=_make_data())
        _, _, _, info = env.step([0.8, 0.8])
        assert info["weights"].max() <= 0.3 + 1e-10


# ---------------------------------------------------------------------------
# Full episode
# ---------------------------------------------------------------------------

class TestFullEpisode:
    def test_run_full_episode(self):
        env = SignalWeightEnv(_base_config())
        data = _make_data(20)
        obs = env.reset(data=data)

        total_reward = 0.0
        step_count = 0
        done = False

        while not done:
            action = [0.3, 0.3]
            obs, reward, done, info = env.step(action)
            total_reward += reward
            step_count += 1

        assert step_count == 19  # n_rows - 1 steps
        assert done
        assert info["portfolio_value"] > 0

    def test_episode_with_execution_sim(self):
        cfg = _base_config(
            execution_sim=ExecutionSimConfig(
                enabled=True,
                slippage_bp=5.0,
                fill_probability=0.95,
            ),
        )
        env = SignalWeightEnv(cfg)
        env.reset(data=_make_data(10))

        for _ in range(9):
            obs, reward, done, info = env.step([0.4, 0.4])
            if done:
                break

        assert info["portfolio_value"] > 0
