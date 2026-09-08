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

        class MockEnv:
            def reset(self, *, seed=None, options=None):
                pass
            def step(self, action):
                raise NotImplementedError
            def render(self):
                pass
            def close(self):
                pass

        spaces_mod.Box = MockBox
        gym_mod.spaces = spaces_mod
        gym_mod.Env = MockEnv
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
    _apply_evaluation_split,
    _build_returns_df,
    _fill_skip,
    _merge_closes_into_features,
    _reconstruct_spec,
    _dataframe_to_env_rows,
    _rollout_non_rl_policy,
    _write_run_artifacts,
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

    def test_missing_close_is_nan_not_minus_100pct(self):
        # A missing close (key absent from dict) must yield NaN, not 0.0 → -100% return
        data = [
            {"timestamp": "2024-01-01", "close": {"AAPL": 100.0}},  # MSFT missing
            {"timestamp": "2024-01-02", "close": {"AAPL": 101.0}},
        ]
        df = _build_returns_df(data, ["AAPL", "MSFT"])
        assert pd.isna(df.iloc[0]["MSFT"]), "missing close must be NaN, not -100%"


class TestDataframeToEnvRowsNaNHandling:
    def test_nan_feature_excluded_not_zeroed(self):
        # NaN feature values must be excluded from per_sym dict, not coerced to 0.0
        df = pd.DataFrame([
            {"timestamp": "2024-01-01", "symbol": "AAPL", "close": 100.0, "dist_20d_high": float("nan")},
            {"timestamp": "2024-01-01", "symbol": "MSFT", "close": 200.0, "dist_20d_high": 0.5},
        ])
        rows = _dataframe_to_env_rows(df, ["AAPL", "MSFT"], ["close", "dist_20d_high"], [])
        assert len(rows) == 1
        # AAPL dist_20d_high was NaN — should be absent from the dict, not 0.0
        assert "AAPL" not in rows[0].get("dist_20d_high", {}), "NaN must not be coerced to 0.0"
        assert rows[0]["dist_20d_high"]["MSFT"] == pytest.approx(0.5)

    def test_nan_regime_col_excluded(self):
        # NaN regime feature must be absent from row, not coerced to 0.0
        df = pd.DataFrame([
            {"timestamp": "2024-01-01", "symbol": "AAPL", "close": 100.0, "market_vol_20d": float("nan")},
        ])
        rows = _dataframe_to_env_rows(df, ["AAPL"], ["close"], ["market_vol_20d"])
        assert len(rows) == 1
        assert "market_vol_20d" not in rows[0], "NaN regime col must not appear in row"


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


# ---------------------------------------------------------------------------
# _rollout_non_rl_policy tests
# ---------------------------------------------------------------------------

def _make_trend_data(n: int = 50) -> list[dict]:
    """Data with a strong upward trend in AAPL and flat MSFT — SMA should differ from equal-weight."""
    rows = []
    for i in range(n):
        rows.append({
            "timestamp": f"2024-01-{i+1:02d}" if i < 31 else f"2024-02-{i-30:02d}",
            "close": {
                "AAPL": 100.0 + i * 2.0,   # strong uptrend
                "MSFT": 200.0,              # flat
            },
        })
    return rows


class TestRolloutNonRlPolicy:
    def test_equal_weight_produces_uniform_weights(self):
        symbols = ["AAPL", "MSFT"]
        data = _make_data_rows(10)
        weights = _rollout_non_rl_policy("equal_weight", data, symbols, {})

        assert len(weights) == len(data) - 1  # skip first row
        for w in weights:
            assert w.shape == (2,)
            np.testing.assert_allclose(w, [0.5, 0.5])

    def test_sma_weights_differ_from_equal_weight_on_trend_data(self):
        """Core acceptance criterion: SMA rollout metrics must differ from equal-weight."""
        symbols = ["AAPL", "MSFT"]
        data = _make_trend_data(50)
        spec_data = {"policy_params": {"short_window": 5, "long_window": 10}}

        sma_weights = _rollout_non_rl_policy("sma", data, symbols, spec_data)
        ew_weights = _rollout_non_rl_policy("equal_weight", data, symbols, {})

        # After SMA warms up (long_window steps), it should diverge from equal-weight
        # because AAPL is trending up and MSFT is flat.
        sma_arr = np.array(sma_weights)
        ew_arr = np.array(ew_weights)

        # At least some rows (after warmup) should differ
        warmed_up = sma_arr[10:]  # skip warmup period
        ew_warmed = ew_arr[10:]
        diffs = np.abs(warmed_up - ew_warmed).sum(axis=1)
        assert diffs.max() > 0.01, (
            "SMA weights should differ from equal-weight after warmup on trending data"
        )

    def test_unsupported_policy_raises(self):
        data = _make_data_rows(5)
        with pytest.raises(ValueError, match="Unsupported policy type"):
            _rollout_non_rl_policy("hierarchical", data, ["AAPL", "MSFT"], {})

    def test_unsupported_rl_policy_raises(self):
        data = _make_data_rows(5)
        with pytest.raises(ValueError, match="Unsupported policy type"):
            _rollout_non_rl_policy("ppo", data, ["AAPL", "MSFT"], {})

    def test_sma_output_sums_to_one(self):
        symbols = ["AAPL", "MSFT", "GOOG"]
        data = [
            {"timestamp": f"t{i}", "close": {"AAPL": 100.0 + i, "MSFT": 200.0, "GOOG": 150.0}}
            for i in range(30)
        ]
        weights = _rollout_non_rl_policy("sma", data, symbols, {})
        for w in weights:
            assert abs(w.sum() - 1.0) < 1e-9, f"Weights must sum to 1, got {w.sum()}"


# ---------------------------------------------------------------------------
# Execution lag alignment tests
# ---------------------------------------------------------------------------

class TestExecutionLagAlignment:
    """Verify that execution_lag_days shifts weight/return alignment correctly.

    Core invariant:
      lag=0 (old): weight from obs(row_i) earns return close(t_i)->close(t_{i+1})
      lag=1 (honest): weight from obs(row_i) earns return close(t_{i+1})->close(t_{i+2})

    We use equal_weight policy (deterministic) and a strictly monotone price series
    so returns are always positive. Under lag=1, the portfolio loses one period's
    observation relative to lag=0, producing shorter (N-2 vs N-1) return series.
    """

    def _make_monotone_data(self, n: int = 6) -> list[dict]:
        """N rows with monotone prices: close[i] = 100 + i for AAPL, 200 + 2i for MSFT."""
        return [
            {
                "timestamp": f"2024-01-{i+1:02d}",
                "close": {"AAPL": 100.0 + i, "MSFT": 200.0 + 2 * i},
            }
            for i in range(n)
        ]

    def _run_lag(self, lag: int, n: int = 6) -> dict:
        """Run equal_weight policy with given lag, return debug info."""
        data = self._make_monotone_data(n)
        symbols = ["AAPL", "MSFT"]

        weights_list = _rollout_non_rl_policy("equal_weight", data, symbols, {})
        returns_df = _build_returns_df(data, symbols)

        skip = 1 + lag
        dates = [row.get("timestamp", f"t{i}") for i, row in enumerate(data[skip:])]
        n = min(len(weights_list), len(dates))
        weights_df = pd.DataFrame(weights_list[:n], index=dates[:n], columns=symbols)

        common_idx = weights_df.index.intersection(returns_df.index)
        weights_df = weights_df.loc[common_idx]
        returns_df = returns_df.loc[common_idx]
        port = (weights_df * returns_df).sum(axis=1)

        return {
            "weight_dates": list(weights_df.index),
            "return_dates": list(returns_df.index),
            "portfolio_returns": list(port.values),
            "n_periods": len(port),
        }

    def test_lag0_weight_earns_contemporaneous_return(self):
        """lag=0: weight indexed at t_{i+1} × return(t_i→t_{i+1}) — same-bar alignment."""
        result = self._run_lag(lag=0, n=6)
        # With 6 rows, lag=0 gives 5 weight/return pairs (rows 1..5)
        assert result["n_periods"] == 5
        # First weight/return pair uses date 2024-01-02 (row 1)
        assert result["weight_dates"][0] == "2024-01-02"
        assert result["return_dates"][0] == "2024-01-02"

    def test_lag1_weight_earns_next_bar_return(self):
        """lag=1: weight indexed at t_{i+2} × return(t_{i+1}→t_{i+2}) — next-bar fill."""
        result = self._run_lag(lag=1, n=6)
        # With 6 rows, lag=1 gives 4 weight/return pairs (rows 2..5)
        assert result["n_periods"] == 4
        # First weight/return pair uses date 2024-01-03 (row 2)
        assert result["weight_dates"][0] == "2024-01-03"
        assert result["return_dates"][0] == "2024-01-03"

    def test_lag1_produces_fewer_periods_than_lag0(self):
        """lag=1 loses one period vs lag=0 (trimmed by the extra shift)."""
        r0 = self._run_lag(lag=0, n=8)
        r1 = self._run_lag(lag=1, n=8)
        assert r1["n_periods"] == r0["n_periods"] - 1

    def test_lag1_portfolio_returns_are_positive_on_monotone_prices(self):
        """On monotone rising prices equal-weight portfolio returns must all be positive."""
        result = self._run_lag(lag=1, n=8)
        assert all(r > 0 for r in result["portfolio_returns"]), (
            f"expected all positive returns on rising prices, got {result['portfolio_returns']}"
        )


# ---------------------------------------------------------------------------
# Fill timing (V11-1 unified execution timing)
# ---------------------------------------------------------------------------


class TestFillSkip:
    """_fill_skip maps (execution_lag_days, fill_timing) to the obs->mark-date offset."""

    def test_next_close_lag1_default(self):
        assert _fill_skip(1, "next_close") == 2

    def test_next_close_lag0_legacy(self):
        assert _fill_skip(0, "next_close") == 1

    def test_next_open_lag1(self):
        # Fill at open(t+1), marked at close(t+1) — same date row as the fill
        assert _fill_skip(1, "next_open") == 1

    def test_default_fill_timing_is_next_close(self):
        assert _fill_skip(1) == 2


class TestNextOpenReturns:
    """_build_returns_df(fill_timing='next_open') arithmetic: r(t) = close(t)/open(t) - 1."""

    def _rows(self):
        return [
            {"timestamp": "2024-01-01", "open": {"AAPL": 100.0}, "close": {"AAPL": 102.0}},
            {"timestamp": "2024-01-02", "open": {"AAPL": 104.0}, "close": {"AAPL": 101.0}},
            {"timestamp": "2024-01-03", "open": {"AAPL": 100.0}, "close": {"AAPL": 105.0}},
        ]

    def test_open_to_close_values(self):
        df = _build_returns_df(self._rows(), ["AAPL"], fill_timing="next_open")
        # First row (t_0) dropped: t_0 is never a fill date under lag >= 1
        assert list(df.index) == ["2024-01-02", "2024-01-03"]
        assert df.loc["2024-01-02", "AAPL"] == pytest.approx(101.0 / 104.0 - 1.0)
        assert df.loc["2024-01-03", "AAPL"] == pytest.approx(105.0 / 100.0 - 1.0)

    def test_next_close_default_unchanged(self):
        """Regression: default output identical to the pre-fill_timing behavior."""
        rows = self._rows()
        df_default = _build_returns_df(rows, ["AAPL"])
        df_explicit = _build_returns_df(rows, ["AAPL"], fill_timing="next_close")
        expected = pd.DataFrame(
            {"AAPL": [101.0 / 102.0 - 1.0, 105.0 / 101.0 - 1.0]},
            index=["2024-01-02", "2024-01-03"],
        )
        pd.testing.assert_frame_equal(df_default, df_explicit)
        pd.testing.assert_frame_equal(df_default, expected)

    def test_missing_open_is_nan(self):
        rows = self._rows()
        del rows[1]["open"]["AAPL"]
        df = _build_returns_df(rows, ["AAPL"], fill_timing="next_open")
        assert pd.isna(df.loc["2024-01-02", "AAPL"])
        assert df.loc["2024-01-03", "AAPL"] == pytest.approx(0.05)

    def test_zero_open_is_nan_not_inf(self):
        rows = self._rows()
        rows[1]["open"]["AAPL"] = 0.0
        df = _build_returns_df(rows, ["AAPL"], fill_timing="next_open")
        assert pd.isna(df.loc["2024-01-02", "AAPL"])

    def test_no_opens_at_all_raises(self):
        rows = [
            {"timestamp": "2024-01-01", "close": {"AAPL": 100.0}},
            {"timestamp": "2024-01-02", "close": {"AAPL": 101.0}},
        ]
        with pytest.raises(ValueError, match="next_open.*requires 'open'"):
            _build_returns_df(rows, ["AAPL"], fill_timing="next_open")


class TestFillTimingAlignment:
    """Weight/return alignment under the unified timing model.

    Convention (V11-1): decision_time = EOD(t_i) for obs row i.
      next_close (lag=1, default): fill close(t_{i+1}); weight indexed at
        t_{i+2} against close(t_{i+1})->close(t_{i+2}).
      next_open (lag=1): fill open(t_{i+1}); weight indexed at t_{i+1}
        against open(t_{i+1})->close(t_{i+1}).
    """

    SYMBOLS = ["AAPL", "MSFT"]

    def _make_data(self, n: int = 6) -> list[dict]:
        """Rows with distinct open/close so open->close != close->close."""
        rows = []
        for i in range(n):
            close = {"AAPL": 100.0 + i, "MSFT": 200.0 + 2 * i}
            opens = {s: c * 0.99 for s, c in close.items()}  # +1.0101% intraday
            rows.append({
                "timestamp": f"2024-01-{i+1:02d}",
                "close": close,
                "open": opens,
            })
        return rows

    def _run(self, fill_timing: str, lag: int = 1, n: int = 6) -> dict:
        data = self._make_data(n)
        weights_list = _rollout_non_rl_policy("equal_weight", data, self.SYMBOLS, {})
        returns_df = _build_returns_df(data, self.SYMBOLS, fill_timing=fill_timing)

        skip = _fill_skip(lag, fill_timing)
        dates = [row["timestamp"] for row in data[skip:]]
        m = min(len(weights_list), len(dates))
        weights_df = pd.DataFrame(weights_list[:m], index=dates[:m], columns=self.SYMBOLS)

        common_idx = weights_df.index.intersection(returns_df.index)
        port = (weights_df.loc[common_idx] * returns_df.loc[common_idx]).sum(axis=1)
        return {
            "dates": list(common_idx),
            "portfolio_returns": list(port.values),
        }

    def test_next_open_marks_weight_one_bar_earlier_than_next_close(self):
        r_open = self._run("next_open")
        r_close = self._run("next_close")
        assert r_open["dates"][0] == "2024-01-02"
        assert r_close["dates"][0] == "2024-01-03"
        assert len(r_open["portfolio_returns"]) == len(r_close["portfolio_returns"]) + 1

    def test_next_open_portfolio_return_is_intraday(self):
        """Each day's portfolio return = equal-weighted close/open - 1."""
        r_open = self._run("next_open")
        # open = 0.99 * close for both symbols → intraday return = 1/0.99 - 1
        expected = 1.0 / 0.99 - 1.0
        for r in r_open["portfolio_returns"]:
            assert r == pytest.approx(expected)

    def test_default_next_close_regression_identical_to_lag_formula(self):
        """Regression: default fill_timing reproduces the old skip = 1 + lag path exactly."""
        data = self._make_data(8)
        weights_list = _rollout_non_rl_policy("equal_weight", data, self.SYMBOLS, {})

        # Old (pre-fill_timing) computation, verbatim
        skip_old = 1 + 1
        dates_old = [row["timestamp"] for row in data[skip_old:]]
        m = min(len(weights_list), len(dates_old))
        weights_old = pd.DataFrame(weights_list[:m], index=dates_old[:m], columns=self.SYMBOLS)
        returns_old = _build_returns_df(data, self.SYMBOLS)
        common = weights_old.index.intersection(returns_old.index)
        port_old = (weights_old.loc[common] * returns_old.loc[common]).sum(axis=1)

        r_new = self._run("next_close", n=8)
        np.testing.assert_allclose(r_new["portfolio_returns"], port_old.values)
        assert r_new["dates"] == list(port_old.index)


class TestLeakyArtifactRegression:
    """A leaky artifact (weight peeks at the return it would earn under the
    legacy same-bar convention) must show collapsed Sharpe under BOTH
    next_close and next_open, while legacy lag=0 rewards it enormously.

    Data construction: the close-to-close return is dominated by the
    overnight gap (sigma 2%), with tiny intraday noise (sigma 0.05%). The
    leaky weight for obs row i is 1 iff close(t_{i+1}) > close(t_i) — future
    information at decision time EOD(t_i).
      - legacy lag=0 next_close: the weight sits at t_{i+1} and earns exactly
        the peeked return -> only positive returns -> huge Sharpe.
      - default next_close (lag=1): earns close(t_{i+1})->close(t_{i+2}),
        independent of the peek -> Sharpe collapses.
      - next_open (lag=1): earns open(t_{i+1})->close(t_{i+1}) = intraday
        noise, (near-)independent of the gap-dominated peek -> collapses.
    """

    def _make_gap_data(self, n: int = 1000, seed: int = 0) -> list[dict]:
        rng = np.random.default_rng(seed)
        rows = []
        close = 100.0
        for i in range(n):
            if i == 0:
                opn = close
            else:
                gap = rng.normal(0.0, 0.02)        # overnight move dominates
                opn = close * (1.0 + gap)
            noise = rng.normal(0.0, 0.0005)         # tiny intraday move
            close = opn * (1.0 + noise)
            rows.append({
                "timestamp": f"t{i:04d}",
                "open": {"AAPL": opn},
                "close": {"AAPL": close},
            })
        return rows

    @staticmethod
    def _sharpe(returns: "pd.Series") -> float:
        arr = np.asarray(returns, dtype=float)
        arr = arr[~np.isnan(arr)]
        if arr.std(ddof=1) == 0:
            return 0.0
        return float(arr.mean() / arr.std(ddof=1) * np.sqrt(252))

    def _leaky_weights(self, data: list[dict]) -> list[np.ndarray]:
        """weights[i] = 1 iff the day-(i+1) close-to-close return is positive."""
        closes = [row["close"]["AAPL"] for row in data]
        weights = []
        for i in range(len(data) - 1):
            w = 1.0 if closes[i + 1] > closes[i] else 0.0
            weights.append(np.array([w]))
        return weights

    def _sharpe_for(self, data, weights_list, lag: int, fill_timing: str) -> float:
        returns_df = _build_returns_df(data, ["AAPL"], fill_timing=fill_timing)
        skip = _fill_skip(lag, fill_timing)
        dates = [row["timestamp"] for row in data[skip:]]
        m = min(len(weights_list), len(dates))
        weights_df = pd.DataFrame(weights_list[:m], index=dates[:m], columns=["AAPL"])
        common = weights_df.index.intersection(returns_df.index)
        port = (weights_df.loc[common] * returns_df.loc[common]).sum(axis=1)
        return self._sharpe(port)

    def test_leak_collapses_under_both_fill_timings(self):
        data = self._make_gap_data()
        weights_list = self._leaky_weights(data)

        sharpe_legacy = self._sharpe_for(data, weights_list, lag=0, fill_timing="next_close")
        sharpe_next_close = self._sharpe_for(data, weights_list, lag=1, fill_timing="next_close")
        sharpe_next_open = self._sharpe_for(data, weights_list, lag=1, fill_timing="next_open")

        # Legacy same-bar fill rewards the peek massively (theoretical annualized
        # Sharpe of the artifact is ~11; allow sampling variation)
        assert sharpe_legacy > 8.0, f"legacy lag=0 Sharpe={sharpe_legacy:.2f}"
        # Both honest timings collapse it
        assert abs(sharpe_next_close) < 2.0, f"next_close Sharpe={sharpe_next_close:.2f}"
        assert abs(sharpe_next_open) < 2.0, f"next_open Sharpe={sharpe_next_open:.2f}"
        assert abs(sharpe_next_close) < sharpe_legacy / 5
        assert abs(sharpe_next_open) < sharpe_legacy / 5


class TestApplyEvaluationSplit:
    def _make_spec(self, **overrides):
        from research.experiments.spec import EvaluationSplitConfig
        spec_data = _make_spec_dict(**overrides)
        return _reconstruct_spec(spec_data)

    def _make_spec_with_split(
        self, train_ratio=0.8, test_ratio=0.2, test_window_months=None,
        label_horizon=0, purge_buffer=0,
    ):
        """Build a spec with an evaluation split.

        label_horizon=0, purge_buffer=0 by default so existing size assertions
        are not perturbed; tests that verify purge set these explicitly.
        """
        spec_data = _make_spec_dict()
        spec_data["evaluation_split"] = {
            "train_ratio": train_ratio,
            "test_ratio": test_ratio,
            "label_horizon": label_horizon,
            "purge_buffer": purge_buffer,
            **({"test_window_months": test_window_months} if test_window_months else {}),
        }
        return _reconstruct_spec(spec_data)

    def test_no_split_returns_full_data_for_both(self):
        spec = self._make_spec()
        data = _make_data_rows(20)
        train, test, meta = _apply_evaluation_split(data, spec)
        assert train is data
        assert test is data
        assert meta == {}

    def test_ratio_split_sizes(self):
        # label_horizon=0, embargo_pct=0 → clean split at split_idx, no overlap
        spec = self._make_spec_with_split(train_ratio=0.8, test_ratio=0.2)
        data = _make_data_rows(100)
        train, test, meta = _apply_evaluation_split(data, spec)
        # split_idx=80, label_horizon=0, embargo_bars=0 → train[:80], test[80:]
        assert len(train) == 80
        assert len(test) == 20

    def test_train_and_test_are_disjoint(self):
        """No overlap bar — train and test must be strictly disjoint."""
        spec = self._make_spec_with_split(train_ratio=0.7, test_ratio=0.3)
        data = _make_data_rows(50)
        train, test, _ = _apply_evaluation_split(data, spec)
        assert train[0] is data[0]
        assert test[-1] is data[-1]
        # No shared bars
        train_ts = {r["timestamp"] for r in train}
        test_ts = {r["timestamp"] for r in test}
        assert train_ts.isdisjoint(test_ts), "Train and test must not share any bars"

    def test_test_window_months_overrides_ratio(self):
        # label_horizon=0, embargo_pct=0 → only test_window controls size
        spec = self._make_spec_with_split(train_ratio=0.8, test_ratio=0.2, test_window_months=3)
        data = _make_data_rows(200)
        train, test, _ = _apply_evaluation_split(data, spec)
        # test_size = min(3*21, 198) = 63; split_idx = 200-63=137
        # No overlap, no purge/embargo → test = data[137:], len=63
        assert len(test) == 63

    def test_very_small_data_clamps_split_idx(self):
        spec = self._make_spec_with_split(train_ratio=0.8, test_ratio=0.2)
        data = _make_data_rows(4)
        train, test, _ = _apply_evaluation_split(data, spec)
        assert len(train) >= 1
        assert len(test) >= 1

    def test_split_enforced_train_does_not_contain_test_bars(self):
        spec = self._make_spec_with_split(train_ratio=0.6, test_ratio=0.4)
        data = _make_data_rows(20)
        train, test, _ = _apply_evaluation_split(data, spec)
        test_timestamps = {r["timestamp"] for r in test}
        train_timestamps = {r["timestamp"] for r in train}
        assert test_timestamps.isdisjoint(train_timestamps), (
            "Test bars must not appear in training data"
        )

    # ------------------------------------------------------------------
    # Purge + embargo tests
    # ------------------------------------------------------------------

    def test_purge_drops_label_horizon_bars_from_train(self):
        """With label_horizon=2, train loses 2 bars from its end."""
        spec = self._make_spec_with_split(train_ratio=0.8, test_ratio=0.2, label_horizon=2)
        data = _make_data_rows(100)
        train, test, meta = _apply_evaluation_split(data, spec)
        # split_idx=80, label_horizon=2, purge_buffer=0 → train[:78], test[80:]
        assert len(train) == 78
        assert meta["purged_label_bars"] == 2

    def test_purge_buffer_widens_train_gap_before_side_only(self):
        """purge_buffer widens the before-side gap; test_start is NOT moved.

        Classic after-embargo is intentionally omitted — the split is forward-only
        (test is the terminal edge), so there is no post-test training to embargo.
        """
        spec = self._make_spec_with_split(
            train_ratio=0.8, test_ratio=0.2, label_horizon=0, purge_buffer=5,
        )
        data = _make_data_rows(100)
        train, test, meta = _apply_evaluation_split(data, spec)
        # split_idx=80, purge_buffer=5 (absolute, before-side only)
        # train = data[:80-5] = data[:75]; test = data[80:] (no after-embargo)
        assert len(train) == 75
        assert len(test) == 20
        assert meta["purged_buffer_bars"] == 5
        # Verify the gap: last train timestamp < first test timestamp
        train_ts_set = {r["timestamp"] for r in train}
        test_ts_set = {r["timestamp"] for r in test}
        assert train_ts_set.isdisjoint(test_ts_set)

    def test_purge_and_buffer_combined(self):
        """label_horizon + purge_buffer bars are both removed from before-side."""
        spec = self._make_spec_with_split(
            train_ratio=0.8, test_ratio=0.2, label_horizon=1, purge_buffer=1,
        )
        data = _make_data_rows(100)
        train, test, meta = _apply_evaluation_split(data, spec)
        # split_idx=80, purge=1, purge_buffer=1 (absolute)
        # train = data[:80-1-1] = data[:78]; test = data[80:] (no after-embargo)
        assert len(train) == 78
        assert len(test) == 20
        assert meta["purged_label_bars"] == 1
        assert meta["purged_buffer_bars"] == 1

    def test_purge_buffer_is_absolute_not_pct(self):
        """purge_buffer is an absolute bar count, independent of dataset size."""
        spec_small = self._make_spec_with_split(
            train_ratio=0.8, test_ratio=0.2, label_horizon=0, purge_buffer=10,
        )
        spec_large = self._make_spec_with_split(
            train_ratio=0.8, test_ratio=0.2, label_horizon=0, purge_buffer=10,
        )
        train_small, _, meta_small = _apply_evaluation_split(_make_data_rows(100), spec_small)
        train_large, _, meta_large = _apply_evaluation_split(_make_data_rows(500), spec_large)
        # Both should drop exactly 10 bars (not 10% of n)
        assert meta_small["purged_buffer_bars"] == 10
        assert meta_large["purged_buffer_bars"] == 10
        # train sizes: 80-10=70 for n=100; 400-10=390 for n=500
        assert len(train_small) == 70
        assert len(train_large) == 390

    def test_purge_metadata_fields(self):
        """split_meta carries boundary dates and separate purge component counts."""
        spec = self._make_spec_with_split(
            train_ratio=0.8, test_ratio=0.2, label_horizon=1, purge_buffer=1,
        )
        data = _make_data_rows(100)
        _, _, meta = _apply_evaluation_split(data, spec)
        assert "purged_label_bars" in meta
        assert "purged_buffer_bars" in meta
        assert "train_boundary_date" in meta
        assert "test_boundary_date" in meta
        assert "train_rows" in meta
        assert "test_rows" in meta

    def test_purge_creates_verified_gap_before_side_only(self):
        """Purge produces a genuine before-side gap; test window is NOT shrunk.

        Without purge, train and test are adjacent (no temporal gap).
        With label_horizon=5 and purge_buffer=4, the gap = 5 + 4 = 9 bars
        on the before-side only (test_start stays at split_idx).
        """
        rng = np.random.default_rng(0)
        n = 200
        data = []
        for i in range(n):
            data.append({
                "timestamp": f"2024-{i+1:03d}",
                "close": {"AAPL": 100.0 + rng.normal(0, 1)},
                "signal": rng.normal(0, 1),
            })

        # Without purge (label_horizon=0, purge_buffer=0)
        spec_leaky = self._make_spec_with_split(
            train_ratio=0.8, test_ratio=0.2, label_horizon=0, purge_buffer=0,
        )
        train_leaky, test_leaky, _ = _apply_evaluation_split(data, spec_leaky)

        # With purge on before-side only
        spec_clean = self._make_spec_with_split(
            train_ratio=0.8, test_ratio=0.2, label_horizon=5, purge_buffer=4,
        )
        train_clean, test_clean, meta = _apply_evaluation_split(data, spec_clean)

        leaky_train_ts = {r["timestamp"] for r in train_leaky}
        leaky_test_ts = {r["timestamp"] for r in test_leaky}
        assert leaky_train_ts.isdisjoint(leaky_test_ts)

        # Verified gap between train-end and test-start
        clean_train_last = train_clean[-1]["timestamp"]
        clean_test_first = test_clean[0]["timestamp"]
        ts_list = [r["timestamp"] for r in data]
        idx_train_end = ts_list.index(clean_train_last)
        idx_test_start = ts_list.index(clean_test_first)
        gap = idx_test_start - idx_train_end - 1
        # gap = label_horizon + purge_buffer = 5 + 4 = 9 (before-side only, no after-embargo)
        assert gap == 9, f"Expected gap=9 (label_horizon=5 + purge_buffer=4), got {gap}"
        assert meta["purged_label_bars"] == 5
        assert meta["purged_buffer_bars"] == 4

    def test_purge_buffer_defaults_to_feature_max_lookback(self):
        """When purge_buffer=None, auto-computes from the feature set's max lookback."""
        import research.features.ohlcv_features  # noqa: F401 — register features
        import research.features.cross_sectional_features  # noqa: F401
        import research.features.fundamental_features  # noqa: F401
        import research.features.regime_features_v1  # noqa: F401

        spec_data = _make_spec_dict()
        spec_data["evaluation_split"] = {
            "train_ratio": 0.8,
            "test_ratio": 0.2,
            "label_horizon": 0,
            # purge_buffer omitted → None → auto from feature registry
        }
        spec = _reconstruct_spec(spec_data)
        # core_v1 max lookback = 252 (mom_12m_excl_1m uses shift(252))
        data = _make_data_rows(600)
        _, _, meta = _apply_evaluation_split(data, spec)
        assert meta["purged_buffer_bars"] == 252


class TestNonRlNoLookahead:
    """Verify _rollout_non_rl_policy uses close[t] to decide weight for r(t→t+1)."""

    def test_weight_uses_prior_bar_close(self):
        """weights[i] must use close from data[i], not data[i+1]."""
        # Construct data where close at bar i equals float(i+1) (distinct values).
        symbols = ["AAPL"]
        data = [
            {"timestamp": f"t{i}", "close": {"AAPL": float(i + 1)}}
            for i in range(5)
        ]

        recorded_prices: list[float] = []

        # Patch EqualWeightPolicy to record the prices passed to act().
        import research.policies.equal_weight_policy as ew_mod
        original_class = ew_mod.EqualWeightPolicy

        class PriceRecordingPolicy:
            def __init__(self, n):
                pass
            def reset(self):
                pass
            def act(self, prices, info):
                recorded_prices.append(float(prices[0]))
                return np.array([1.0])

        ew_mod.EqualWeightPolicy = PriceRecordingPolicy
        try:
            _rollout_non_rl_policy("equal_weight", data, symbols, {})
        finally:
            ew_mod.EqualWeightPolicy = original_class

        # With the fix (data[:-1] iteration):
        #   first call sees data[0].close = 1.0
        # With the bug (data[1:] iteration):
        #   first call sees data[1].close = 2.0
        assert recorded_prices[0] == pytest.approx(1.0), (
            f"Expected first weight from data[0].close=1.0, got {recorded_prices[0]:.1f}. "
            "This indicates the lookahead bug (data[1:] iteration) is still present."
        )
        assert len(recorded_prices) == len(data) - 1  # N-1 weights for N bars


class TestWriteRunArtifacts:
    def test_artifacts_written(self, tmp_path):
        symbols = ["AAPL", "MSFT"]
        data = _make_data_rows(5)
        weights_df = pd.DataFrame(
            np.full((4, 2), 0.5),
            index=[f"2024-01-0{i+2}" for i in range(4)],
            columns=symbols,
        )
        metrics = {
            "performance": {"sharpe": 1.2, "total_return": 0.1},
            "trading": {"win_rate": 0.55},
            "safety": {},
        }
        features = {"data_hash": "abc123", "observation_columns": ["close"], "feature_set": "core_v1"}
        spec_data = {"policy": "equal_weight", "symbols": symbols, "feature_set": "core_v1"}

        _write_run_artifacts(
            data_root=tmp_path,
            experiment_id="test_exp",
            spec_data=spec_data,
            weights_df=weights_df,
            metrics=metrics,
            features=features,
            dagster_run_id="run-123",
        )

        rollout_path = tmp_path / "experiments" / "test_exp" / "runs" / "rollout.json"
        summary_path = tmp_path / "experiments" / "test_exp" / "logs" / "run_summary.json"

        assert rollout_path.exists(), "runs/rollout.json must be written"
        assert summary_path.exists(), "logs/run_summary.json must be written"

        import json
        rollout = json.loads(rollout_path.read_text())
        assert rollout["metadata"]["experiment_id"] == "test_exp"
        assert rollout["performance"]["sharpe"] == pytest.approx(1.2)
        assert rollout["trading"]["win_rate"] == pytest.approx(0.55)
        assert "AAPL" in rollout["series"]["weights"]

        summary = json.loads(summary_path.read_text())
        assert summary["experiment_id"] == "test_exp"
        assert summary["dagster_run_id"] == "run-123"
        assert "rollout_json" in summary["artifacts"]


# ---------------------------------------------------------------------------
# _merge_closes_into_features tests
# ---------------------------------------------------------------------------


class TestMergeClosesIntoFeatures:
    def _features_df(self, rows):
        return pd.DataFrame(rows)

    def _closes_df(self, rows):
        return pd.DataFrame(rows)

    def test_close_column_present_in_merged_result(self):
        features = self._features_df([
            {"timestamp": "2024-01-01", "symbol": "AAPL", "sma_20d": 0.5},
            {"timestamp": "2024-01-01", "symbol": "MSFT", "sma_20d": 0.7},
        ])
        closes = self._closes_df([
            {"timestamp": "2024-01-01", "symbol": "AAPL", "close": 100.0},
            {"timestamp": "2024-01-01", "symbol": "MSFT", "close": 200.0},
        ])
        merged, n_dropped = _merge_closes_into_features(features, closes)

        assert n_dropped == 0
        assert "close" in merged.columns
        aapl = merged[merged["symbol"] == "AAPL"].iloc[0]
        assert aapl["close"] == pytest.approx(100.0)
        msft = merged[merged["symbol"] == "MSFT"].iloc[0]
        assert msft["close"] == pytest.approx(200.0)

    def test_missing_close_row_is_dropped(self):
        features = self._features_df([
            {"timestamp": "2024-01-01", "symbol": "AAPL", "sma_20d": 0.5},
            {"timestamp": "2024-01-01", "symbol": "MSFT", "sma_20d": 0.7},
            {"timestamp": "2024-01-02", "symbol": "AAPL", "sma_20d": 0.6},
            {"timestamp": "2024-01-02", "symbol": "MSFT", "sma_20d": 0.8},  # no close
        ])
        closes = self._closes_df([
            {"timestamp": "2024-01-01", "symbol": "AAPL", "close": 100.0},
            {"timestamp": "2024-01-01", "symbol": "MSFT", "close": 200.0},
            {"timestamp": "2024-01-02", "symbol": "AAPL", "close": 101.0},
            # MSFT 2024-01-02 intentionally absent
        ])
        merged, n_dropped = _merge_closes_into_features(features, closes)

        assert n_dropped == 1
        assert len(merged) == 3
        missing = merged[(merged["symbol"] == "MSFT") & (merged["timestamp"] == "2024-01-02")]
        assert len(missing) == 0

    def test_empty_features_returns_empty_unchanged(self):
        merged, n_dropped = _merge_closes_into_features(
            pd.DataFrame(),
            self._closes_df([{"timestamp": "2024-01-01", "symbol": "AAPL", "close": 100.0}]),
        )
        assert n_dropped == 0
        assert merged.empty

    def test_env_rows_contain_close_after_merge(self):
        """Full pipeline: merge → _dataframe_to_env_rows produces rows with 'close'."""
        features = self._features_df([
            {"timestamp": "2024-01-01", "symbol": "AAPL", "sma_20d": 0.5},
            {"timestamp": "2024-01-01", "symbol": "MSFT", "sma_20d": 0.7},
        ])
        closes = self._closes_df([
            {"timestamp": "2024-01-01", "symbol": "AAPL", "close": 150.0},
            {"timestamp": "2024-01-01", "symbol": "MSFT", "close": 300.0},
        ])
        merged, _ = _merge_closes_into_features(features, closes)
        rows = _dataframe_to_env_rows(merged, ["AAPL", "MSFT"], ["close", "sma_20d"], [])

        assert len(rows) == 1
        assert "close" in rows[0], f"'close' missing from env row keys: {list(rows[0].keys())}"
        assert rows[0]["close"]["AAPL"] == pytest.approx(150.0)
        assert rows[0]["close"]["MSFT"] == pytest.approx(300.0)

    def test_dropped_rows_not_nan_filled(self):
        """Dropped rows must be absent, not NaN-filled (close is load-bearing)."""
        features = self._features_df([
            {"timestamp": "2024-01-01", "symbol": "AAPL", "sma_20d": 0.5},
            {"timestamp": "2024-01-01", "symbol": "MISSING", "sma_20d": 0.9},
        ])
        closes = self._closes_df([
            {"timestamp": "2024-01-01", "symbol": "AAPL", "close": 100.0},
        ])
        merged, n_dropped = _merge_closes_into_features(features, closes)

        assert n_dropped == 1
        assert "MISSING" not in merged["symbol"].values
        assert merged["close"].notna().all()


# ---------------------------------------------------------------------------
# fetch_features integration test (requires live QuestDB)
# ---------------------------------------------------------------------------


@pytest.mark.live_db
class TestFetchFeaturesLiveDB:
    def test_fetch_features_env_rows_contain_close(self):
        """Integration: fetch_features joins canonical closes; env rows have 'close'."""
        from dagster import build_op_context
        from pipelines.yats_pipelines.jobs.experiment_run import (
            ExperimentRunConfig,
            fetch_features,
        )

        context = build_op_context()
        config = ExperimentRunConfig(experiment_id="live_integration_test", allow_empty_data=True)
        spec_data = {
            "symbols": ["AAPL", "MSFT"],
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "feature_set": "core_v1",
            "policy": "ppo",
            "policy_params": {},
        }

        result = fetch_features(context, config, spec_data)

        assert "data" in result
        assert "observation_columns" in result
        assert "close" in result["observation_columns"], (
            "observation_columns must include 'close'"
        )
        if result["data"]:
            row = result["data"][0]
            assert "close" in row, (
                f"fetch_features env row missing 'close'; keys present: {list(row.keys())}"
            )

    def test_fetch_features_filters_by_feature_set(self):
        """Multiple feature sets coexist in the features table; the query must
        filter by the spec's feature_set or each (symbol, timestamp) yields one
        row per set - two of them partially null - corrupting env rows."""
        from dagster import build_op_context
        from pipelines.yats_pipelines.jobs.experiment_run import (
            ExperimentRunConfig,
            fetch_features,
        )

        context = build_op_context()
        config = ExperimentRunConfig(experiment_id="live_fs_filter_test", allow_empty_data=True)
        spec_data = {
            "symbols": ["AAPL"],
            "start_date": "2026-06-01",
            "end_date": "2026-06-30",
            "feature_set": "full_v1",
            "policy": "ppo",
            "policy_params": {},
        }

        result = fetch_features(context, config, spec_data)
        if not result["data"]:
            pytest.skip("no full_v1 rows in live DB for the window")

        # Exactly one env row per timestamp; without the feature_set filter,
        # core_v1/options_v1/full_v1 rows for the same (symbol, timestamp)
        # would all be fetched.
        timestamps = [r["timestamp"] for r in result["data"]]
        assert len(timestamps) == len(set(timestamps)), "duplicate env rows per timestamp"
        # full_v1 rows carry BOTH core and options columns
        assert "atm_iv" in result["observation_columns"]
        assert "ret_1d" in result["observation_columns"]
