"""Tests for research.eval.evaluate — core evaluation pipeline."""

from __future__ import annotations

import json
import tempfile
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from research.experiments.spec import CostConfig, ExperimentSpec
from research.eval.evaluate import evaluate, evaluate_to_json


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_spec(**overrides) -> ExperimentSpec:
    defaults = dict(
        experiment_name="test_eval",
        symbols=("AAPL", "MSFT"),
        start_date=date(2023, 1, 1),
        end_date=date(2024, 1, 1),
        interval="daily",
        feature_set="core_v1",
        policy="equal_weight",
        policy_params={},
        cost_config=CostConfig(transaction_cost_bp=5.0),
        seed=42,
    )
    defaults.update(overrides)
    return ExperimentSpec(**defaults)


def _make_test_data(n: int = 252, n_symbols: int = 2, seed: int = 42):
    """Generate synthetic weights and returns for testing."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2023-01-01", periods=n)
    symbols = [f"SYM{i}" for i in range(n_symbols)]

    # Equal weights
    weights = pd.DataFrame(
        np.full((n, n_symbols), 1.0 / n_symbols),
        index=dates, columns=symbols,
    )
    # Random returns
    returns = pd.DataFrame(
        rng.normal(0.0005, 0.01, (n, n_symbols)),
        index=dates, columns=symbols,
    )
    return weights, returns


def _make_regime_features(dates: pd.DatetimeIndex, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    n = len(dates)
    return pd.DataFrame({
        "market_vol_20d": rng.uniform(0.08, 0.30, n),
        "market_trend_20d": rng.normal(0.0, 0.05, n),
        "dispersion_20d": rng.uniform(0.01, 0.05, n),
        "corr_mean_20d": rng.uniform(0.2, 0.8, n),
    }, index=dates)


# ---------------------------------------------------------------------------
# evaluate()
# ---------------------------------------------------------------------------

class TestEvaluate:
    def test_basic_evaluation(self):
        spec = _make_spec()
        weights, returns = _make_test_data()
        result = evaluate(spec, weights, returns)

        # Check all required top-level sections
        assert "metadata" in result
        assert "performance" in result
        assert "trading" in result
        assert "safety" in result
        assert "performance_by_regime" in result
        assert "regime" in result
        assert "series" in result
        assert "inputs_used" in result
        assert "config" in result

    def test_metadata(self):
        spec = _make_spec()
        weights, returns = _make_test_data()
        result = evaluate(spec, weights, returns)

        meta = result["metadata"]
        assert meta["experiment_id"] == spec.experiment_id
        assert meta["feature_set_version"] == "core_v1"
        assert "start" in meta["data_range"]
        assert "end" in meta["data_range"]

    def test_performance_fields(self):
        spec = _make_spec()
        weights, returns = _make_test_data()
        result = evaluate(spec, weights, returns)

        perf = result["performance"]
        for key in ["sharpe", "calmar", "sortino", "total_return",
                     "annualized_return", "max_drawdown", "max_drawdown_duration"]:
            assert key in perf, f"Missing performance key: {key}"

    def test_trading_fields(self):
        spec = _make_spec()
        weights, returns = _make_test_data()
        result = evaluate(spec, weights, returns)

        trading = result["trading"]
        for key in ["turnover_1d_mean", "turnover_1d_std",
                     "avg_holding_period", "win_rate", "profit_factor"]:
            assert key in trading, f"Missing trading key: {key}"

    def test_safety_fields(self):
        spec = _make_spec()
        weights, returns = _make_test_data()
        result = evaluate(spec, weights, returns)

        safety = result["safety"]
        assert "nan_inf_violations" in safety
        assert "constraint_violations" in safety
        assert safety["nan_inf_violations"] == 0

    def test_with_regime_slicing(self):
        spec = _make_spec(regime_labeling="v1")
        weights, returns = _make_test_data()
        regime_features = _make_regime_features(weights.index)

        result = evaluate(spec, weights, returns, regime_features=regime_features)

        assert len(result["performance_by_regime"]) > 0
        assert result["regime"]["labeling_version"] == "v1"

        # Check each bucket has expected keys
        for bucket, metrics in result["performance_by_regime"].items():
            assert "sharpe" in metrics
            assert "n_days" in metrics

    def test_without_regime(self):
        spec = _make_spec()
        weights, returns = _make_test_data()
        result = evaluate(spec, weights, returns)

        assert result["performance_by_regime"] == {}
        assert result["regime"] == {}

    def test_series_output(self):
        spec = _make_spec()
        weights, returns = _make_test_data()
        result = evaluate(spec, weights, returns)

        assert "equity_curve" in result["series"]
        assert "drawdown_series" in result["series"]
        assert len(result["series"]["equity_curve"]) > 0

    def test_config_embedded(self):
        spec = _make_spec()
        weights, returns = _make_test_data()
        result = evaluate(spec, weights, returns)

        config = result["config"]
        assert config["experiment_name"] == "test_eval"
        assert config["experiment_id"] == spec.experiment_id
        assert config["policy"] == "equal_weight"

    def test_deterministic(self):
        """Same input → same output."""
        spec = _make_spec()
        weights, returns = _make_test_data()

        r1 = evaluate(spec, weights, returns)
        r2 = evaluate(spec, weights, returns)

        assert r1["performance"] == r2["performance"]
        assert r1["trading"] == r2["trading"]
        assert r1["metadata"]["experiment_id"] == r2["metadata"]["experiment_id"]

    def test_data_hash_passthrough(self):
        spec = _make_spec()
        weights, returns = _make_test_data()
        result = evaluate(spec, weights, returns, data_hash="abc123")
        assert result["inputs_used"]["data_hash"] == "abc123"


# ---------------------------------------------------------------------------
# evaluate_to_json()
# ---------------------------------------------------------------------------

class TestEvaluateToJson:
    def test_writes_metrics_json(self):
        spec = _make_spec()
        weights, returns = _make_test_data()

        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "evaluation" / "metrics.json"
            result = evaluate_to_json(spec, weights, returns, out)

            assert out.exists()
            with open(out) as f:
                loaded = json.load(f)
            assert loaded["metadata"]["experiment_id"] == spec.experiment_id

    def test_writes_timeseries_json(self):
        spec = _make_spec()
        weights, returns = _make_test_data()

        with tempfile.TemporaryDirectory() as tmpdir:
            metrics_path = Path(tmpdir) / "metrics.json"
            ts_path = Path(tmpdir) / "timeseries.json"

            evaluate_to_json(
                spec, weights, returns, metrics_path,
                timeseries_path=ts_path,
            )

            assert ts_path.exists()
            with open(ts_path) as f:
                ts = json.load(f)
            assert "equity_curve" in ts
            assert "drawdown_series" in ts
            assert "weight_history" in ts
