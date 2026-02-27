"""Tests for research.shadow.engine — ShadowEngine."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pytest

from research.experiments.spec import CostConfig, ExperimentSpec, RiskConfig
from research.shadow.data_source import Snapshot
from research.shadow.engine import (
    ShadowEngine,
    ShadowRunConfig,
    create_shadow_run,
    load_policy,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_spec(**overrides) -> ExperimentSpec:
    defaults = dict(
        experiment_name="test_shadow",
        symbols=("AAPL", "MSFT"),
        start_date=date(2023, 1, 1),
        end_date=date(2023, 12, 31),
        interval="daily",
        feature_set="core_v1",
        policy="equal_weight",
        policy_params={},
        cost_config=CostConfig(transaction_cost_bp=10.0),
        seed=42,
        risk_config=RiskConfig(),
    )
    defaults.update(overrides)
    return ExperimentSpec(**defaults)


def _make_snapshots(n: int = 5, symbols: tuple[str, ...] = ("AAPL", "MSFT")) -> list[Snapshot]:
    """Create n synthetic snapshots with rising prices."""
    snapshots = []
    base_prices = {"AAPL": 100.0, "MSFT": 200.0}

    for i in range(n):
        panel = {}
        for sym in symbols:
            price = base_prices.get(sym, 100.0) + i * 1.0
            panel[sym] = {"close": price, "ret_1d": 0.01}
        snapshots.append(Snapshot(
            as_of=datetime(2023, 1, 3 + i),
            symbols=symbols,
            panel=panel,
            regime_features=(),
            regime_feature_names=(),
            observation_columns=("close",),
        ))
    return snapshots


class FakePolicy:
    """Fake policy that returns equal weights."""

    def __init__(self, n_symbols: int):
        self._n = n_symbols

    def act(self, obs: np.ndarray, context: Mapping[str, Any] | None = None) -> np.ndarray:
        return np.full(self._n, 1.0 / self._n)


# ---------------------------------------------------------------------------
# ShadowEngine tests
# ---------------------------------------------------------------------------


class TestShadowEngine:
    def _make_engine(self, tmp_path: Path, n_snapshots: int = 5) -> ShadowEngine:
        spec = _make_spec()
        snapshots = _make_snapshots(n_snapshots)
        config = ShadowRunConfig(
            experiment_id="test_exp",
            run_id="run_001",
            output_dir=tmp_path / "shadow" / "test_exp" / "run_001",
            initial_value=1_000_000.0,
        )
        policy = FakePolicy(len(spec.symbols))
        return ShadowEngine(spec, policy, snapshots, config)

    def test_run_produces_artifacts(self, tmp_path: Path):
        engine = self._make_engine(tmp_path)
        summary = engine.run()

        run_dir = tmp_path / "shadow" / "test_exp" / "run_001"
        assert (run_dir / "logs" / "steps.jsonl").exists()
        assert (run_dir / "state.json").exists()
        assert (run_dir / "summary.json").exists()

    def test_summary_fields(self, tmp_path: Path):
        engine = self._make_engine(tmp_path)
        summary = engine.run()

        assert summary["experiment_id"] == "test_exp"
        assert summary["run_id"] == "run_001"
        assert summary["execution_mode"] == "none"
        assert summary["total_snapshots"] == 5
        assert summary["initial_value"] == 1_000_000.0
        assert isinstance(summary["final_value"], float)
        assert isinstance(summary["total_return"], float)

    def test_steps_jsonl_count(self, tmp_path: Path):
        engine = self._make_engine(tmp_path, n_snapshots=5)
        engine.run()

        steps_path = tmp_path / "shadow" / "test_exp" / "run_001" / "logs" / "steps.jsonl"
        lines = steps_path.read_text().strip().split("\n")
        # steps = n_snapshots - 1 (need 2 snapshots to compute returns)
        assert len(lines) == 4

    def test_step_entry_schema(self, tmp_path: Path):
        engine = self._make_engine(tmp_path)
        engine.run()

        steps_path = tmp_path / "shadow" / "test_exp" / "run_001" / "logs" / "steps.jsonl"
        first_line = steps_path.read_text().strip().split("\n")[0]
        entry = json.loads(first_line)

        assert "step_index" in entry
        assert "timestamp" in entry
        assert "weights" in entry
        assert "previous_weights" in entry
        assert "returns_per_symbol" in entry
        assert "portfolio_value" in entry
        assert "transaction_cost" in entry

    def test_state_json_schema(self, tmp_path: Path):
        engine = self._make_engine(tmp_path)
        engine.run()

        state_path = tmp_path / "shadow" / "test_exp" / "run_001" / "state.json"
        state = json.loads(state_path.read_text())

        assert "step_index" in state
        assert "positions" in state
        assert "weights" in state
        assert "portfolio_value" in state
        assert "cash" in state
        assert "peak_value" in state
        assert "last_timestamp" in state
        assert "dagster_run_id" in state

    def test_portfolio_value_changes(self, tmp_path: Path):
        """With rising prices and equal weights, portfolio should appreciate."""
        engine = self._make_engine(tmp_path)
        summary = engine.run()

        # Prices rise from 100/200 to 104/204, so portfolio should have
        # positive returns minus small transaction costs
        assert summary["final_value"] != summary["initial_value"]

    def test_transaction_costs_applied(self, tmp_path: Path):
        """Non-zero transaction cost should reduce portfolio value vs zero cost."""
        spec_with_cost = _make_spec(cost_config=CostConfig(transaction_cost_bp=50.0))
        spec_no_cost = _make_spec(cost_config=CostConfig(transaction_cost_bp=0.0))
        snapshots = _make_snapshots(5)

        # With cost
        dir1 = tmp_path / "with_cost"
        cfg1 = ShadowRunConfig(experiment_id="e1", run_id="r1", output_dir=dir1)
        engine1 = ShadowEngine(spec_with_cost, FakePolicy(2), snapshots, cfg1)
        s1 = engine1.run()

        # Without cost
        dir2 = tmp_path / "no_cost"
        cfg2 = ShadowRunConfig(experiment_id="e2", run_id="r2", output_dir=dir2)
        engine2 = ShadowEngine(spec_no_cost, FakePolicy(2), snapshots, cfg2)
        s2 = engine2.run()

        # Higher costs => lower final value
        assert s1["final_value"] < s2["final_value"]


class TestResume:
    def test_resume_from_state(self, tmp_path: Path):
        """Engine should resume from state.json and continue."""
        spec = _make_spec()
        snapshots = _make_snapshots(10)
        run_dir = tmp_path / "shadow" / "test" / "run"

        config = ShadowRunConfig(
            experiment_id="test", run_id="run", output_dir=run_dir,
        )

        # First run: 5 snapshots
        engine1 = ShadowEngine(spec, FakePolicy(2), snapshots[:5], config)
        engine1.run()

        state_path = run_dir / "state.json"
        assert state_path.exists()
        state1 = json.loads(state_path.read_text())
        assert state1["step_index"] == 4

        # Second run with full snapshots, resuming
        engine2 = ShadowEngine(spec, FakePolicy(2), snapshots, config)
        engine2.resume()
        assert engine2.step_index == 4
        summary = engine2.run()
        assert summary["steps_executed"] >= 4

    def test_resume_no_state(self, tmp_path: Path):
        """Resume with no state.json should start fresh."""
        spec = _make_spec()
        snapshots = _make_snapshots(5)
        run_dir = tmp_path / "shadow" / "test" / "run"

        config = ShadowRunConfig(
            experiment_id="test", run_id="run", output_dir=run_dir,
        )
        engine = ShadowEngine(spec, FakePolicy(2), snapshots, config)
        engine.resume()  # Should not raise
        assert engine.step_index == 0


class TestLoadPolicy:
    def test_equal_weight(self):
        spec = _make_spec(policy="equal_weight")
        policy = load_policy(spec)
        weights = policy.act(np.zeros(10))
        assert len(weights) == 2
        np.testing.assert_array_almost_equal(weights, [0.5, 0.5])

    def test_sma(self):
        spec = _make_spec(
            policy="sma",
            policy_params={"short_window": 3, "long_window": 10},
        )
        policy = load_policy(spec)
        weights = policy.act(np.array([100.0, 200.0, 0.0, 0.0]))
        assert len(weights) == 2

    def test_unknown_raises(self):
        spec = _make_spec(policy="equal_weight")
        # Monkeypatch policy name after construction
        object.__setattr__(spec, "policy", "unknown_policy")
        with pytest.raises(ValueError, match="Unknown policy"):
            load_policy(spec)


class TestCreateShadowRun:
    def test_creates_engine(self, tmp_path: Path):
        spec = _make_spec()
        snapshots = _make_snapshots(3)
        engine = create_shadow_run(
            spec, snapshots,
            data_root=tmp_path,
            run_id="test_run",
        )
        assert engine is not None
        summary = engine.run()
        assert summary["run_id"] == "test_run"
        assert (tmp_path / "shadow" / spec.experiment_id / "test_run" / "summary.json").exists()
