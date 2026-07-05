"""End-to-end test: hierarchical policy runs through ShadowEngine, emits mode_timeline.json."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pytest

from research.experiments.spec import CostConfig, ExperimentSpec, RiskConfig
from research.shadow.data_source import Snapshot
from research.shadow.engine import ShadowEngine, ShadowRunConfig, load_policy


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_CONTROLLER_CFG = {
    "update_frequency": "every_k_steps",
    "k_steps": 1,
    "vol_threshold_high": 0.3,
    "trend_threshold_high": 0.05,
    "dispersion_threshold_high": 0.02,
    "min_hold_steps": 0,
}

_ALLOCATOR_BY_MODE = {
    "risk_on": {"type": "equal_weight"},
    "neutral": {"type": "equal_weight"},
    "defensive": {"type": "equal_weight"},
}


def _make_hierarchical_spec(**overrides) -> ExperimentSpec:
    defaults = dict(
        experiment_name="test_hierarchical",
        symbols=("AAPL", "MSFT"),
        start_date=date(2023, 1, 1),
        end_date=date(2023, 12, 31),
        interval="daily",
        feature_set="core_v1",
        policy="hierarchical",
        policy_params={},
        cost_config=CostConfig(transaction_cost_bp=5.0),
        seed=42,
        risk_config=RiskConfig(),
        hierarchy_enabled=True,
        controller_config=_CONTROLLER_CFG,
        allocator_by_mode=_ALLOCATOR_BY_MODE,
    )
    defaults.update(overrides)
    return ExperimentSpec(**defaults)


def _make_snapshots_with_regime(symbols=("AAPL", "MSFT")) -> list[Snapshot]:
    """6 snapshots: first 3 low-vol (neutral), last 3 high-vol (defensive)."""
    feature_names = ("market_vol_20d", "market_trend_20d", "dispersion_20d")
    snapshots = []
    for i in range(6):
        panel = {sym: {"close": 100.0 + i, "ret_1d": 0.01} for sym in symbols}
        # Steps 0-2: vol=0.1 → neutral; steps 3-5: vol=0.5 → defensive
        vol = 0.1 if i < 3 else 0.5
        regime = (vol, 0.0, 0.0)
        snapshots.append(Snapshot(
            as_of=datetime(2023, 1, 3 + i),
            symbols=symbols,
            panel=panel,
            regime_features=regime,
            regime_feature_names=feature_names,
            observation_columns=("close",),
        ))
    return snapshots


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestHierarchicalEndToEnd:
    def test_load_policy_returns_hierarchical_policy(self):
        from research.hierarchy.policy_wrapper import HierarchicalPolicy
        spec = _make_hierarchical_spec()
        policy = load_policy(spec)
        assert isinstance(policy, HierarchicalPolicy)

    def test_load_policy_wrong_policy_name_raises(self):
        spec = _make_hierarchical_spec(policy="hierarchical", hierarchy_enabled=False)
        with pytest.raises(ValueError, match="hierarchy_enabled"):
            load_policy(spec)

    def test_shadow_run_emits_mode_timeline(self, tmp_path: Path):
        spec = _make_hierarchical_spec()
        policy = load_policy(spec)
        snapshots = _make_snapshots_with_regime()
        config = ShadowRunConfig(
            experiment_id="test_hier",
            run_id="run_001",
            output_dir=tmp_path / "shadow" / "test_hier" / "run_001",
        )
        engine = ShadowEngine(spec, policy, snapshots, config)
        engine.run()

        timeline_path = tmp_path / "shadow" / "test_hier" / "run_001" / "evaluation" / "mode_timeline.json"
        assert timeline_path.exists(), "mode_timeline.json was not written"

        timeline = json.loads(timeline_path.read_text())
        assert isinstance(timeline, list)
        assert len(timeline) >= 1
        assert all("step" in entry and "mode" in entry for entry in timeline)

    def test_shadow_run_timeline_contains_mode_transition(self, tmp_path: Path):
        """Regime shift mid-replay must produce at least one mode transition."""
        spec = _make_hierarchical_spec()
        policy = load_policy(spec)
        snapshots = _make_snapshots_with_regime()
        config = ShadowRunConfig(
            experiment_id="test_hier2",
            run_id="run_002",
            output_dir=tmp_path / "shadow" / "test_hier2" / "run_002",
        )
        engine = ShadowEngine(spec, policy, snapshots, config)
        engine.run()

        timeline_path = tmp_path / "shadow" / "test_hier2" / "run_002" / "evaluation" / "mode_timeline.json"
        timeline = json.loads(timeline_path.read_text())
        modes = [e["mode"] for e in timeline]
        # Should see neutral initially and defensive after high-vol regime
        assert "neutral" in modes
        assert "defensive" in modes

    def test_non_hierarchical_policy_no_timeline(self, tmp_path: Path):
        """Non-hierarchical policies must not emit mode_timeline.json."""
        from research.experiments.spec import CostConfig, ExperimentSpec
        spec = ExperimentSpec(
            experiment_name="test_eq",
            symbols=("AAPL", "MSFT"),
            start_date=date(2023, 1, 1),
            end_date=date(2023, 12, 31),
            interval="daily",
            feature_set="core_v1",
            policy="equal_weight",
            policy_params={},
            cost_config=CostConfig(transaction_cost_bp=5.0),
            seed=42,
        )
        policy = load_policy(spec)
        snapshots = _make_snapshots_with_regime()
        config = ShadowRunConfig(
            experiment_id="test_eq",
            run_id="run_003",
            output_dir=tmp_path / "shadow" / "test_eq" / "run_003",
        )
        engine = ShadowEngine(spec, policy, snapshots, config)
        engine.run()

        timeline_path = tmp_path / "shadow" / "test_eq" / "run_003" / "evaluation" / "mode_timeline.json"
        assert not timeline_path.exists()
