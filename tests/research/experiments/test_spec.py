"""Tests for research.experiments.spec — ExperimentSpec + composable inheritance."""

from __future__ import annotations

import json
from datetime import date

import pytest

from research.experiments.spec import (
    CostConfig,
    EvaluationSplitConfig,
    ExecutionSimConfig,
    ExperimentSpec,
    RiskConfig,
    _deep_merge,
    _normalize,
    resolve_inheritance,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _base_kwargs(**overrides) -> dict:
    """Minimal valid ExperimentSpec kwargs with optional overrides."""
    defaults = dict(
        experiment_name="test_exp",
        symbols=("AAPL", "MSFT"),
        start_date=date(2023, 1, 1),
        end_date=date(2024, 1, 1),
        interval="daily",
        feature_set="core_v1",
        policy="ppo",
        policy_params={"learning_rate": 0.001},
        cost_config=CostConfig(transaction_cost_bp=5.0),
        seed=42,
    )
    defaults.update(overrides)
    return defaults


def _make_spec(**overrides) -> ExperimentSpec:
    return ExperimentSpec(**_base_kwargs(**overrides))


# ===================================================================
# CostConfig
# ===================================================================

class TestCostConfig:
    def test_valid(self):
        c = CostConfig(transaction_cost_bp=5.0, slippage_bp=1.0)
        assert c.transaction_cost_bp == 5.0
        assert c.slippage_bp == 1.0

    def test_default_slippage(self):
        c = CostConfig(transaction_cost_bp=3.0)
        assert c.slippage_bp == 0.0

    def test_negative_transaction_cost(self):
        with pytest.raises(ValueError, match="transaction_cost_bp"):
            CostConfig(transaction_cost_bp=-1.0)

    def test_negative_slippage(self):
        with pytest.raises(ValueError, match="slippage_bp"):
            CostConfig(transaction_cost_bp=5.0, slippage_bp=-0.5)

    def test_frozen(self):
        c = CostConfig(transaction_cost_bp=5.0)
        with pytest.raises(AttributeError):
            c.transaction_cost_bp = 10.0


# ===================================================================
# EvaluationSplitConfig
# ===================================================================

class TestEvaluationSplitConfig:
    def test_valid(self):
        e = EvaluationSplitConfig(train_ratio=0.8, test_ratio=0.2)
        assert e.train_ratio == 0.8
        assert e.test_ratio == 0.2
        assert e.test_window_months is None

    def test_valid_with_window(self):
        e = EvaluationSplitConfig(train_ratio=0.7, test_ratio=0.3, test_window_months=6)
        assert e.test_window_months == 6

    def test_ratios_must_sum_to_one(self):
        with pytest.raises(ValueError, match="must equal 1.0"):
            EvaluationSplitConfig(train_ratio=0.6, test_ratio=0.3)

    def test_train_ratio_positive(self):
        with pytest.raises(ValueError, match="train_ratio"):
            EvaluationSplitConfig(train_ratio=0.0, test_ratio=1.0)

    def test_test_ratio_positive(self):
        with pytest.raises(ValueError, match="test_ratio"):
            EvaluationSplitConfig(train_ratio=1.0, test_ratio=0.0)

    def test_invalid_window_months(self):
        with pytest.raises(ValueError, match="test_window_months"):
            EvaluationSplitConfig(train_ratio=0.8, test_ratio=0.2, test_window_months=5)

    def test_valid_window_months_all(self):
        for m in (1, 3, 4, 6, 12):
            e = EvaluationSplitConfig(train_ratio=0.8, test_ratio=0.2, test_window_months=m)
            assert e.test_window_months == m


# ===================================================================
# RiskConfig
# ===================================================================

class TestRiskConfig:
    def test_defaults(self):
        r = RiskConfig()
        assert r.max_gross_exposure == 1.0
        assert r.min_cash == 0.0
        assert r.max_symbol_weight == 1.0
        assert r.max_daily_turnover == 1.0
        assert r.max_active_positions == 999
        assert r.daily_loss_limit == -1.0
        assert r.trailing_drawdown_limit == -1.0

    def test_custom(self):
        r = RiskConfig(max_gross_exposure=0.8, min_cash=0.02)
        assert r.max_gross_exposure == 0.8
        assert r.min_cash == 0.02


# ===================================================================
# ExecutionSimConfig
# ===================================================================

class TestExecutionSimConfig:
    def test_defaults(self):
        e = ExecutionSimConfig()
        assert e.enabled is False
        assert e.slippage_model == "flat"
        assert e.slippage_bp == 5.0
        assert e.fill_probability == 0.99
        assert e.range_shrink == 0.5

    def test_volume_scaled(self):
        e = ExecutionSimConfig(slippage_model="volume_scaled")
        assert e.slippage_model == "volume_scaled"

    def test_invalid_slippage_model(self):
        with pytest.raises(ValueError, match="slippage_model"):
            ExecutionSimConfig(slippage_model="quadratic")

    def test_negative_slippage_bp(self):
        with pytest.raises(ValueError, match="slippage_bp"):
            ExecutionSimConfig(slippage_bp=-1.0)

    def test_fill_probability_bounds(self):
        ExecutionSimConfig(fill_probability=0.0)
        ExecutionSimConfig(fill_probability=1.0)
        with pytest.raises(ValueError, match="fill_probability"):
            ExecutionSimConfig(fill_probability=1.1)
        with pytest.raises(ValueError, match="fill_probability"):
            ExecutionSimConfig(fill_probability=-0.1)

    def test_range_shrink_bounds(self):
        ExecutionSimConfig(range_shrink=0.01)
        ExecutionSimConfig(range_shrink=1.0)
        with pytest.raises(ValueError, match="range_shrink"):
            ExecutionSimConfig(range_shrink=0.0)
        with pytest.raises(ValueError, match="range_shrink"):
            ExecutionSimConfig(range_shrink=1.5)


# ===================================================================
# ExperimentSpec — Construction & Validation
# ===================================================================

class TestExperimentSpecConstruction:
    def test_minimal_valid(self):
        spec = _make_spec()
        assert spec.experiment_name == "test_exp"
        assert spec.symbols == ("AAPL", "MSFT")
        assert spec.interval == "daily"
        assert spec.policy == "ppo"
        assert spec.seed == 42

    def test_symbols_sorted_deduped(self):
        spec = _make_spec(symbols=("MSFT", "AAPL", "MSFT", "GOOG"))
        assert spec.symbols == ("AAPL", "GOOG", "MSFT")

    def test_empty_name_rejected(self):
        with pytest.raises(ValueError, match="experiment_name"):
            _make_spec(experiment_name="")

    def test_empty_symbols_rejected(self):
        with pytest.raises(ValueError, match="symbols"):
            _make_spec(symbols=())

    def test_date_order_enforced(self):
        with pytest.raises(ValueError, match="start_date.*before.*end_date"):
            _make_spec(start_date=date(2025, 1, 1), end_date=date(2024, 1, 1))

    def test_same_dates_rejected(self):
        with pytest.raises(ValueError, match="start_date.*before.*end_date"):
            _make_spec(start_date=date(2024, 1, 1), end_date=date(2024, 1, 1))

    def test_interval_must_be_daily(self):
        with pytest.raises(ValueError, match="interval"):
            _make_spec(interval="hourly")

    def test_empty_feature_set_rejected(self):
        with pytest.raises(ValueError, match="feature_set"):
            _make_spec(feature_set="")

    def test_valid_policies(self):
        for p in ("ppo", "sac", "sma", "equal_weight"):
            spec = _make_spec(policy=p)
            assert spec.policy == p

    def test_sac_variant(self):
        spec = _make_spec(policy="sac_custom")
        assert spec.policy == "sac_custom"

    def test_invalid_policy(self):
        with pytest.raises(ValueError, match="policy"):
            _make_spec(policy="random_forest")

    def test_optional_defaults(self):
        spec = _make_spec()
        assert spec.evaluation_split is None
        assert isinstance(spec.risk_config, RiskConfig)
        assert spec.execution_sim is None
        assert spec.notes is None
        assert spec.regime_feature_set is None
        assert spec.regime_labeling is None
        assert spec.hierarchy_enabled is False
        assert spec.controller_config is None
        assert spec.allocator_by_mode is None

    def test_frozen(self):
        spec = _make_spec()
        with pytest.raises(AttributeError):
            spec.experiment_name = "changed"

    def test_regime_labeling_valid(self):
        for v in ("v1", "v2"):
            spec = _make_spec(regime_labeling=v)
            assert spec.regime_labeling == v

    def test_regime_labeling_invalid(self):
        with pytest.raises(ValueError, match="regime_labeling"):
            _make_spec(regime_labeling="v3")

    def test_regime_labeling_none_ok(self):
        spec = _make_spec(regime_labeling=None)
        assert spec.regime_labeling is None


# ===================================================================
# ExperimentSpec — Hierarchy Validation
# ===================================================================

class TestHierarchyValidation:
    _CONTROLLER = {
        "update_frequency": "weekly",
        "vol_threshold_high": 0.2,
        "trend_threshold_high": 0.02,
        "dispersion_threshold_high": 0.1,
        "min_hold_steps": 5,
    }
    _ALLOCATORS = {
        "risk_on": {"type": "momentum"},
        "neutral": {"type": "balanced"},
        "defensive": {"type": "min_vol"},
    }

    def test_valid_hierarchy(self):
        spec = _make_spec(
            hierarchy_enabled=True,
            controller_config=self._CONTROLLER,
            allocator_by_mode=self._ALLOCATORS,
        )
        assert spec.hierarchy_enabled is True

    def test_missing_controller(self):
        with pytest.raises(ValueError, match="controller_config.*required"):
            _make_spec(hierarchy_enabled=True, allocator_by_mode=self._ALLOCATORS)

    def test_missing_allocator(self):
        with pytest.raises(ValueError, match="allocator_by_mode.*required"):
            _make_spec(hierarchy_enabled=True, controller_config=self._CONTROLLER)

    def test_missing_controller_keys(self):
        with pytest.raises(ValueError, match="missing required keys"):
            _make_spec(
                hierarchy_enabled=True,
                controller_config={"update_frequency": "weekly"},
                allocator_by_mode=self._ALLOCATORS,
            )

    def test_invalid_update_frequency(self):
        bad = {**self._CONTROLLER, "update_frequency": "daily"}
        with pytest.raises(ValueError, match="update_frequency"):
            _make_spec(
                hierarchy_enabled=True,
                controller_config=bad,
                allocator_by_mode=self._ALLOCATORS,
            )

    def test_every_k_steps_requires_k(self):
        cfg = {**self._CONTROLLER, "update_frequency": "every_k_steps"}
        with pytest.raises(ValueError, match="k_steps"):
            _make_spec(
                hierarchy_enabled=True,
                controller_config=cfg,
                allocator_by_mode=self._ALLOCATORS,
            )

    def test_every_k_steps_with_k(self):
        cfg = {**self._CONTROLLER, "update_frequency": "every_k_steps", "k_steps": 10}
        spec = _make_spec(
            hierarchy_enabled=True,
            controller_config=cfg,
            allocator_by_mode=self._ALLOCATORS,
        )
        assert spec.controller_config["k_steps"] == 10

    def test_missing_allocator_mode(self):
        bad_alloc = {
            "risk_on": {"type": "momentum"},
            "neutral": {"type": "balanced"},
            # missing "defensive"
        }
        with pytest.raises(ValueError, match="allocator_by_mode.*missing.*defensive"):
            _make_spec(
                hierarchy_enabled=True,
                controller_config=self._CONTROLLER,
                allocator_by_mode=bad_alloc,
            )

    def test_hierarchy_disabled_no_validation(self):
        # controller_config without hierarchy_enabled — no error
        spec = _make_spec(
            hierarchy_enabled=False,
            controller_config=self._CONTROLLER,
        )
        assert spec.hierarchy_enabled is False


# ===================================================================
# Canonical JSON + ID Derivation
# ===================================================================

class TestCanonicalSerialization:
    def test_canonical_dict_has_all_fields(self):
        spec = _make_spec()
        d = spec.to_canonical_dict()
        from dataclasses import fields as dc_fields
        for f in dc_fields(ExperimentSpec):
            assert f.name in d

    def test_id_is_deterministic(self):
        spec1 = _make_spec()
        spec2 = _make_spec()
        assert spec1.experiment_id == spec2.experiment_id

    def test_id_changes_with_different_fields(self):
        spec1 = _make_spec(seed=42)
        spec2 = _make_spec(seed=99)
        assert spec1.experiment_id != spec2.experiment_id

    def test_id_is_sha256_hex(self):
        spec = _make_spec()
        eid = spec.experiment_id
        assert len(eid) == 64
        assert all(c in "0123456789abcdef" for c in eid)

    def test_canonical_json_sorted_compact(self):
        spec = _make_spec()
        d = spec.to_canonical_dict()
        canonical = json.dumps(d, sort_keys=True, separators=(",", ":"))
        # Compact format: no spaces after separators
        assert ", " not in canonical
        assert ": " not in canonical
        # json.dumps(sort_keys=True) ensures sorted output
        parsed_keys = list(json.loads(canonical).keys())
        assert parsed_keys == sorted(parsed_keys)

    def test_dates_serialized_as_iso(self):
        spec = _make_spec()
        d = spec.to_canonical_dict()
        assert d["start_date"] == "2023-01-01"
        assert d["end_date"] == "2024-01-01"

    def test_tuples_serialized_as_lists(self):
        spec = _make_spec()
        d = spec.to_canonical_dict()
        assert isinstance(d["symbols"], list)

    def test_sub_configs_serialized(self):
        spec = _make_spec()
        d = spec.to_canonical_dict()
        assert isinstance(d["cost_config"], dict)
        assert d["cost_config"]["transaction_cost_bp"] == 5.0

    def test_symbol_order_doesnt_affect_id(self):
        # Symbols are sorted, so different input order → same result
        spec1 = _make_spec(symbols=("MSFT", "AAPL"))
        spec2 = _make_spec(symbols=("AAPL", "MSFT"))
        assert spec1.experiment_id == spec2.experiment_id


# ===================================================================
# Composable Inheritance
# ===================================================================

class TestComposableInheritance:
    def test_no_extends_passthrough(self):
        d = {"experiment_name": "foo", "seed": 42}
        result = resolve_inheritance(d)
        assert result == d

    def test_extends_with_loader(self):
        base = {"experiment_name": "base", "seed": 1, "policy": "ppo"}
        child = {
            "extends": "base_spec",
            "overrides": {"seed": 99, "experiment_name": "child"},
        }

        def loader(name):
            assert name == "base_spec"
            return base.copy()

        result = resolve_inheritance(child, loader=loader)
        assert result["experiment_name"] == "child"
        assert result["seed"] == 99
        assert result["policy"] == "ppo"
        assert "extends" not in result
        assert "overrides" not in result
        assert "_base_spec_hash" in result

    def test_base_spec_hash_set(self):
        base = {"experiment_name": "base", "seed": 1}
        child = {"extends": "x", "overrides": {"seed": 2}}

        result = resolve_inheritance(child, loader=lambda _: base.copy())
        assert isinstance(result["_base_spec_hash"], str)
        assert len(result["_base_spec_hash"]) == 64

    def test_base_spec_hash_is_content_addressed(self):
        base = {"experiment_name": "base", "seed": 1}
        child1 = {"extends": "x", "overrides": {"seed": 2}}
        child2 = {"extends": "x", "overrides": {"seed": 99}}

        r1 = resolve_inheritance(child1, loader=lambda _: base.copy())
        r2 = resolve_inheritance(child2, loader=lambda _: base.copy())
        # Same base → same base hash
        assert r1["_base_spec_hash"] == r2["_base_spec_hash"]

    def test_different_base_different_hash(self):
        base_a = {"experiment_name": "a", "seed": 1}
        base_b = {"experiment_name": "b", "seed": 1}
        child = {"extends": "x", "overrides": {"seed": 2}}

        r_a = resolve_inheritance(child.copy(), loader=lambda _: base_a.copy())
        r_b = resolve_inheritance(child.copy(), loader=lambda _: base_b.copy())
        assert r_a["_base_spec_hash"] != r_b["_base_spec_hash"]

    def test_deep_merge_nested(self):
        base = {"a": {"b": 1, "c": 2}, "d": 3}
        child = {
            "extends": "x",
            "overrides": {"a": {"b": 99}, "e": 5},
        }
        result = resolve_inheritance(child, loader=lambda _: base.copy())
        assert result["a"]["b"] == 99
        assert result["a"]["c"] == 2
        assert result["d"] == 3
        assert result["e"] == 5

    def test_no_mutation_of_input(self):
        base = {"experiment_name": "base", "seed": 1}
        child = {"extends": "x", "overrides": {"seed": 2}}
        original = child.copy()
        resolve_inheritance(child, loader=lambda _: base.copy())
        assert child == original

    def test_recursive_inheritance(self):
        grandparent = {"name": "gp", "a": 1, "b": 2, "c": 3}
        parent = {"extends": "gp", "overrides": {"b": 20}}
        child = {"extends": "parent", "overrides": {"c": 300}}

        def loader(name):
            if name == "gp":
                return grandparent.copy()
            if name == "parent":
                return parent.copy()
            raise FileNotFoundError(name)

        result = resolve_inheritance(child, loader=loader)
        assert result["a"] == 1
        assert result["b"] == 20
        assert result["c"] == 300
        assert result["name"] == "gp"


# ===================================================================
# _deep_merge
# ===================================================================

class TestDeepMerge:
    def test_flat(self):
        assert _deep_merge({"a": 1}, {"b": 2}) == {"a": 1, "b": 2}

    def test_override(self):
        assert _deep_merge({"a": 1}, {"a": 2}) == {"a": 2}

    def test_nested(self):
        base = {"x": {"a": 1, "b": 2}}
        over = {"x": {"b": 3, "c": 4}}
        assert _deep_merge(base, over) == {"x": {"a": 1, "b": 3, "c": 4}}

    def test_no_mutation(self):
        base = {"x": {"a": 1}}
        over = {"x": {"b": 2}}
        _deep_merge(base, over)
        assert base == {"x": {"a": 1}}


# ===================================================================
# _normalize
# ===================================================================

class TestNormalize:
    def test_none(self):
        assert _normalize(None) is None

    def test_primitives(self):
        assert _normalize(42) == 42
        assert _normalize(3.14) == 3.14
        assert _normalize("hello") == "hello"
        assert _normalize(True) is True

    def test_date(self):
        assert _normalize(date(2024, 6, 15)) == "2024-06-15"

    def test_tuple_to_list(self):
        assert _normalize(("a", "b")) == ["a", "b"]

    def test_mapping_sorted(self):
        result = _normalize({"z": 1, "a": 2})
        assert list(result.keys()) == ["a", "z"]

    def test_dataclass(self):
        c = CostConfig(transaction_cost_bp=5.0, slippage_bp=1.0)
        result = _normalize(c)
        assert result == {"transaction_cost_bp": 5.0, "slippage_bp": 1.0}
