"""Tests for HierarchicalPolicy wrapper."""

import numpy as np
import pytest

from research.hierarchy.controller import ModeController
from research.hierarchy.policy_wrapper import HierarchicalPolicy
from research.policies.equal_weight_policy import EqualWeightPolicy
from research.policies.sma_weight_policy import SMAWeightPolicy


def _make_controller(**overrides):
    defaults = dict(
        vol_threshold_high=0.3,
        trend_threshold_high=0.05,
        dispersion_threshold_high=0.02,
        update_frequency="every_k_steps",
        min_hold_steps=0,
        k_steps=1,
    )
    defaults.update(overrides)
    return ModeController(**defaults)


def _make_allocators(n_symbols=3):
    return {
        "risk_on": EqualWeightPolicy(n_symbols),
        "neutral": EqualWeightPolicy(n_symbols),
        "defensive": EqualWeightPolicy(n_symbols),
    }


class TestHierarchicalPolicy:
    def test_delegates_to_correct_allocator(self):
        """Each mode's allocator should be called."""
        ctrl = _make_controller()
        n = 3
        # Use different allocators that produce identifiable weights
        risk_on_alloc = EqualWeightPolicy(n)
        neutral_alloc = EqualWeightPolicy(n)
        defensive_alloc = EqualWeightPolicy(n)

        allocators = {
            "risk_on": risk_on_alloc,
            "neutral": neutral_alloc,
            "defensive": defensive_alloc,
        }
        policy = HierarchicalPolicy(ctrl, allocators)

        obs = np.zeros(10)
        # Low vol -> neutral
        context = {"regime_features": {"market_vol_20d": 0.1, "market_trend_20d": 0.0, "dispersion_20d": 0.0}}
        weights = policy.act(obs, context)
        assert policy.current_mode == "neutral"
        assert weights.shape == (n,)
        np.testing.assert_allclose(weights.sum(), 1.0)

    def test_mode_changes_switch_allocator(self):
        ctrl = _make_controller()
        allocators = _make_allocators(2)
        policy = HierarchicalPolicy(ctrl, allocators)
        obs = np.zeros(5)

        # Start neutral
        policy.act(obs, {"regime_features": {"market_vol_20d": 0.1, "market_trend_20d": 0.0, "dispersion_20d": 0.0}})
        assert policy.current_mode == "neutral"

        # Switch to defensive
        policy.act(obs, {"regime_features": {"market_vol_20d": 0.5, "market_trend_20d": 0.0, "dispersion_20d": 0.0}})
        assert policy.current_mode == "defensive"

    def test_timeline_tracks_transitions(self):
        ctrl = _make_controller()
        policy = HierarchicalPolicy(ctrl, _make_allocators())
        obs = np.zeros(5)

        policy.act(obs, {"regime_features": {"market_vol_20d": 0.5, "market_trend_20d": 0.0, "dispersion_20d": 0.0}})
        policy.act(obs, {"regime_features": {"market_vol_20d": 0.1, "market_trend_20d": 0.0, "dispersion_20d": 0.0}})

        tl = policy.timeline
        assert len(tl) == 3
        assert [e["mode"] for e in tl] == ["neutral", "defensive", "neutral"]

    def test_reset_clears_all_state(self):
        ctrl = _make_controller()
        allocators = {
            "risk_on": SMAWeightPolicy(3, short_window=2, long_window=4),
            "neutral": EqualWeightPolicy(3),
            "defensive": EqualWeightPolicy(3),
        }
        policy = HierarchicalPolicy(ctrl, allocators)
        obs = np.zeros(5)
        policy.act(obs, {"regime_features": {"market_vol_20d": 0.5, "market_trend_20d": 0.0, "dispersion_20d": 0.0}})
        assert policy.current_mode == "defensive"

        policy.reset()
        assert policy.current_mode == "neutral"
        assert len(policy.timeline) == 1

    def test_missing_allocator_raises(self):
        ctrl = _make_controller()
        with pytest.raises(ValueError, match="Missing allocators"):
            HierarchicalPolicy(ctrl, {"risk_on": EqualWeightPolicy(3)})

    def test_enriched_context_includes_mode(self):
        """Allocator receives 'mode' in context."""

        class ContextCapture:
            last_context: dict = {}

            def act(self, obs, context=None):
                self.last_context = dict(context or {})
                return np.array([0.5, 0.5])

        capture = ContextCapture()
        ctrl = _make_controller()
        allocators = {
            "risk_on": capture,
            "neutral": capture,
            "defensive": capture,
        }
        policy = HierarchicalPolicy(ctrl, allocators)
        policy.act(np.zeros(5), {"regime_features": {"market_vol_20d": 0.1, "market_trend_20d": 0.0, "dispersion_20d": 0.0}})
        assert capture.last_context["mode"] == "neutral"

    def test_empty_context_handled(self):
        ctrl = _make_controller()
        policy = HierarchicalPolicy(ctrl, _make_allocators())
        # No context at all — should use empty regime features
        weights = policy.act(np.zeros(5))
        assert weights.shape == (3,)
