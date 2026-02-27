"""Tests for ModeController."""

import json
import tempfile
from pathlib import Path

import pytest

from research.hierarchy.controller import ModeController


def _features(vol=0.1, trend=0.0, dispersion=0.01):
    return {
        "market_vol_20d": vol,
        "market_trend_20d": trend,
        "dispersion_20d": dispersion,
    }


class TestModeControllerDecisionLogic:
    def _make_controller(self, **overrides):
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

    def test_high_vol_triggers_defensive(self):
        ctrl = self._make_controller()
        mode = ctrl.step(_features(vol=0.5))
        assert mode == "defensive"

    def test_high_trend_and_dispersion_triggers_risk_on(self):
        ctrl = self._make_controller()
        mode = ctrl.step(_features(vol=0.1, trend=0.1, dispersion=0.03))
        assert mode == "risk_on"

    def test_default_is_neutral(self):
        ctrl = self._make_controller()
        mode = ctrl.step(_features(vol=0.1, trend=0.0, dispersion=0.0))
        assert mode == "neutral"

    def test_vol_takes_priority_over_risk_on(self):
        """Even with high trend+dispersion, high vol => defensive."""
        ctrl = self._make_controller()
        mode = ctrl.step(_features(vol=0.5, trend=0.1, dispersion=0.05))
        assert mode == "defensive"

    def test_trend_without_dispersion_stays_neutral(self):
        ctrl = self._make_controller()
        mode = ctrl.step(_features(vol=0.1, trend=0.1, dispersion=0.01))
        assert mode == "neutral"


class TestModeControllerUpdateCadence:
    def test_weekly_updates_every_5_steps(self):
        ctrl = ModeController(
            vol_threshold_high=0.3,
            trend_threshold_high=0.05,
            dispersion_threshold_high=0.02,
            update_frequency="weekly",
            min_hold_steps=0,
        )
        # Steps 1-4: no update (stays neutral)
        for _ in range(4):
            mode = ctrl.step(_features(vol=0.5))
            assert mode == "neutral"

        # Step 5: update triggers, switches to defensive
        mode = ctrl.step(_features(vol=0.5))
        assert mode == "defensive"

    def test_monthly_updates_every_21_steps(self):
        ctrl = ModeController(
            vol_threshold_high=0.3,
            trend_threshold_high=0.05,
            dispersion_threshold_high=0.02,
            update_frequency="monthly",
            min_hold_steps=0,
        )
        for _ in range(20):
            mode = ctrl.step(_features(vol=0.5))
            assert mode == "neutral"
        mode = ctrl.step(_features(vol=0.5))
        assert mode == "defensive"

    def test_every_k_steps(self):
        ctrl = ModeController(
            vol_threshold_high=0.3,
            trend_threshold_high=0.05,
            dispersion_threshold_high=0.02,
            update_frequency="every_k_steps",
            min_hold_steps=0,
            k_steps=3,
        )
        for _ in range(2):
            mode = ctrl.step(_features(vol=0.5))
            assert mode == "neutral"
        mode = ctrl.step(_features(vol=0.5))
        assert mode == "defensive"


class TestModeControllerHoldPeriod:
    def test_hold_prevents_immediate_change(self):
        ctrl = ModeController(
            vol_threshold_high=0.3,
            trend_threshold_high=0.05,
            dispersion_threshold_high=0.02,
            update_frequency="every_k_steps",
            min_hold_steps=3,
            k_steps=1,
        )
        # Steps 1-2: hold on initial neutral prevents change
        ctrl.step(_features(vol=0.5))
        assert ctrl.current_mode == "neutral"
        ctrl.step(_features(vol=0.5))
        assert ctrl.current_mode == "neutral"

        # Step 3: hold elapsed (3 steps since step 0), can change
        ctrl.step(_features(vol=0.5))
        assert ctrl.current_mode == "defensive"

        # Steps 4-5: hold prevents change back
        ctrl.step(_features(vol=0.1))
        assert ctrl.current_mode == "defensive"
        ctrl.step(_features(vol=0.1))
        assert ctrl.current_mode == "defensive"

        # Step 6: hold elapsed (3 steps since change), can change back
        ctrl.step(_features(vol=0.1))
        assert ctrl.current_mode == "neutral"


class TestModeControllerTimeline:
    def test_initial_timeline(self):
        ctrl = ModeController(
            vol_threshold_high=0.3,
            trend_threshold_high=0.05,
            dispersion_threshold_high=0.02,
            update_frequency="every_k_steps",
            min_hold_steps=0,
            k_steps=1,
        )
        tl = ctrl.timeline
        assert len(tl) == 1
        assert tl[0]["step"] == 0
        assert tl[0]["mode"] == "neutral"

    def test_transitions_logged(self):
        ctrl = ModeController(
            vol_threshold_high=0.3,
            trend_threshold_high=0.05,
            dispersion_threshold_high=0.02,
            update_frequency="every_k_steps",
            min_hold_steps=0,
            k_steps=1,
        )
        ctrl.step(_features(vol=0.5))  # -> defensive
        ctrl.step(_features(vol=0.1))  # -> neutral
        tl = ctrl.timeline
        assert len(tl) == 3
        assert tl[0]["mode"] == "neutral"
        assert tl[1]["mode"] == "defensive"
        assert tl[2]["mode"] == "neutral"

    def test_no_duplicate_log_when_mode_unchanged(self):
        ctrl = ModeController(
            vol_threshold_high=0.3,
            trend_threshold_high=0.05,
            dispersion_threshold_high=0.02,
            update_frequency="every_k_steps",
            min_hold_steps=0,
            k_steps=1,
        )
        ctrl.step(_features(vol=0.1))  # stays neutral
        ctrl.step(_features(vol=0.1))  # stays neutral
        assert len(ctrl.timeline) == 1

    def test_save_timeline(self):
        ctrl = ModeController(
            vol_threshold_high=0.3,
            trend_threshold_high=0.05,
            dispersion_threshold_high=0.02,
            update_frequency="every_k_steps",
            min_hold_steps=0,
            k_steps=1,
        )
        ctrl.step(_features(vol=0.5))

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "evaluation" / "mode_timeline.json"
            ctrl.save_timeline(path)
            data = json.loads(path.read_text())
            assert len(data) == 2
            assert data[0]["mode"] == "neutral"
            assert data[1]["mode"] == "defensive"


class TestModeControllerFromConfig:
    def test_from_config(self):
        config = {
            "vol_threshold_high": 0.3,
            "trend_threshold_high": 0.05,
            "dispersion_threshold_high": 0.02,
            "update_frequency": "weekly",
            "min_hold_steps": 5,
        }
        ctrl = ModeController.from_config(config)
        assert ctrl.vol_threshold_high == 0.3
        assert ctrl.update_frequency == "weekly"
        assert ctrl.min_hold_steps == 5

    def test_from_config_with_k_steps(self):
        config = {
            "vol_threshold_high": 0.3,
            "trend_threshold_high": 0.05,
            "dispersion_threshold_high": 0.02,
            "update_frequency": "every_k_steps",
            "min_hold_steps": 0,
            "k_steps": 10,
        }
        ctrl = ModeController.from_config(config)
        assert ctrl.k_steps == 10


class TestModeControllerReset:
    def test_reset_clears_state(self):
        ctrl = ModeController(
            vol_threshold_high=0.3,
            trend_threshold_high=0.05,
            dispersion_threshold_high=0.02,
            update_frequency="every_k_steps",
            min_hold_steps=0,
            k_steps=1,
        )
        ctrl.step(_features(vol=0.5))
        assert ctrl.current_mode == "defensive"
        assert len(ctrl.timeline) == 2

        ctrl.reset()
        assert ctrl.current_mode == "neutral"
        assert len(ctrl.timeline) == 1


class TestModeControllerValidation:
    def test_invalid_frequency(self):
        with pytest.raises(ValueError, match="update_frequency"):
            ModeController(
                vol_threshold_high=0.3,
                trend_threshold_high=0.05,
                dispersion_threshold_high=0.02,
                update_frequency="daily",
                min_hold_steps=0,
            )

    def test_k_steps_required_for_every_k(self):
        with pytest.raises(ValueError, match="k_steps required"):
            ModeController(
                vol_threshold_high=0.3,
                trend_threshold_high=0.05,
                dispersion_threshold_high=0.02,
                update_frequency="every_k_steps",
                min_hold_steps=0,
            )

    def test_negative_hold_steps(self):
        with pytest.raises(ValueError, match="min_hold_steps must be >= 0"):
            ModeController(
                vol_threshold_high=0.3,
                trend_threshold_high=0.05,
                dispersion_threshold_high=0.02,
                update_frequency="weekly",
                min_hold_steps=-1,
            )
