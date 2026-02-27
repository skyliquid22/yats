"""Tests for research.risk.project_weights — deterministic risk projection."""

from __future__ import annotations

import numpy as np
import pytest

from research.experiments.spec import RiskConfig
from research.risk.project_weights import project_weights


class TestLongOnlyClamp:
    def test_negative_weights_clamped_to_zero(self):
        w = project_weights(np.array([-0.5, 0.3, -0.1]), RiskConfig())
        assert (w >= 0).all()

    def test_positive_weights_unchanged(self):
        w = project_weights(np.array([0.3, 0.2]), RiskConfig())
        np.testing.assert_array_almost_equal(w, [0.3, 0.2])


class TestMaxSymbolWeight:
    def test_clamp_to_max(self):
        risk = RiskConfig(max_symbol_weight=0.25)
        w = project_weights(np.array([0.5, 0.3, 0.1]), risk)
        assert w.max() <= 0.25 + 1e-10

    def test_below_max_unchanged(self):
        risk = RiskConfig(max_symbol_weight=0.5)
        w = project_weights(np.array([0.3, 0.2]), risk)
        np.testing.assert_array_almost_equal(w, [0.3, 0.2])


class TestExposureCap:
    def test_rescale_to_max_exposure(self):
        risk = RiskConfig(max_gross_exposure=0.5)
        w = project_weights(np.array([0.4, 0.4]), risk)
        assert w.sum() <= 0.5 + 1e-10

    def test_min_cash_reduces_exposure(self):
        risk = RiskConfig(max_gross_exposure=1.0, min_cash=0.3)
        w = project_weights(np.array([0.5, 0.5]), risk)
        assert w.sum() <= 0.7 + 1e-10

    def test_zero_weights_no_division_error(self):
        w = project_weights(np.array([0.0, 0.0]), RiskConfig())
        np.testing.assert_array_almost_equal(w, [0.0, 0.0])


class TestMaxActivePositions:
    def test_zero_out_smallest(self):
        risk = RiskConfig(max_active_positions=2)
        w = project_weights(np.array([0.3, 0.1, 0.2, 0.05]), risk)
        nonzero = (w > 0).sum()
        assert nonzero <= 2

    def test_no_effect_when_within_limit(self):
        risk = RiskConfig(max_active_positions=5)
        raw = np.array([0.3, 0.2])
        w = project_weights(raw, risk)
        np.testing.assert_array_almost_equal(w, raw)


class TestMaxTurnover:
    def test_turnover_limited(self):
        risk = RiskConfig(max_daily_turnover=0.2)
        prev = np.array([0.3, 0.3])
        raw = np.array([0.8, 0.0])  # L1 delta = 0.8
        w = project_weights(raw, risk, prev_weights=prev)
        delta = np.abs(w - prev).sum()
        assert delta <= 0.2 + 1e-10

    def test_no_prev_weights_skips_turnover(self):
        risk = RiskConfig(max_daily_turnover=0.1)
        raw = np.array([0.5, 0.5])
        w = project_weights(raw, risk, prev_weights=None)
        # Without prev_weights, turnover constraint is skipped
        np.testing.assert_array_almost_equal(w, [0.5, 0.5])


class TestConstraintOrder:
    def test_all_constraints_applied(self):
        risk = RiskConfig(
            max_gross_exposure=0.6,
            max_symbol_weight=0.4,
            min_cash=0.1,
            max_daily_turnover=0.5,
        )
        prev = np.array([0.2, 0.2, 0.2])
        raw = np.array([0.9, 0.8, 0.7])
        w = project_weights(raw, risk, prev_weights=prev)

        assert (w >= 0).all()
        assert w.max() <= 0.4 + 1e-10
        assert w.sum() <= 0.6 + 1e-10
