"""Tests for research.promotion.regime_criteria — regime-specific gates."""

from __future__ import annotations

import pytest

from research.promotion.regime_criteria import (
    evaluate_regime_hard_gates,
    evaluate_regime_soft_gates,
)


# ---------------------------------------------------------------------------
# Regime hard gates
# ---------------------------------------------------------------------------

class TestRegimeHardGates:
    def _make_metrics(self, hv_drawdown=-0.05, hv_exposure=0.30):
        return {
            "performance_by_regime": {
                "high_vol": {
                    "max_drawdown": hv_drawdown,
                    "gross_exposure_mean": hv_exposure,
                },
            },
        }

    def test_all_pass(self):
        candidate = self._make_metrics(hv_drawdown=-0.05)
        baseline = self._make_metrics(hv_drawdown=-0.05)
        results = evaluate_regime_hard_gates(candidate, baseline)
        assert all(r.passed for r in results)

    def test_highvol_drawdown_regression_fail(self):
        candidate = self._make_metrics(hv_drawdown=-0.20)
        baseline = self._make_metrics(hv_drawdown=-0.05)
        results = evaluate_regime_hard_gates(candidate, baseline)
        dd_gate = next(r for r in results if r.name == "highvol_drawdown_regression")
        assert not dd_gate.passed

    def test_highvol_exposure_cap_fail(self):
        candidate = self._make_metrics(hv_exposure=0.50)
        baseline = self._make_metrics()
        results = evaluate_regime_hard_gates(candidate, baseline)
        exp_gate = next(r for r in results if r.name == "highvol_exposure_cap")
        assert not exp_gate.passed

    def test_skip_delta_checks(self):
        candidate = self._make_metrics(hv_drawdown=-0.50)
        baseline = self._make_metrics(hv_drawdown=-0.05)
        results = evaluate_regime_hard_gates(
            candidate, baseline, skip_delta_checks=True,
        )
        dd_gate = next(r for r in results if r.name == "highvol_drawdown_regression")
        assert dd_gate.passed  # Skipped

    def test_no_regime_data(self):
        candidate = {"performance_by_regime": {}}
        baseline = {"performance_by_regime": {}}
        results = evaluate_regime_hard_gates(candidate, baseline)
        assert all(r.passed for r in results)


# ---------------------------------------------------------------------------
# Regime soft gates
# ---------------------------------------------------------------------------

class TestRegimeSoftGates:
    def _make_metrics(self, hv_sharpe=1.0, hv_turnover=0.05):
        return {
            "performance_by_regime": {
                "high_vol": {
                    "sharpe": hv_sharpe,
                    "turnover_1d_mean": hv_turnover,
                },
            },
            "regime": {},
        }

    def test_all_pass(self):
        candidate = self._make_metrics(hv_sharpe=1.0)
        baseline = self._make_metrics(hv_sharpe=1.0)
        results = evaluate_regime_soft_gates(candidate, baseline)
        assert all(r.passed for r in results)

    def test_sharpe_degradation(self):
        candidate = self._make_metrics(hv_sharpe=0.5)
        baseline = self._make_metrics(hv_sharpe=1.0)
        results = evaluate_regime_soft_gates(candidate, baseline)
        sharpe_gate = next(r for r in results if r.name == "regime_sharpe_degradation")
        assert not sharpe_gate.passed

    def test_hierarchy_adds_defensive_gate(self):
        candidate = self._make_metrics()
        baseline = self._make_metrics()
        results = evaluate_regime_soft_gates(
            candidate, baseline, hierarchy_enabled=True,
        )
        names = [r.name for r in results]
        assert "defensive_exposure_cap" in names

    def test_no_hierarchy_skips_defensive(self):
        candidate = self._make_metrics()
        baseline = self._make_metrics()
        results = evaluate_regime_soft_gates(
            candidate, baseline, hierarchy_enabled=False,
        )
        names = [r.name for r in results]
        assert "defensive_exposure_cap" not in names
