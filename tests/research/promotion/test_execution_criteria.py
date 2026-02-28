"""Tests for research.promotion.execution_criteria — execution gates."""

from __future__ import annotations

import pytest

from research.promotion.execution_criteria import (
    evaluate_execution_hard_gates,
    evaluate_execution_soft_gates,
)


# ---------------------------------------------------------------------------
# Hard gates
# ---------------------------------------------------------------------------

class TestExecutionHardGates:
    def test_all_pass(self):
        metrics = {
            "fill_rate": 0.995,
            "reject_rate": 0.005,
            "p95_slippage_bps": 20.0,
            "execution_halts": 0,
        }
        results = evaluate_execution_hard_gates(metrics)
        assert all(r.passed for r in results)

    def test_no_metrics(self):
        results = evaluate_execution_hard_gates(None)
        assert len(results) == 4
        assert all(not r.passed for r in results)

    def test_fill_rate_fail(self):
        metrics = {
            "fill_rate": 0.95,
            "reject_rate": 0.005,
            "p95_slippage_bps": 20.0,
            "execution_halts": 0,
        }
        results = evaluate_execution_hard_gates(metrics)
        fill_gate = next(r for r in results if r.name == "fill_rate")
        assert not fill_gate.passed

    def test_reject_rate_fail(self):
        metrics = {
            "fill_rate": 0.995,
            "reject_rate": 0.05,
            "p95_slippage_bps": 20.0,
            "execution_halts": 0,
        }
        results = evaluate_execution_hard_gates(metrics)
        rr_gate = next(r for r in results if r.name == "reject_rate")
        assert not rr_gate.passed

    def test_slippage_fail(self):
        metrics = {
            "fill_rate": 0.995,
            "reject_rate": 0.005,
            "p95_slippage_bps": 30.0,
            "execution_halts": 0,
        }
        results = evaluate_execution_hard_gates(metrics)
        slip_gate = next(r for r in results if r.name == "p95_slippage")
        assert not slip_gate.passed

    def test_halts_fail(self):
        metrics = {
            "fill_rate": 0.995,
            "reject_rate": 0.005,
            "p95_slippage_bps": 20.0,
            "execution_halts": 1,
        }
        results = evaluate_execution_hard_gates(metrics)
        halt_gate = next(r for r in results if r.name == "execution_halts")
        assert not halt_gate.passed


# ---------------------------------------------------------------------------
# Soft gates
# ---------------------------------------------------------------------------

class TestExecutionSoftGates:
    def test_all_pass(self):
        candidate = {
            "avg_slippage_bps": 10.0,
            "total_fees": 1000.0,
            "total_turnover": 5.0,
        }
        baseline = {
            "avg_slippage_bps": 8.0,
            "total_fees": 950.0,
            "total_turnover": 4.8,
        }
        results = evaluate_execution_soft_gates(candidate, baseline)
        assert all(r.passed for r in results)

    def test_skip_delta_checks(self):
        results = evaluate_execution_soft_gates(
            {"avg_slippage_bps": 100.0}, {"avg_slippage_bps": 1.0},
            skip_delta_checks=True,
        )
        assert all(r.passed for r in results)
        assert all("Skipped" in r.detail for r in results)

    def test_no_candidate(self):
        results = evaluate_execution_soft_gates(None, {"avg_slippage_bps": 1.0})
        assert all(r.passed for r in results)  # soft, no data = pass

    def test_slippage_delta_fail(self):
        candidate = {
            "avg_slippage_bps": 20.0,
            "total_fees": 1000.0,
            "total_turnover": 5.0,
        }
        baseline = {
            "avg_slippage_bps": 10.0,
            "total_fees": 1000.0,
            "total_turnover": 5.0,
        }
        results = evaluate_execution_soft_gates(candidate, baseline)
        slip_gate = next(r for r in results if r.name == "avg_slippage_delta")
        assert not slip_gate.passed

    def test_fees_delta_fail(self):
        candidate = {
            "avg_slippage_bps": 10.0,
            "total_fees": 1200.0,
            "total_turnover": 5.0,
        }
        baseline = {
            "avg_slippage_bps": 10.0,
            "total_fees": 1000.0,
            "total_turnover": 5.0,
        }
        results = evaluate_execution_soft_gates(candidate, baseline)
        fees_gate = next(r for r in results if r.name == "total_fees_delta")
        assert not fees_gate.passed
