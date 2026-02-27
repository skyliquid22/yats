"""Tests for research.eval.timeseries — time series output."""

from __future__ import annotations

import numpy as np
import pandas as pd

from research.eval.timeseries import (
    compute_drawdown_series,
    build_timeseries_output,
)


class TestDrawdownSeries:
    def test_basic(self):
        equity = pd.Series([100.0, 110.0, 90.0, 95.0, 115.0])
        dd = compute_drawdown_series(equity)
        assert dd.iloc[0] == 0.0  # first point is always 0
        assert dd.iloc[1] == 0.0  # new high
        assert dd.iloc[2] < 0  # drawdown
        assert dd.iloc[4] == 0.0  # new high

    def test_monotonic_up(self):
        equity = pd.Series([100.0, 110.0, 120.0])
        dd = compute_drawdown_series(equity)
        assert (dd == 0.0).all()


class TestBuildTimeseriesOutput:
    def test_structure(self):
        dates = pd.bdate_range("2023-01-01", periods=5)
        equity = pd.Series([1.0, 1.01, 0.99, 1.02, 1.03], index=dates)
        dd = compute_drawdown_series(equity)
        weights = pd.DataFrame({"A": [0.5] * 5, "B": [0.5] * 5}, index=dates)

        output = build_timeseries_output(equity, dd, weights)

        assert len(output["equity_curve"]) == 5
        assert len(output["drawdown_series"]) == 5
        assert len(output["weight_history"]) == 5

        # Check record structure
        rec = output["equity_curve"][0]
        assert "date" in rec
        assert "value" in rec

        wrec = output["weight_history"][0]
        assert "date" in wrec
        assert "weights" in wrec
        assert "A" in wrec["weights"]
