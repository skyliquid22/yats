"""Tests for fundamental_features — pass-through mapping and as-of/no-lookahead."""

from __future__ import annotations

import pandas as pd
import pytest

# Importing the module registers all fundamental features as a side effect
import research.features.fundamental_features  # noqa: F401
from research.features.feature_registry import registry


_FUNDAMENTAL_MAP = {
    "pe_ttm": "pe_ratio",
    "ps_ttm": "ps_ratio",
    "pb": "pb_ratio",
    "ev_ebitda": "ev_ebitda",
    "roe": "roe",
    "gross_margin": "gross_margin",
    "operating_margin": "operating_margin",
    "fcf_margin": "fcf_margin",
    "debt_equity": "debt_to_equity",
    "eps_growth_1y": "eps_growth_yoy",
    "revenue_growth_1y": "revenue_growth_yoy",
}


class TestFundamentalPassThrough:
    def test_all_features_registered(self):
        for feat_name in _FUNDAMENTAL_MAP:
            fn = registry.get(feat_name)
            assert callable(fn)

    def test_each_feature_extracts_correct_column(self):
        for feat_name, source_col in _FUNDAMENTAL_MAP.items():
            df = pd.DataFrame({source_col: [1.5, 2.5, 3.5]})
            fn = registry.get(feat_name)
            result = fn(df)
            pd.testing.assert_series_equal(result, df[source_col])

    def test_pe_ttm_returns_pe_ratio(self):
        df = pd.DataFrame({"pe_ratio": [15.0, 20.0, 25.0]})
        fn = registry.get("pe_ttm")
        result = fn(df)
        assert list(result) == [15.0, 20.0, 25.0]

    def test_debt_equity_maps_to_debt_to_equity(self):
        df = pd.DataFrame({"debt_to_equity": [0.5, 1.2, 2.0]})
        fn = registry.get("debt_equity")
        result = fn(df)
        assert list(result) == [0.5, 1.2, 2.0]


class TestAsOfNoLookahead:
    """Verify point-in-time / as-of semantics for fundamental forward-fill.

    The canonicalizer forward-fills fundamentals so that each trading day has
    the most recent known value — no future data leaks back in time.
    We simulate this here to verify the expected behavior.
    """

    def test_forward_fill_no_lookahead(self):
        """Metric filed on day 3 must NOT appear on days 0-2."""
        dates = pd.date_range("2023-01-01", periods=10)
        # Observation filed on day 3, updated on day 7
        raw = pd.DataFrame({
            "timestamp": [dates[3], dates[7]],
            "pe_ratio": [15.0, 20.0],
        }).set_index("timestamp")

        # Reindex to all dates and forward-fill
        filled = raw.reindex(dates).ffill()

        # Days before first observation: NaN (no lookahead)
        assert filled["pe_ratio"].iloc[0:3].isna().all()
        # Day 3 onward: 15.0
        assert filled["pe_ratio"].iloc[3] == 15.0
        assert filled["pe_ratio"].iloc[6] == 15.0
        # Day 7 onward: 20.0
        assert filled["pe_ratio"].iloc[7] == 20.0
        assert filled["pe_ratio"].iloc[9] == 20.0

    def test_as_of_direction_forward_not_backward(self):
        """Forward-fill must propagate values forward — backfill would be lookahead."""
        dates = pd.date_range("2023-01-01", periods=5)
        raw = pd.DataFrame({
            "timestamp": [dates[2]],
            "pe_ratio": [25.0],
        }).set_index("timestamp")

        filled = raw.reindex(dates).ffill()
        backfilled = raw.reindex(dates).bfill()

        # Forward-fill: days 0-1 are NaN
        assert filled["pe_ratio"].iloc[0:2].isna().all()
        # Backfill: days 0-1 get 25.0 — this is lookahead, should NOT be used
        assert backfilled["pe_ratio"].iloc[0] == 25.0
        # Confirm the two approaches differ
        assert not filled.equals(backfilled)

    def test_latest_report_supersedes_older(self):
        """If two reports exist for same period, the later report_date wins."""
        dates = pd.date_range("2023-01-01", periods=5)
        # Two filings — day 1 first, day 3 supersedes
        raw = pd.DataFrame({
            "timestamp": [dates[1], dates[3]],
            "pe_ratio": [10.0, 15.0],
        }).set_index("timestamp")

        filled = raw.reindex(dates).ffill()

        # On day 2, value is 10.0 (only day 1 known)
        assert filled["pe_ratio"].iloc[2] == 10.0
        # On day 3 and after, value is 15.0 (superseded)
        assert filled["pe_ratio"].iloc[3] == 15.0
        assert filled["pe_ratio"].iloc[4] == 15.0

    def test_feature_function_uses_prefilled_column(self):
        """Feature function just reads column — pipeline pre-fills before calling."""
        df = pd.DataFrame({"pe_ratio": [15.0, 15.0, 20.0, 20.0, 20.0]})
        fn = registry.get("pe_ttm")
        result = fn(df)
        assert list(result) == [15.0, 15.0, 20.0, 20.0, 20.0]
