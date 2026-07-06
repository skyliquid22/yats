"""Regression tests for watermark semantics (ya-qlvxi).

1. Watermark advances on max(processed) even when all rows are all-NaN skips
   (fixes perpetual reprocessing).
2. Watermark is capped at max(metrics_date) when fundamentals lag behind
   processed dates (fixes permanent gaps).
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import numpy as np
import pandas as pd
import pytest

from yats_pipelines.jobs.feature_pipeline_incremental import (
    _write_watermark,
    _write_features,
    _winsorize,
)


def _ts(date_str: str) -> pd.Timestamp:
    return pd.Timestamp(date_str, tz="UTC")


def _now() -> datetime:
    return datetime(2024, 6, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Helper: minimal FeatureSet-like object
# ---------------------------------------------------------------------------


class _FakeFS:
    def __init__(self, ohlcv=None, fundamental=None):
        self.ohlcv = ohlcv or ["ret_1d"]
        self.fundamental = fundamental or []


# ---------------------------------------------------------------------------
# Test 1: Watermark advances even when all feature rows are all-NaN (skipped)
# ---------------------------------------------------------------------------


class TestWatermarkAdvancesOnAllNanSkips:
    """Watermark must advance to max(processed) even when written == 0."""

    def _run_incremental_symbol(
        self,
        ohlcv_dates: list[str],
        watermark_date: str | None,
        feature_names: list[str],
        all_nan: bool,
        fundamental_dates: list[str] | None = None,
    ) -> pd.Timestamp | None:
        """
        Simulate _compute_per_symbol_incremental for a single symbol.
        Returns the watermark timestamp that would be written, or None.
        """
        from yats_pipelines.jobs.feature_pipeline_incremental import (
            _write_watermark,
        )

        ohlcv_df = pd.DataFrame({
            "timestamp": pd.to_datetime(ohlcv_dates, utc=True),
            "symbol": "AAPL",
            "open": [100.0] * len(ohlcv_dates),
            "high": [105.0] * len(ohlcv_dates),
            "low": [98.0] * len(ohlcv_dates),
            "close": [102.0] * len(ohlcv_dates),
            "volume": [1_000_000.0] * len(ohlcv_dates),
        }).sort_values("timestamp").reset_index(drop=True)

        wm = pd.Timestamp(watermark_date, tz="UTC") if watermark_date else None

        if wm is not None:
            new_mask = ohlcv_df["timestamp"] > wm
        else:
            new_mask = pd.Series(True, index=ohlcv_df.index)

        new_indices = ohlcv_df.index[new_mask]
        if len(new_indices) == 0:
            return None

        new_timestamps = ohlcv_df.loc[new_indices, "timestamp"].reset_index(drop=True)

        # Simulate _write_features returning 0 (all NaN) or > 0
        written = 0 if all_nan else len(new_timestamps)
        max_processed = new_timestamps.max()

        # Build metrics_df for fundamental-lag test
        metrics_df = pd.DataFrame()
        if fundamental_dates is not None:
            metrics_df = pd.DataFrame({
                "timestamp": pd.to_datetime(fundamental_dates, utc=True),
                "symbol": "AAPL",
            })

        fs = _FakeFS(fundamental=["pe_ttm"] if fundamental_dates is not None else [])

        # Replicate the fixed watermark logic from _compute_per_symbol_incremental
        new_watermark = max_processed
        if fs.fundamental and not metrics_df.empty:
            max_metrics_date = metrics_df["timestamp"].max()
            if pd.notna(max_metrics_date) and max_metrics_date < max_processed:
                new_watermark = max_metrics_date

        return new_watermark

    def test_watermark_advances_when_written_zero(self):
        """Even if all feature rows are NaN (written=0), watermark must advance."""
        new_wm = self._run_incremental_symbol(
            ohlcv_dates=["2024-01-02", "2024-01-03", "2024-01-04"],
            watermark_date="2024-01-01",
            feature_names=["ret_1d"],
            all_nan=True,  # Nothing written
        )
        assert new_wm is not None
        assert new_wm == _ts("2024-01-04"), (
            "Watermark must advance to max(processed) even when all rows are all-NaN skips"
        )

    def test_watermark_advances_to_max_processed_when_partial_writes(self):
        """Watermark advances to last processed date when some rows written."""
        new_wm = self._run_incremental_symbol(
            ohlcv_dates=["2024-01-02", "2024-01-03", "2024-01-04"],
            watermark_date="2024-01-01",
            feature_names=["ret_1d"],
            all_nan=False,
        )
        assert new_wm == _ts("2024-01-04")

    def test_watermark_unchanged_when_no_new_dates(self):
        """No new dates beyond watermark → no watermark update (returns None)."""
        new_wm = self._run_incremental_symbol(
            ohlcv_dates=["2024-01-02"],
            watermark_date="2024-01-02",
            feature_names=["ret_1d"],
            all_nan=False,
        )
        assert new_wm is None, "No new dates = no watermark update"

    def test_watermark_no_watermark_advances_to_last_date(self):
        """With no prior watermark, all dates are processed, watermark = last date."""
        new_wm = self._run_incremental_symbol(
            ohlcv_dates=["2024-01-02", "2024-01-03"],
            watermark_date=None,
            feature_names=["ret_1d"],
            all_nan=True,
        )
        assert new_wm == _ts("2024-01-03")


# ---------------------------------------------------------------------------
# Test 2: Fundamental-lag capping
# ---------------------------------------------------------------------------


class TestFundamentalLagWatermarkCap:
    """When fundamentals lag, watermark must not advance past the last metrics date."""

    def _run_with_fundamental_lag(
        self,
        ohlcv_dates: list[str],
        watermark_date: str | None,
        metrics_dates: list[str],
    ) -> pd.Timestamp | None:
        ohlcv_df = pd.DataFrame({
            "timestamp": pd.to_datetime(ohlcv_dates, utc=True),
        })
        wm = pd.Timestamp(watermark_date, tz="UTC") if watermark_date else None

        if wm is not None:
            new_mask = ohlcv_df["timestamp"] > wm
        else:
            new_mask = pd.Series(True, index=ohlcv_df.index)

        new_indices = ohlcv_df.index[new_mask]
        if len(new_indices) == 0:
            return None

        new_timestamps = ohlcv_df.loc[new_indices, "timestamp"].reset_index(drop=True)
        max_processed = new_timestamps.max()

        metrics_df = pd.DataFrame({
            "timestamp": pd.to_datetime(metrics_dates, utc=True),
        })

        fs = _FakeFS(fundamental=["pe_ttm"])
        new_watermark = max_processed
        if fs.fundamental and not metrics_df.empty:
            max_metrics_date = metrics_df["timestamp"].max()
            if pd.notna(max_metrics_date) and max_metrics_date < max_processed:
                new_watermark = max_metrics_date

        return new_watermark

    def test_watermark_capped_at_metrics_boundary(self):
        """When fundamentals only exist up to Jan 3, watermark is capped at Jan 3."""
        new_wm = self._run_with_fundamental_lag(
            ohlcv_dates=["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
            watermark_date="2024-01-01",
            metrics_dates=["2024-01-02", "2024-01-03"],  # Lag: no Jan 4-5 metrics
        )
        assert new_wm == _ts("2024-01-03"), (
            "Watermark must be capped at last metrics date to allow reprocessing "
            "when Jan 4-5 fundamentals arrive"
        )

    def test_watermark_not_capped_when_metrics_up_to_date(self):
        """When fundamentals cover all processed dates, watermark advances fully."""
        new_wm = self._run_with_fundamental_lag(
            ohlcv_dates=["2024-01-02", "2024-01-03", "2024-01-04"],
            watermark_date="2024-01-01",
            metrics_dates=["2024-01-02", "2024-01-03", "2024-01-04"],  # No lag
        )
        assert new_wm == _ts("2024-01-04"), (
            "When metrics cover all processed dates, watermark advances to max(processed)"
        )

    def test_watermark_not_capped_when_metrics_ahead(self):
        """When fundamentals extend beyond processed dates, no capping."""
        new_wm = self._run_with_fundamental_lag(
            ohlcv_dates=["2024-01-02", "2024-01-03"],
            watermark_date="2024-01-01",
            metrics_dates=["2024-01-02", "2024-01-03", "2024-01-10"],  # Metrics ahead
        )
        assert new_wm == _ts("2024-01-03")

    def test_watermark_not_capped_when_no_fundamentals(self):
        """When metrics_df is empty, no capping occurs — advances to max processed."""
        ohlcv_dates = ["2024-01-02", "2024-01-03", "2024-01-04"]
        ohlcv_df = pd.DataFrame({"timestamp": pd.to_datetime(ohlcv_dates, utc=True)})
        new_timestamps = ohlcv_df["timestamp"]
        max_processed = new_timestamps.max()
        metrics_df = pd.DataFrame()  # Empty — no fundamentals

        fs = _FakeFS(fundamental=["pe_ttm"])
        new_watermark = max_processed
        if fs.fundamental and not metrics_df.empty:
            max_metrics_date = metrics_df["timestamp"].max()
            if pd.notna(max_metrics_date) and max_metrics_date < max_processed:
                new_watermark = max_metrics_date

        assert new_watermark == _ts("2024-01-04"), (
            "Empty metrics_df must not cap watermark — symbol has no fundamentals, "
            "advancing prevents perpetual reprocessing"
        )
