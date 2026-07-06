"""Tests for feature_pipeline._write_features and regime alignment (ya-muvu0)."""
from datetime import datetime, timezone
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from yats_pipelines.jobs.feature_pipeline import _write_features


def _make_sender():
    return MagicMock()


def _now():
    return datetime(2024, 1, 5, tzinfo=timezone.utc)


def _ts(date_str):
    return pd.Timestamp(date_str, tz="UTC")


class TestWriteFeaturesNdarray:
    """_write_features must handle ndarray values without raising TypeError."""

    def test_ndarray_value_does_not_raise(self):
        """Regression: float() on full ndarray raised TypeError before fix."""
        sender = _make_sender()
        timestamps = pd.Series([_ts("2024-01-03"), _ts("2024-01-04")])
        # Simulate the regime broadcast storing an ndarray instead of a Series
        features = {"ret_1d": np.array([0.01, 0.02])}

        written = _write_features(
            sender, "AAPL", timestamps, features, "core_v1", "1.0", _now()
        )

        assert written == 2
        assert sender.row.call_count == 2

    def test_ndarray_values_indexed_per_row(self):
        """Each row gets its own element, not the full array."""
        sender = _make_sender()
        timestamps = pd.Series([_ts("2024-01-03"), _ts("2024-01-04")])
        features = {"ret_1d": np.array([0.10, 0.20])}

        _write_features(
            sender, "AAPL", timestamps, features, "core_v1", "1.0", _now()
        )

        first_call_cols = sender.row.call_args_list[0][1]["columns"]
        second_call_cols = sender.row.call_args_list[1][1]["columns"]
        assert abs(first_call_cols["ret_1d"] - 0.10) < 1e-9
        assert abs(second_call_cols["ret_1d"] - 0.20) < 1e-9

    def test_series_value_still_works(self):
        """Series path is unaffected by the ndarray fix."""
        sender = _make_sender()
        timestamps = pd.Series([_ts("2024-01-03"), _ts("2024-01-04")])
        features = {"ret_1d": pd.Series([0.05, 0.06])}

        written = _write_features(
            sender, "AAPL", timestamps, features, "core_v1", "1.0", _now()
        )

        assert written == 2

    def test_nan_ndarray_rows_skipped(self):
        """NaN elements in an ndarray are skipped (same as Series NaN)."""
        sender = _make_sender()
        timestamps = pd.Series([_ts("2024-01-03"), _ts("2024-01-04")])
        features = {"ret_1d": np.array([float("nan"), 0.03])}

        written = _write_features(
            sender, "AAPL", timestamps, features, "core_v1", "1.0", _now()
        )

        assert written == 1


class TestRegimeReindexAlignment:
    """Regime reindex with ffill must produce one value per OHLCV timestamp."""

    def test_reindex_aligns_by_timestamp_values(self):
        """regime_df.reindex(sym_ts, ffill) produces one value per OHLCV row."""
        ohlcv_timestamps = pd.Series(
            pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"], utc=True)
        )
        # Regime has values on Jan 2 and Jan 4, not Jan 3
        regime_index = pd.DatetimeIndex(
            pd.to_datetime(["2024-01-02", "2024-01-04"], utc=True)
        )
        regime_series = pd.Series([1.0, 2.0], index=regime_index, name="market_vol_20d")

        regime_aligned = regime_series.reindex(ohlcv_timestamps, method="ffill")

        assert len(regime_aligned) == len(ohlcv_timestamps), (
            "reindex must produce one row per OHLCV timestamp"
        )
        # Jan 3 should get Jan 2's value via ffill
        assert regime_aligned.iloc[1] == 1.0
        assert regime_aligned.iloc[2] == 2.0

    def test_regime_aligned_as_series_works_in_write_features(self):
        """End-to-end: regime Series aligned to OHLCV timestamps writes correctly."""
        sender = _make_sender()
        ohlcv_timestamps = pd.Series(
            pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"], utc=True)
        )
        regime_index = pd.DatetimeIndex(
            pd.to_datetime(["2024-01-02", "2024-01-04"], utc=True)
        )
        regime_series = pd.Series([0.15, 0.25], index=regime_index)
        regime_aligned = regime_series.reindex(ohlcv_timestamps, method="ffill")

        features = {"market_vol_20d": regime_aligned}
        written = _write_features(
            sender, "SPY", ohlcv_timestamps, features, "core_v1", "1.0", _now()
        )

        assert written == 3
        assert sender.row.call_count == 3
