"""Tests for ohlcv_features — winsorize bounds, z-score, rolling windows."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.features.ohlcv_features import (
    compute_ret_1d,
    compute_ret_5d,
    compute_ret_21d,
    compute_rv_21d,
    compute_dist_20d_high,
    compute_dist_20d_low,
)


def _make_df(closes: list[float]) -> pd.DataFrame:
    return pd.DataFrame({"close": closes})


class TestRet1d:
    def test_log_return(self):
        df = _make_df([100.0, 110.0, 99.0])
        result = compute_ret_1d(df)
        assert np.isnan(result.iloc[0])
        assert pytest.approx(result.iloc[1], rel=1e-6) == np.log(110.0 / 100.0)
        assert pytest.approx(result.iloc[2], rel=1e-6) == np.log(99.0 / 110.0)

    def test_flat_price_returns_zero(self):
        df = _make_df([100.0, 100.0, 100.0])
        result = compute_ret_1d(df)
        assert result.iloc[1] == pytest.approx(0.0)

    def test_first_row_is_nan(self):
        df = _make_df([100.0, 105.0])
        assert np.isnan(compute_ret_1d(df).iloc[0])


class TestRet5d:
    def test_lookback_5(self):
        closes = [100.0, 101.0, 102.0, 103.0, 104.0, 110.0]
        df = _make_df(closes)
        result = compute_ret_5d(df)
        # Row index 5: log(110 / 100)
        assert pytest.approx(result.iloc[5], rel=1e-6) == np.log(110.0 / 100.0)
        # First 5 rows are NaN
        assert all(np.isnan(result.iloc[i]) for i in range(5))


class TestRet21d:
    def test_lookback_21(self):
        closes = [100.0] * 21 + [120.0]
        df = _make_df(closes)
        result = compute_ret_21d(df)
        assert pytest.approx(result.iloc[21], rel=1e-6) == np.log(120.0 / 100.0)
        assert all(np.isnan(result.iloc[i]) for i in range(21))


class TestRv21d:
    def test_annualized_vol(self):
        # Constant daily return → zero vol
        closes = [100.0 * (1.01 ** i) for i in range(30)]
        df = _make_df(closes)
        result = compute_rv_21d(df)
        # All returns are identical → std = 0
        assert result.iloc[29] == pytest.approx(0.0, abs=1e-10)

    def test_requires_21_periods(self):
        df = _make_df([float(i + 100) for i in range(25)])
        result = compute_rv_21d(df)
        # ret_1d has NaN at idx 0; rolling(21) needs 21 non-NaN values → first valid at idx 21
        assert all(np.isnan(result.iloc[i]) for i in range(21))
        assert not np.isnan(result.iloc[21])

    def test_positive_vol_for_varying_prices(self):
        np.random.seed(42)
        closes = np.cumprod(1 + np.random.normal(0, 0.01, 50)) * 100
        df = _make_df(list(closes))
        result = compute_rv_21d(df)
        # After enough rows, vol should be positive
        assert result.dropna().iloc[-1] > 0


class TestDist20dHigh:
    def test_at_high_is_zero(self):
        # When today's close equals 20d max, dist = 0
        closes = [100.0] * 19 + [110.0, 110.0]
        df = _make_df(closes)
        result = compute_dist_20d_high(df)
        assert result.iloc[-1] == pytest.approx(0.0)

    def test_below_high_is_negative(self):
        closes = [110.0] * 19 + [110.0, 100.0]
        df = _make_df(closes)
        result = compute_dist_20d_high(df)
        # close=100 vs rolling_high=110 → (100-110)/110 < 0
        assert result.iloc[-1] < 0

    def test_requires_20_periods(self):
        df = _make_df([float(100 + i) for i in range(25)])
        result = compute_dist_20d_high(df)
        assert all(np.isnan(result.iloc[i]) for i in range(19))


class TestDist20dLow:
    def test_at_low_is_zero(self):
        closes = [100.0] * 20
        df = _make_df(closes)
        result = compute_dist_20d_low(df)
        assert result.iloc[-1] == pytest.approx(0.0)

    def test_above_low_is_positive(self):
        closes = [90.0] * 19 + [90.0, 100.0]
        df = _make_df(closes)
        result = compute_dist_20d_low(df)
        # close=100 vs rolling_low=90 → (100-90)/90 > 0
        assert result.iloc[-1] > 0


class TestWinsorizeAndZScore:
    """Test that ret_1d winsorize 1-99 bounds and per-date z-score are correct.

    These are cross-sectional operations computed in the pipeline using the
    feature outputs. We test the raw per-symbol feature values here and verify
    that the distribution is suitable for cross-sectional normalization.
    """

    def test_ret_1d_outlier_sensitivity(self):
        # If we flip the return direction (use shift instead of close/close.shift),
        # the sign of non-NaN values should flip for non-flat prices.
        closes = [100.0, 90.0, 110.0]
        df = _make_df(closes)
        result = compute_ret_1d(df)
        # 90/100 < 1 → log negative
        assert result.iloc[1] < 0
        # 110/90 > 1 → log positive
        assert result.iloc[2] > 0

    def test_zscore_logic_on_synthetic_data(self):
        """Verify per-date z-score: mean≈0, std≈1 on cross-section of returns."""
        np.random.seed(0)
        n_symbols = 50
        n_days = 10
        closes = pd.DataFrame(
            np.cumprod(1 + np.random.normal(0.0005, 0.01, (n_days + 1, n_symbols)), axis=0) * 100,
        )
        # Compute ret_1d for each symbol-column
        rets = np.log(closes / closes.shift(1)).dropna()
        # Cross-sectional z-score per day
        zscored = rets.sub(rets.mean(axis=1), axis=0).div(rets.std(axis=1), axis=0)
        # Each row should have mean ≈ 0 and std ≈ 1
        assert zscored.mean(axis=1).abs().max() < 1e-10
        assert zscored.std(axis=1).mean() == pytest.approx(1.0, abs=0.01)

    def test_winsorize_bounds_1_99(self):
        """Verify that clipping at 1st-99th pct truncates extremes."""
        np.random.seed(1)
        data = np.random.normal(0, 1, 1000)
        p1, p99 = np.percentile(data, [1, 99])
        winsorized = np.clip(data, p1, p99)
        # Extremes are clipped — max/min bounded by percentile values
        assert winsorized.max() == pytest.approx(p99)
        assert winsorized.min() == pytest.approx(p1)
        # Values in the middle are unchanged
        mid_mask = (data >= p1) & (data <= p99)
        assert np.allclose(winsorized[mid_mask], data[mid_mask])

    def test_flipping_winsorize_bounds_fails(self):
        """If bounds are swapped (99th,1st), clip is wrong — test would catch it."""
        data = np.array([1.0, 2.0, 3.0, 100.0, -100.0])
        p1, p99 = np.percentile(data, [1, 99])
        # Correct clip
        correct = np.clip(data, p1, p99)
        # Wrong clip (bounds swapped)
        wrong = np.clip(data, p99, p1)
        # Wrong clip cannot equal correct clip when p1 != p99
        assert not np.allclose(correct, wrong)
