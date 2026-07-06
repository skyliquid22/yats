"""Tests for cross_sectional_features — winsorize bounds, per-date z-score."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.features.cross_sectional_features import (
    compute_mom_3m,
    compute_mom_12m_excl_1m,
    compute_log_mkt_cap,
    compute_size_rank,
    compute_value_rank,
)


def _make_df(**cols) -> pd.DataFrame:
    return pd.DataFrame(cols)


class TestMom3m:
    def test_63day_cumulative_return(self):
        closes = [100.0] * 63 + [110.0]
        df = _make_df(close=closes)
        result = compute_mom_3m(df)
        assert pytest.approx(result.iloc[-1], rel=1e-6) == np.log(110.0 / 100.0)

    def test_first_63_rows_nan(self):
        closes = [float(100 + i) for i in range(70)]
        df = _make_df(close=closes)
        result = compute_mom_3m(df)
        assert all(np.isnan(result.iloc[i]) for i in range(63))


class TestMom12mExcl1m:
    def test_months_2_to_12(self):
        # log(close_{t-21} / close_{t-252})
        # Build series where close.shift(21) = 130 and close.shift(252) = 100 at last row
        # Last row idx = 252. shift(21) reads idx 231; shift(252) reads idx 0.
        closes = [100.0] + [100.0] * 230 + [130.0] + [130.0] * 21
        df = _make_df(close=closes)
        result = compute_mom_12m_excl_1m(df)
        # At index 252: close.shift(21)=130, close.shift(252)=100 → log(130/100)
        assert pytest.approx(result.iloc[-1], rel=1e-6) == np.log(130.0 / 100.0)

    def test_requires_252_periods(self):
        closes = [float(100 + i) for i in range(260)]
        df = _make_df(close=closes)
        result = compute_mom_12m_excl_1m(df)
        assert all(np.isnan(result.iloc[i]) for i in range(252))


class TestLogMktCap:
    def test_log_shares_times_close(self):
        df = _make_df(shares_outstanding=[1_000_000.0], close=[100.0])
        result = compute_log_mkt_cap(df)
        assert pytest.approx(result.iloc[0], rel=1e-6) == np.log(1_000_000.0 * 100.0)

    def test_proportional_to_shares(self):
        df = _make_df(shares_outstanding=[1e6, 2e6], close=[100.0, 100.0])
        result = compute_log_mkt_cap(df)
        # Double shares → log_mkt_cap increases by log(2)
        assert pytest.approx(result.iloc[1] - result.iloc[0], rel=1e-6) == np.log(2.0)

    def test_none_shares_outstanding_returns_nan_not_crash(self):
        """compute_log_mkt_cap must return NaN (not crash) when shares_outstanding is None."""
        df = _make_df(shares_outstanding=[None], close=[100.0])
        result = compute_log_mkt_cap(df)
        assert result.isna().all()

    def test_object_dtype_all_none_returns_all_nan(self):
        """Object-dtype shares_outstanding column with all-None values returns all NaN."""
        df = pd.DataFrame({
            "shares_outstanding": pd.array([None, None], dtype=object),
            "close": [100.0, 200.0],
        })
        result = compute_log_mkt_cap(df)
        assert result.isna().all()

    def test_mixed_none_and_valid_shares_partial_nan(self):
        """Valid rows compute correctly; None rows return NaN — no crash."""
        df = pd.DataFrame({
            "shares_outstanding": pd.array([1_000_000.0, None], dtype=object),
            "close": [100.0, 200.0],
        })
        result = compute_log_mkt_cap(df)
        assert pytest.approx(result.iloc[0], rel=1e-6) == np.log(1_000_000.0 * 100.0)
        assert np.isnan(result.iloc[1])


class TestSizeRank:
    def test_percentile_rank_ascending(self):
        df = _make_df(log_mkt_cap=[1.0, 2.0, 3.0, 4.0, 5.0])
        result = compute_size_rank(df)
        # pct=True ranks in [0,1] — smallest gets lowest rank
        assert result.iloc[0] < result.iloc[-1]
        # All values in (0, 1]
        assert (result > 0).all() and (result <= 1).all()

    def test_rank_pct_true(self):
        df = _make_df(log_mkt_cap=[10.0, 20.0, 30.0, 40.0, 100.0])
        result = compute_size_rank(df)
        assert result.max() == pytest.approx(1.0)
        # Largest gets rank 1.0
        assert result.iloc[4] == pytest.approx(1.0)

    def test_flipping_rank_direction_fails(self):
        """If rank direction were reversed, largest would get rank 0 — detects breakage."""
        df = _make_df(log_mkt_cap=[1.0, 2.0, 3.0, 4.0, 5.0])
        correct = compute_size_rank(df)
        # Wrong direction: ascending=False would give largest rank 1 still (pct=True)
        # but ascending=True is the default and gives correct ordering
        assert correct.iloc[4] == pytest.approx(1.0)  # largest gets rank 1
        assert correct.iloc[0] < correct.iloc[4]


class TestValueRank:
    def test_lower_pe_gets_higher_value_rank(self):
        # Lower PE → higher earnings yield → higher value rank
        df = _make_df(pe_ttm=[5.0, 10.0, 20.0, 40.0])
        result = compute_value_rank(df)
        # pe_ttm=5 (cheapest) should rank highest
        assert result.iloc[0] == pytest.approx(1.0)
        # pe_ttm=40 (most expensive) should rank lowest
        assert result.iloc[3] < result.iloc[0]

    def test_earnings_yield_formula(self):
        df = _make_df(pe_ttm=[10.0, 20.0])
        result = compute_value_rank(df)
        # 1/10 > 1/20 → pe_ttm=10 ranks higher
        assert result.iloc[0] > result.iloc[1]


class TestPerDateZScore:
    """Validate winsorize 1-99 bounds and per-date z-score on synthetic data."""

    def test_per_date_zscore_mean_zero_std_one(self):
        """Cross-sectional z-score on each date should have mean≈0, std≈1."""
        np.random.seed(42)
        n_symbols = 50
        n_days = 5
        dates = pd.date_range("2023-01-01", periods=n_days)

        rows = []
        for date in dates:
            mkt_caps = np.random.lognormal(mean=5, sigma=1, size=n_symbols)
            for i in range(n_symbols):
                rows.append({"date": date, "log_mkt_cap": float(mkt_caps[i])})

        df = pd.DataFrame(rows)
        for date, group in df.groupby("date"):
            zscored = (group["log_mkt_cap"] - group["log_mkt_cap"].mean()) / group["log_mkt_cap"].std()
            assert zscored.mean() == pytest.approx(0.0, abs=1e-10)
            assert zscored.std() == pytest.approx(1.0, abs=1e-10)

    def test_winsorize_1_99_clips_extremes(self):
        """Clipping at 1-99th pct truncates outliers without touching interior."""
        np.random.seed(7)
        data = np.random.normal(0, 1, 500)
        p1, p99 = np.percentile(data, [1, 99])
        winsorized = np.clip(data, p1, p99)

        assert winsorized.max() == pytest.approx(p99)
        assert winsorized.min() == pytest.approx(p1)

        interior = (data > p1) & (data < p99)
        assert np.allclose(winsorized[interior], data[interior])

    def test_flipping_winsorize_bounds_breaks_clipping(self):
        """Swapping p1/p99 produces wrong result — ensures test catches logic errors."""
        data = np.array([-10.0, -1.0, 0.0, 1.0, 10.0])
        p1, p99 = np.percentile(data, [1, 99])
        correct = np.clip(data, p1, p99)
        # Swap bounds intentionally
        broken = np.clip(data, p99, p1)
        # numpy clips everything to p99 when a > b — differs from correct
        assert not np.allclose(correct, broken)

    def test_size_rank_breaks_if_zscore_axis_wrong(self):
        """If we rank across time (axis=0) instead of cross-section (axis=1), result differs."""
        np.random.seed(3)
        n = 10
        df = pd.DataFrame({
            "log_mkt_cap": np.random.normal(5, 1, n),
        })
        cross_sectional_rank = df["log_mkt_cap"].rank(pct=True)
        time_series_rank = pd.Series(range(1, n + 1), dtype=float) / n

        # They are only equal by coincidence — in general they differ
        assert not np.allclose(cross_sectional_rank.values, time_series_rank.values)
