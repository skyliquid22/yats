"""Tests for research.alpha.transforms — cross-sectional rank normalization."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.alpha.transforms import rank_normalize_cross_sectional


def _panel(dates, symbols, values_dict) -> pd.DataFrame:
    """Build a simple (date, symbol) panel from value arrays per column."""
    rows = []
    for d in dates:
        for sym in symbols:
            row = {"date": d, "symbol": sym}
            for col, arr in values_dict.items():
                row[col] = arr[d][sym]
            rows.append(row)
    return pd.DataFrame(rows)


class TestRankNormalizeCrossSectional:
    def test_output_shape_unchanged(self):
        df = pd.DataFrame({
            "date": ["2024-01-01"] * 4,
            "symbol": ["A", "B", "C", "D"],
            "feat": [1.0, 2.0, 3.0, 4.0],
        })
        result = rank_normalize_cross_sectional(df, ["feat"])
        assert result.shape == df.shape

    def test_per_date_mean_near_zero(self):
        """After normalization, within-date mean of each feature ≈ 0."""
        df = pd.DataFrame({
            "date": ["2024-01-01"] * 4 + ["2024-01-02"] * 4,
            "symbol": ["A", "B", "C", "D"] * 2,
            "feat": [10.0, 20.0, 30.0, 40.0, 5.0, 15.0, 25.0, 35.0],
        })
        result = rank_normalize_cross_sectional(df, ["feat"])
        for dt, grp in result.groupby("date"):
            assert abs(grp["feat"].mean()) < 1e-10, f"mean non-zero for date {dt}"

    def test_per_date_std_near_one(self):
        """After normalization, within-date std of each feature ≈ 1 (with ≥3 symbols)."""
        symbols = ["A", "B", "C", "D", "E"]
        df = pd.DataFrame({
            "date": ["2024-01-01"] * len(symbols),
            "symbol": symbols,
            "feat": [1.0, 3.0, 5.0, 7.0, 9.0],
        })
        result = rank_normalize_cross_sectional(df, ["feat"])
        std = result.groupby("date")["feat"].std()
        assert abs(float(std.iloc[0]) - 1.0) < 0.2  # allow some tolerance for small N

    def test_missing_col_skipped(self):
        """Columns not present in df are silently skipped."""
        df = pd.DataFrame({
            "date": ["2024-01-01"] * 3,
            "symbol": ["A", "B", "C"],
            "feat": [1.0, 2.0, 3.0],
        })
        result = rank_normalize_cross_sectional(df, ["feat", "nonexistent"])
        assert "nonexistent" not in result.columns

    def test_nan_values_propagate(self):
        """NaN in a feature stays NaN after normalization."""
        df = pd.DataFrame({
            "date": ["2024-01-01"] * 4,
            "symbol": ["A", "B", "C", "D"],
            "feat": [1.0, np.nan, 3.0, 4.0],
        })
        result = rank_normalize_cross_sectional(df, ["feat"])
        assert np.isnan(result.loc[result["symbol"] == "B", "feat"].iloc[0])

    def test_preserves_relative_order(self):
        """Within a date, relative order of non-NaN values is preserved."""
        df = pd.DataFrame({
            "date": ["2024-01-01"] * 4,
            "symbol": ["A", "B", "C", "D"],
            "feat": [1.0, 2.0, 3.0, 4.0],
        })
        result = rank_normalize_cross_sectional(df, ["feat"])
        normalized = result.sort_values("symbol")["feat"].values
        assert all(normalized[i] < normalized[i + 1] for i in range(len(normalized) - 1))

    def test_two_dates_independent(self):
        """Normalization on date A does not affect date B rankings."""
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"],
            "symbol": ["A", "B", "A", "B"],
            "feat": [10.0, 20.0, 200.0, 100.0],  # reversed order on date 2
        })
        result = rank_normalize_cross_sectional(df, ["feat"])
        d1 = result[result["date"] == "2024-01-01"].sort_values("symbol")
        d2 = result[result["date"] == "2024-01-02"].sort_values("symbol")
        # On date 1: A < B; on date 2: A > B (reversed)
        assert d1.iloc[0]["feat"] < d1.iloc[1]["feat"]   # A < B on date 1
        assert d2.iloc[0]["feat"] > d2.iloc[1]["feat"]   # A > B on date 2

    def test_original_df_not_mutated(self):
        """The input DataFrame is not modified in place."""
        df = pd.DataFrame({
            "date": ["2024-01-01"] * 3,
            "symbol": ["A", "B", "C"],
            "feat": [1.0, 2.0, 3.0],
        })
        original_feat = df["feat"].copy()
        rank_normalize_cross_sectional(df, ["feat"])
        pd.testing.assert_series_equal(df["feat"], original_feat)
