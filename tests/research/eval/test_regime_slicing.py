"""Tests for research.eval.regime_slicing — regime labeling and performance slicing."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.eval.regime_slicing import (
    label_v1,
    label_v2,
    label_regimes,
    slice_by_regime,
    regime_slices_to_dict,
    RegimeBucketMetrics,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_regime_features(n: int = 300, seed: int = 42) -> pd.DataFrame:
    """Generate synthetic regime features for testing."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2023-01-01", periods=n)
    return pd.DataFrame({
        "market_vol_20d": rng.uniform(0.08, 0.30, n),
        "market_trend_20d": rng.normal(0.0, 0.05, n),
        "dispersion_20d": rng.uniform(0.01, 0.05, n),
        "corr_mean_20d": rng.uniform(0.2, 0.8, n),
    }, index=dates)


# ---------------------------------------------------------------------------
# v1 labeling
# ---------------------------------------------------------------------------

class TestLabelV1:
    def test_produces_three_buckets(self):
        features = _make_regime_features()
        labels = label_v1(features)
        assert set(labels.unique()) == {"low_vol", "mid_vol", "high_vol"}

    def test_roughly_equal_distribution(self):
        features = _make_regime_features(n=900)
        labels = label_v1(features)
        counts = labels.value_counts()
        # Each bucket should have roughly 1/3 of the data
        for bucket in ["low_vol", "mid_vol", "high_vol"]:
            assert counts[bucket] > 200  # out of 900

    def test_empty_input(self):
        features = pd.DataFrame({"market_vol_20d": pd.Series(dtype=float)})
        labels = label_v1(features)
        assert len(labels) == 0


# ---------------------------------------------------------------------------
# v2 labeling
# ---------------------------------------------------------------------------

class TestLabelV2:
    def test_produces_six_buckets(self):
        # Construct data that will hit all 6 buckets
        rng = np.random.default_rng(42)
        n = 600
        dates = pd.bdate_range("2023-01-01", periods=n)
        features = pd.DataFrame({
            "market_vol_20d": np.concatenate([
                np.full(100, 0.08),   # low_vol
                np.full(100, 0.15),   # mid_vol
                np.full(100, 0.25),   # high_vol
                np.full(100, 0.08),
                np.full(100, 0.15),
                np.full(100, 0.25),
            ]),
            "market_trend_20d": np.concatenate([
                np.full(100, 0.05),   # trend_up
                np.full(100, -0.05),  # trend_down
                np.full(100, 0.0),    # trend_flat
                np.full(100, -0.05),
                np.full(100, 0.05),
                np.full(100, 0.05),
            ]),
        }, index=dates)

        thresholds = {"vol_high": 0.20, "vol_low": 0.12, "trend_up": 0.02, "trend_down": -0.02}
        labels = label_v2(features, thresholds=thresholds)
        unique_labels = set(labels.unique())
        assert "high_vol_trend_up" in unique_labels
        assert "low_vol_trend_up" in unique_labels
        assert "mid_vol_trend_down" in unique_labels

    def test_handles_nan(self):
        dates = pd.bdate_range("2023-01-01", periods=5)
        features = pd.DataFrame({
            "market_vol_20d": [0.1, np.nan, 0.25, 0.15, 0.08],
            "market_trend_20d": [0.05, 0.01, np.nan, -0.05, 0.0],
        }, index=dates)
        thresholds = {"vol_high": 0.20, "vol_low": 0.12, "trend_up": 0.02, "trend_down": -0.02}
        labels = label_v2(features, thresholds=thresholds)
        assert labels.isna().sum() == 2  # rows 1 and 2


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

class TestLabelRegimes:
    def test_v1_dispatch(self):
        features = _make_regime_features()
        labels = label_regimes(features, "v1")
        assert "low_vol" in labels.values

    def test_v2_dispatch(self):
        features = _make_regime_features()
        thresholds = {"vol_high": 0.20, "vol_low": 0.12, "trend_up": 0.02, "trend_down": -0.02}
        labels = label_regimes(features, "v2", thresholds=thresholds)
        assert len(labels) > 0

    def test_invalid_version(self):
        features = _make_regime_features()
        with pytest.raises(ValueError, match="Unknown labeling version"):
            label_regimes(features, "v99")


# ---------------------------------------------------------------------------
# Regime slicing
# ---------------------------------------------------------------------------

class TestSliceByRegime:
    def test_basic_slicing(self):
        n = 300
        dates = pd.bdate_range("2023-01-01", periods=n)
        rng = np.random.default_rng(42)

        portfolio_returns = pd.Series(rng.normal(0.0005, 0.01, n), index=dates)
        equity_curve = (1.0 + portfolio_returns).cumprod()

        labels = pd.Series(
            ["low_vol"] * 100 + ["mid_vol"] * 100 + ["high_vol"] * 100,
            index=dates,
        )

        slices = slice_by_regime(portfolio_returns, equity_curve, labels)
        assert len(slices) == 3
        assert all(isinstance(s, RegimeBucketMetrics) for s in slices)
        assert sum(s.n_days for s in slices) == n

    def test_to_dict(self):
        slices = [
            RegimeBucketMetrics("low_vol", 1.5, 0.1, 0.1, -0.05, 100),
            RegimeBucketMetrics("high_vol", 0.5, 0.03, 0.03, -0.15, 100),
        ]
        d = regime_slices_to_dict(slices)
        assert "low_vol" in d
        assert d["low_vol"]["sharpe"] == 1.5
        assert d["high_vol"]["n_days"] == 100
