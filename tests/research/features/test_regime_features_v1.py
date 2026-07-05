"""Tests for regime_features_v1 — market volatility, trend, dispersion, correlation."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.features.regime_features_v1 import compute_regime_features


def _make_prices(n_days: int = 60, seed: int = 42) -> pd.DataFrame:
    np.random.seed(seed)
    dates = pd.date_range("2023-01-01", periods=n_days)
    data = {}
    for sym in ["SPY", "QQQ", "IWM"]:
        returns = np.random.normal(0.0005, 0.01, n_days)
        prices = 100.0 * np.cumprod(1 + returns)
        data[sym] = prices
    return pd.DataFrame(data, index=dates)


class TestComputeRegimeFeatures:
    def test_output_columns(self):
        df = _make_prices()
        result = compute_regime_features(df)
        assert set(result.columns) == {
            "market_vol_20d", "market_trend_20d",
            "dispersion_20d", "corr_mean_20d",
        }

    def test_nan_before_window(self):
        df = _make_prices(60)
        result = compute_regime_features(df, window=20)
        # First 20 rows should be NaN (min_periods=20)
        assert result["market_vol_20d"].iloc[:20].isna().all()
        assert result["market_trend_20d"].iloc[:20].isna().all()

    def test_market_vol_annualized(self):
        df = _make_prices()
        result = compute_regime_features(df, window=20)
        log_returns = np.log(df / df.shift(1))
        portfolio_returns = log_returns.mean(axis=1)
        expected_vol = portfolio_returns.rolling(window=20, min_periods=20).std() * np.sqrt(252)
        pd.testing.assert_series_equal(
            result["market_vol_20d"].dropna(),
            expected_vol.dropna(),
            check_names=False,
        )

    def test_market_vol_positive(self):
        df = _make_prices()
        result = compute_regime_features(df, window=20)
        non_nan = result["market_vol_20d"].dropna()
        assert (non_nan >= 0).all()

    def test_market_vol_zero_for_constant_prices(self):
        """Constant prices → zero returns → zero vol."""
        dates = pd.date_range("2023-01-01", periods=30)
        df = pd.DataFrame({"SPY": [100.0] * 30, "QQQ": [200.0] * 30}, index=dates)
        result = compute_regime_features(df, window=20)
        non_nan = result["market_vol_20d"].dropna()
        assert (non_nan.abs() < 1e-10).all()

    def test_market_trend_cumulative_return(self):
        df = _make_prices()
        result = compute_regime_features(df, window=20)
        log_returns = np.log(df / df.shift(1))
        portfolio_returns = log_returns.mean(axis=1)
        expected_trend = portfolio_returns.rolling(window=20, min_periods=20).sum()
        pd.testing.assert_series_equal(
            result["market_trend_20d"].dropna(),
            expected_trend.dropna(),
            check_names=False,
        )

    def test_dispersion_nonnegative(self):
        df = _make_prices()
        result = compute_regime_features(df, window=20)
        non_nan = result["dispersion_20d"].dropna()
        assert (non_nan >= 0).all()

    def test_single_symbol_corr_is_nan(self):
        dates = pd.date_range("2023-01-01", periods=30)
        df = pd.DataFrame({"SPY": np.cumprod(1 + np.random.normal(0, 0.01, 30)) * 100}, index=dates)
        result = compute_regime_features(df, window=20)
        # Single symbol → no pairs → NaN correlation
        assert result["corr_mean_20d"].isna().all()

    def test_corr_mean_bounded(self):
        """Pairwise correlation must be in [-1, 1]."""
        df = _make_prices()
        result = compute_regime_features(df, window=20)
        non_nan = result["corr_mean_20d"].dropna()
        assert (non_nan >= -1.0 - 1e-10).all()
        assert (non_nan <= 1.0 + 1e-10).all()

    def test_corr_mean_one_for_identical_symbols(self):
        """Perfectly correlated symbols → mean pairwise correlation = 1."""
        np.random.seed(99)
        dates = pd.date_range("2023-01-01", periods=40)
        prices = np.cumprod(1 + np.random.normal(0, 0.01, 40)) * 100
        df = pd.DataFrame({"SPY": prices, "QQQ": prices, "IWM": prices}, index=dates)
        result = compute_regime_features(df, window=20)
        non_nan = result["corr_mean_20d"].dropna()
        assert ((non_nan - 1.0).abs() < 1e-6).all()


class TestRegimeFeatureRegistration:
    def test_individual_features_registered(self):
        import research.features.regime_features_v1  # noqa: F401
        from research.features.feature_registry import registry

        for name in ["market_vol_20d", "market_trend_20d", "dispersion_20d", "corr_mean_20d"]:
            fn = registry.get(name)
            assert callable(fn)

    def test_registered_features_extract_column(self):
        from research.features.feature_registry import registry

        df = pd.DataFrame({
            "market_vol_20d": [0.15, 0.20],
            "market_trend_20d": [0.02, 0.03],
            "dispersion_20d": [0.005, 0.007],
            "corr_mean_20d": [0.8, 0.75],
        })

        for name in ["market_vol_20d", "market_trend_20d", "dispersion_20d", "corr_mean_20d"]:
            fn = registry.get(name)
            result = fn(df)
            pd.testing.assert_series_equal(result, df[name])
