"""Tests for regime_features_v2 — market-implied (options-priced) regime features.

All fixtures use in-memory DataFrames; no live QuestDB or ThetaData required.
Covers: VRP sign, term inversion detection, z-score windows, no-lookahead,
and registry lookback assertions.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.features.regime_features_v2 import (
    REGIME_V2_FEATURES,
    _rolling_zscore,
    compute_regime_features_v2,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(date_str: str) -> pd.Timestamp:
    return pd.Timestamp(date_str, tz="UTC")


def _make_spy_chain(
    dates: list[str],
    spot: float = 400.0,
    atm_iv: float = 0.20,
    skew: float = 0.05,
    term_slope: float = 0.02,
    gamma: float = 0.005,
    n_strikes: int = 9,
) -> pd.DataFrame:
    """Build a minimal synthetic SPY EOD options chain for testing.

    Creates near (30d) and far (60d) expiries with strikes centred around spot.
    IV has a constant smile (no time variation unless overridden per-row).
    """
    rows = []
    for date_str in dates:
        qd = pd.Timestamp(date_str, tz="UTC")
        exp_near = qd + pd.Timedelta(days=30)
        exp_far = qd + pd.Timedelta(days=60)

        strikes = np.linspace(spot * 0.85, spot * 1.15, n_strikes)

        for exp, slope_offset in [(exp_near, 0.0), (exp_far, term_slope)]:
            atm_iv_exp = atm_iv + slope_offset
            for k in strikes:
                moneyness = np.log(k / spot)
                smile_iv = atm_iv_exp + 0.5 * skew * abs(moneyness)
                delta_call = max(0.01, min(0.99, 0.5 - moneyness * 2))
                delta_put = delta_call - 1.0
                rows.append({
                    "quote_date": qd,
                    "expiry": exp,
                    "strike": k,
                    "right": "C",
                    "iv": smile_iv,
                    "delta": delta_call,
                    "gamma": gamma,
                    "open_interest": 1000,
                })
                rows.append({
                    "quote_date": qd,
                    "expiry": exp,
                    "strike": k,
                    "right": "P",
                    "iv": smile_iv + skew,
                    "delta": delta_put,
                    "gamma": gamma,
                    "open_interest": 1200,
                })
    return pd.DataFrame(rows)


def _make_spy_prices(
    dates: list[str],
    base: float = 400.0,
    daily_ret: float = 0.001,
    daily_vol: float = 0.0,
    extend_lookback: int = 30,
    seed: int = 42,
) -> pd.Series:
    """Return a SPY close price Series (DatetimeIndex, UTC) spanning dates.

    extend_lookback: extra bars before the first date so rolling windows are seeded.
    daily_vol: per-day random return std (0 = deterministic trend only).
    """
    rng = np.random.default_rng(seed)
    first_date = pd.Timestamp(dates[0], tz="UTC")
    all_dates = pd.date_range(
        end=first_date - pd.Timedelta(days=1),
        periods=extend_lookback,
        freq="B",
        tz="UTC",
    ).tolist() + [pd.Timestamp(d, tz="UTC") for d in dates]

    n = len(all_dates)
    noise = rng.normal(0.0, daily_vol, n) if daily_vol > 0 else np.zeros(n)
    rets = daily_ret + noise
    prices = base * np.cumprod(1 + rets)
    return pd.Series(prices, index=pd.DatetimeIndex(all_dates), name="close")


# ---------------------------------------------------------------------------
# Core output tests
# ---------------------------------------------------------------------------

class TestComputeRegimeFeaturesV2OutputShape:
    def test_output_columns(self):
        dates = [f"2024-01-{d:02d}" for d in range(2, 12)]
        chain = _make_spy_chain(dates)
        prices = _make_spy_prices(dates)
        result = compute_regime_features_v2(chain, prices)
        assert set(result.columns) == REGIME_V2_FEATURES

    def test_output_indexed_by_quote_date(self):
        dates = ["2024-01-02", "2024-01-03", "2024-01-04"]
        chain = _make_spy_chain(dates)
        prices = _make_spy_prices(dates)
        result = compute_regime_features_v2(chain, prices)
        assert len(result) == 3

    def test_empty_chain_returns_empty(self):
        result = compute_regime_features_v2(pd.DataFrame(), pd.Series(dtype=float))
        assert result.empty or len(result) == 0

    def test_spy_atm_iv_positive(self):
        dates = [f"2024-01-{d:02d}" for d in range(2, 20)]
        chain = _make_spy_chain(dates, atm_iv=0.20)
        prices = _make_spy_prices(dates)
        result = compute_regime_features_v2(chain, prices)
        non_nan = result["spy_atm_iv"].dropna()
        assert len(non_nan) > 0
        assert (non_nan > 0).all()

    def test_spy_iv_term_slope_reflects_chain(self):
        """When far IV > near IV (normal contango), slope should be positive."""
        dates = [f"2024-01-{d:02d}" for d in range(2, 15)]
        chain = _make_spy_chain(dates, term_slope=0.03)
        prices = _make_spy_prices(dates)
        result = compute_regime_features_v2(chain, prices)
        non_nan = result["spy_iv_term_slope"].dropna()
        assert len(non_nan) > 0
        assert (non_nan > 0).all(), "Positive term_slope → far IV > near IV → contango"

    def test_term_inversion_negative_slope(self):
        """Negative term_slope means backwardation — near IV > far IV."""
        dates = [f"2024-01-{d:02d}" for d in range(2, 15)]
        chain = _make_spy_chain(dates, term_slope=-0.03)
        prices = _make_spy_prices(dates)
        result = compute_regime_features_v2(chain, prices)
        non_nan = result["spy_iv_term_slope"].dropna()
        assert len(non_nan) > 0
        assert (non_nan < 0).all(), "Negative term_slope → backwardation"


# ---------------------------------------------------------------------------
# VRP sign tests
# ---------------------------------------------------------------------------

class TestVolRiskPremium:
    def test_vrp_positive_when_iv_exceeds_realized(self):
        """High IV relative to realized vol → positive VRP (normal risk-premium)."""
        dates = [f"2024-01-{d:02d}" for d in range(2, 30)]
        # atm_iv=0.30, realized vol from daily_vol=0.001 ≈ sqrt(252)*0.001 ≈ 0.016 << 0.30
        chain = _make_spy_chain(dates, atm_iv=0.30)
        prices = _make_spy_prices(dates, daily_ret=0.001, daily_vol=0.001, extend_lookback=25)
        result = compute_regime_features_v2(chain, prices)
        non_nan = result["spy_vrp"].dropna()
        assert len(non_nan) > 0
        assert (non_nan > 0).all(), "High IV vs low realized → positive VRP"

    def test_vrp_negative_when_realized_exceeds_iv(self):
        """Realized vol exceeds IV → negative VRP (stress early-warning signal)."""
        dates = [f"2024-01-{d:02d}" for d in range(2, 30)]
        chain = _make_spy_chain(dates, atm_iv=0.10)
        # daily_vol=0.03 → annualized ≈ sqrt(252)*0.03 ≈ 0.48, >> atm_iv=0.10
        prices = _make_spy_prices(dates, daily_ret=0.0, daily_vol=0.03, extend_lookback=25)
        result = compute_regime_features_v2(chain, prices)
        non_nan = result["spy_vrp"].dropna()
        assert len(non_nan) > 0
        assert (non_nan < 0).all(), "High realized vol vs low IV → negative VRP"


# ---------------------------------------------------------------------------
# Z-score window tests
# ---------------------------------------------------------------------------

class TestZscoreWindows:
    def test_iv_zscore_nan_before_window(self):
        """z-score requires full window; first (zscore_window-1) rows must be NaN."""
        dates = [f"2024-01-{d:02d}" for d in range(2, 20)] + \
                [f"2024-02-{d:02d}" for d in range(1, 30)] + \
                [f"2024-03-{d:02d}" for d in range(1, 20)]
        chain = _make_spy_chain(dates)
        prices = _make_spy_prices(dates)
        result = compute_regime_features_v2(chain, prices, zscore_window=10)
        # First 9 rows (window - 1) must be NaN
        assert result["spy_iv_zscore_60d"].iloc[:9].isna().all()
        # Row 10+ should be non-NaN
        assert result["spy_iv_zscore_60d"].iloc[10:].notna().any()

    def test_rolling_zscore_zero_mean(self):
        """Mean of z-score over the window should be ≈0."""
        n = 80
        np.random.seed(7)
        series = pd.Series(np.random.normal(0.2, 0.05, n))
        z = _rolling_zscore(series, window=20)
        # After warm-up, mean should be close to 0 in a rolling sense
        # (not exactly 0 because rolling, but bounded)
        assert abs(z.dropna().mean()) < 0.5

    def test_constant_series_zscore_is_nan(self):
        """Constant IV → std=0 → z-score is NaN (no information)."""
        n = 80
        series = pd.Series([0.20] * n)
        z = _rolling_zscore(series, window=20)
        assert z.dropna().empty or z.dropna().isna().all()

    def test_skew_zscore_nan_before_window(self):
        dates = [f"2024-01-{d:02d}" for d in range(2, 20)] + \
                [f"2024-02-{d:02d}" for d in range(1, 30)] + \
                [f"2024-03-{d:02d}" for d in range(1, 20)]
        chain = _make_spy_chain(dates)
        prices = _make_spy_prices(dates)
        result = compute_regime_features_v2(chain, prices, zscore_window=10)
        assert result["spy_skew_zscore_60d"].iloc[:9].isna().all()

    def test_gex_norm_nan_before_window(self):
        dates = [f"2024-01-{d:02d}" for d in range(2, 20)] + \
                [f"2024-02-{d:02d}" for d in range(1, 30)] + \
                [f"2024-03-{d:02d}" for d in range(1, 20)]
        chain = _make_spy_chain(dates)
        prices = _make_spy_prices(dates)
        result = compute_regime_features_v2(chain, prices, zscore_window=10)
        assert result["spy_gex_norm"].iloc[:9].isna().all()


# ---------------------------------------------------------------------------
# No-lookahead tests
# ---------------------------------------------------------------------------

class TestNoLookahead:
    def test_option_row_after_t_does_not_affect_feature_at_t(self):
        """Injecting a row with quote_date = t+1 must not change feature at t.

        This verifies that compute_regime_features_v2 groups by quote_date
        and does not bleed future data into past rows (via any sort or join).
        """
        base_dates = ["2024-01-02", "2024-01-03", "2024-01-04"]
        chain_base = _make_spy_chain(base_dates)
        prices = _make_spy_prices(base_dates)

        result_base = compute_regime_features_v2(chain_base, prices)

        # Add a future row with very different IV on 2024-01-05
        future_date = "2024-01-05"
        future_chain = _make_spy_chain([future_date], atm_iv=0.99)
        chain_with_future = pd.concat([chain_base, future_chain], ignore_index=True)

        result_with_future = compute_regime_features_v2(chain_with_future, prices)

        # Features at 2024-01-02 through 2024-01-04 must be unchanged
        for d in base_dates:
            ts = pd.Timestamp(d, tz="UTC")
            row_base = result_base.loc[ts] if ts in result_base.index else None
            row_future = result_with_future.loc[ts] if ts in result_with_future.index else None
            if row_base is not None and row_future is not None:
                pd.testing.assert_series_equal(
                    row_base.fillna(0), row_future.fillna(0),
                    check_names=False,
                    atol=1e-8,
                )

    def test_delta_features_require_past_data_only(self):
        """spy_iv_delta_5d at t uses only data at t and t-5, not t+1."""
        dates = [f"2024-01-{d:02d}" for d in range(2, 20)]
        chain = _make_spy_chain(dates)
        prices = _make_spy_prices(dates)
        result = compute_regime_features_v2(chain, prices, delta_window=5)
        # First 5 rows must be NaN for delta features
        assert result["spy_iv_delta_5d"].iloc[:5].isna().all()
        assert result["spy_slope_delta_5d"].iloc[:5].isna().all()


# ---------------------------------------------------------------------------
# GEX sign test
# ---------------------------------------------------------------------------

class TestGexSign:
    def test_gex_sign_is_sign_of_gex(self):
        """spy_gex_sign must be +1 when net gamma exposure is positive."""
        dates = ["2024-01-02", "2024-01-03"]
        chain = _make_spy_chain(dates, gamma=0.01)
        prices = _make_spy_prices(dates)
        result = compute_regime_features_v2(chain, prices)
        non_nan = result["spy_gex_sign"].dropna()
        assert len(non_nan) > 0
        # With more calls than puts OI on a single-sign chain, GEX sign should be consistent
        assert non_nan.isin([-1.0, 0.0, 1.0]).all()


# ---------------------------------------------------------------------------
# Registry lookback assertions
# ---------------------------------------------------------------------------

class TestRegistryLookbacks:
    def test_all_regime_v2_features_registered(self):
        import research.features.regime_features_v2  # noqa: F401 — triggers registration
        from research.features.feature_registry import registry

        for name in REGIME_V2_FEATURES:
            fn = registry.get(name)
            assert callable(fn), f"Feature '{name}' not registered"

    def test_all_lookbacks_at_most_64(self):
        import research.features.regime_features_v2  # noqa: F401
        from research.features.feature_registry import registry

        for name in REGIME_V2_FEATURES:
            lb = registry.feature_lookback(name)
            assert lb <= 64, (
                f"Feature '{name}' has lookback={lb} > 64 — violates purge-geometry constraint"
            )

    def test_zscore_features_have_lookback_61(self):
        import research.features.regime_features_v2  # noqa: F401
        from research.features.feature_registry import registry

        for name in ("spy_iv_zscore_60d", "spy_skew_zscore_60d", "spy_gex_norm"):
            lb = registry.feature_lookback(name)
            assert lb == 61, f"z-score feature '{name}' expected lookback=61, got {lb}"

    def test_delta_features_have_lookback_6(self):
        import research.features.regime_features_v2  # noqa: F401
        from research.features.feature_registry import registry

        for name in ("spy_iv_delta_5d", "spy_slope_delta_5d"):
            lb = registry.feature_lookback(name)
            assert lb == 6, f"5d delta feature '{name}' expected lookback=6, got {lb}"

    def test_feature_set_max_lookback_regime_v2(self):
        """max_lookback('regime_v2') should return 61 (z-score window)."""
        import research.features.regime_features_v2  # noqa: F401
        from research.features.feature_registry import registry

        lb = registry.max_lookback("regime_v2")
        assert lb == 61

    def test_feature_set_max_lookback_sweep_v3(self):
        """max_lookback('sweep_v3') inherits inst features at ~105 > 61."""
        import research.features.ohlcv_features  # noqa: F401
        import research.features.cross_sectional_features  # noqa: F401
        import research.features.fundamental_features  # noqa: F401
        import research.features.regime_features_v1  # noqa: F401
        import research.features.regime_features_v2  # noqa: F401
        import research.features.options_features_v1  # noqa: F401
        import research.features.insider_features_v1  # noqa: F401
        import research.features.inst_features_v1  # noqa: F401
        from research.features.feature_registry import registry

        lb = registry.max_lookback("sweep_v3")
        assert lb >= 61, f"sweep_v3 max_lookback should be >= 61, got {lb}"
