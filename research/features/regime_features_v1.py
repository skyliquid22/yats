"""Regime features v1 — Appendix I.1.

Input: aligned close prices for regime universe symbols (default: SPY, QQQ, IWM).
Regime features are market-level — same value for all symbols on a given date.
"""

import numpy as np
import pandas as pd

from research.features.feature_registry import feature

DEFAULT_REGIME_UNIVERSE = ["SPY", "QQQ", "IWM"]


def compute_regime_features(
    regime_prices: pd.DataFrame,
    window: int = 20,
) -> pd.DataFrame:
    """Compute all regime features from regime universe close prices.

    Args:
        regime_prices: DataFrame with columns = regime symbols, index = dates,
                       values = close prices.
        window: Rolling window size (default 20 days).

    Returns:
        DataFrame indexed by date with regime feature columns.
    """
    # Log returns per symbol
    log_returns = np.log(regime_prices / regime_prices.shift(1))

    # Equal-weight portfolio returns
    portfolio_returns = log_returns.mean(axis=1)

    # market_vol_20d: annualized std of portfolio returns
    market_vol_20d = portfolio_returns.rolling(
        window=window, min_periods=window
    ).std() * np.sqrt(252)

    # market_trend_20d: cumulative log return of portfolio
    market_trend_20d = portfolio_returns.rolling(
        window=window, min_periods=window
    ).sum()

    # dispersion_20d: std of individual symbol returns per date, 20d rolling mean
    daily_dispersion = log_returns.std(axis=1)
    dispersion_20d = daily_dispersion.rolling(
        window=window, min_periods=window
    ).mean()

    # corr_mean_20d: mean pairwise correlation of symbol returns, 20d
    n_symbols = log_returns.shape[1]
    if n_symbols < 2:
        corr_mean_20d = pd.Series(np.nan, index=regime_prices.index)
    else:
        corr_mean_20d = log_returns.rolling(
            window=window, min_periods=window
        ).corr().groupby(level=0).apply(
            lambda c: c.values[np.triu_indices(n_symbols, k=1)].mean()
            if len(c) == n_symbols else np.nan
        )

    return pd.DataFrame({
        "market_vol_20d": market_vol_20d,
        "market_trend_20d": market_trend_20d,
        "dispersion_20d": dispersion_20d,
        "corr_mean_20d": corr_mean_20d,
    })


# Register individual regime features. The pipeline calls
# compute_regime_features() directly for efficiency, but individual
# registrations are needed for the registry validation.

@feature("market_vol_20d")
def _market_vol_20d(df: pd.DataFrame) -> pd.Series:
    return df["market_vol_20d"]


@feature("market_trend_20d")
def _market_trend_20d(df: pd.DataFrame) -> pd.Series:
    return df["market_trend_20d"]


@feature("dispersion_20d")
def _dispersion_20d(df: pd.DataFrame) -> pd.Series:
    return df["dispersion_20d"]


@feature("corr_mean_20d")
def _corr_mean_20d(df: pd.DataFrame) -> pd.Series:
    return df["corr_mean_20d"]
