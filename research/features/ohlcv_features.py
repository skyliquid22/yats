"""OHLCV per-symbol features — Appendix J.1.

All computed from canonical_equity_ohlcv close/high/low prices.
Each function operates on a per-symbol DataFrame sorted by timestamp.
"""

import numpy as np
import pandas as pd

from research.features.feature_registry import feature


@feature("ret_1d")
def compute_ret_1d(df: pd.DataFrame) -> pd.Series:
    """log(close_t / close_{t-1}), lookback 1d."""
    return np.log(df["close"] / df["close"].shift(1))


@feature("ret_5d")
def compute_ret_5d(df: pd.DataFrame) -> pd.Series:
    """log(close_t / close_{t-5}), lookback 5d."""
    return np.log(df["close"] / df["close"].shift(5))


@feature("ret_21d")
def compute_ret_21d(df: pd.DataFrame) -> pd.Series:
    """log(close_t / close_{t-21}), lookback 21d."""
    return np.log(df["close"] / df["close"].shift(21))


@feature("rv_21d")
def compute_rv_21d(df: pd.DataFrame) -> pd.Series:
    """std(ret_1d) * sqrt(252) over 21 days."""
    ret_1d = np.log(df["close"] / df["close"].shift(1))
    return ret_1d.rolling(window=21, min_periods=21).std() * np.sqrt(252)


@feature("rv_63d")
def compute_rv_63d(df: pd.DataFrame) -> pd.Series:
    """std(ret_1d) * sqrt(252) over 63 days."""
    ret_1d = np.log(df["close"] / df["close"].shift(1))
    return ret_1d.rolling(window=63, min_periods=63).std() * np.sqrt(252)


@feature("dist_20d_high")
def compute_dist_20d_high(df: pd.DataFrame) -> pd.Series:
    """(close - max(close, 20d)) / max(close, 20d)."""
    rolling_high = df["close"].rolling(window=20, min_periods=20).max()
    return (df["close"] - rolling_high) / rolling_high


@feature("dist_20d_low")
def compute_dist_20d_low(df: pd.DataFrame) -> pd.Series:
    """(close - min(close, 20d)) / min(close, 20d)."""
    rolling_low = df["close"].rolling(window=20, min_periods=20).min()
    return (df["close"] - rolling_low) / rolling_low
