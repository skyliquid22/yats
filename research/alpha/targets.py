"""Residualized forward return targets for supervised alpha models.

Computes h-day forward returns and residualizes against SPY beta (rolling OLS)
to produce market-neutral prediction targets.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def compute_forward_returns(
    df: pd.DataFrame,
    horizon: int,
    *,
    close_col: str = "close",
    date_col: str = "date",
    symbol_col: str = "symbol",
) -> pd.Series:
    """Compute h-day forward returns for each (date, symbol).

    forward_return(t) = close(t+h) / close(t) - 1

    Returns NaN for the last h rows per symbol (no future available).
    """
    df_sorted = df.sort_values([symbol_col, date_col])
    fwd = df_sorted.groupby(symbol_col, sort=False)[close_col].transform(
        lambda s: s.shift(-horizon) / s - 1.0
    )
    fwd.index = df_sorted.index
    return fwd.reindex(df.index)


def compute_rolling_beta(
    daily_ret: pd.Series,
    spy_daily_ret: pd.Series,
    window: int = 60,
) -> pd.Series:
    """Estimate rolling OLS beta of daily_ret vs spy_daily_ret.

    beta(t) = rolling_cov(r_i, r_SPY, window) / rolling_var(r_SPY, window)

    Aligns on index. Returns NaN for the first (window-1) observations.
    """
    aligned_spy = spy_daily_ret.reindex(daily_ret.index)
    roll_cov = daily_ret.rolling(window, min_periods=window // 2).cov(aligned_spy)
    roll_var = aligned_spy.rolling(window, min_periods=window // 2).var()
    beta = roll_cov / roll_var.replace(0, np.nan)
    return beta.clip(-5.0, 5.0)  # cap extreme betas


def residualize_vs_spy(
    df: pd.DataFrame,
    fwd_col: str,
    spy_fwd: pd.Series,
    *,
    date_col: str = "date",
    symbol_col: str = "symbol",
    close_col: str = "close",
    spy_symbol: str = "SPY",
    beta_window: int = 60,
) -> pd.Series:
    """Residualize forward returns by removing rolling SPY beta exposure.

    For each (date, symbol):
        resid_i(t) = fwd_i(t) - beta_i(t) * fwd_SPY(t)

    where:
        - fwd_i(t) is the h-day forward return for stock i at date t
        - beta_i(t) is the rolling OLS beta of stock i's daily returns vs SPY
        - fwd_SPY(t) is SPY's h-day forward return at date t

    SPY itself is left as-is (residual = 0 by convention for the market proxy).

    Args:
        df: Panel DataFrame with date_col, symbol_col, close_col, fwd_col.
        fwd_col: Column name of h-day forward returns in df.
        spy_fwd: SPY forward returns indexed by date (from compute_forward_returns).
        date_col: Column name for dates.
        symbol_col: Column name for symbols.
        close_col: Column name for close price.
        spy_symbol: Symbol name for SPY in the panel.
        beta_window: Rolling window in trading days for beta estimation.

    Returns:
        Series (same index as df) with residualized forward returns.
    """
    # Compute daily returns per symbol
    df_sorted = df.sort_values([symbol_col, date_col])
    daily_ret = df_sorted.groupby(symbol_col, sort=False)[close_col].transform(
        lambda s: s.pct_change()
    )
    daily_ret.index = df_sorted.index

    # SPY daily returns as a date-indexed series
    spy_mask = df[symbol_col] == spy_symbol
    spy_daily = (
        daily_ret[spy_mask]
        .reset_index(drop=True)
    )
    spy_dates_series = df.loc[spy_mask, date_col].values
    spy_daily_indexed = pd.Series(daily_ret[spy_mask].values, index=spy_dates_series)
    spy_daily_indexed = spy_daily_indexed.sort_index()

    # Forward returns for each symbol (from df)
    fwd_series = df[fwd_col].copy()

    # Compute per-symbol rolling beta and residualize
    residuals = fwd_series.copy()
    for sym, grp in df.groupby(symbol_col, sort=False):
        if sym == spy_symbol:
            continue  # SPY: residual = fwd (no adjustment needed)
        grp_sorted = grp.sort_values(date_col)
        sym_daily = daily_ret.reindex(grp_sorted.index)
        sym_daily_indexed = pd.Series(sym_daily.values, index=grp_sorted[date_col].values)
        sym_daily_indexed = sym_daily_indexed.sort_index()

        beta = compute_rolling_beta(sym_daily_indexed, spy_daily_indexed, window=beta_window)
        beta_aligned = beta.reindex(grp_sorted[date_col].values)

        # SPY forward return aligned to same dates
        spy_fwd_aligned = spy_fwd.reindex(grp_sorted[date_col].values)

        # Residual: r_i - beta_i * r_SPY
        resid_values = grp_sorted[fwd_col].values - (
            beta_aligned.fillna(0.0).values * spy_fwd_aligned.fillna(0.0).values
        )
        residuals.loc[grp_sorted.index] = resid_values

    return residuals
