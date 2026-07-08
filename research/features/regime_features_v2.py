"""Regime features v2 — market-implied (options-priced, forward-looking).

Motivation: realized-vol-based regime features (v1) detect regime breaks
~2–3 weeks late. Options prices are forward-looking.

Features (market-wide scalars; broadcast to all symbols — same value on a given date):

  spy_atm_iv        — SPY ATM IV at ~30d tenor (VIX proxy, raw level)
  spy_iv_zscore_60d — 60d z-score of SPY ATM IV (normalized stress level)
  spy_vrp           — SPY VRP = atm_iv - realized_vol_20d
                      (negative/compressing = early stress warning)
  spy_iv_term_slope — SPY iv_term_slope at ~60d−30d
                      (negative = backwardation = acute stress)
  spy_skew_zscore_60d — 60d z-score of SPY 25d risk reversal (crash pricing)
  spy_gex_sign      — sign of SPY net_gamma_exposure (–1/0/+1)
                      (negative-GEX regimes amplify moves)
  spy_gex_norm      — 60d z-score of SPY net_gamma_exposure (normalized magnitude)
  spy_iv_delta_5d   — 5d change in spy_atm_iv (regime transition direction)
  spy_slope_delta_5d— 5d change in spy_iv_term_slope (term-structure regime shift)

Design constraints (non-negotiable per bead ya-3rkix):
  - All lookbacks <= 64 bars; max here is 61 (60d z-score window).
  - Continuous features in obs — no hard bucketing in training.
  - as-of discipline: caller must pass only EOD rows (source_vendor='thetadata_eod').
  - Reuses compute_options_features() from options_features_v1 (no logic duplication).
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from research.features.feature_registry import feature
from research.features.options_features_v1 import compute_options_features

logger = logging.getLogger(__name__)

# Exported set so the pipeline can detect when v2 loading is required.
REGIME_V2_FEATURES: frozenset[str] = frozenset({
    "spy_atm_iv",
    "spy_iv_zscore_60d",
    "spy_vrp",
    "spy_iv_term_slope",
    "spy_skew_zscore_60d",
    "spy_gex_sign",
    "spy_gex_norm",
    "spy_iv_delta_5d",
    "spy_slope_delta_5d",
})

_ZSCORE_WINDOW = 60   # bars; <= 64 constraint
_DELTA_WINDOW = 5     # bars for transition features
_RV_WINDOW = 20       # bars for realized vol (VRP)


def _rolling_zscore(series: pd.Series, window: int) -> pd.Series:
    """Rolling z-score with min_periods=window (no partial windows)."""
    mu = series.rolling(window=window, min_periods=window).mean()
    sigma = series.rolling(window=window, min_periods=window).std()
    return (series - mu) / sigma.replace(0, np.nan)


def compute_regime_features_v2(
    spy_chain: pd.DataFrame,
    spy_prices: pd.Series,
    zscore_window: int = _ZSCORE_WINDOW,
    delta_window: int = _DELTA_WINDOW,
    rv_window: int = _RV_WINDOW,
) -> pd.DataFrame:
    """Compute all regime_v2 features from SPY options chain + close prices.

    Args:
        spy_chain: Rows from canonical_options_chain for SPY (already filtered
                   to source_vendor='thetadata_eod'). Required columns: quote_date,
                   expiry, strike, right, iv, delta, gamma, open_interest.
        spy_prices: SPY close price Series with DatetimeIndex (UTC timestamps).
                    Must span at least rv_window bars before the first options date
                    for VRP to be non-NaN on that date.
        zscore_window: Rolling z-score lookback in bars (default 60; must be <= 64).
        delta_window: Window for 5d transition features.
        rv_window: Window for realized vol used in VRP.

    Returns:
        DataFrame indexed by date (UTC timestamps from spy_chain quote_dates)
        with columns: spy_atm_iv, spy_iv_zscore_60d, spy_vrp, spy_iv_term_slope,
        spy_skew_zscore_60d, spy_gex_sign, spy_gex_norm, spy_iv_delta_5d,
        spy_slope_delta_5d. Index aligns with spy_chain's unique quote_dates.
    """
    if spy_chain.empty:
        logger.warning("regime_v2: empty spy_chain — all features will be NaN")
        return pd.DataFrame(columns=[
            "spy_atm_iv", "spy_iv_zscore_60d", "spy_vrp",
            "spy_iv_term_slope", "spy_skew_zscore_60d",
            "spy_gex_sign", "spy_gex_norm",
            "spy_iv_delta_5d", "spy_slope_delta_5d",
        ])

    # ------------------------------------------------------------------ #
    # Step 1: build per-date spot map from SPY close prices               #
    # ------------------------------------------------------------------ #
    spot_by_date: dict[pd.Timestamp, float] = {}
    if not spy_prices.empty:
        for ts, price in spy_prices.items():
            spot_by_date[pd.Timestamp(ts).normalize().tz_localize("UTC") if pd.Timestamp(ts).tz is None
                         else pd.Timestamp(ts).normalize()] = float(price)

    # ------------------------------------------------------------------ #
    # Step 2: compute raw options features per date using options_v1      #
    # ------------------------------------------------------------------ #
    raw_rows: list[dict] = []
    for quote_date, day_chain in spy_chain.groupby("quote_date"):
        qd = pd.Timestamp(quote_date)
        if qd.tz is None:
            qd = qd.tz_localize("UTC")
        spot = spot_by_date.get(qd.normalize(), float("nan"))

        feats = compute_options_features(
            day_chain, float(spot), qd, underlying="SPY"
        )
        raw_rows.append({
            "quote_date": qd,
            "atm_iv": feats["atm_iv"],
            "skew_25d": feats["skew_25d"],
            "iv_term_slope": feats["iv_term_slope"],
            "net_gamma_exposure": feats["net_gamma_exposure"],
        })

    if not raw_rows:
        logger.warning("regime_v2: no options rows produced — all features will be NaN")
        return pd.DataFrame()

    raw = (
        pd.DataFrame(raw_rows)
        .sort_values("quote_date")
        .set_index("quote_date")
    )

    # ------------------------------------------------------------------ #
    # Step 3: compute realized vol from SPY prices (for VRP)              #
    # ------------------------------------------------------------------ #
    rv_20d = pd.Series(dtype=float, name="rv_20d")
    if not spy_prices.empty:
        px = spy_prices.sort_index()
        log_ret = np.log(px / px.shift(1))
        rv_20d = log_ret.rolling(window=rv_window, min_periods=rv_window).std() * np.sqrt(252)
        # Align to raw index
        rv_20d.index = pd.DatetimeIndex(rv_20d.index).normalize()
        if rv_20d.index.tz is None:
            rv_20d.index = rv_20d.index.tz_localize("UTC")
        else:
            rv_20d.index = rv_20d.index.tz_convert("UTC")
        rv_20d = rv_20d.reindex(raw.index.normalize())
        rv_20d.index = raw.index

    # ------------------------------------------------------------------ #
    # Step 4: derive all regime_v2 features                               #
    # ------------------------------------------------------------------ #
    out = pd.DataFrame(index=raw.index)

    # iv_level group
    out["spy_atm_iv"] = raw["atm_iv"]
    out["spy_iv_zscore_60d"] = _rolling_zscore(raw["atm_iv"], zscore_window)

    # vol_risk_premium
    if not rv_20d.empty:
        out["spy_vrp"] = raw["atm_iv"] - rv_20d.values
    else:
        out["spy_vrp"] = np.nan

    # term_inversion
    out["spy_iv_term_slope"] = raw["iv_term_slope"]

    # crash_pricing
    out["spy_skew_zscore_60d"] = _rolling_zscore(raw["skew_25d"], zscore_window)

    # market_gex
    out["spy_gex_sign"] = np.sign(raw["net_gamma_exposure"])
    out["spy_gex_norm"] = _rolling_zscore(raw["net_gamma_exposure"], zscore_window)

    # transition features
    out["spy_iv_delta_5d"] = raw["atm_iv"] - raw["atm_iv"].shift(delta_window)
    out["spy_slope_delta_5d"] = raw["iv_term_slope"] - raw["iv_term_slope"].shift(delta_window)

    return out


# ---------------------------------------------------------------------------
# Feature registry stubs — @feature registers lookbacks at module import.
# The pipeline calls compute_regime_features_v2() directly for efficiency.
# ---------------------------------------------------------------------------

@feature("spy_atm_iv", lookback=1)
def _spy_atm_iv(df: pd.DataFrame) -> pd.Series:
    return df["spy_atm_iv"]


@feature("spy_iv_zscore_60d", lookback=61)
def _spy_iv_zscore_60d(df: pd.DataFrame) -> pd.Series:
    return df["spy_iv_zscore_60d"]


@feature("spy_vrp", lookback=21)
def _spy_vrp(df: pd.DataFrame) -> pd.Series:
    return df["spy_vrp"]


@feature("spy_iv_term_slope", lookback=1)
def _spy_iv_term_slope(df: pd.DataFrame) -> pd.Series:
    return df["spy_iv_term_slope"]


@feature("spy_skew_zscore_60d", lookback=61)
def _spy_skew_zscore_60d(df: pd.DataFrame) -> pd.Series:
    return df["spy_skew_zscore_60d"]


@feature("spy_gex_sign", lookback=1)
def _spy_gex_sign(df: pd.DataFrame) -> pd.Series:
    return df["spy_gex_sign"]


@feature("spy_gex_norm", lookback=61)
def _spy_gex_norm(df: pd.DataFrame) -> pd.Series:
    return df["spy_gex_norm"]


@feature("spy_iv_delta_5d", lookback=6)
def _spy_iv_delta_5d(df: pd.DataFrame) -> pd.Series:
    return df["spy_iv_delta_5d"]


@feature("spy_slope_delta_5d", lookback=6)
def _spy_slope_delta_5d(df: pd.DataFrame) -> pd.Series:
    return df["spy_slope_delta_5d"]
