"""Options-implied features v1 — per-underlying, per-day from canonical_options_chain.

Computes 5 features from a day's chain snapshot (no lookahead):
  atm_iv           — ATM IV at ~30d tenor (nearest strike to spot, nearest expiry ~30d)
  skew_25d         — IV(25-delta put) - IV(25-delta call) at ~30d tenor
  iv_term_slope    — ATM IV at ~60d minus ATM IV at ~30d (term-structure slope)
  put_call_oi_ratio — sum(put OI) / sum(call OI) across the near-tenor chain
  net_gamma_exposure — GEX: sum(gamma*OI, calls) - sum(gamma*OI, puts)
                       Positive = dealers net long gamma; negative = net short gamma.

Sign convention for net_gamma_exposure (GEX / dealer-positioning proxy):
  Calls contribute +gamma*OI (dealers sold calls → long gamma).
  Puts contribute  -gamma*OI (dealers sold puts → long gamma, but puts are always net
  negative because the standard practice is net call-put asymmetry).
  i.e. GEX = Σ(gamma_i * OI_i * sign_i) where sign=+1 for calls, -1 for puts.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from research.features.feature_registry import feature

logger = logging.getLogger(__name__)

# Tenor targets (calendar days)
_TARGET_NEAR_DTE = 30
_TARGET_FAR_DTE = 60
_MIN_DTE = 7          # skip expiries less than a week out (distorted by pin risk)
_TENOR_WINDOW = 15    # ±15d window around each target


def _select_expiry(chain: pd.DataFrame, quote_date: pd.Timestamp, target_dte: int) -> pd.DataFrame:
    """Return the subset of chain at the expiry closest to target_dte.

    Returns empty DataFrame if no expiry is within _TENOR_WINDOW days of target,
    or all candidates are within _MIN_DTE days.
    """
    if chain.empty:
        return chain

    expiries = chain["expiry"].unique()
    dtes = {exp: (pd.Timestamp(exp) - quote_date).days for exp in expiries}

    candidates = {
        exp: dte for exp, dte in dtes.items()
        if dte >= _MIN_DTE and abs(dte - target_dte) <= _TENOR_WINDOW
    }
    if not candidates:
        return pd.DataFrame(columns=chain.columns)

    best_exp = min(candidates, key=lambda e: abs(candidates[e] - target_dte))
    return chain[chain["expiry"] == best_exp].copy()


def _atm_iv_from_slice(slice_df: pd.DataFrame, spot: float) -> float:
    """Interpolate ATM IV from a single-expiry chain slice.

    Finds the two strikes bracketing spot and linearly interpolates IV.
    Falls back to nearest-strike IV when no bracketing pair exists.
    Returns NaN on empty input or missing IV.
    """
    if slice_df.empty or spot <= 0:
        return float("nan")

    # Use average of call and put IV at each strike where both exist
    # (or whichever side is available)
    per_strike = slice_df.groupby("strike")["iv"].mean().dropna()
    if per_strike.empty:
        return float("nan")

    strikes = per_strike.index.to_numpy()
    ivs = per_strike.to_numpy()

    below = strikes[strikes <= spot]
    above = strikes[strikes > spot]

    if len(below) == 0 or len(above) == 0:
        # Extrapolate — just use nearest strike
        nearest_idx = np.argmin(np.abs(strikes - spot))
        return float(ivs[nearest_idx])

    k_lo = below[-1]
    k_hi = above[0]
    iv_lo = ivs[strikes == k_lo][0]
    iv_hi = ivs[strikes == k_hi][0]

    # Linear interpolation by log-strike
    t = (np.log(spot / k_lo)) / (np.log(k_hi / k_lo))
    return float(iv_lo + t * (iv_hi - iv_lo))


def _interp_iv_at_delta(
    slice_df: pd.DataFrame,
    right: str,
    target_abs_delta: float,
) -> float:
    """Interpolate IV to a target |delta| for a given right ('C' or 'P').

    Delta for puts is negative; we work in |delta| throughout.
    Returns NaN if fewer than 2 valid contracts or target is out of range.
    """
    sub = slice_df[slice_df["right"] == right].copy()
    sub = sub.dropna(subset=["iv", "delta"])
    sub["abs_delta"] = sub["delta"].abs()

    # Remove degenerate cases
    sub = sub[(sub["abs_delta"] > 0) & (sub["abs_delta"] <= 1.0)]
    if sub.empty:
        return float("nan")

    # Sort by abs_delta descending (deep ITM → OTM)
    sub = sub.sort_values("abs_delta", ascending=False)

    # For calls, abs_delta ~ 0.5 at ATM, decreasing toward OTM
    # For puts, abs_delta ~ 0.5 at ATM, decreasing toward OTM
    # 25-delta contracts: abs_delta ≈ 0.25

    # Check if target is within range
    min_d, max_d = sub["abs_delta"].min(), sub["abs_delta"].max()
    if target_abs_delta < min_d or target_abs_delta > max_d:
        # Out of range — return nearest endpoint IV
        if target_abs_delta <= min_d:
            return float(sub[sub["abs_delta"] == min_d]["iv"].mean())
        return float(sub[sub["abs_delta"] == max_d]["iv"].mean())

    # Find bracketing contracts
    above = sub[sub["abs_delta"] >= target_abs_delta]
    below = sub[sub["abs_delta"] < target_abs_delta]

    if above.empty or below.empty:
        nearest_row = sub.iloc[(sub["abs_delta"] - target_abs_delta).abs().argsort()[:1]]
        return float(nearest_row["iv"].iloc[0])

    # Closest above and below
    row_hi = above.iloc[above["abs_delta"].sub(target_abs_delta).abs().argsort()[:1].values[0]]
    row_lo = below.iloc[below["abs_delta"].sub(target_abs_delta).abs().argsort()[:1].values[0]]

    d_hi = row_hi["abs_delta"]
    d_lo = row_lo["abs_delta"]
    iv_hi = row_hi["iv"]
    iv_lo = row_lo["iv"]

    if abs(d_hi - d_lo) < 1e-8:
        return float((iv_hi + iv_lo) / 2)

    t = (target_abs_delta - d_lo) / (d_hi - d_lo)
    return float(iv_lo + t * (iv_hi - iv_lo))


def compute_options_features(
    chain: pd.DataFrame,
    spot: float,
    quote_date: pd.Timestamp,
    underlying: str = "",
) -> dict[str, float]:
    """Compute all 5 options features for one (underlying, date) snapshot.

    Args:
        chain: Rows from canonical_options_chain for this underlying+date.
               Required columns: expiry, strike, right, iv, delta, gamma,
               open_interest. Missing columns produce NaN for dependent features.
        spot: Underlying close price for this date (from canonical_equity_ohlcv).
        quote_date: The date of this snapshot (UTC midnight).
        underlying: Ticker string for logging context (optional).

    Returns:
        Dict with keys: atm_iv, skew_25d, iv_term_slope, put_call_oi_ratio,
        net_gamma_exposure. Each value is float (NaN when unavailable).
    """
    label = underlying or "unknown"
    result: dict[str, float] = {
        "atm_iv": float("nan"),
        "skew_25d": float("nan"),
        "iv_term_slope": float("nan"),
        "put_call_oi_ratio": float("nan"),
        "net_gamma_exposure": float("nan"),
    }

    if chain.empty:
        logger.debug("options_features: empty chain for %s on %s", label, quote_date.date())
        return result

    near_chain = _select_expiry(chain, quote_date, _TARGET_NEAR_DTE)
    far_chain = _select_expiry(chain, quote_date, _TARGET_FAR_DTE)

    near_empty = near_chain.empty
    far_empty = far_chain.empty

    if near_empty:
        logger.debug(
            "options_features: no near-tenor (~%dd) contracts for %s on %s",
            _TARGET_NEAR_DTE, label, quote_date.date(),
        )

    # ------------------------------------------------------------------ #
    # 1. atm_iv — ATM IV at ~30d tenor
    # ------------------------------------------------------------------ #
    if not near_empty:
        result["atm_iv"] = _atm_iv_from_slice(near_chain, spot)

    # ------------------------------------------------------------------ #
    # 2. skew_25d — 25-delta risk reversal at ~30d tenor
    # ------------------------------------------------------------------ #
    if not near_empty:
        iv_put_25d = _interp_iv_at_delta(near_chain, "P", 0.25)
        iv_call_25d = _interp_iv_at_delta(near_chain, "C", 0.25)
        if not np.isnan(iv_put_25d) and not np.isnan(iv_call_25d):
            result["skew_25d"] = iv_put_25d - iv_call_25d
        else:
            missing = []
            if np.isnan(iv_put_25d):
                missing.append("put_25d")
            if np.isnan(iv_call_25d):
                missing.append("call_25d")
            logger.debug(
                "options_features: skew_25d NaN for %s on %s (missing: %s)",
                label, quote_date.date(), missing,
            )

    # ------------------------------------------------------------------ #
    # 3. iv_term_slope — ATM IV at ~60d minus ~30d
    # ------------------------------------------------------------------ #
    if not near_empty and not far_empty:
        iv_near = _atm_iv_from_slice(near_chain, spot)
        iv_far = _atm_iv_from_slice(far_chain, spot)
        if not np.isnan(iv_near) and not np.isnan(iv_far):
            result["iv_term_slope"] = iv_far - iv_near
        else:
            logger.debug(
                "options_features: iv_term_slope NaN for %s on %s (near=%.4f, far=%.4f)",
                label, quote_date.date(), iv_near, iv_far,
            )
    elif far_empty:
        logger.debug(
            "options_features: no far-tenor (~%dd) contracts for %s on %s",
            _TARGET_FAR_DTE, label, quote_date.date(),
        )

    # ------------------------------------------------------------------ #
    # 4. put_call_oi_ratio — sum(put OI) / sum(call OI) at near-tenor
    # ------------------------------------------------------------------ #
    if not near_empty and "open_interest" in near_chain.columns:
        near_valid = near_chain.dropna(subset=["open_interest"])
        put_oi = near_valid[near_valid["right"] == "P"]["open_interest"].sum()
        call_oi = near_valid[near_valid["right"] == "C"]["open_interest"].sum()
        if call_oi > 0:
            result["put_call_oi_ratio"] = float(put_oi) / float(call_oi)
        else:
            logger.debug(
                "options_features: put_call_oi_ratio NaN for %s on %s (call_oi=0)",
                label, quote_date.date(),
            )

    # ------------------------------------------------------------------ #
    # 5. net_gamma_exposure — GEX across full chain
    #    Convention: calls +, puts -
    #    Requires non-null gamma (ThetaData PRO second-order data)
    # ------------------------------------------------------------------ #
    if "gamma" in chain.columns and "open_interest" in chain.columns:
        gex_df = chain.dropna(subset=["gamma", "open_interest"]).copy()
        if not gex_df.empty:
            gex_df["sign"] = gex_df["right"].map({"C": 1.0, "P": -1.0}).fillna(0.0)
            gex = (gex_df["gamma"] * gex_df["open_interest"].astype(float) * gex_df["sign"]).sum()
            result["net_gamma_exposure"] = float(gex)
        else:
            logger.debug(
                "options_features: net_gamma_exposure NaN for %s on %s (no valid gamma rows)",
                label, quote_date.date(),
            )

    return result


# ---------------------------------------------------------------------------
# Feature registry stubs — required for registry validation when options_v1
# feature set is loaded. The pipeline calls compute_options_features() directly
# for efficiency (same as regime features pattern).
# ---------------------------------------------------------------------------

@feature("atm_iv")
def _atm_iv(df: pd.DataFrame) -> pd.Series:
    return df["atm_iv"]


@feature("skew_25d")
def _skew_25d(df: pd.DataFrame) -> pd.Series:
    return df["skew_25d"]


@feature("iv_term_slope")
def _iv_term_slope(df: pd.DataFrame) -> pd.Series:
    return df["iv_term_slope"]


@feature("put_call_oi_ratio")
def _put_call_oi_ratio(df: pd.DataFrame) -> pd.Series:
    return df["put_call_oi_ratio"]


@feature("net_gamma_exposure")
def _net_gamma_exposure(df: pd.DataFrame) -> pd.Series:
    return df["net_gamma_exposure"]
