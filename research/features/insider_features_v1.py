"""Insider trading features v1 — per-symbol, per-day from canonical_insider_trades.

Computes 4 features using trailing windows of insider *filing* dates (not
transaction_date — filing date is the point-in-time gate for no-lookahead):

  insider_net_buy_90d      — (buy_value - sell_value) / (buy_value + sell_value)
                             over trailing 90d of open-market filings
  insider_buy_intensity_30d — net buy value / market cap over 30d
  insider_cluster_30d      — distinct insiders with net buy > 0 over 30d
                             (Cohen-Malloy-Pomorski cluster signal)
  exec_net_buy_90d         — insider_net_buy_90d restricted to execs / directors

Open-market transaction codes (INCLUDE): P (purchase), S (sale).
All other codes are EXCLUDED: A (award), M (exercise), G (gift), F (tax
withholding), D (company disposition), J (other), I (discretionary),
K (equity swap), W (will/bequest), U (tender offer), Z (trust).

Live transaction_type values observed in canonical_insider_trades (2022-2026):
  P  — open-market purchase  ← INCLUDE
  S  — open-market sale      ← INCLUDE
  A  — grant/award           ← EXCLUDE (no economic signal)
  M  — exercise/conversion   ← EXCLUDE
  G  — gift                  ← EXCLUDE
  F  — tax withholding       ← EXCLUDE
  D  — disposition to issuer ← EXCLUDE

ETF note: SPY/QQQ have no insider trades (ETFs — FD API returns 400 for
insider-trades on them; ingest skips with a warning). All insider_* features
will be structurally NaN for ETFs. This is intentional — the training env
skips NaN per-symbol, consistent with early-window fundamentals behaviour.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from research.features.feature_registry import feature

logger = logging.getLogger(__name__)

# Open-market codes only — P = purchase, S = sale
OPEN_MARKET_CODES: frozenset[str] = frozenset({"P", "S"})

# Title substrings that qualify as exec/director (case-insensitive)
_EXEC_TITLES: tuple[str, ...] = (
    "ceo", "cfo", "coo", "president", "chair", "chief executive",
    "chief financial", "chief operating",
)

_90D = pd.Timedelta(days=90)
_30D = pd.Timedelta(days=30)


def _is_exec(row: pd.Series) -> bool:
    """Return True if the insider qualifies as executive/director."""
    if row.get("is_board_director"):
        return True
    title = str(row.get("insider_title") or "").lower()
    return any(kw in title for kw in _EXEC_TITLES)


def compute_insider_features(
    trades_df: pd.DataFrame,
    as_of_date: pd.Timestamp,
    shares_outstanding: float,
    close: float,
    symbol: str = "",
) -> dict[str, float]:
    """Compute all 4 insider features for one (symbol, date) observation.

    Args:
        trades_df: Rows from canonical_insider_trades for this symbol,
                   sorted by filing_date ascending. May be empty (ETF case).
                   Required columns: filing_date, insider_name, transaction_type,
                   total_value, is_board_director, insider_title.
        as_of_date: Observation date (UTC midnight). Only filings with
                    filing_date <= as_of_date are eligible (no lookahead).
        shares_outstanding: Float shares for this date; used for market cap.
        close: Close price for this date; used for market cap.
        symbol: Ticker for logging context.

    Returns:
        Dict with keys: insider_net_buy_90d, insider_buy_intensity_30d,
        insider_cluster_30d, exec_net_buy_90d.
        All NaN when trades_df is empty (e.g. ETFs).
    """
    result: dict[str, float] = {
        "insider_net_buy_90d": float("nan"),
        "insider_buy_intensity_30d": float("nan"),
        "insider_cluster_30d": float("nan"),
        "exec_net_buy_90d": float("nan"),
    }

    if trades_df.empty:
        return result

    # Gate: no lookahead — only filings on or before observation date
    eligible = trades_df[
        (trades_df["filing_date"] <= as_of_date)
        & (trades_df["transaction_type"].isin(OPEN_MARKET_CODES))
    ].copy()

    if eligible.empty:
        return result

    # ------------------------------------------------------------------ #
    # 90-day window — insider_net_buy_90d and exec_net_buy_90d
    # ------------------------------------------------------------------ #
    cutoff_90d = as_of_date - _90D
    w90 = eligible[eligible["filing_date"] >= cutoff_90d]

    if not w90.empty:
        buys_90 = w90.loc[w90["transaction_type"] == "P", "total_value"].sum()
        sells_90 = w90.loc[w90["transaction_type"] == "S", "total_value"].sum()
        denom_90 = buys_90 + sells_90
        if denom_90 > 0:
            result["insider_net_buy_90d"] = float((buys_90 - sells_90) / denom_90)

        # exec_net_buy_90d: restrict to board directors / executive titles
        exec_mask = w90.apply(_is_exec, axis=1)
        w90_exec = w90[exec_mask]
        if not w90_exec.empty:
            buys_e = w90_exec.loc[w90_exec["transaction_type"] == "P", "total_value"].sum()
            sells_e = w90_exec.loc[w90_exec["transaction_type"] == "S", "total_value"].sum()
            denom_e = buys_e + sells_e
            if denom_e > 0:
                result["exec_net_buy_90d"] = float((buys_e - sells_e) / denom_e)

    # ------------------------------------------------------------------ #
    # 30-day window — insider_buy_intensity_30d and insider_cluster_30d
    # ------------------------------------------------------------------ #
    cutoff_30d = as_of_date - _30D
    w30 = eligible[eligible["filing_date"] >= cutoff_30d]

    if not w30.empty:
        net_buy_30 = (
            w30.loc[w30["transaction_type"] == "P", "total_value"].sum()
            - w30.loc[w30["transaction_type"] == "S", "total_value"].sum()
        )

        # insider_buy_intensity_30d: net buy value / market cap
        if (
            not np.isnan(shares_outstanding)
            and not np.isnan(close)
            and shares_outstanding > 0
            and close > 0
        ):
            mkt_cap = shares_outstanding * close
            if mkt_cap > 0:
                result["insider_buy_intensity_30d"] = float(net_buy_30 / mkt_cap)

        # insider_cluster_30d: distinct insiders with net buy > 0
        per_insider = (
            w30.assign(
                signed_value=w30.apply(
                    lambda r: r["total_value"] if r["transaction_type"] == "P"
                    else -r["total_value"],
                    axis=1,
                )
            )
            .groupby("insider_name")["signed_value"]
            .sum()
        )
        n_net_buyers = int((per_insider > 0).sum())
        result["insider_cluster_30d"] = float(n_net_buyers)

    return result


# ---------------------------------------------------------------------------
# Feature registry stubs — required so the registry can validate feature sets
# that reference insider features. The pipeline calls compute_insider_features()
# directly per (symbol, date); these stubs are pass-throughs used only when
# the registry resolves feature names.
# ---------------------------------------------------------------------------

@feature("insider_net_buy_90d", lookback=90)
def _insider_net_buy_90d(df: pd.DataFrame) -> pd.Series:
    return df["insider_net_buy_90d"]


@feature("insider_buy_intensity_30d", lookback=30)
def _insider_buy_intensity_30d(df: pd.DataFrame) -> pd.Series:
    return df["insider_buy_intensity_30d"]


@feature("insider_cluster_30d", lookback=30)
def _insider_cluster_30d(df: pd.DataFrame) -> pd.Series:
    return df["insider_cluster_30d"]


@feature("exec_net_buy_90d", lookback=90)
def _exec_net_buy_90d(df: pd.DataFrame) -> pd.Series:
    return df["exec_net_buy_90d"]
