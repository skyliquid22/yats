"""Institutional ownership features v1 — per-symbol from canonical_inst_ownership
and canonical_institutional_holdings.

Computes 2 features using as-of filing dates (no lookahead):

  inst_ownership_pct   — canonical_inst_ownership.total_shares / shares_outstanding
                         as-of the latest filing with filing_date <= observation date.
                         Lookback ~105 bars (one quarterly report + ~45d filing lag).
                         Uses total_shares (NOT value_usd — FD value_usd unreliable
                         before 2025-06; total_shares is consistent across splits).

  inst_top10_share     — top-10 filers' value / total filer value from the same quarter.
                         Replaces inst_holder_breadth which saturates at 200 rows
                         (the FD institutional-holdings endpoint hard-caps at 200 rows
                         and ignores offset — breadth is useless as a raw count).
                         Within-quarter value ratio: uniform scaling from the vendor
                         backfill artifact cancels out (all rows from the same filing
                         date use the same implied price). Lookback ~105 bars.

DEFERRED (do not implement — >2 quarter lookback, infeasible purge on 502 bars):
  inst_ownership_chg_qoq — needs 2 report periods (~180-bar memory); unlocks with
                            the span-extension bead.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from research.features.feature_registry import feature

logger = logging.getLogger(__name__)

# Approximate lookback: one report period (~63 bars) + 45-day filing lag (~45 bars)
# Using 105 as the registered lookback — within the 502-bar purge constraint.
_INST_LOOKBACK = 105


def compute_inst_features(
    ownership_df: pd.DataFrame,
    holdings_df: pd.DataFrame,
    shares_outstanding: float,
    as_of_date: pd.Timestamp,
    symbol: str = "",
) -> dict[str, float]:
    """Compute both institutional features for one (symbol, date) observation.

    Args:
        ownership_df: Rows from canonical_inst_ownership for this symbol,
                      sorted by filing_date ascending. Columns: filing_date,
                      symbol, report_period, total_shares, filer_count.
        holdings_df: Rows from canonical_institutional_holdings for this symbol,
                     sorted by filing_date ascending. Columns: filing_date,
                     symbol, report_period, filer_cik, shares, value_usd.
        shares_outstanding: Float shares for this date (from financial metrics).
        as_of_date: Observation date (UTC midnight). Only filings with
                    filing_date <= as_of_date are eligible (no lookahead).
        symbol: Ticker for logging context.

    Returns:
        Dict with keys: inst_ownership_pct, inst_top10_share.
        NaN when no eligible data.
    """
    result: dict[str, float] = {
        "inst_ownership_pct": float("nan"),
        "inst_top10_share": float("nan"),
    }

    # ------------------------------------------------------------------ #
    # inst_ownership_pct — total_shares / shares_outstanding
    # ------------------------------------------------------------------ #
    if not ownership_df.empty and not np.isnan(shares_outstanding) and shares_outstanding > 0:
        eligible_own = ownership_df[ownership_df["filing_date"] <= as_of_date]
        if not eligible_own.empty:
            latest = eligible_own.iloc[-1]  # last row = most recent (sorted asc)
            total_shares = latest["total_shares"]
            if not pd.isna(total_shares) and float(total_shares) >= 0:
                result["inst_ownership_pct"] = float(total_shares) / float(shares_outstanding)

    # ------------------------------------------------------------------ #
    # inst_top10_share — top-10 filers' value / total value (same quarter)
    # Within-quarter ratio: uniform backfill-price scaling cancels out.
    # ------------------------------------------------------------------ #
    if not holdings_df.empty:
        eligible_hold = holdings_df[holdings_df["filing_date"] <= as_of_date]
        if not eligible_hold.empty:
            # Get the most recent filing_date's report_period
            latest_filing_date = eligible_hold["filing_date"].max()
            quarter_rows = eligible_hold[
                eligible_hold["filing_date"] == latest_filing_date
            ].copy()

            quarter_rows = quarter_rows.dropna(subset=["value_usd"])
            quarter_rows = quarter_rows[quarter_rows["value_usd"] > 0]

            if not quarter_rows.empty:
                total_value = quarter_rows["value_usd"].sum()
                top10_value = (
                    quarter_rows.nlargest(10, "value_usd")["value_usd"].sum()
                )
                if total_value > 0:
                    result["inst_top10_share"] = float(top10_value / total_value)

    return result


# ---------------------------------------------------------------------------
# Feature registry stubs — required so the registry can validate feature sets
# that reference inst features. Pipeline calls compute_inst_features() directly.
# ---------------------------------------------------------------------------

@feature("inst_ownership_pct", lookback=_INST_LOOKBACK)
def _inst_ownership_pct(df: pd.DataFrame) -> pd.Series:
    return df["inst_ownership_pct"]


@feature("inst_top10_share", lookback=_INST_LOOKBACK)
def _inst_top10_share(df: pd.DataFrame) -> pd.Series:
    return df["inst_top10_share"]
