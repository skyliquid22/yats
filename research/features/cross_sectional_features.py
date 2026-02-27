"""Cross-sectional features — Appendix J.2.

Computed across the entire universe per date. These require all symbols'
data for each date to compute rankings and z-scores.
"""

import numpy as np
import pandas as pd

from research.features.feature_registry import feature


@feature("mom_3m")
def compute_mom_3m(df: pd.DataFrame) -> pd.Series:
    """Cumulative return over 63 trading days.

    Per-symbol computation: log(close_t / close_{t-63}).
    """
    return np.log(df["close"] / df["close"].shift(63))


@feature("mom_12m_excl_1m")
def compute_mom_12m_excl_1m(df: pd.DataFrame) -> pd.Series:
    """Cumulative return months 2-12 (skip most recent month).

    log(close_{t-21} / close_{t-252}).
    """
    return np.log(df["close"].shift(21) / df["close"].shift(252))


@feature("log_mkt_cap")
def compute_log_mkt_cap(df: pd.DataFrame) -> pd.Series:
    """log(shares_outstanding * close_price).

    Requires shares_outstanding from canonical_financial_metrics merged
    into the DataFrame.
    """
    return np.log(df["shares_outstanding"] * df["close"])


@feature("size_rank")
def compute_size_rank(group: pd.DataFrame) -> pd.Series:
    """Percentile rank of log_mkt_cap within universe on each date.

    This is a cross-sectional operation — called per date with all symbols.
    """
    return group["log_mkt_cap"].rank(pct=True)


@feature("value_rank")
def compute_value_rank(group: pd.DataFrame) -> pd.Series:
    """Percentile rank of 1/pe_ttm within universe on each date.

    Higher 1/pe_ttm = cheaper stock = higher value rank.
    """
    earnings_yield = 1.0 / group["pe_ttm"]
    return earnings_yield.rank(pct=True)
