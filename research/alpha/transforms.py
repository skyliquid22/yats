"""Cross-sectional rank normalization for supervised alpha features."""
from __future__ import annotations

import numpy as np
import pandas as pd


def rank_normalize_cross_sectional(
    df: pd.DataFrame,
    feature_cols: list[str],
    *,
    date_col: str = "date",
) -> pd.DataFrame:
    """Rank-normalize features cross-sectionally per date.

    For each date and feature: percentile rank across symbols, then z-score.
    Symbols with NaN in a feature are excluded from that feature's rank.

    Args:
        df: Panel DataFrame with a date column (one row per date×symbol).
        feature_cols: Columns to normalize. Columns not present are skipped.
        date_col: Column identifying the date grouping.

    Returns:
        Copy of df with feature_cols replaced by cross-sectional z-scores.
    """
    result = df.copy()
    for col in feature_cols:
        if col not in result.columns:
            continue
        # Percentile rank within each date (NaN excluded automatically by rank)
        pct = result.groupby(date_col, sort=False)[col].rank(pct=True, na_option="keep")
        # Z-score per date (mean/std of the pct-rank values)
        grp_mean = pct.groupby(result[date_col]).transform("mean")
        grp_std = pct.groupby(result[date_col]).transform("std")
        result[col] = np.where(
            grp_std > 1e-10,
            (pct - grp_mean) / grp_std,
            0.0,
        )
    return result
