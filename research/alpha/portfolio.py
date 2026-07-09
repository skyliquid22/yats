"""Rank-weighted long-only portfolio construction from cross-sectional alpha scores.

v1: simple rank-weighting capped at max_symbol_weight.
Hook exists for optimizer integration in ALPHA-3 (ya-whx95 or similar).
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def rank_weighted_portfolio(
    scores: pd.Series,
    *,
    max_symbol_weight: float = 0.30,
) -> pd.Series:
    """Construct rank-weighted long-only portfolio weights from alpha scores.

    Algorithm:
        1. Rank scores (higher alpha = higher rank, 1-indexed).
        2. Assign raw weight proportional to rank (linear in rank).
        3. Zero-out non-positive ranks (not used in long-only).
        4. Normalize to sum to 1.0.
        5. Cap each weight at max_symbol_weight and renormalize.

    Args:
        scores: Series of alpha scores {symbol: score}. May contain NaN
            (treated as 0.0 / neutral).
        max_symbol_weight: Maximum weight per symbol in [0, 1].

    Returns:
        Series of portfolio weights summing to 1.0 (or all-zero if scores
        are all NaN/zero/negative).
    """
    if scores.isna().all():
        return pd.Series(0.0, index=scores.index)

    filled = scores.fillna(0.0)
    n = len(filled)

    # Rank ascending (lowest score = rank 1)
    ranks = filled.rank(method="average", ascending=True)
    # Long-only: keep only positive scores (above median or above 0)
    # Strategy: weight ∝ max(0, score - min_score), i.e. positive margin
    # Simpler and more robust: use rank directly, long top half
    weights = np.maximum(ranks - n / 2.0, 0.0)

    total = weights.sum()
    if total < 1e-10:
        return pd.Series(0.0, index=scores.index)

    weights = weights / total

    # Cap with equal redistribution to all non-capped symbols.
    # Proportional redistribution creates cycles (A+B capped → excess to C;
    # C capped → excess back to A+B). Equal redistribution converges in O(n).
    for _ in range(len(weights) + 1):
        cap_mask = weights > max_symbol_weight
        if not cap_mask.any():
            break
        excess = (weights[cap_mask] - max_symbol_weight).sum()
        weights[cap_mask] = max_symbol_weight
        free_mask = ~cap_mask
        n_free = int(free_mask.sum())
        if n_free == 0:
            break
        weights[free_mask] += excess / n_free

    # Always final-normalize: handles infeasible caps (n*max_weight < 1) gracefully
    s = weights.sum()
    if s > 1e-10:
        weights = weights / s

    return pd.Series(weights.values, index=scores.index)


def portfolio_returns_from_weights(
    weights_by_date: dict[str, pd.Series],
    returns_by_date: dict[str, pd.Series],
    symbols: list[str],
) -> list[float]:
    """Compute daily portfolio returns from date-keyed weights and returns.

    Args:
        weights_by_date: {date: Series of weights per symbol}
        returns_by_date: {date: Series of 1-day returns per symbol}
        symbols: Ordered symbol list.

    Returns:
        List of daily portfolio returns in chronological date order.
    """
    dates = sorted(set(weights_by_date) & set(returns_by_date))
    port_returns = []
    for dt in dates:
        w = weights_by_date[dt].reindex(symbols).fillna(0.0)
        r = returns_by_date[dt].reindex(symbols).fillna(0.0)
        port_returns.append(float((w * r).sum()))
    return port_returns
