"""Factor-neutralized targets via trailing PCA (SPAN-16 family, label variant N).

Residualizes forward returns against the top-k principal components of the
universe's daily-return covariance. Registered spec
(docs/research/preregistrations/2026-09-16_span16_family.md):

- Factors are estimated on a trailing ``window``-bar slice of daily returns
  ending strictly BEFORE the estimation date (t-1 causal).
- Estimation dates are the membership rebalance dates; the eigenvectors are
  held fixed until the next rebalance (registered as "re-estimated each
  rebalance date").
- Factor forward returns are computed per date from the SAME forward-return
  column being neutralized: F_k(t) = sum_i V[i,k] * fwd_i(t) over non-NaN i.
- Residual: fwd_i(t) - sum_k V[i,k] * F_k(t)  (PCA reconstruction removal).
- Symbols with insufficient trailing coverage carry NaN residuals for that
  inter-rebalance block (excluded from training, mirroring the no-fill
  philosophy). Labels legitimately contain future information; only the
  factor ESTIMATION is constrained to trailing data.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_N_FACTORS = 5
DEFAULT_WINDOW = 252
DEFAULT_MIN_COVERAGE = 0.8


def trailing_pca_eigenvectors(
    rets: pd.DataFrame,
    asof,
    window: int = DEFAULT_WINDOW,
    n_factors: int = DEFAULT_N_FACTORS,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
) -> pd.DataFrame | None:
    """Top-``n_factors`` eigenvectors from the trailing return window.

    Args:
        rets: dates x symbols daily-return matrix (may contain NaN).
        asof: estimation date; only rows with index < asof are used.
        window: trailing bars.
        n_factors: components to keep.
        min_coverage: minimum non-NaN share a symbol needs in the window.

    Returns:
        DataFrame (symbols x n_factors) of eigenvector entries for covered
        symbols, or None if the window has fewer than ``window // 2`` rows.
    """
    hist = rets.loc[rets.index < asof].tail(window)
    if len(hist) < window // 2:
        return None
    coverage = hist.notna().mean()
    covered = coverage[coverage >= min_coverage].index
    if len(covered) < n_factors * 2:
        return None
    x = hist[covered].to_numpy(dtype=float)
    mu = np.nanmean(x, axis=0)
    x = np.where(np.isnan(x), mu, x) - mu
    # right singular vectors of the (T x N) matrix = covariance eigenvectors
    _, _, vt = np.linalg.svd(x, full_matrices=False)
    k = min(n_factors, vt.shape[0])
    return pd.DataFrame(vt[:k].T, index=covered, columns=range(k))


def pca_neutralize_column(
    panel: pd.DataFrame,
    fwd_col: str,
    closes: pd.DataFrame,
    rebalance_dates: list,
    n_factors: int = DEFAULT_N_FACTORS,
    window: int = DEFAULT_WINDOW,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    date_col: str = "date",
    symbol_col: str = "symbol",
) -> pd.Series:
    """PCA-neutralized version of ``panel[fwd_col]`` (see module docstring).

    Args:
        panel: long panel with date/symbol/``fwd_col``.
        closes: long frame with date/symbol/close for the SAME universe
            (used only to build the daily-return matrix).
        rebalance_dates: sorted estimation dates (membership rebalances).

    Returns:
        Series aligned to ``panel.index``; NaN where the symbol is
        uncovered by the block's factor estimate or ``fwd_col`` is NaN.
    """
    px = closes.pivot_table(index=date_col, columns=symbol_col, values="close", aggfunc="first").sort_index()
    rets = px.pct_change()

    dates = np.array(sorted(panel[date_col].unique()))
    rebalances = sorted(pd.Timestamp(d) for d in rebalance_dates)
    out = pd.Series(np.nan, index=panel.index, name=f"{fwd_col}_pca_resid")

    fwd_wide = panel.pivot_table(index=date_col, columns=symbol_col, values=fwd_col, aggfunc="first").sort_index()

    for i, rb in enumerate(rebalances):
        block_end = rebalances[i + 1] if i + 1 < len(rebalances) else pd.Timestamp.max
        block_dates = [d for d in dates if rb <= pd.Timestamp(d) < block_end]
        if not block_dates:
            continue
        vecs = trailing_pca_eigenvectors(rets, rb, window, n_factors, min_coverage)
        if vecs is None:
            logger.warning("pca_neutralize: no factor estimate at %s — block left NaN", rb)
            continue
        cols = [c for c in vecs.index if c in fwd_wide.columns]
        v = vecs.loc[cols].to_numpy(dtype=float)              # N x k
        blk = fwd_wide.loc[fwd_wide.index.isin(block_dates), cols]
        y = blk.to_numpy(dtype=float)                          # T x N
        y0 = np.where(np.isnan(y), 0.0, y)
        f = y0 @ v                                             # T x k factor fwd returns
        resid = y - f @ v.T                                    # NaN propagates from y
        resid_df = pd.DataFrame(resid, index=blk.index, columns=cols)
        long = resid_df.stack(future_stack=True).rename("val").reset_index()
        long.columns = [date_col, symbol_col, "val"]
        merged = panel[[date_col, symbol_col]].merge(long, on=[date_col, symbol_col], how="left")
        mask = merged["val"].notna().to_numpy()
        out.iloc[np.flatnonzero(mask)] = merged.loc[mask, "val"].to_numpy()
    return out
