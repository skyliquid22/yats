"""Factor attribution via OLS regression — PRD §8.3 performance.attribution.

Regresses portfolio daily returns on factor returns to decompose alpha
from systematic (factor) exposure.  Results are stored under
performance.attribution in metrics.json.

Interpretation:
  alpha_annualized: annualised daily intercept — return unexplained by factors.
    A positive, significant alpha (|alpha_t_stat| > 2) suggests the strategy
    earns excess return beyond factor compensation.
  betas: factor loadings — how much the portfolio moves per unit of factor.
    A high market beta (≈ 1) means the strategy closely tracks the market.
  r_squared: fraction of portfolio variance explained by the factor model.
    Low r_squared (< 0.3) suggests idiosyncratic alpha; high r_squared (> 0.7)
    means returns are largely factor-driven.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

ANNUALIZATION_FACTOR = 252


def compute_factor_attribution(
    portfolio_returns: pd.Series,
    factor_returns: pd.DataFrame,
) -> dict:
    """Regress portfolio returns on factor returns via OLS.

    Args:
        portfolio_returns: Daily portfolio returns, date-indexed Series.
        factor_returns: Daily factor returns, date-indexed DataFrame.
            Columns name the factors (e.g. "market", "momentum").

    Returns:
        Dict with keys:
            alpha_annualized  float   Annualised daily intercept (alpha * 252).
            alpha_t_stat      float   t-statistic for the intercept.
            betas             dict    Factor loadings keyed by column name.
            r_squared         float   Coefficient of determination (clamped [0,1]).
            n_obs             int     Number of aligned date observations used.
            factors_used      list    Factor column names in regression order.
        All numeric values are None when there is insufficient data.
    """
    factor_cols = list(factor_returns.columns)

    aligned = pd.concat(
        [portfolio_returns.rename("_portfolio"), factor_returns], axis=1,
    ).dropna()
    n = len(aligned)

    # Need at least (k + 2) observations for k factors: intercept + k betas + ≥1 dof
    k = len(factor_cols)
    if n < k + 2:
        return _empty_attribution(factor_cols)

    y = aligned["_portfolio"].values
    X_raw = aligned[factor_cols].values

    # Design matrix: intercept column followed by factors
    X = np.column_stack([np.ones(n), X_raw])  # (n, 1 + k)

    try:
        coeffs, _, rank, _ = np.linalg.lstsq(X, y, rcond=None)
    except np.linalg.LinAlgError:
        return _empty_attribution(factor_cols)

    if rank < X.shape[1]:
        return _empty_attribution(factor_cols)

    alpha_daily = float(coeffs[0])
    betas = {col: float(coeffs[i + 1]) for i, col in enumerate(factor_cols)}

    y_hat = X @ coeffs
    resid = y - y_hat
    dof = n - (k + 1)
    sigma2 = float(np.dot(resid, resid) / dof)

    XtX = X.T @ X
    try:
        XtX_inv = np.linalg.inv(XtX)
    except np.linalg.LinAlgError:
        return _empty_attribution(factor_cols)

    var_coeffs = np.diag(XtX_inv) * sigma2
    se = np.sqrt(np.maximum(var_coeffs, 0.0))

    alpha_se = float(se[0])
    alpha_t_stat = float(alpha_daily / alpha_se) if alpha_se > 1e-15 else 0.0

    ss_tot = float(np.var(y) * n)
    ss_res = float(np.dot(resid, resid))
    r_squared = float(1.0 - ss_res / ss_tot) if ss_tot > 1e-15 else 0.0
    r_squared = max(0.0, min(1.0, r_squared))

    return {
        "alpha_annualized": alpha_daily * ANNUALIZATION_FACTOR,
        "alpha_t_stat": alpha_t_stat,
        "betas": betas,
        "r_squared": r_squared,
        "n_obs": n,
        "factors_used": factor_cols,
    }


def _empty_attribution(factor_cols: list[str]) -> dict:
    return {
        "alpha_annualized": None,
        "alpha_t_stat": None,
        "betas": {c: None for c in factor_cols},
        "r_squared": None,
        "n_obs": 0,
        "factors_used": factor_cols,
    }
