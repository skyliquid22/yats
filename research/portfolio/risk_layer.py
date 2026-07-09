"""Portfolio-level risk layer — vol targeting and beta-neutral option.

Applied as an optional post-policy transform after project_weights.
Operates on trailing return windows to compute scaling factors.

Two transforms (applied in order when enabled):
  1. VOL TARGETING: scale weights so trailing portfolio vol ≈ vol_target.
     Scale DOWN only (max_leverage=1.0, long-only). v1 implementation.
  2. BETA-NEUTRAL (optional, off by default): reduce portfolio beta vs SPY.
     True beta-neutrality (β=0) requires shorting SPY; not available in
     long-only mode. Implemented as beta-CAPPED: scale weights so
     portfolio β ≤ beta_cap. Enable shorts in execution env for true β=0.

Config via ExperimentSpec.portfolio_risk (PortfolioRiskConfig from spec.py).
When portfolio_risk is None, no transform is applied.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from research.experiments.spec import PortfolioRiskConfig


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _compute_vol_scale(
    portfolio_returns: np.ndarray, config: PortfolioRiskConfig,
) -> float:
    """Compute vol-targeting scale factor from trailing portfolio returns.

    Returns a scale in (0, 1] — never amplifies, only attenuates.
    Returns 1.0 when insufficient history or vol is zero.
    """
    window = portfolio_returns[-config.vol_lookback :]
    if len(window) < 2:
        return 1.0
    trailing_vol = float(np.std(window, ddof=1)) * np.sqrt(252.0)
    if trailing_vol <= 0.0:
        return 1.0
    return min(1.0, config.vol_target / trailing_vol)


def compute_betas(
    symbol_returns: np.ndarray,
    spy_returns: np.ndarray,
) -> np.ndarray:
    """Compute trailing betas of each symbol vs SPY (β = Cov(r_i, r_spy) / Var(r_spy)).

    Args:
        symbol_returns: Shape (T, n_symbols) — trailing daily returns per symbol.
        spy_returns: Shape (T,) — trailing SPY daily returns.

    Returns:
        Array of shape (n_symbols,) with beta estimates.
        Returns zeros when Var(spy_returns) == 0.
    """
    if symbol_returns.ndim == 1:
        symbol_returns = symbol_returns[:, np.newaxis]
    n_symbols = symbol_returns.shape[1]
    betas = np.zeros(n_symbols)

    spy_var = float(np.var(spy_returns, ddof=1))
    if spy_var <= 0.0:
        return betas

    for i in range(n_symbols):
        cov_matrix = np.cov(symbol_returns[:, i], spy_returns, ddof=1)
        betas[i] = cov_matrix[0, 1] / spy_var

    return betas


def _compute_beta_cap_scale(
    weights: np.ndarray,
    betas: np.ndarray,
    config: PortfolioRiskConfig,
) -> float:
    """Compute beta-cap scale factor.

    Long-only portfolios cannot achieve β=0 without shorting SPY.
    Instead, scale weights down so portfolio β ≤ config.beta_cap.

    Returns scale in (0, 1]; returns 1.0 when portfolio β ≤ beta_cap.
    """
    portfolio_beta = float(np.dot(weights, betas))
    if portfolio_beta <= 0.0 or portfolio_beta <= config.beta_cap:
        return 1.0
    return config.beta_cap / portfolio_beta


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def apply_risk_layer(
    weights: np.ndarray,
    portfolio_returns: np.ndarray,
    symbol_returns: np.ndarray | None,
    spy_returns: np.ndarray | None,
    config: PortfolioRiskConfig,
) -> np.ndarray:
    """Apply portfolio risk transforms to a single weight vector.

    Step-by-step version for use in the shadow engine. All history is passed
    by the caller; this function is stateless.

    Args:
        weights: Current weight vector (n_symbols,), after project_weights.
        portfolio_returns: Trailing portfolio daily returns for vol estimate.
        symbol_returns: Trailing per-symbol returns (T, n_symbols) for beta.
                        Pass None when beta_neutral=False or unavailable.
        spy_returns: Trailing SPY daily returns (T,).
                     Pass None when beta_neutral=False or unavailable.
        config: PortfolioRiskConfig from ExperimentSpec.

    Returns:
        Scaled weight vector — always ≤ input weights component-wise.
    """
    w = weights.copy()

    # 1. Vol targeting
    if len(portfolio_returns) >= 2:
        scale = _compute_vol_scale(portfolio_returns, config)
        w = w * scale

    # 2. Beta adjustment (long-only: beta-cap mode)
    if (
        config.beta_neutral
        and symbol_returns is not None
        and spy_returns is not None
        and spy_returns.ndim == 1
        and len(spy_returns) >= 2
        and symbol_returns.shape[0] >= 2
    ):
        betas = compute_betas(symbol_returns, spy_returns)
        beta_scale = _compute_beta_cap_scale(w, betas, config)
        w = w * beta_scale

    return w


def apply_risk_layer_batch(
    weights: pd.DataFrame,
    returns: pd.DataFrame,
    spy_returns: pd.Series | None,
    config: PortfolioRiskConfig,
) -> pd.DataFrame:
    """Apply portfolio risk transforms to a full weights DataFrame (WFO eval path).

    Applies the risk layer row-by-row using strictly causal (backward-looking)
    windows to avoid lookahead bias. The vol estimate at bar t uses portfolio
    returns from bars [t-vol_lookback, t) — excludes bar t itself.

    Vol estimate uses raw policy weights × historical returns as an approximation;
    pre-scaling vol upper-bounds post-scaling vol so this is conservative.

    Args:
        weights: DataFrame (T, n_symbols) of policy weights.
        returns: DataFrame (T, n_symbols) of per-symbol daily returns.
        spy_returns: Series of SPY returns aligned to returns.index.
                     Used only when config.beta_neutral=True. May be None.
        config: PortfolioRiskConfig.

    Returns:
        DataFrame of same shape / index / columns as weights, risk-layer applied.
    """
    # Build portfolio return series from raw policy weights (causal approximation)
    port_ret_arr = (weights * returns).sum(axis=1).values.astype(np.float64)

    ret_arr = returns.values.astype(np.float64)
    spy_arr = spy_returns.values.astype(np.float64) if spy_returns is not None else None

    scaled_arr = weights.values.astype(np.float64).copy()

    for t in range(len(weights)):
        w_t = scaled_arr[t].copy()

        # Trailing portfolio returns: [t-lookback, t) — causal window
        vol_start = max(0, t - config.vol_lookback)
        port_hist = port_ret_arr[vol_start:t]

        if len(port_hist) >= 2:
            vol_scale = _compute_vol_scale(port_hist, config)
            w_t = w_t * vol_scale

        # Beta adjustment
        if config.beta_neutral and spy_arr is not None and t >= 2:
            beta_start = max(0, t - config.beta_lookback)
            sym_hist = ret_arr[beta_start:t]
            spy_hist = spy_arr[beta_start:t]
            if len(spy_hist) >= 2 and sym_hist.shape[0] >= 2:
                betas = compute_betas(sym_hist, spy_hist)
                beta_scale = _compute_beta_cap_scale(w_t, betas, config)
                w_t = w_t * beta_scale

        scaled_arr[t] = w_t

    return pd.DataFrame(scaled_arr, index=weights.index, columns=weights.columns)
