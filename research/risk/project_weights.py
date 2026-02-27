"""Deterministic weight projection with risk constraints.

Applies a sequence of risk constraints to raw portfolio weights:
1. Long-only clamp (all weights >= 0)
2. Per-symbol max weight clamp
3. Exposure cap (sum of weights <= max, ensuring min cash)
4. Max active positions
5. Optional max turnover vs previous weights (L1 delta)

PRD Appendix A.4.
"""

from __future__ import annotations

import numpy as np

from research.experiments.spec import RiskConfig


def project_weights(
    raw_weights: np.ndarray,
    risk_config: RiskConfig,
    prev_weights: np.ndarray | None = None,
) -> np.ndarray:
    """Project raw weights through deterministic risk constraints.

    Args:
        raw_weights: Unconstrained target weights (n_symbols,).
        risk_config: Risk configuration with constraint parameters.
        prev_weights: Previous step weights for turnover constraint.
            If None, turnover constraint is skipped.

    Returns:
        Projected weights satisfying all constraints.
    """
    w = np.array(raw_weights, dtype=np.float64)

    # 1. Long-only clamp
    np.maximum(w, 0.0, out=w)

    # 2. Per-symbol max weight
    np.minimum(w, risk_config.max_symbol_weight, out=w)

    # 3. Exposure cap: sum <= max_gross_exposure, ensuring min_cash
    max_exposure = min(risk_config.max_gross_exposure, 1.0 - risk_config.min_cash)
    total = w.sum()
    if total > max_exposure and total > 0:
        w *= max_exposure / total

    # 4. Max active positions: zero out smallest weights beyond limit
    if risk_config.max_active_positions < len(w):
        threshold_idx = np.argsort(w)[: len(w) - risk_config.max_active_positions]
        w[threshold_idx] = 0.0

    # 5. Max daily turnover (L1 delta from previous weights)
    if prev_weights is not None and risk_config.max_daily_turnover < 2.0:
        delta = w - prev_weights
        l1 = np.abs(delta).sum()
        if l1 > risk_config.max_daily_turnover and l1 > 0:
            scale = risk_config.max_daily_turnover / l1
            w = prev_weights + delta * scale
            np.maximum(w, 0.0, out=w)

    return w
