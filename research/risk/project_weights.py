"""Deterministic weight projection with full risk constraints.

Implements all 15 RISK_POLICY constraints from PRD Appendix G in the
canonical evaluation order:

  1. Vol regime brakes (adjust limits)
  2. Volatility scaling (adjust order sizes)
  3. Global limits (gross exposure, turnover, net exposure, leverage)
  4. Per-symbol limits (symbol weight, active positions, concentration, ADV)
  5. Signal constraints (confidence, holding period)
  6. Cash floor

Each constraint produces a RiskDecision for audit logging.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import numpy as np

from research.experiments.spec import RiskConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Risk decision types
# ---------------------------------------------------------------------------


class Decision(Enum):
    PASS = "pass"
    SIZE_REDUCE = "size_reduce"
    REJECT = "reject"
    HALT = "halt"


@dataclass
class RiskDecision:
    """A single constraint evaluation result."""

    rule_id: str
    decision: Decision
    details: dict[str, Any] = field(default_factory=dict)
    original_weights: np.ndarray | None = None
    reduced_weights: np.ndarray | None = None


@dataclass
class RiskResult:
    """Full result of weight projection through all constraints."""

    weights: np.ndarray
    decisions: list[RiskDecision] = field(default_factory=list)
    halted: bool = False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def project_weights(
    raw_weights: np.ndarray,
    risk_config: RiskConfig,
    prev_weights: np.ndarray | None = None,
    *,
    current_vol: float | None = None,
    adv_shares: np.ndarray | None = None,
    prices: np.ndarray | None = None,
    confidences: np.ndarray | None = None,
    holding_bars: np.ndarray | None = None,
    nav: float | None = None,
) -> np.ndarray:
    """Project raw weights through deterministic risk constraints.

    Backward-compatible signature: the original 3-arg call still works.
    New callers can pass optional context for advanced constraints.

    Args:
        raw_weights: Unconstrained target weights (n_symbols,).
        risk_config: Risk configuration with constraint parameters.
        prev_weights: Previous step weights for turnover constraint.
        current_vol: Current realized volatility (annualized).
        adv_shares: 20-day average daily volume in shares per symbol.
        prices: Latest close prices per symbol (required with adv_shares).
        confidences: Signal confidence per symbol.
        holding_bars: Bars held per symbol (0 for new positions).
        nav: Current portfolio NAV (for ADV pct computation).

    Returns:
        Projected weights satisfying all constraints.
    """
    result = project_weights_full(
        raw_weights,
        risk_config,
        prev_weights,
        current_vol=current_vol,
        adv_shares=adv_shares,
        prices=prices,
        confidences=confidences,
        holding_bars=holding_bars,
        nav=nav,
    )
    return result.weights


def project_weights_full(
    raw_weights: np.ndarray,
    risk_config: RiskConfig,
    prev_weights: np.ndarray | None = None,
    *,
    current_vol: float | None = None,
    adv_shares: np.ndarray | None = None,
    prices: np.ndarray | None = None,
    confidences: np.ndarray | None = None,
    holding_bars: np.ndarray | None = None,
    nav: float | None = None,
) -> RiskResult:
    """Project weights with full decision audit trail.

    Returns a RiskResult with projected weights and per-constraint decisions.

    Constraint-ordering semantics: constraints are applied in the canonical order
    defined in the module docstring.  Hard caps (eff_max_symbol_weight,
    eff_max_gross_exposure) are re-enforced in a final validation pass so that
    soft constraints applied later (holding-period reverts, min-order reverts,
    concentration, ADV) cannot reopen exposure that was already capped.
    Turnover violations that survive the final pass are logged as warnings but
    not reversed — forced holds and min-order reverts may legitimately increase
    the L1 delta beyond the original cap.
    """
    w = np.array(raw_weights, dtype=np.float64)
    n = len(w)
    decisions: list[RiskDecision] = []

    # Effective limits (may be adjusted by vol regime brakes)
    eff_max_symbol_weight = risk_config.max_symbol_weight
    eff_max_gross_exposure = risk_config.max_gross_exposure

    # --- Group 0: Vol regime brakes (adjust limits before other checks) ---
    if current_vol is not None and current_vol > risk_config.vol_regime_threshold:
        eff_max_symbol_weight *= (1.0 - risk_config.vol_brake_position_reduction)
        eff_max_gross_exposure *= (1.0 - risk_config.vol_brake_exposure_reduction)
        decisions.append(RiskDecision(
            rule_id="vol_regime_brakes",
            decision=Decision.SIZE_REDUCE,
            details={
                "current_vol": current_vol,
                "threshold": risk_config.vol_regime_threshold,
                "eff_max_symbol_weight": eff_max_symbol_weight,
                "eff_max_gross_exposure": eff_max_gross_exposure,
            },
        ))
    else:
        decisions.append(RiskDecision(
            rule_id="vol_regime_brakes",
            decision=Decision.PASS,
            details={
                "current_vol": current_vol,
                "threshold": risk_config.vol_regime_threshold,
            },
        ))

    # --- Group 0b: Volatility scaling (adjust order sizes) ---
    if current_vol is not None and current_vol > 0 and risk_config.target_vol > 0:
        vol_scale = risk_config.target_vol / current_vol
        vol_scale = min(vol_scale, 1.0)  # Never scale UP beyond 1x
        original = w.copy()
        w = w * vol_scale
        decisions.append(RiskDecision(
            rule_id="volatility_scaling",
            decision=Decision.SIZE_REDUCE if vol_scale < 1.0 else Decision.PASS,
            details={
                "target_vol": risk_config.target_vol,
                "current_vol": current_vol,
                "scale_factor": vol_scale,
            },
            original_weights=original,
            reduced_weights=w.copy(),
        ))
    else:
        decisions.append(RiskDecision(
            rule_id="volatility_scaling",
            decision=Decision.PASS,
        ))

    # --- 1. Long-only clamp ---
    original = w.copy()
    np.maximum(w, 0.0, out=w)
    if not np.array_equal(original, w):
        decisions.append(RiskDecision(
            rule_id="long_only",
            decision=Decision.SIZE_REDUCE,
        ))
    else:
        decisions.append(RiskDecision(
            rule_id="long_only",
            decision=Decision.PASS,
        ))

    # --- 2. Per-symbol max weight (using effective limit) ---
    original = w.copy()
    np.minimum(w, eff_max_symbol_weight, out=w)
    if not np.array_equal(original, w):
        decisions.append(RiskDecision(
            rule_id="max_symbol_weight",
            decision=Decision.SIZE_REDUCE,
            details={"limit": eff_max_symbol_weight},
        ))
    else:
        decisions.append(RiskDecision(
            rule_id="max_symbol_weight",
            decision=Decision.PASS,
            details={"limit": eff_max_symbol_weight},
        ))

    # --- 3. Gross exposure cap (using effective limit, ensuring min cash) ---
    max_exposure = min(eff_max_gross_exposure, 1.0 - risk_config.min_cash)
    total = w.sum()
    if total > max_exposure and total > 0:
        original = w.copy()
        w *= max_exposure / total
        decisions.append(RiskDecision(
            rule_id="max_gross_exposure",
            decision=Decision.SIZE_REDUCE,
            details={"total": float(total), "limit": max_exposure},
        ))
    else:
        decisions.append(RiskDecision(
            rule_id="max_gross_exposure",
            decision=Decision.PASS,
            details={"total": float(total), "limit": max_exposure},
        ))

    # --- 4. Max daily turnover (L1 delta from previous weights) ---
    if prev_weights is not None and risk_config.max_daily_turnover < 2.0:
        delta = w - prev_weights
        l1 = np.abs(delta).sum()
        if l1 > risk_config.max_daily_turnover and l1 > 0:
            original = w.copy()
            scale = risk_config.max_daily_turnover / l1
            w = prev_weights + delta * scale
            np.maximum(w, 0.0, out=w)
            decisions.append(RiskDecision(
                rule_id="max_daily_turnover",
                decision=Decision.SIZE_REDUCE,
                details={
                    "l1_delta": float(l1),
                    "limit": risk_config.max_daily_turnover,
                },
            ))
        else:
            decisions.append(RiskDecision(
                rule_id="max_daily_turnover",
                decision=Decision.PASS,
                details={"l1_delta": float(l1)},
            ))

        # Interpolating toward prev_weights can push gross back above the
        # vol-brake-reduced eff_max_gross_exposure — re-apply immediately.
        total_post_tv = w.sum()
        if total_post_tv > max_exposure and total_post_tv > 0:
            w *= max_exposure / total_post_tv

    # --- 5. Net exposure (|sum of weights|) ---
    net = w.sum()  # long-only so net == gross, but kept for completeness
    if abs(net) > risk_config.max_net_exposure and abs(net) > 0:
        original = w.copy()
        w *= risk_config.max_net_exposure / abs(net)
        decisions.append(RiskDecision(
            rule_id="max_net_exposure",
            decision=Decision.SIZE_REDUCE,
            details={"net": float(net), "limit": risk_config.max_net_exposure},
        ))
    else:
        decisions.append(RiskDecision(
            rule_id="max_net_exposure",
            decision=Decision.PASS,
            details={"net": float(net)},
        ))

    # --- 6. Leverage ---
    gross = w.sum()
    if gross > risk_config.max_leverage and gross > 0:
        original = w.copy()
        w *= risk_config.max_leverage / gross
        decisions.append(RiskDecision(
            rule_id="max_leverage",
            decision=Decision.SIZE_REDUCE,
            details={"gross": float(gross), "limit": risk_config.max_leverage},
        ))
    else:
        decisions.append(RiskDecision(
            rule_id="max_leverage",
            decision=Decision.PASS,
            details={"gross": float(gross)},
        ))

    # --- 7. Max active positions: zero out smallest weights beyond limit ---
    if risk_config.max_active_positions < n:
        active = (w > 0).sum()
        if active > risk_config.max_active_positions:
            original = w.copy()
            threshold_idx = np.argsort(w)[: n - risk_config.max_active_positions]
            w[threshold_idx] = 0.0
            decisions.append(RiskDecision(
                rule_id="max_active_positions",
                decision=Decision.REJECT,
                details={
                    "active": int(active),
                    "limit": risk_config.max_active_positions,
                    "zeroed": int(len(threshold_idx)),
                },
            ))
        else:
            decisions.append(RiskDecision(
                rule_id="max_active_positions",
                decision=Decision.PASS,
            ))
    else:
        decisions.append(RiskDecision(
            rule_id="max_active_positions",
            decision=Decision.PASS,
        ))

    # --- 8. Concentration: top N positions <= max_top_n_concentration ---
    top_n = risk_config.top_n
    if top_n > 0 and top_n < n:
        sorted_desc = np.sort(w)[::-1]
        top_n_sum = sorted_desc[:top_n].sum()
        if top_n_sum > risk_config.max_top_n_concentration and top_n_sum > 0:
            # Scale down the top N weights proportionally
            original = w.copy()
            top_n_indices = np.argsort(w)[::-1][:top_n]
            scale = risk_config.max_top_n_concentration / top_n_sum
            w[top_n_indices] *= scale
            decisions.append(RiskDecision(
                rule_id="concentration_limit",
                decision=Decision.SIZE_REDUCE,
                details={
                    "top_n": top_n,
                    "top_n_sum": float(top_n_sum),
                    "limit": risk_config.max_top_n_concentration,
                },
            ))
        else:
            decisions.append(RiskDecision(
                rule_id="concentration_limit",
                decision=Decision.PASS,
                details={"top_n_sum": float(top_n_sum)},
            ))
    else:
        decisions.append(RiskDecision(
            rule_id="concentration_limit",
            decision=Decision.PASS,
        ))

    # --- 9. ADV participation: cap per-symbol weight by ADV ---
    if adv_shares is not None and nav is not None and nav > 0:
        adv = np.asarray(adv_shares, dtype=np.float64)
        if prices is None:
            # Cannot compute ADV notional without prices — skip and warn.
            logger.warning(
                "max_adv_pct constraint skipped: adv_shares provided but prices is None; "
                "pass prices= (latest closes) to enable ADV capping"
            )
            decisions.append(RiskDecision(
                rule_id="max_adv_pct",
                decision=Decision.PASS,
                details={"max_adv_pct": risk_config.max_adv_pct, "skipped": "prices_missing"},
            ))
        else:
            px = np.asarray(prices, dtype=np.float64)
            capped = False
            for i in range(n):
                if adv[i] > 0 and px[i] > 0:
                    # max_w = max_adv_pct * (adv_shares * price) / nav
                    max_w = risk_config.max_adv_pct * adv[i] * px[i] / nav
                    if w[i] > max_w:
                        w[i] = max_w
                        capped = True
            decisions.append(RiskDecision(
                rule_id="max_adv_pct",
                decision=Decision.SIZE_REDUCE if capped else Decision.PASS,
                details={"max_adv_pct": risk_config.max_adv_pct},
            ))
    else:
        decisions.append(RiskDecision(
            rule_id="max_adv_pct",
            decision=Decision.PASS,
        ))

    # --- 10. Confidence gating ---
    if confidences is not None and risk_config.min_confidence > 0:
        conf = np.asarray(confidences, dtype=np.float64)
        rejected = False
        for i in range(n):
            if conf[i] < risk_config.min_confidence and w[i] > 0:
                w[i] = 0.0
                rejected = True
        decisions.append(RiskDecision(
            rule_id="min_confidence",
            decision=Decision.REJECT if rejected else Decision.PASS,
            details={"min_confidence": risk_config.min_confidence},
        ))
    else:
        decisions.append(RiskDecision(
            rule_id="min_confidence",
            decision=Decision.PASS,
        ))

    # --- 11. Min holding period ---
    if (
        holding_bars is not None
        and prev_weights is not None
        and risk_config.min_holding_period > 1
    ):
        bars = np.asarray(holding_bars, dtype=np.float64)
        blocked = False
        for i in range(n):
            # Block reduction of positions held less than min period
            if (
                bars[i] > 0
                and bars[i] < risk_config.min_holding_period
                and prev_weights[i] > 0
                and w[i] < prev_weights[i]
            ):
                # Clamp to eff_max_symbol_weight — prev may have been set under
                # a less restrictive vol regime than today's.
                w[i] = min(prev_weights[i], eff_max_symbol_weight)
                blocked = True
        decisions.append(RiskDecision(
            rule_id="min_holding_period",
            decision=Decision.REJECT if blocked else Decision.PASS,
            details={"min_bars": risk_config.min_holding_period},
        ))
    else:
        decisions.append(RiskDecision(
            rule_id="min_holding_period",
            decision=Decision.PASS,
        ))

    # --- 12. Size reduce: zero out weights below minimum_order_threshold ---
    if prev_weights is not None:
        min_thresh = risk_config.minimum_order_threshold
        for i in range(n):
            delta = abs(w[i] - prev_weights[i])
            if delta > 0 and delta < min_thresh and w[i] > 0:
                # Clamp to eff_max_symbol_weight — prev may exceed today's limit.
                w[i] = min(prev_weights[i], eff_max_symbol_weight)
    decisions.append(RiskDecision(
        rule_id="minimum_order_threshold",
        decision=Decision.PASS,
        details={"threshold": risk_config.minimum_order_threshold},
    ))

    # --- 13. Cash floor: final check ensuring min_cash ---
    total = w.sum()
    cash_available = 1.0 - total
    if cash_available < risk_config.min_cash and total > 0:
        target_exposure = 1.0 - risk_config.min_cash
        w *= target_exposure / total
        decisions.append(RiskDecision(
            rule_id="min_cash",
            decision=Decision.SIZE_REDUCE,
            details={
                "cash_available": float(cash_available),
                "min_cash": risk_config.min_cash,
            },
        ))
    else:
        decisions.append(RiskDecision(
            rule_id="min_cash",
            decision=Decision.PASS,
            details={"cash_available": float(cash_available)},
        ))

    # --- 14. Final validation: re-enforce hard caps broken by later soft constraints ---
    # Soft constraints (steps 7-13) can reopen exposure capped in steps 2-4:
    # concentration / ADV / min_cash rescale the whole portfolio; holding-period
    # and min-order reverts restore prev_weights that may individually exceed
    # eff_max_symbol_weight.  Re-apply the two hard caps here so the output
    # always satisfies them.  Turnover is not re-capped because forced holds and
    # min-order reverts legitimately increase the L1 delta; log a warning instead.
    w_before_val = w.copy()
    np.minimum(w, eff_max_symbol_weight, out=w)
    sym_capped = not np.array_equal(w_before_val, w)

    total_final = w.sum()
    gross_capped = False
    if total_final > max_exposure and total_final > 0:
        w *= max_exposure / total_final
        gross_capped = True

    if prev_weights is not None and risk_config.max_daily_turnover < 2.0:
        final_l1 = float(np.abs(w - prev_weights).sum())
        if final_l1 > risk_config.max_daily_turnover + 1e-9:
            logger.warning(
                "max_daily_turnover cap conflict: final L1 delta %.4f exceeds limit %.4f "
                "(caused by forced holds / min-order reverts — not reversed)",
                final_l1,
                risk_config.max_daily_turnover,
            )

    decisions.append(RiskDecision(
        rule_id="final_validation",
        decision=Decision.SIZE_REDUCE if (sym_capped or gross_capped) else Decision.PASS,
        details={
            "eff_max_symbol_weight": eff_max_symbol_weight,
            "eff_max_gross_exposure": eff_max_gross_exposure,
            "sym_weight_recapped": sym_capped,
            "gross_exposure_recapped": gross_capped,
        },
    ))

    return RiskResult(weights=w, decisions=decisions)
