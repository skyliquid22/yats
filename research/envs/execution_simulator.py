"""ExecutionSimulator — simulates order execution with slippage and fill modeling.

Resolves target weights into realized weights by simulating execution against
OHLC data with configurable slippage models and fill probability.

PRD Appendix A.7.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Any

import numpy as np

from research.experiments.spec import ExecutionSimConfig


@dataclass
class ExecutionResult:
    """Result of simulating execution for one step."""

    realized_weights: np.ndarray
    execution_slippage_bps: float
    missed_fill_ratio: float
    unfilled_notional: float
    order_type_counts: dict[str, int]


class ExecutionSimulator:
    """Simulates order execution with slippage and partial fills.

    For each symbol, computes weight delta, determines fill based on
    OHLC band shrinkage, and aggregates slippage / missed-fill stats.
    """

    def __init__(self, config: ExecutionSimConfig, rng: random.Random | None = None):
        self.config = config
        self._rng = rng or random.Random()

    def simulate(
        self,
        prev_weights: np.ndarray,
        target_weights: np.ndarray,
        row_data: dict[str, dict[str, float]],
        portfolio_value: float,
    ) -> ExecutionResult:
        """Simulate execution for one step.

        Args:
            prev_weights: Previous portfolio weights (n_symbols,).
            target_weights: Target weights after projection (n_symbols,).
            row_data: Per-symbol OHLC data. Keys are symbols (sorted),
                values are dicts with 'open', 'high', 'low', 'close'.
            portfolio_value: Current portfolio value for notional computation.

        Returns:
            ExecutionResult with realized weights and execution stats.
        """
        n = len(target_weights)
        realized = np.copy(prev_weights)
        total_slippage_notional = 0.0
        total_traded_notional = 0.0
        missed_count = 0
        trade_count = 0
        unfilled_notional = 0.0
        order_type_counts: dict[str, int] = {"buy": 0, "sell": 0, "hold": 0}

        symbols = sorted(row_data.keys())

        for i, sym in enumerate(symbols):
            delta = target_weights[i] - prev_weights[i]

            if abs(delta) < 1e-10:
                order_type_counts["hold"] += 1
                continue

            trade_count += 1
            order_type = "buy" if delta > 0 else "sell"
            order_type_counts[order_type] += 1

            ohlc = row_data[sym]
            o, h, l, c = ohlc["open"], ohlc["high"], ohlc["low"], ohlc["close"]

            # Shrink the OHLC range
            mid = (h + l) / 2.0
            shrunk_h = mid + (h - mid) * self.config.range_shrink
            shrunk_l = mid - (mid - l) * self.config.range_shrink

            # Apply slippage to the band
            slippage_frac = self._compute_slippage(ohlc)
            if order_type == "buy":
                # Buying pushes price up
                shrunk_l += (shrunk_h - shrunk_l) * slippage_frac
                shrunk_h += (shrunk_h - shrunk_l) * slippage_frac
            else:
                # Selling pushes price down
                shrunk_h -= (shrunk_h - shrunk_l) * slippage_frac
                shrunk_l -= (shrunk_h - shrunk_l) * slippage_frac

            # Test fill probability
            if self._rng.random() > self.config.fill_probability:
                # Missed fill — keep previous weight
                missed_count += 1
                notional = abs(delta) * portfolio_value
                unfilled_notional += notional
                continue

            # Fill succeeded — check if close is within the effective band
            if shrunk_l <= c <= shrunk_h:
                # Full fill at target weight
                realized[i] = target_weights[i]
                traded_notional = abs(delta) * portfolio_value
                total_traded_notional += traded_notional

                # Slippage: difference between ideal (close) and execution band midpoint
                exec_price_offset = slippage_frac * c
                total_slippage_notional += exec_price_offset / c * traded_notional
            else:
                # Price outside band — partial fill at band edge
                realized[i] = target_weights[i]
                traded_notional = abs(delta) * portfolio_value
                total_traded_notional += traded_notional
                total_slippage_notional += slippage_frac * traded_notional

        # Compute aggregate stats
        if total_traded_notional > 0:
            slippage_bps = (total_slippage_notional / total_traded_notional) * 10000
        else:
            slippage_bps = 0.0

        if trade_count > 0:
            missed_ratio = missed_count / trade_count
        else:
            missed_ratio = 0.0

        return ExecutionResult(
            realized_weights=realized,
            execution_slippage_bps=slippage_bps,
            missed_fill_ratio=missed_ratio,
            unfilled_notional=unfilled_notional,
            order_type_counts=order_type_counts,
        )

    def _compute_slippage(self, ohlc: dict[str, float]) -> float:
        """Compute slippage fraction based on the configured model."""
        base_frac = self.config.slippage_bp / 10000.0

        if self.config.slippage_model == "flat":
            return base_frac

        # volume_scaled: scale by inverse of volume (higher volume = less slippage)
        volume = ohlc.get("volume", 0.0)
        if volume > 0:
            # Normalize: assume baseline volume of 1M shares
            scale = min(1e6 / volume, 5.0)  # cap at 5x
            return base_frac * scale
        return base_frac * 5.0  # max slippage if no volume
