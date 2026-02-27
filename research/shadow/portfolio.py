"""Shadow portfolio tracker — value, positions, weights, cash, drawdown.

Tracks portfolio state across shadow replay steps.  Used by ShadowEngine
to maintain and persist the portfolio through direct-rebalance execution.

PRD Appendix F.2 / P3.2.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np


@dataclass
class ShadowPortfolio:
    """Mutable portfolio state for shadow replay.

    Attributes:
        portfolio_value: Current total portfolio value.
        cash: Cash portion (unused in direct-rebalance mode, tracked for resume).
        weights: Current weight vector (n_symbols,).
        positions: Per-symbol notional positions (derived from weights * value).
        peak_value: High-water mark for drawdown calculation.
        symbols: Ordered symbol names.
    """

    symbols: tuple[str, ...]
    portfolio_value: float = 1_000_000.0
    cash: float = 0.0
    peak_value: float = 0.0
    weights: np.ndarray = field(default_factory=lambda: np.array([]))
    positions: dict[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.weights.size == 0:
            self.weights = np.zeros(len(self.symbols))
        if not self.positions:
            self.positions = {s: 0.0 for s in self.symbols}
        if self.peak_value == 0.0:
            self.peak_value = self.portfolio_value

    def apply_step(
        self,
        new_weights: np.ndarray,
        weighted_return: float,
        cost: float,
    ) -> None:
        """Apply a single step's returns and costs to the portfolio.

        Args:
            new_weights: Projected weights for this step.
            weighted_return: Portfolio-weighted return for this period.
            cost: Transaction cost as a fraction (not bps).
        """
        self.portfolio_value = self.portfolio_value * (1.0 + weighted_return - cost)
        self.weights = new_weights.copy()

        # Update positions from weights * value
        for i, sym in enumerate(self.symbols):
            self.positions[sym] = float(self.weights[i] * self.portfolio_value)

        # Cash = value not allocated
        allocated = float(self.weights.sum() * self.portfolio_value)
        self.cash = self.portfolio_value - allocated

        # Track high-water mark
        if self.portfolio_value > self.peak_value:
            self.peak_value = self.portfolio_value

    @property
    def drawdown(self) -> float:
        """Current drawdown from peak (negative value, 0.0 = no drawdown)."""
        if self.peak_value <= 0:
            return 0.0
        return (self.portfolio_value - self.peak_value) / self.peak_value

    def to_state_dict(self) -> dict[str, Any]:
        """Serialize to a dict suitable for state.json."""
        return {
            "positions": dict(self.positions),
            "weights": self.weights.tolist(),
            "portfolio_value": self.portfolio_value,
            "cash": self.cash,
            "peak_value": self.peak_value,
        }

    @classmethod
    def from_state_dict(
        cls, symbols: tuple[str, ...], state: dict[str, Any],
    ) -> ShadowPortfolio:
        """Restore from a state.json dict."""
        return cls(
            symbols=symbols,
            portfolio_value=state["portfolio_value"],
            cash=state.get("cash", 0.0),
            peak_value=state.get("peak_value", state["portfolio_value"]),
            weights=np.array(state["weights"], dtype=np.float64),
            positions=state.get("positions", {}),
        )
