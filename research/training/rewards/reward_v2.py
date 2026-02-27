"""reward_v2 — Multi-component shaped reward.

Applies turnover, drawdown, and cost penalties to the base log-return.
PRD §7.2 / Appendix B.3.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np


@dataclass
class RewardV2Config:
    """Configuration for reward_v2 penalty scaling."""

    turnover_scale: float = 0.10
    drawdown_scale: float = 0.50
    cost_scale: float = 0.00
    clip_reward: tuple[float, float] | None = None


class RewardV2State:
    """Tracks state needed across steps for reward_v2 (drawdown tracking)."""

    def __init__(self) -> None:
        self._peak_value: float = 1.0
        self._prev_drawdown: float = 0.0

    def reset(self) -> None:
        self._peak_value = 1.0
        self._prev_drawdown = 0.0

    def update(self, portfolio_value: float) -> float:
        """Update drawdown tracking and return drawdown increase.

        Args:
            portfolio_value: Current portfolio value after step.

        Returns:
            Non-negative drawdown increase since last step.
        """
        self._peak_value = max(self._peak_value, portfolio_value)
        current_drawdown = (
            (self._peak_value - portfolio_value) / self._peak_value
            if self._peak_value > 0
            else 0.0
        )
        drawdown_increase = max(0.0, current_drawdown - self._prev_drawdown)
        self._prev_drawdown = current_drawdown
        return drawdown_increase


def reward_v2(
    base_reward: float,
    info: dict[str, Any],
    config: RewardV2Config,
    state: RewardV2State,
    prev_weights: np.ndarray,
) -> tuple[float, dict[str, Any]]:
    """Multi-component shaped reward with configurable penalties.

    Args:
        base_reward: Raw log-return from SignalWeightEnv.
        info: Step info dict from the base environment.
        config: Penalty scaling configuration.
        state: Drawdown tracking state (mutated in place).
        prev_weights: Portfolio weights before this step.

    Returns:
        (reward, info) tuple with shaped reward and reward_components added to info.
    """
    realized_weights = info["weight_realized"]

    # Turnover penalty: L1 weight change, halved
    turnover = 0.5 * float(np.sum(np.abs(realized_weights - prev_weights)))
    turnover_penalty = turnover * config.turnover_scale

    # Drawdown penalty: only penalize increases
    portfolio_value = info["portfolio_value"]
    drawdown_increase = state.update(portfolio_value)
    drawdown_penalty = drawdown_increase * config.drawdown_scale

    # Cost penalty (off by default)
    cost_amount = info.get("cost_paid", 0.0)
    # prev portfolio value = current / exp(base_reward) approximation
    # More precise: we use the value before this step's return
    prev_portfolio_value = (
        portfolio_value / np.exp(base_reward) if base_reward > -10.0 else portfolio_value
    )
    cost_penalty = (
        (cost_amount / prev_portfolio_value) * config.cost_scale
        if prev_portfolio_value > 0
        else 0.0
    )

    # Final reward
    final_reward = base_reward - turnover_penalty - drawdown_penalty - cost_penalty

    # Optional clipping
    if config.clip_reward is not None:
        lo, hi = config.clip_reward
        final_reward = max(lo, min(hi, final_reward))

    # Extend info dict
    info["reward_components"] = {
        "base": base_reward,
        "turnover_penalty": turnover_penalty,
        "drawdown_penalty": drawdown_penalty,
        "cost_penalty": cost_penalty,
        "final": final_reward,
    }

    return final_reward, info
