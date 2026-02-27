"""reward_v1 — Identity reward function.

Returns the base environment reward unchanged. PRD §7.2 / Appendix B.2.
"""

from __future__ import annotations

from typing import Any


def reward_v1(base_reward: float, info: dict[str, Any]) -> tuple[float, dict[str, Any]]:
    """Identity reward — pass through base reward unchanged.

    Args:
        base_reward: Raw log-return from SignalWeightEnv.
        info: Step info dict from the base environment.

    Returns:
        (reward, info) tuple with reward unchanged and info unmodified.
    """
    return base_reward, info
