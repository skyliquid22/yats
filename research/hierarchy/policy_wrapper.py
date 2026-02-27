"""Hierarchical policy wrapper — ModeController + per-mode allocators.

Wraps ModeController and per-mode allocators into a single policy that
can be used by the runner, shadow engine, and execution loop.
PRD Appendix E (§E.5).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Protocol

import numpy as np

from research.hierarchy.controller import ModeController


class Allocator(Protocol):
    """Protocol for allocators — any callable matching the PRD E.5 interface."""

    def act(self, obs: np.ndarray, context: Mapping[str, Any] | None = None) -> np.ndarray:
        ...


class HierarchicalPolicy:
    """Wraps ModeController + per-mode allocators into a single policy.

    The controller selects a mode based on regime features, then the
    corresponding allocator produces raw weights.

    Args:
        controller: ModeController instance.
        allocators: Mapping from mode name to allocator instance.
    """

    def __init__(
        self,
        controller: ModeController,
        allocators: Mapping[str, Allocator],
    ):
        required_modes = {"risk_on", "neutral", "defensive"}
        missing = required_modes - set(allocators)
        if missing:
            raise ValueError(f"Missing allocators for modes: {sorted(missing)}")
        self._controller = controller
        self._allocators = dict(allocators)

    @property
    def controller(self) -> ModeController:
        return self._controller

    @property
    def current_mode(self) -> str:
        return self._controller.current_mode

    @property
    def timeline(self) -> list[dict[str, Any]]:
        return self._controller.timeline

    def act(
        self,
        obs: np.ndarray,
        context: Mapping[str, Any] | None = None,
    ) -> np.ndarray:
        """Produce weights from the active mode's allocator.

        Steps the controller with regime features from context, then
        delegates to the corresponding allocator.

        Args:
            obs: Observation vector.
            context: Must contain 'regime_features' dict with keys
                'market_vol_20d', 'market_trend_20d', 'dispersion_20d'.

        Returns:
            Raw weight vector (n_symbols,) from the active allocator.
        """
        context = context or {}
        regime_features = context.get("regime_features", {})

        # Step the controller to get current mode
        mode = self._controller.step(regime_features)

        # Delegate to the mode's allocator
        allocator = self._allocators[mode]
        enriched_context = dict(context)
        enriched_context["mode"] = mode
        return allocator.act(obs, enriched_context)

    def reset(self) -> None:
        """Reset controller and allocators for a new episode."""
        self._controller.reset()
        for allocator in self._allocators.values():
            if hasattr(allocator, "reset"):
                allocator.reset()

    def save_timeline(self, path: Path | str) -> None:
        """Write mode_timeline.json."""
        self._controller.save_timeline(path)
