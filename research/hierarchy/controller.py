"""ModeController — rule-based regime mode selection.

Implements PRD Appendix E (§E.2-E.4): three modes (risk_on, neutral, defensive),
update cadence, hold periods, and mode transition logging.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


VALID_MODES = ("risk_on", "neutral", "defensive")
VALID_FREQUENCIES = ("weekly", "monthly", "every_k_steps")
_FREQ_STEPS = {"weekly": 5, "monthly": 21}


@dataclass
class ModeController:
    """Rule-based mode controller per PRD Appendix E.

    Evaluates regime features at a configurable cadence and selects one of
    three modes: risk_on, neutral, defensive. Respects minimum hold period
    after mode changes.

    Args:
        vol_threshold_high: Vol level that triggers defensive mode.
        trend_threshold_high: Trend level for risk_on consideration.
        dispersion_threshold_high: Dispersion level for risk_on consideration.
        update_frequency: When to re-evaluate ('weekly', 'monthly', 'every_k_steps').
        min_hold_steps: Minimum steps after a mode change before next change.
        k_steps: Step interval when update_frequency='every_k_steps'.
    """

    vol_threshold_high: float
    trend_threshold_high: float
    dispersion_threshold_high: float
    update_frequency: str
    min_hold_steps: int
    k_steps: int | None = None

    # Internal state
    _current_mode: str = field(default="neutral", init=False, repr=False)
    _step_count: int = field(default=0, init=False, repr=False)
    _steps_since_last_change: int = field(default=0, init=False, repr=False)
    _steps_since_last_update: int = field(default=0, init=False, repr=False)
    _timeline: list[dict[str, Any]] = field(default_factory=list, init=False, repr=False)

    def __post_init__(self):
        if self.update_frequency not in VALID_FREQUENCIES:
            raise ValueError(
                f"update_frequency must be one of {VALID_FREQUENCIES}, "
                f"got '{self.update_frequency}'"
            )
        if self.update_frequency == "every_k_steps" and self.k_steps is None:
            raise ValueError("k_steps required when update_frequency='every_k_steps'")
        if self.min_hold_steps < 0:
            raise ValueError(f"min_hold_steps must be >= 0, got {self.min_hold_steps}")

        # Log initial mode
        self._timeline.append({
            "step": 0,
            "mode": self._current_mode,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> ModeController:
        """Create from controller_config dict (as in ExperimentSpec)."""
        return cls(
            vol_threshold_high=config["vol_threshold_high"],
            trend_threshold_high=config["trend_threshold_high"],
            dispersion_threshold_high=config["dispersion_threshold_high"],
            update_frequency=config["update_frequency"],
            min_hold_steps=config["min_hold_steps"],
            k_steps=config.get("k_steps"),
        )

    @property
    def current_mode(self) -> str:
        return self._current_mode

    @property
    def timeline(self) -> list[dict[str, Any]]:
        return list(self._timeline)

    def step(self, regime_features: Mapping[str, float]) -> str:
        """Advance one step and return the current mode.

        Re-evaluates mode if update is due and hold period has elapsed.
        Otherwise returns the previous mode unchanged.

        Args:
            regime_features: Dict with keys 'market_vol_20d', 'market_trend_20d',
                'dispersion_20d'.

        Returns:
            The active mode string.
        """
        self._step_count += 1
        self._steps_since_last_change += 1
        self._steps_since_last_update += 1

        if self._should_update():
            self._steps_since_last_update = 0
            if self._steps_since_last_change >= self.min_hold_steps:
                new_mode = self._select_mode(regime_features)
                if new_mode != self._current_mode:
                    self._current_mode = new_mode
                    self._steps_since_last_change = 0
                    self._timeline.append({
                        "step": self._step_count,
                        "mode": new_mode,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    })

        return self._current_mode

    def _should_update(self) -> bool:
        """Check if it's time to re-evaluate the mode."""
        if self.update_frequency == "every_k_steps":
            return self._steps_since_last_update >= self.k_steps
        interval = _FREQ_STEPS[self.update_frequency]
        return self._steps_since_last_update >= interval

    def _select_mode(self, regime_features: Mapping[str, float]) -> str:
        """Apply the decision logic from PRD E.2."""
        vol = regime_features.get("market_vol_20d", 0.0)
        trend = regime_features.get("market_trend_20d", 0.0)
        dispersion = regime_features.get("dispersion_20d", 0.0)

        if vol >= self.vol_threshold_high:
            return "defensive"
        elif (trend >= self.trend_threshold_high
              and dispersion >= self.dispersion_threshold_high):
            return "risk_on"
        else:
            return "neutral"

    def reset(self) -> None:
        """Reset controller state for a new episode."""
        self._current_mode = "neutral"
        self._step_count = 0
        self._steps_since_last_change = 0
        self._steps_since_last_update = 0
        self._timeline = [{
            "step": 0,
            "mode": "neutral",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }]

    def save_timeline(self, path: Path | str) -> None:
        """Write mode_timeline.json per PRD E.4."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(self._timeline, f, indent=2)
