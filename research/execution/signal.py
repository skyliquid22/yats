"""Signal interface — model output schema for execution pipeline.

Implements PRD §13.1 Signal Interface:
- Signals are bar-aligned, forward-looking, generated after bar close
- Models output target position signals (percentage of NAV), not raw orders
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Signal:
    """Target position signal from a model.

    Signals express desired portfolio state as a fraction of NAV,
    not raw order instructions. The order translator converts these
    into concrete OrderRequests by diffing against current positions.
    """

    timestamp: datetime  # bar-aligned UTC timestamp
    symbol: str  # ticker
    target_position_pct: float  # fraction of NAV, e.g. -0.3 to +0.3
    model_version: str  # policy identifier
    confidence_score: float | None = None  # optional, used for confidence gating
