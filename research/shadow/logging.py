"""Shadow step logging — write per-step entries to steps.jsonl.

Each line is a JSON object capturing the full state of a single shadow
replay step: timestamp, weights, returns, costs, portfolio_value.

PRD Appendix F.2 / P3.2.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class StepLogger:
    """Append-only JSONL writer for shadow replay steps.

    Each call to :meth:`log_step` appends one JSON line to the output file.
    The file is opened in append mode so resume runs continue from where
    they left off.
    """

    def __init__(self, path: Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def log_step(self, entry: dict[str, Any]) -> None:
        """Append a single step entry as a JSON line."""
        with open(self._path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=_json_default) + "\n")

    @property
    def path(self) -> Path:
        return self._path


def build_step_entry(
    *,
    step_index: int,
    timestamp: str,
    weights: list[float],
    previous_weights: list[float],
    symbols: tuple[str, ...],
    returns_per_symbol: list[float],
    weighted_return: float,
    cost: float,
    portfolio_value: float,
    peak_value: float,
    drawdown: float,
) -> dict[str, Any]:
    """Build a step log entry dict per PRD F.2."""
    return {
        "step_index": step_index,
        "timestamp": timestamp,
        "symbols": list(symbols),
        "weights": weights,
        "previous_weights": previous_weights,
        "returns_per_symbol": returns_per_symbol,
        "weighted_return": weighted_return,
        "transaction_cost": cost,
        "portfolio_value": portfolio_value,
        "peak_value": peak_value,
        "drawdown": drawdown,
    }


def _json_default(obj: Any) -> Any:
    """JSON serialization fallback for numpy types etc."""
    import numpy as np

    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.float32, np.float64)):
        return float(obj)
    if isinstance(obj, (np.int32, np.int64)):
        return int(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
