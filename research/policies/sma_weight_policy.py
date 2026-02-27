"""SMA weight policy — simple moving average crossover signals.

Takes observation vector, produces target weights based on SMA crossover.
Loadable by name as 'sma'. PRD §7.3.
"""

from __future__ import annotations

from collections import deque
from typing import Any, Mapping

import numpy as np


class SMAWeightPolicy:
    """SMA crossover-based weight generation.

    Maintains rolling windows of close prices per symbol. When the short SMA
    is above the long SMA for a symbol, that symbol gets a positive signal.
    Weights are normalized so they sum to 1.0 (if any signals are active).

    Implements the allocator interface:
        act(obs, context) -> raw weight vector (n_symbols,)
    """

    def __init__(
        self,
        n_symbols: int,
        short_window: int = 5,
        long_window: int = 20,
    ):
        if n_symbols <= 0:
            raise ValueError(f"n_symbols must be > 0, got {n_symbols}")
        if short_window <= 0:
            raise ValueError(f"short_window must be > 0, got {short_window}")
        if long_window <= short_window:
            raise ValueError(
                f"long_window ({long_window}) must be > short_window ({short_window})"
            )
        self._n_symbols = n_symbols
        self._short_window = short_window
        self._long_window = long_window
        self._price_history: list[deque[float]] = [
            deque(maxlen=long_window) for _ in range(n_symbols)
        ]

    def act(self, obs: np.ndarray, context: Mapping[str, Any] | None = None) -> np.ndarray:
        """Produce weights from SMA crossover signals.

        Extracts close prices from context['close_prices'] (array of n_symbols)
        if available, otherwise falls back to extracting from the observation
        vector (first n_symbols values assumed to be close prices).

        Args:
            obs: Observation vector.
            context: Additional context. May contain 'close_prices' key.

        Returns:
            Weight vector of shape (n_symbols,). Normalized signals or
            equal-weight fallback if insufficient data.
        """
        context = context or {}

        # Extract close prices
        close_prices = context.get("close_prices")
        if close_prices is not None:
            prices = np.asarray(close_prices, dtype=np.float64)
        else:
            # Fallback: first n_symbols values of obs are close prices
            obs_arr = np.asarray(obs, dtype=np.float64)
            prices = obs_arr[: self._n_symbols]

        # Update price history
        for i in range(self._n_symbols):
            self._price_history[i].append(float(prices[i]))

        # Compute signals
        signals = np.zeros(self._n_symbols)
        for i in range(self._n_symbols):
            history = self._price_history[i]
            if len(history) < self._long_window:
                # Insufficient data — no signal
                continue
            arr = np.array(history)
            short_sma = arr[-self._short_window :].mean()
            long_sma = arr.mean()
            if short_sma > long_sma:
                signals[i] = 1.0

        # Normalize to sum to 1.0 (or equal-weight fallback)
        total = signals.sum()
        if total > 0:
            return signals / total
        else:
            # No bullish signals — equal weight fallback
            return np.full(self._n_symbols, 1.0 / self._n_symbols)

    def reset(self) -> None:
        """Clear price history for a new episode."""
        for h in self._price_history:
            h.clear()
