"""Equal-weight policy — uniform allocation across universe.

Each symbol gets 1/n weight. Loadable by name as 'equal_weight'.
PRD §7.3.
"""

from __future__ import annotations

from typing import Any, Mapping

import numpy as np


class EqualWeightPolicy:
    """Uniform allocation across all symbols in the universe.

    Implements the allocator interface:
        act(obs, context) -> raw weight vector (n_symbols,)
    """

    def __init__(self, n_symbols: int):
        if n_symbols <= 0:
            raise ValueError(f"n_symbols must be > 0, got {n_symbols}")
        self._n_symbols = n_symbols
        self._weight = 1.0 / n_symbols

    def act(self, obs: np.ndarray, context: Mapping[str, Any] | None = None) -> np.ndarray:
        """Return equal weights for all symbols.

        Args:
            obs: Observation vector (ignored).
            context: Additional context (ignored).

        Returns:
            Weight vector of shape (n_symbols,) with 1/n per symbol.
        """
        return np.full(self._n_symbols, self._weight)
