"""SB3-compatible Gymnasium wrapper for RewardAdapterEnv.

Adapts the old Gym API (reset() -> obs, step() -> (obs, r, done, info))
to the Gymnasium API (reset() -> (obs, info), step() -> (obs, r, term, trunc, info))
with proper Box observation/action spaces for stable-baselines3.

PRD §7.2.
"""

from __future__ import annotations

from typing import Any

import numpy as np


class SB3EnvWrapper:
    """Gymnasium-compatible wrapper around RewardAdapterEnv for SB3.

    SB3 requires:
      - observation_space: gymnasium.spaces.Box
      - action_space: gymnasium.spaces.Box
      - reset() -> (obs_array, info_dict)
      - step(action) -> (obs_array, reward, terminated, truncated, info)

    Args:
        env: A RewardAdapterEnv instance (wrapping SignalWeightEnv).
        n_symbols: Number of symbols in the portfolio.
        data: Pre-built feature rows for the episode.
    """

    def __init__(
        self,
        env: Any,
        n_symbols: int,
        data: list[dict[str, Any]],
    ) -> None:
        import gymnasium
        from gymnasium import spaces

        self._env = env
        self._data = data
        self._n_symbols = n_symbols
        obs_len = env.observation_length

        self.observation_space = spaces.Box(
            low=-np.inf, high=np.inf, shape=(obs_len,), dtype=np.float32,
        )
        self.action_space = spaces.Box(
            low=0.0, high=1.0, shape=(n_symbols,), dtype=np.float32,
        )

        # Gymnasium metadata
        self.metadata: dict[str, Any] = {}
        self.render_mode = None
        self.spec = None

    def reset(
        self,
        *,
        seed: int | None = None,
        options: dict[str, Any] | None = None,
    ) -> tuple[np.ndarray, dict[str, Any]]:
        """Reset the environment (Gymnasium API).

        Returns:
            (observation, info) tuple.
        """
        obs_tuple = self._env.reset(data=self._data)
        obs = np.array(obs_tuple, dtype=np.float32)
        return obs, {}

    def step(self, action: np.ndarray) -> tuple[np.ndarray, float, bool, bool, dict[str, Any]]:
        """Execute one step (Gymnasium API).

        Returns:
            (observation, reward, terminated, truncated, info) tuple.
        """
        obs_tuple, reward, done, info = self._env.step(action)
        obs = np.array(obs_tuple, dtype=np.float32)
        return obs, float(reward), done, False, info

    def render(self) -> None:
        pass

    def close(self) -> None:
        pass
