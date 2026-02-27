"""RewardAdapterEnv — versioned reward wrapper for SignalWeightEnv.

Selects reward function based on policy_params.reward_version from the
experiment spec. PRD §7.2 / Appendix B.1.
"""

from __future__ import annotations

from typing import Any

import numpy as np

from research.envs.signal_weight_env import SignalWeightEnv
from research.training.rewards.reward_v1 import reward_v1
from research.training.rewards.reward_v2 import (
    RewardV2Config,
    RewardV2State,
    reward_v2,
)

# Registry of reward functions by version string
_REWARD_VERSIONS = {"v1", "v2"}


class RewardAdapterEnv:
    """Wrapper env that applies versioned reward shaping to SignalWeightEnv.

    Wraps a SignalWeightEnv and intercepts step() to transform the raw
    log-return reward via the selected reward function.

    Implements old Gym API:
        reset() -> observation (tuple)
        step(action) -> (observation, reward, done, info)

    Args:
        env: The base SignalWeightEnv to wrap.
        policy_params: Policy parameters dict. Must contain 'reward_version'.
    """

    def __init__(self, env: SignalWeightEnv, policy_params: dict[str, Any]) -> None:
        self._env = env
        self._reward_version = str(policy_params.get("reward_version", "v1"))

        if self._reward_version not in _REWARD_VERSIONS:
            raise ValueError(
                f"Unknown reward_version '{self._reward_version}', "
                f"must be one of {sorted(_REWARD_VERSIONS)}"
            )

        # State for reward_v2
        self._v2_config: RewardV2Config | None = None
        self._v2_state: RewardV2State | None = None
        self._prev_weights: np.ndarray = np.array([])

        if self._reward_version == "v2":
            self._v2_config = RewardV2Config(
                turnover_scale=float(policy_params.get("turnover_scale", 0.10)),
                drawdown_scale=float(policy_params.get("drawdown_scale", 0.50)),
                cost_scale=float(policy_params.get("cost_scale", 0.00)),
                clip_reward=policy_params.get("clip_reward"),
            )
            self._v2_state = RewardV2State()

    @property
    def reward_version(self) -> str:
        """The active reward version string."""
        return self._reward_version

    @property
    def observation_length(self) -> int:
        """Delegate to wrapped env."""
        return self._env.observation_length

    def reset(self, data: list[dict[str, Any]] | None = None) -> tuple:
        """Reset the wrapped environment and reward state.

        Args:
            data: Pre-built feature rows (passed through to base env).

        Returns:
            Initial observation from the base environment.
        """
        obs = self._env.reset(data=data)
        n_symbols = self._env._n_symbols
        self._prev_weights = np.zeros(n_symbols)

        if self._v2_state is not None:
            self._v2_state.reset()

        return obs

    def step(self, action: np.ndarray | list | tuple, **kwargs: Any) -> tuple:
        """Execute one step with reward shaping applied.

        Args:
            action: Target portfolio weights.
            **kwargs: Passed through to base env.

        Returns:
            (observation, shaped_reward, done, info) tuple.
        """
        obs, base_reward, done, info = self._env.step(action, **kwargs)

        if self._reward_version == "v1":
            reward, info = reward_v1(base_reward, info)
        else:
            reward, info = reward_v2(
                base_reward,
                info,
                self._v2_config,
                self._v2_state,
                self._prev_weights,
            )

        # Track weights for next step's turnover calculation
        self._prev_weights = info["weight_realized"].copy()

        return obs, reward, done, info
