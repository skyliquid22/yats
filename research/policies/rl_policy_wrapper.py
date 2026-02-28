"""RL policy wrapper — adapts SB3 PPO/SAC models to PolicyProtocol.

Loads a trained SB3 checkpoint and wraps it so that `act(obs) -> weights`
matches the interface expected by ShadowEngine.

PRD Appendix F.2: RL policy loading for shadow replay.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Mapping

import numpy as np

logger = logging.getLogger(__name__)


class RLPolicyWrapper:
    """Wraps a stable-baselines3 model to match PolicyProtocol.act().

    SB3 models expose `predict(obs, deterministic=True) -> (action, states)`.
    This wrapper calls predict and returns the action as a float64 weight vector.
    """

    def __init__(self, model: Any, n_symbols: int) -> None:
        self._model = model
        self._n_symbols = n_symbols

    def act(
        self, obs: np.ndarray, context: Mapping[str, Any] | None = None,
    ) -> np.ndarray:
        """Produce target weights from observation.

        Args:
            obs: Flat observation vector (same format as SignalWeightEnv).
            context: Ignored — SB3 models don't use context.

        Returns:
            Weight vector of shape (n_symbols,).
        """
        obs_f32 = np.asarray(obs, dtype=np.float32)
        action, _ = self._model.predict(obs_f32, deterministic=True)
        return np.asarray(action, dtype=np.float64).flatten()[: self._n_symbols]


def load_rl_checkpoint(
    policy_name: str,
    checkpoint_dir: Path,
    n_symbols: int,
) -> RLPolicyWrapper:
    """Load an RL checkpoint and return a wrapped policy.

    Args:
        policy_name: "ppo", "sac", or "sac_*" variant.
        checkpoint_dir: Directory containing the checkpoint file
            (e.g. .yats_data/experiments/<ID>/runs/).
        n_symbols: Number of symbols in the universe.

    Returns:
        RLPolicyWrapper matching PolicyProtocol.

    Raises:
        FileNotFoundError: If checkpoint file doesn't exist.
        ValueError: If policy_name is not a recognized RL policy.
    """
    if policy_name == "ppo":
        from stable_baselines3 import PPO

        checkpoint_path = checkpoint_dir / "ppo_checkpoint"
        if not checkpoint_path.exists() and not checkpoint_path.with_suffix(".zip").exists():
            raise FileNotFoundError(
                f"PPO checkpoint not found at {checkpoint_path} "
                f"(looked for .zip too)"
            )
        model = PPO.load(str(checkpoint_path))
        logger.info("Loaded PPO checkpoint from %s", checkpoint_path)

    elif policy_name == "sac" or policy_name.startswith("sac_"):
        from stable_baselines3 import SAC

        checkpoint_path = checkpoint_dir / "sac_checkpoint"
        if not checkpoint_path.exists() and not checkpoint_path.with_suffix(".zip").exists():
            raise FileNotFoundError(
                f"SAC checkpoint not found at {checkpoint_path} "
                f"(looked for .zip too)"
            )
        model = SAC.load(str(checkpoint_path))
        logger.info("Loaded SAC checkpoint from %s", checkpoint_path)

    else:
        raise ValueError(f"Unknown RL policy: {policy_name}")

    return RLPolicyWrapper(model, n_symbols)
