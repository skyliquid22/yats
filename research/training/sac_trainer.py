"""SAC Trainer — trains a SAC policy on SignalWeightEnv via stable-baselines3.

Same env wrapping and reward versioning as PPO, different algorithm.

PRD §7.2 (SAC Trainer).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from research.envs.signal_weight_env import SignalWeightEnv, SignalWeightEnvConfig
from research.experiments.spec import ExperimentSpec
from research.training.reward_registry import RewardAdapterEnv
from research.training.sb3_wrapper import SB3EnvWrapper

logger = logging.getLogger(__name__)


@dataclass
class SACTrainResult:
    """Result of a SAC training run."""

    checkpoint_path: Path
    total_timesteps: int
    final_mean_reward: float
    training_info: dict[str, Any] = field(default_factory=dict)


def train_sac(
    spec: ExperimentSpec,
    data: list[dict[str, Any]],
    output_dir: Path,
    *,
    observation_columns: list[str] | None = None,
    regime_feature_names: list[str] | None = None,
) -> SACTrainResult:
    """Train a SAC agent on the given experiment spec and data.

    Args:
        spec: Experiment specification (policy_params must contain SAC hyperparameters).
        data: Pre-built feature rows for training episodes.
        output_dir: Directory to save checkpoints (typically .yats_data/experiments/<ID>/runs/).
        observation_columns: Feature column names for the env.
        regime_feature_names: Regime feature columns.

    Returns:
        SACTrainResult with checkpoint path and training metadata.
    """
    from stable_baselines3 import SAC

    params = dict(spec.policy_params)

    # Build SignalWeightEnv
    if observation_columns is None:
        observation_columns = _infer_observation_columns(data)

    env_config = SignalWeightEnvConfig(
        symbols=list(spec.symbols),
        observation_columns=observation_columns,
        transaction_cost_bp=spec.cost_config.transaction_cost_bp,
        slippage_bp=spec.cost_config.slippage_bp,
        execution_sim=spec.execution_sim,
        regime_feature_names=regime_feature_names,
        risk_config=spec.risk_config,
    )
    base_env = SignalWeightEnv(env_config)

    # Wrap with RewardAdapterEnv
    reward_env = RewardAdapterEnv(base_env, params)

    # Wrap with SB3-compatible Gymnasium wrapper
    gym_env = SB3EnvWrapper(reward_env, len(spec.symbols), data)

    # Extract SAC hyperparameters from policy_params
    learning_rate = float(params.get("learning_rate", 3e-4))
    batch_size = int(params.get("batch_size", 256))
    gamma = float(params.get("gamma", 0.99))
    tau = float(params.get("tau", 0.005))
    ent_coef = params.get("ent_coef", "auto")
    learning_starts = int(params.get("learning_starts", 100))
    buffer_size = int(params.get("buffer_size", 1_000_000))
    train_freq = int(params.get("train_freq", 1))
    gradient_steps = int(params.get("gradient_steps", 1))
    total_timesteps = int(params.get("total_timesteps", 10000))

    # Create SAC model
    model = SAC(
        "MlpPolicy",
        gym_env,
        learning_rate=learning_rate,
        batch_size=batch_size,
        gamma=gamma,
        tau=tau,
        ent_coef=ent_coef,
        learning_starts=learning_starts,
        buffer_size=buffer_size,
        train_freq=train_freq,
        gradient_steps=gradient_steps,
        seed=spec.seed,
        verbose=0,
    )

    # Train
    logger.info(
        "Training SAC for %d timesteps (lr=%g, batch=%d)",
        total_timesteps, learning_rate, batch_size,
    )
    model.learn(total_timesteps=total_timesteps)

    # Save checkpoint
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = output_dir / "sac_checkpoint"
    model.save(str(checkpoint_path))

    # Save training metadata
    meta = {
        "algorithm": "SAC",
        "total_timesteps": total_timesteps,
        "hyperparameters": {
            "learning_rate": learning_rate,
            "batch_size": batch_size,
            "gamma": gamma,
            "tau": tau,
            "ent_coef": str(ent_coef),
            "learning_starts": learning_starts,
            "buffer_size": buffer_size,
            "train_freq": train_freq,
            "gradient_steps": gradient_steps,
        },
        "reward_version": params.get("reward_version", "v1"),
        "seed": spec.seed,
    }
    meta_path = output_dir / "training_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")

    logger.info("SAC checkpoint saved to %s", checkpoint_path)

    return SACTrainResult(
        checkpoint_path=checkpoint_path,
        total_timesteps=total_timesteps,
        final_mean_reward=0.0,
        training_info=meta,
    )


def rollout_sac(
    spec: ExperimentSpec,
    data: list[dict[str, Any]],
    checkpoint_path: Path,
    *,
    observation_columns: list[str] | None = None,
    regime_feature_names: list[str] | None = None,
) -> tuple[list[np.ndarray], list[float], list[dict[str, Any]]]:
    """Roll out a trained SAC policy on data for evaluation.

    Args:
        spec: Experiment specification.
        data: Pre-built feature rows.
        checkpoint_path: Path to saved SB3 model.
        observation_columns: Feature column names.
        regime_feature_names: Regime feature columns.

    Returns:
        (weights_list, rewards_list, infos_list) for each step.
    """
    from stable_baselines3 import SAC

    params = dict(spec.policy_params)

    if observation_columns is None:
        observation_columns = _infer_observation_columns(data)

    env_config = SignalWeightEnvConfig(
        symbols=list(spec.symbols),
        observation_columns=observation_columns,
        transaction_cost_bp=spec.cost_config.transaction_cost_bp,
        slippage_bp=spec.cost_config.slippage_bp,
        execution_sim=spec.execution_sim,
        regime_feature_names=regime_feature_names,
        risk_config=spec.risk_config,
    )
    base_env = SignalWeightEnv(env_config)
    reward_env = RewardAdapterEnv(base_env, params)
    gym_env = SB3EnvWrapper(reward_env, len(spec.symbols), data)

    model = SAC.load(str(checkpoint_path), env=gym_env)

    obs, _ = gym_env.reset()
    weights_list: list[np.ndarray] = []
    rewards_list: list[float] = []
    infos_list: list[dict[str, Any]] = []

    done = False
    while not done:
        action, _ = model.predict(obs, deterministic=True)
        obs, reward, done, _, info = gym_env.step(action)
        weights_list.append(info["weight_realized"].copy())
        rewards_list.append(reward)
        infos_list.append(info)

    return weights_list, rewards_list, infos_list


def _infer_observation_columns(data: list[dict[str, Any]]) -> list[str]:
    """Infer observation columns from data rows."""
    if not data:
        return ["close"]
    row = data[0]
    cols = []
    for key, val in sorted(row.items()):
        if key == "timestamp":
            continue
        if isinstance(val, dict):
            cols.append(key)
    if "close" not in cols:
        cols.append("close")
    return cols
