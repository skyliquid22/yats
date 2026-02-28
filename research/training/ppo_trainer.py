"""PPO Trainer — trains a PPO policy on SignalWeightEnv via stable-baselines3.

Wraps SignalWeightEnv with RewardAdapterEnv, then with the SB3 Gymnasium
wrapper, and trains using SB3's PPO implementation.

PRD §7.2 (PPO Trainer).
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
from research.risk.loader import merge_risk_overrides
from research.training.reward_registry import RewardAdapterEnv
from research.training.sb3_wrapper import SB3EnvWrapper

logger = logging.getLogger(__name__)


@dataclass
class PPOTrainResult:
    """Result of a PPO training run."""

    checkpoint_path: Path
    total_timesteps: int
    final_mean_reward: float
    training_info: dict[str, Any] = field(default_factory=dict)


def train_ppo(
    spec: ExperimentSpec,
    data: list[dict[str, Any]],
    output_dir: Path,
    *,
    observation_columns: list[str] | None = None,
    regime_feature_names: list[str] | None = None,
) -> PPOTrainResult:
    """Train a PPO agent on the given experiment spec and data.

    Args:
        spec: Experiment specification (policy_params must contain PPO hyperparameters).
        data: Pre-built feature rows for training episodes.
        output_dir: Directory to save checkpoints (typically .yats_data/experiments/<ID>/runs/).
        observation_columns: Feature column names for the env. Defaults to extracting from data.
        regime_feature_names: Regime feature columns (scalar features).

    Returns:
        PPOTrainResult with checkpoint path and training metadata.
    """
    from stable_baselines3 import PPO
    from stable_baselines3.common.callbacks import BaseCallback

    params = dict(spec.policy_params)

    # Build SignalWeightEnv
    if observation_columns is None:
        observation_columns = _infer_observation_columns(data)

    # Apply risk overrides for training (PRD §12.2)
    training_risk_config = merge_risk_overrides(spec.risk_config, spec.risk_overrides)

    env_config = SignalWeightEnvConfig(
        symbols=list(spec.symbols),
        observation_columns=observation_columns,
        transaction_cost_bp=spec.cost_config.transaction_cost_bp,
        slippage_bp=spec.cost_config.slippage_bp,
        execution_sim=spec.execution_sim,
        regime_feature_names=regime_feature_names,
        risk_config=training_risk_config,
    )
    base_env = SignalWeightEnv(env_config)

    # Wrap with RewardAdapterEnv
    reward_env = RewardAdapterEnv(base_env, params)

    # Wrap with SB3-compatible Gymnasium wrapper
    gym_env = SB3EnvWrapper(reward_env, len(spec.symbols), data)

    # Extract PPO hyperparameters from policy_params
    learning_rate = float(params.get("learning_rate", 3e-4))
    batch_size = int(params.get("batch_size", 64))
    n_epochs = int(params.get("n_epochs", 10))
    clip_range = float(params.get("clip_range", 0.2))
    gamma = float(params.get("gamma", 0.99))
    gae_lambda = float(params.get("gae_lambda", 0.95))
    ent_coef = float(params.get("ent_coef", 0.0))
    vf_coef = float(params.get("vf_coef", 0.5))
    max_grad_norm = float(params.get("max_grad_norm", 0.5))
    n_steps = int(params.get("n_steps", 2048))
    total_timesteps = int(params.get("total_timesteps", 10000))

    # Create PPO model
    model = PPO(
        "MlpPolicy",
        gym_env,
        learning_rate=learning_rate,
        batch_size=batch_size,
        n_epochs=n_epochs,
        clip_range=clip_range,
        gamma=gamma,
        gae_lambda=gae_lambda,
        ent_coef=ent_coef,
        vf_coef=vf_coef,
        max_grad_norm=max_grad_norm,
        n_steps=n_steps,
        seed=spec.seed,
        verbose=0,
    )

    # Train
    logger.info(
        "Training PPO for %d timesteps (lr=%g, batch=%d, epochs=%d)",
        total_timesteps, learning_rate, batch_size, n_epochs,
    )
    model.learn(total_timesteps=total_timesteps)

    # Save checkpoint
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = output_dir / "ppo_checkpoint"
    model.save(str(checkpoint_path))

    # Save training metadata
    meta = {
        "algorithm": "PPO",
        "total_timesteps": total_timesteps,
        "hyperparameters": {
            "learning_rate": learning_rate,
            "batch_size": batch_size,
            "n_epochs": n_epochs,
            "clip_range": clip_range,
            "gamma": gamma,
            "gae_lambda": gae_lambda,
            "ent_coef": ent_coef,
            "vf_coef": vf_coef,
            "max_grad_norm": max_grad_norm,
            "n_steps": n_steps,
        },
        "reward_version": params.get("reward_version", "v1"),
        "seed": spec.seed,
    }
    meta_path = output_dir / "training_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")

    logger.info("PPO checkpoint saved to %s", checkpoint_path)

    return PPOTrainResult(
        checkpoint_path=checkpoint_path,
        total_timesteps=total_timesteps,
        final_mean_reward=0.0,  # SB3 doesn't expose this easily without callbacks
        training_info=meta,
    )


def rollout_ppo(
    spec: ExperimentSpec,
    data: list[dict[str, Any]],
    checkpoint_path: Path,
    *,
    observation_columns: list[str] | None = None,
    regime_feature_names: list[str] | None = None,
) -> tuple[list[np.ndarray], list[float], list[dict[str, Any]]]:
    """Roll out a trained PPO policy on data for evaluation.

    Args:
        spec: Experiment specification.
        data: Pre-built feature rows.
        checkpoint_path: Path to saved SB3 model.
        observation_columns: Feature column names.
        regime_feature_names: Regime feature columns.

    Returns:
        (weights_list, rewards_list, infos_list) for each step.
    """
    from stable_baselines3 import PPO

    params = dict(spec.policy_params)

    if observation_columns is None:
        observation_columns = _infer_observation_columns(data)

    # Apply risk overrides for rollout (PRD §12.2)
    rollout_risk_config = merge_risk_overrides(spec.risk_config, spec.risk_overrides)

    env_config = SignalWeightEnvConfig(
        symbols=list(spec.symbols),
        observation_columns=observation_columns,
        transaction_cost_bp=spec.cost_config.transaction_cost_bp,
        slippage_bp=spec.cost_config.slippage_bp,
        execution_sim=spec.execution_sim,
        regime_feature_names=regime_feature_names,
        risk_config=rollout_risk_config,
    )
    base_env = SignalWeightEnv(env_config)
    reward_env = RewardAdapterEnv(base_env, params)
    gym_env = SB3EnvWrapper(reward_env, len(spec.symbols), data)

    model = PPO.load(str(checkpoint_path), env=gym_env)

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
    """Infer observation columns from data rows.

    Returns columns whose values are dicts (per-symbol data).
    Always includes 'close'.
    """
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
