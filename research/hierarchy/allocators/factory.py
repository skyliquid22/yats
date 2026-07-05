"""Allocator factory — builds per-mode allocators from ExperimentSpec.allocator_by_mode.

Each allocator config dict must have a 'type' key. Supported types:
  equal_weight  — EqualWeightPolicy
  sma           — SMAWeightPolicy (params: short_window, long_window)
  ppo / sac / sac_*  — RL checkpoint (requires spec for checkpoint path)
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping

if TYPE_CHECKING:
    from research.experiments.spec import ExperimentSpec


def build_allocator(
    config: Mapping[str, Any],
    n_symbols: int,
    spec: "ExperimentSpec | None" = None,
) -> Any:
    """Build a single allocator from a config dict.

    Args:
        config: Must contain 'type'. Extra keys are passed as constructor params.
        n_symbols: Number of symbols in the universe.
        spec: ExperimentSpec — required for RL checkpoint allocators.

    Returns:
        An allocator instance with act(obs, context) -> np.ndarray.
    """
    alloc_type = config.get("type")
    if alloc_type is None:
        raise ValueError("Allocator config must include a 'type' key")

    if alloc_type == "equal_weight":
        from research.policies.equal_weight_policy import EqualWeightPolicy
        return EqualWeightPolicy(n_symbols)

    if alloc_type == "sma":
        from research.policies.sma_weight_policy import SMAWeightPolicy
        params = {k: v for k, v in config.items() if k != "type"}
        return SMAWeightPolicy(n_symbols, **params)

    if alloc_type in ("ppo", "sac") or alloc_type.startswith("sac_"):
        from research.policies.rl_policy_wrapper import load_rl_checkpoint
        data_root = Path(".yats_data")
        if spec is not None:
            checkpoint_dir = data_root / "experiments" / spec.experiment_id / "runs"
        else:
            checkpoint_dir = data_root / "experiments" / "unknown" / "runs"
        return load_rl_checkpoint(alloc_type, checkpoint_dir, n_symbols)

    raise ValueError(f"Unknown allocator type: {alloc_type!r}")


def build_allocators(
    allocator_by_mode: Mapping[str, Mapping[str, Any]],
    n_symbols: int,
    spec: "ExperimentSpec | None" = None,
) -> dict[str, Any]:
    """Build per-mode allocator dict from ExperimentSpec.allocator_by_mode.

    Args:
        allocator_by_mode: Mapping from mode name to allocator config dict.
        n_symbols: Number of symbols in the universe.
        spec: ExperimentSpec — required for RL checkpoint allocators.

    Returns:
        Dict mapping mode name to allocator instance.
    """
    return {
        mode: build_allocator(cfg, n_symbols, spec)
        for mode, cfg in allocator_by_mode.items()
    }
