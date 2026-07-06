"""Walk-Forward Optimization (WFO) harness — rolling / anchored (expanding).

Stage 2 of CPCV adoption (ya-ddnk2).  Builds sequential train/test folds with
purge+embargo (ya-2mt9e) between each fold's train window and test block.

Key design decisions
--------------------
* **Default mode = anchored (expanding)**: roll i trains on [0, test_start_i).
  This ensures the regime/hierarchical controller has seen every historical
  market mode (2008 crisis, COVID etc.) in every fold.  A fixed-length rolling
  window can silently exclude all past crises and leave defensive-mode behaviour
  untrained.
* **Full convergence per fold**: total_timesteps is never reduced.  Truncating
  introduces sample-efficiency selection bias (ya-ddnk2 spec, explicitly vetoed).
* **WFO rank-decay metric**: RL-compatible analog of de Prado's CSCV PBO.
  Measures rank degradation across sequential rolls.  Labelled
  "WFO rank-decay overfitting metric" — it is NOT the literal CSCV PBO number.
* **Anchored Sharpe caveat**: absolute per-fold Sharpe is confounded by growing
  training-data size under anchored mode.  DSR/PSR (run on the fixed concatenated
  OOS blocks) and the rank-decay metric (within-fold comparison at equal data
  budget) are unaffected.  Do NOT interpret a fold-1 → fold-N Sharpe trend as
  alpha-decay.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from research.experiments.spec import WFOConfig

logger = logging.getLogger(__name__)

# Minimum bars required in a training window for PPO to converge.
# Validation raises if fold-1 train_window is shorter than this.
_MIN_TRAIN_BARS_FOR_CONVERGENCE = 100


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class WFOFold:
    """Per-fold geometry and OOS results."""

    fold_index: int          # 0-based roll index
    train_start: int         # inclusive bar index into full data
    train_end: int           # exclusive bar index (train slice = data[train_start:train_end])
    test_start: int          # inclusive bar index (after purge+embargo gap)
    test_end: int            # exclusive bar index (test slice = data[test_start:test_end])
    purge_bars: int          # label_horizon bars dropped before test
    buffer_bars: int         # purge_buffer bars dropped before test
    oos_sharpe: float | None = None
    oos_returns: list[float] = field(default_factory=list)
    checkpoint_path: str | None = None
    total_timesteps_trained: int = 0


@dataclass
class WFOResult:
    """Full WFO run result."""

    folds: list[WFOFold]
    # Concatenated OOS returns in chronological order (no gaps beyond embargo)
    concatenated_oos_returns: list[float] = field(default_factory=list)
    # Per-fold OOS Sharpe series (fold 0 → fold n-1)
    per_fold_oos_sharpe: list[float | None] = field(default_factory=list)
    # WFO rank-decay overfitting metric (higher = more overfit)
    rank_decay_metric: float | None = None
    # Summary
    median_oos_sharpe: float | None = None
    n_folds: int = 0


# ---------------------------------------------------------------------------
# Geometry
# ---------------------------------------------------------------------------

def _resolve_purge_buffer(wfo_config: WFOConfig, feature_set: str | None) -> int:
    """Resolve purge_buffer: explicit override > auto from feature registry."""
    if wfo_config.purge_buffer is not None:
        return wfo_config.purge_buffer
    if feature_set is None:
        return 0
    try:
        from research.features.feature_registry import registry
        return registry.max_lookback(feature_set)
    except Exception:
        return 0


def build_wfo_folds(
    n_bars: int,
    wfo_config: WFOConfig,
    feature_set: str | None = None,
) -> list[WFOFold]:
    """Compute train/test slice boundaries for all WFO folds.

    Raises ValueError when data is too short to fit the requested geometry
    (e.g. train_window-too-short or not enough bars for n_periods folds).

    Under **anchored** mode:
        fold i trains on [0, train_window + i * test_window)
        test block = next test_window bars after purge+embargo gap

    Under **rolling** mode:
        fold i trains on [i * test_window, i * test_window + train_window)
        test block = next test_window bars after purge+embargo gap

    Both modes apply the same purge+embargo between each train window and its
    test block:
        effective_train_end = raw_train_end - label_horizon - buffer_bars
        test_start = raw_train_end  (no post-test embargo — forward-only split)
    """
    purge_buffer = _resolve_purge_buffer(wfo_config, feature_set)
    label_horizon = wfo_config.label_horizon
    gap = label_horizon + purge_buffer
    n_periods = wfo_config.n_periods
    train_window = wfo_config.train_window
    mode = wfo_config.mode

    # Derive test_window: explicit override, or divide remaining bars equally.
    if wfo_config.test_window is not None:
        test_window = wfo_config.test_window
    else:
        # Under anchored mode the total bars consumed:
        #   train_window (fold 1 anchor) + gap + n_periods * test_window <= n_bars
        # Solve for test_window:
        available = n_bars - train_window - gap
        if available <= 0:
            raise ValueError(
                f"Not enough data for WFO: n_bars={n_bars} but train_window={train_window} "
                f"plus purge gap={gap} already exceeds available bars."
            )
        test_window = max(1, available // n_periods)

    # Validate: fold-1 train window must be long enough for convergence.
    if train_window < _MIN_TRAIN_BARS_FOR_CONVERGENCE:
        raise ValueError(
            f"train_window={train_window} bars is too short for PPO convergence "
            f"(minimum {_MIN_TRAIN_BARS_FOR_CONVERGENCE} bars). "
            "Increase train_window or use a shorter history check."
        )

    # Validate: enough bars for all folds.
    if mode == "anchored":
        required = train_window + gap + n_periods * test_window
    else:  # rolling
        required = train_window + (n_periods - 1) * test_window + gap + test_window
    if n_bars < required:
        raise ValueError(
            f"Not enough data for {n_periods} WFO folds: need {required} bars "
            f"but only {n_bars} available "
            f"(train_window={train_window}, test_window={test_window}, gap={gap})."
        )

    folds: list[WFOFold] = []
    for i in range(n_periods):
        if mode == "anchored":
            raw_train_start = 0
            raw_train_end = train_window + i * test_window
        else:  # rolling
            raw_train_start = i * test_window
            raw_train_end = raw_train_start + train_window

        effective_train_end = raw_train_end - label_horizon - purge_buffer
        effective_train_end = max(raw_train_start + 1, effective_train_end)

        test_start = raw_train_end
        test_end = test_start + test_window

        if test_end > n_bars:
            break  # not enough data for this fold

        folds.append(WFOFold(
            fold_index=i,
            train_start=raw_train_start,
            train_end=effective_train_end,
            test_start=test_start,
            test_end=test_end,
            purge_bars=label_horizon,
            buffer_bars=purge_buffer,
        ))

    if len(folds) < 2:
        raise ValueError(
            f"WFO geometry produced only {len(folds)} fold(s); need ≥2. "
            "Increase n_bars or reduce train_window / test_window."
        )

    return folds


# ---------------------------------------------------------------------------
# Rank-decay metric
# ---------------------------------------------------------------------------

def compute_rank_decay(
    config_sharpes: list[list[float | None]],
) -> float:
    """Compute the WFO rank-decay overfitting metric across sequential folds.

    Args:
        config_sharpes: list of length n_configs, each entry is a list of
            per-fold OOS Sharpe values (length n_folds).  None = fold missing.

    Returns:
        Rank-decay metric in [0, 1].  0 = no decay (rankings stable),
        1 = maximum decay (rankings perfectly inverted).

    The metric is the mean absolute rank-change of configs between the first
    half of folds and the second half, normalised by the maximum possible
    rank-change.  Configs ranked in the top half of fold-average Sharpe in
    the first half of folds that fall to the bottom half in the second half
    produce high rank-decay scores.

    This is labelled "WFO rank-decay overfitting metric" and is NOT the
    literal de Prado CSCV PBO number.
    """
    n_configs = len(config_sharpes)
    if n_configs < 2:
        return 0.0

    n_folds = max(len(s) for s in config_sharpes)
    if n_folds < 2:
        return 0.0

    mid = n_folds // 2

    # Compute mean OOS Sharpe per config for first-half and second-half folds.
    def _mean_sharpe(sharpes: list[float | None], fold_indices: range) -> float:
        vals = [sharpes[j] for j in fold_indices if j < len(sharpes) and sharpes[j] is not None]
        return float(np.mean(vals)) if vals else 0.0

    first_half = range(0, mid)
    second_half = range(mid, n_folds)

    first_means = [_mean_sharpe(s, first_half) for s in config_sharpes]
    second_means = [_mean_sharpe(s, second_half) for s in config_sharpes]

    # Rank configs (higher Sharpe = lower rank number, i.e. rank 1 = best)
    def _ranks(values: list[float]) -> list[int]:
        order = sorted(range(len(values)), key=lambda i: -values[i])
        ranks = [0] * len(values)
        for rank, idx in enumerate(order):
            ranks[idx] = rank + 1
        return ranks

    ranks_first = _ranks(first_means)
    ranks_second = _ranks(second_means)

    # Mean absolute rank-change, normalised by max possible = n_configs - 1
    abs_changes = [abs(r1 - r2) for r1, r2 in zip(ranks_first, ranks_second)]
    mean_abs_change = float(np.mean(abs_changes))
    max_possible = max(1, n_configs - 1)
    return min(1.0, mean_abs_change / max_possible)


# ---------------------------------------------------------------------------
# Main harness
# ---------------------------------------------------------------------------

def run_wfo(
    data: list[dict[str, Any]],
    wfo_config: WFOConfig,
    *,
    train_fn: Any,
    eval_fn: Any,
    feature_set: str | None = None,
) -> WFOResult:
    """Execute the WFO harness: build folds, train, evaluate each fold.

    Args:
        data: List of row dicts in chronological order (full history).
        wfo_config: WFO geometry configuration.
        train_fn: Callable(train_data) -> checkpoint_path (or None for non-RL).
            Must train to FULL CONVERGENCE (not reduce total_timesteps).
        eval_fn: Callable(test_data, checkpoint_path) -> (returns: list[float], sharpe: float).
        feature_set: Feature set name for auto purge_buffer resolution.

    Returns:
        WFOResult with per-fold results and concatenated OOS track record.
    """
    n_bars = len(data)
    folds = build_wfo_folds(n_bars, wfo_config, feature_set=feature_set)
    logger.info(
        "WFO: mode=%s, %d folds, n_bars=%d",
        wfo_config.mode, len(folds), n_bars,
    )

    concatenated_returns: list[float] = []
    per_fold_sharpe: list[float | None] = []

    for fold in folds:
        train_data = data[fold.train_start:fold.train_end]
        test_data = data[fold.test_start:fold.test_end]

        logger.info(
            "WFO fold %d/%d: train=[%d,%d) (%d bars), test=[%d,%d) (%d bars), "
            "purge=%d, buffer=%d",
            fold.fold_index + 1, len(folds),
            fold.train_start, fold.train_end, len(train_data),
            fold.test_start, fold.test_end, len(test_data),
            fold.purge_bars, fold.buffer_bars,
        )

        checkpoint_path = train_fn(train_data)
        fold.checkpoint_path = str(checkpoint_path) if checkpoint_path is not None else None

        oos_returns, oos_sharpe = eval_fn(test_data, checkpoint_path)
        fold.oos_returns = list(oos_returns)
        fold.oos_sharpe = oos_sharpe

        concatenated_returns.extend(fold.oos_returns)
        per_fold_sharpe.append(fold.oos_sharpe)

        logger.info("WFO fold %d OOS Sharpe: %.4f", fold.fold_index + 1, oos_sharpe or 0.0)

    valid_sharpes = [s for s in per_fold_sharpe if s is not None]
    median_sharpe = float(np.median(valid_sharpes)) if valid_sharpes else None

    # Rank-decay metric: requires at least 2 configs (single-config WFO → 0)
    rank_decay = None
    if len(folds) >= 2 and per_fold_sharpe:
        # Single-config rank-decay: wrap as list of one config
        rank_decay = compute_rank_decay([per_fold_sharpe])

    return WFOResult(
        folds=folds,
        concatenated_oos_returns=concatenated_returns,
        per_fold_oos_sharpe=per_fold_sharpe,
        rank_decay_metric=rank_decay,
        median_oos_sharpe=median_sharpe,
        n_folds=len(folds),
    )


# ---------------------------------------------------------------------------
# Multi-config rank-decay (used by sweep-level WFO overfitting check)
# ---------------------------------------------------------------------------

def compute_sweep_wfo_rank_decay(
    sweep_fold_sharpes: list[list[float | None]],
) -> float:
    """Compute WFO rank-decay across multiple sweep configs.

    Args:
        sweep_fold_sharpes: list[config_idx][fold_idx] = OOS Sharpe or None.

    Returns:
        Rank-decay metric in [0, 1].  Below ~0.5 indicates acceptable stability.
    """
    return compute_rank_decay(sweep_fold_sharpes)


# ---------------------------------------------------------------------------
# Qualification helpers
# ---------------------------------------------------------------------------

def wfo_qualification_gates(
    wfo_result: WFOResult,
    *,
    min_median_oos_sharpe: float = 0.0,
    max_rank_decay: float = 0.7,
) -> list[dict[str, Any]]:
    """Evaluate WFO-specific qualification gates.

    Returns a list of gate dicts compatible with research.promotion.criteria.GateResult.
    """
    gates: list[dict[str, Any]] = []

    # Gate 1: median OOS Sharpe across rolls
    median_sr = wfo_result.median_oos_sharpe
    gates.append({
        "name": "wfo_median_oos_sharpe",
        "passed": median_sr is not None and median_sr > min_median_oos_sharpe,
        "value": median_sr,
        "threshold": min_median_oos_sharpe,
        "gate_type": "hard",
        "detail": f"Median OOS Sharpe across {wfo_result.n_folds} WFO folds",
    })

    # Gate 2: WFO rank-decay overfitting metric
    rd = wfo_result.rank_decay_metric
    gates.append({
        "name": "wfo_rank_decay",
        "passed": rd is None or rd <= max_rank_decay,
        "value": rd,
        "threshold": max_rank_decay,
        "gate_type": "soft",
        "detail": (
            "WFO rank-decay overfitting metric (NOT de Prado CSCV PBO). "
            "Measures rank stability of configs across sequential folds."
        ),
    })

    return gates
