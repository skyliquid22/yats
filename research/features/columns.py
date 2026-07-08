"""Shared feature column loading from feature set YAML configs.

Single source of truth for (observation_columns, regime_columns) — used by
both the training path (experiment_run.fetch_features + SignalWeightEnv) and
the shadow replay path (ReplayMarketDataSource + ShadowEngine) to guarantee
observation shape parity.
"""

from __future__ import annotations

from pathlib import Path

import yaml


def load_feature_columns(
    feature_set: str,
    configs_dir: Path,
) -> tuple[list[str], list[str]]:
    """Load (observation_columns, regime_columns) from a feature set YAML.

    Returns:
        observation_columns: ["close"] + all non-regime, non-close features
            in the order defined by the YAML
            (ohlcv → cross_sectional → fundamental → options)
        regime_columns: regime features from the YAML's "regime" section,
            in YAML order; empty list if no "regime" section exists
    """
    fs_path = configs_dir / "feature_sets" / f"{feature_set}.yml"
    if fs_path.exists():
        with open(fs_path) as f:
            fs_config = yaml.safe_load(f) or {}
    else:
        fs_config = {}

    ohlcv_cols: list[str] = fs_config.get("ohlcv", [])
    cs_cols: list[str] = fs_config.get("cross_sectional", [])
    fund_cols: list[str] = fs_config.get("fundamental", [])
    options_cols: list[str] = fs_config.get("options", [])
    regime_cols: list[str] = fs_config.get("regime", [])

    all_feature_cols = ohlcv_cols + cs_cols + fund_cols + options_cols
    observation_columns = ["close"] + [c for c in all_feature_cols if c != "close"]

    return observation_columns, regime_cols
