"""Regime labeling and performance slicing — PRD §6.4, Appendix I.

Pluggable regime detection: v1 (quantile-based) and v2 (threshold-based).
Slices evaluation metrics by regime bucket for per-regime performance analysis.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from research.eval.metrics import (
    compute_max_drawdown,
    compute_sharpe,
    compute_annualized_return,
    compute_total_return,
)


CONFIGS_DIR = Path(__file__).resolve().parents[2] / "configs"


# ---------------------------------------------------------------------------
# Regime labeling
# ---------------------------------------------------------------------------

def label_v1(regime_features: pd.DataFrame) -> pd.Series:
    """Quantile-based regime labeling (v1).

    Bucket market_vol_20d into terciles:
    - Bottom tercile → low_vol
    - Middle tercile → mid_vol
    - Top tercile → high_vol
    """
    vol = regime_features["market_vol_20d"].dropna()
    if len(vol) == 0:
        return pd.Series(dtype=str)

    q33 = vol.quantile(1 / 3)
    q67 = vol.quantile(2 / 3)

    labels = pd.Series("mid_vol", index=vol.index)
    labels[vol <= q33] = "low_vol"
    labels[vol > q67] = "high_vol"
    return labels


def label_v2(
    regime_features: pd.DataFrame,
    thresholds: dict[str, float] | None = None,
) -> pd.Series:
    """Threshold-based regime labeling (v2).

    Uses explicit thresholds from configs/regime_thresholds.yml.
    Produces 6 buckets (vol × trend).
    """
    if thresholds is None:
        thresholds = load_regime_thresholds()

    vol = regime_features["market_vol_20d"]
    trend = regime_features["market_trend_20d"]

    # Classify volatility
    vol_label = pd.Series("mid_vol", index=regime_features.index)
    vol_label[vol >= thresholds["vol_high"]] = "high_vol"
    vol_label[vol <= thresholds["vol_low"]] = "low_vol"

    # Classify trend
    trend_label = pd.Series("trend_flat", index=regime_features.index)
    trend_label[trend >= thresholds["trend_up"]] = "trend_up"
    trend_label[trend <= thresholds["trend_down"]] = "trend_down"

    # Combine: high_vol_trend_up, low_vol_trend_down, etc.
    labels = vol_label.str.cat(trend_label, sep="_")

    # Drop rows where either feature was NaN
    mask = vol.isna() | trend.isna()
    labels[mask] = np.nan
    return labels


def load_regime_thresholds(path: Path | None = None) -> dict[str, float]:
    """Load regime thresholds from YAML config."""
    if path is None:
        path = CONFIGS_DIR / "regime_thresholds.yml"
    with open(path) as f:
        return yaml.safe_load(f)


def load_regime_detector_config(name: str) -> dict[str, Any]:
    """Load a regime detector config by name from configs/regime_detectors/."""
    path = CONFIGS_DIR / "regime_detectors" / f"{name}.yml"
    if not path.exists():
        raise FileNotFoundError(f"Regime detector config not found: {path}")
    with open(path) as f:
        return yaml.safe_load(f)


def label_regimes(
    regime_features: pd.DataFrame,
    labeling: str,
    thresholds: dict[str, float] | None = None,
) -> pd.Series:
    """Dispatch to the correct labeling function by version string."""
    if labeling == "v1":
        return label_v1(regime_features)
    elif labeling == "v2":
        return label_v2(regime_features, thresholds=thresholds)
    else:
        raise ValueError(f"Unknown labeling version: {labeling!r} (expected 'v1' or 'v2')")


# ---------------------------------------------------------------------------
# Regime performance slicing
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RegimeBucketMetrics:
    bucket: str
    sharpe: float
    total_return: float
    annualized_return: float
    max_drawdown: float
    n_days: int


def slice_by_regime(
    portfolio_returns: pd.Series,
    equity_curve: pd.Series,
    regime_labels: pd.Series,
) -> list[RegimeBucketMetrics]:
    """Slice evaluation metrics by regime bucket.

    Returns per-bucket metrics: sharpe, return, drawdown, n_days.
    """
    # Align on common dates
    common_idx = portfolio_returns.index.intersection(
        regime_labels.dropna().index
    )
    returns_aligned = portfolio_returns.loc[common_idx]
    labels_aligned = regime_labels.loc[common_idx]

    results: list[RegimeBucketMetrics] = []
    for bucket in sorted(labels_aligned.unique()):
        mask = labels_aligned == bucket
        bucket_returns = returns_aligned[mask]
        n_days = len(bucket_returns)
        if n_days < 2:
            results.append(RegimeBucketMetrics(
                bucket=bucket, sharpe=0.0, total_return=0.0,
                annualized_return=0.0, max_drawdown=0.0, n_days=n_days,
            ))
            continue

        # Build equity curve for this bucket's days
        bucket_equity = (1.0 + bucket_returns).cumprod()
        total_ret = compute_total_return(bucket_equity)
        ann_ret = compute_annualized_return(total_ret, n_days)
        max_dd, _ = compute_max_drawdown(bucket_equity)

        results.append(RegimeBucketMetrics(
            bucket=bucket,
            sharpe=compute_sharpe(bucket_returns),
            total_return=total_ret,
            annualized_return=ann_ret,
            max_drawdown=max_dd,
            n_days=n_days,
        ))

    return results


def regime_slices_to_dict(slices: list[RegimeBucketMetrics]) -> dict[str, Any]:
    """Convert regime slices to a serializable dict keyed by bucket."""
    return {
        s.bucket: {
            "sharpe": s.sharpe,
            "total_return": s.total_return,
            "annualized_return": s.annualized_return,
            "max_drawdown": s.max_drawdown,
            "n_days": s.n_days,
        }
        for s in slices
    }
