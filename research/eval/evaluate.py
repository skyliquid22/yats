"""Core evaluation pipeline — PRD §8.3.

Deterministic evaluation producing evaluation/metrics.json with all sections:
metadata, performance, trading, safety, performance_by_regime, regime, series,
inputs_used, config.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.eval.metrics import (
    compute_performance_metrics,
    compute_trading_metrics,
    compute_safety_metrics,
)
from research.eval.regime_slicing import (
    label_regimes,
    load_regime_thresholds,
    slice_by_regime,
    regime_slices_to_dict,
)
from research.eval.timeseries import (
    build_timeseries_output,
    compute_drawdown_series,
)
from research.experiments.spec import ExperimentSpec


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def evaluate(
    spec: ExperimentSpec,
    weights: pd.DataFrame,
    returns: pd.DataFrame,
    regime_features: pd.DataFrame | None = None,
    data_hash: str | None = None,
    feature_versions: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Run deterministic evaluation pipeline.

    Args:
        spec: The experiment specification.
        weights: DataFrame indexed by date, columns = symbols, values = portfolio weights.
        returns: DataFrame indexed by date, columns = symbols, values = daily returns.
        regime_features: Optional DataFrame with regime feature columns (market_vol_20d, etc.).
        data_hash: Optional hash of the input data for reproducibility tracking.
        feature_versions: Optional dict of feature name → version for lineage.

    Returns:
        Complete metrics dict matching PRD §8.3 schema, ready for JSON serialization.
    """
    # Portfolio-level returns (weighted sum)
    portfolio_returns = (weights * returns).sum(axis=1)
    portfolio_returns = portfolio_returns.dropna()

    # Equity curve from portfolio returns
    equity_curve = (1.0 + portfolio_returns).cumprod()

    # Daily PnL (same as portfolio returns for evaluation purposes)
    daily_pnl = portfolio_returns

    # --- Performance ---
    perf = compute_performance_metrics(portfolio_returns, equity_curve)

    # --- Trading ---
    trading = compute_trading_metrics(weights, daily_pnl)

    # --- Safety ---
    safety = compute_safety_metrics(
        weights, returns,
        max_gross_exposure=spec.risk_config.max_gross_exposure,
        max_symbol_weight=spec.risk_config.max_symbol_weight,
    )

    # --- Regime slicing ---
    regime_section: dict[str, Any] = {}
    performance_by_regime: dict[str, Any] = {}

    if regime_features is not None and spec.regime_labeling is not None:
        thresholds = None
        if spec.regime_labeling == "v2":
            thresholds = load_regime_thresholds()

        regime_labels = label_regimes(
            regime_features, spec.regime_labeling, thresholds=thresholds,
        )

        slices = slice_by_regime(portfolio_returns, equity_curve, regime_labels)
        performance_by_regime = regime_slices_to_dict(slices)

        regime_section = {
            "labeling_version": spec.regime_labeling,
            "thresholds_used": thresholds if thresholds else None,
            "detector_version": spec.regime_detector_version or None,
        }

    # --- Time series ---
    drawdown_series = compute_drawdown_series(equity_curve)
    timeseries = build_timeseries_output(equity_curve, drawdown_series, weights)

    # --- Assemble output ---
    data_range = _data_range(portfolio_returns)

    result: dict[str, Any] = {
        "metadata": {
            "experiment_id": spec.experiment_id,
            "spec_path": None,  # Set by caller if saving from file
            "data_range": data_range,
            "feature_set_version": spec.feature_set,
        },
        "performance": asdict(perf),
        "trading": asdict(trading),
        "safety": asdict(safety),
        "performance_by_regime": performance_by_regime,
        "regime": regime_section,
        "series": {
            "equity_curve": timeseries["equity_curve"],
            "drawdown_series": timeseries["drawdown_series"],
        },
        "inputs_used": {
            "data_hash": data_hash,
            "feature_versions": feature_versions or {},
            "canonical_data_range": data_range,
        },
        "config": _spec_summary(spec),
    }

    return result


def evaluate_to_json(
    spec: ExperimentSpec,
    weights: pd.DataFrame,
    returns: pd.DataFrame,
    output_path: Path | str,
    regime_features: pd.DataFrame | None = None,
    data_hash: str | None = None,
    feature_versions: dict[str, str] | None = None,
    timeseries_path: Path | str | None = None,
) -> dict[str, Any]:
    """Run evaluation and write metrics.json (and optionally timeseries.json).

    Args:
        output_path: Path to write metrics.json.
        timeseries_path: Optional path to write timeseries.json (equity, drawdown, weights).
        Other args: same as evaluate().

    Returns:
        The metrics dict that was written.
    """
    metrics = evaluate(
        spec=spec,
        weights=weights,
        returns=returns,
        regime_features=regime_features,
        data_hash=data_hash,
        feature_versions=feature_versions,
    )

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(metrics, f, indent=2, default=_json_default)

    # Optionally write full timeseries separately
    if timeseries_path is not None:
        drawdown = compute_drawdown_series(
            (1.0 + (weights * returns).sum(axis=1).dropna()).cumprod()
        )
        equity = (1.0 + (weights * returns).sum(axis=1).dropna()).cumprod()
        ts = build_timeseries_output(equity, drawdown, weights)

        timeseries_path = Path(timeseries_path)
        timeseries_path.parent.mkdir(parents=True, exist_ok=True)
        with open(timeseries_path, "w") as f:
            json.dump(ts, f, indent=2, default=_json_default)

    return metrics


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _data_range(series: pd.Series) -> dict[str, str]:
    """Extract start/end date from a date-indexed series."""
    if len(series) == 0:
        return {"start": "", "end": ""}
    start = series.index[0]
    end = series.index[-1]
    return {
        "start": start.isoformat() if hasattr(start, "isoformat") else str(start),
        "end": end.isoformat() if hasattr(end, "isoformat") else str(end),
    }


def _spec_summary(spec: ExperimentSpec) -> dict[str, Any]:
    """Embedded experiment spec summary for metrics.json."""
    return {
        "experiment_name": spec.experiment_name,
        "experiment_id": spec.experiment_id,
        "symbols": list(spec.symbols),
        "start_date": spec.start_date.isoformat(),
        "end_date": spec.end_date.isoformat(),
        "feature_set": spec.feature_set,
        "policy": spec.policy,
        "seed": spec.seed,
        "regime_labeling": spec.regime_labeling,
        "regime_detector_version": spec.regime_detector_version,
    }


def _json_default(obj: Any) -> Any:
    """JSON serializer for non-standard types."""
    if isinstance(obj, (date,)):
        return obj.isoformat()
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
