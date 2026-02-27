"""Time series output for evaluation — PRD §8.3.

Generates timeseries.json: equity curve, drawdown series, weight history.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def compute_drawdown_series(equity_curve: pd.Series) -> pd.Series:
    """Compute drawdown series from equity curve."""
    running_max = equity_curve.cummax()
    return (equity_curve - running_max) / running_max


def build_timeseries_output(
    equity_curve: pd.Series,
    drawdown_series: pd.Series,
    weights: pd.DataFrame,
) -> dict[str, Any]:
    """Build the timeseries.json structure.

    Returns:
        Dict with equity_curve, drawdown_series, weight_history — all
        serializable (dates as ISO strings, values as floats).
    """
    return {
        "equity_curve": _series_to_records(equity_curve),
        "drawdown_series": _series_to_records(drawdown_series),
        "weight_history": _dataframe_to_records(weights),
    }


def _series_to_records(s: pd.Series) -> list[dict[str, Any]]:
    """Convert a date-indexed Series to list of {date, value} records."""
    records = []
    for idx, val in s.items():
        records.append({
            "date": _date_str(idx),
            "value": float(val) if pd.notna(val) else None,
        })
    return records


def _dataframe_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert a date-indexed DataFrame to list of {date, weights: {sym: w}} records."""
    records = []
    for idx, row in df.iterrows():
        records.append({
            "date": _date_str(idx),
            "weights": {col: float(v) for col, v in row.items()},
        })
    return records


def _date_str(idx: Any) -> str:
    """Convert index value to ISO date string."""
    if hasattr(idx, "isoformat"):
        return idx.isoformat()
    return str(idx)
