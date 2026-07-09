"""Cross-sectional alpha models: Ridge regression and LightGBM.

Both models are trained on (date, symbol) cross-sectional panels and predict
residualized forward returns. At inference time, they output alpha scores
suitable for rank-weighted portfolio construction.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge


@dataclass
class AlphaModelResult:
    """Fitted model artifact from a WFO training fold."""

    model: Any
    feature_cols: list[str]
    model_type: str  # "ridge" or "lgbm"
    horizon: int
    reg: float
    n_train_samples: int


def fit_ridge(
    X: np.ndarray,
    y: np.ndarray,
    *,
    alpha: float = 1.0,
) -> Ridge:
    """Fit a Ridge regression model."""
    model = Ridge(alpha=alpha, fit_intercept=True)
    model.fit(X, y)
    return model


def fit_lgbm(
    X: np.ndarray,
    y: np.ndarray,
    *,
    num_leaves: int = 31,
    n_estimators: int = 200,
    learning_rate: float = 0.05,
    reg_lambda: float = 0.1,
) -> Any:
    """Fit a LightGBM regressor."""
    import lightgbm as lgb

    model = lgb.LGBMRegressor(
        num_leaves=num_leaves,
        n_estimators=n_estimators,
        learning_rate=learning_rate,
        reg_lambda=reg_lambda,
        verbosity=-1,
        random_state=42,
    )
    model.fit(X, y)
    return model


def train_alpha_model(
    panel: pd.DataFrame,
    feature_cols: list[str],
    target_col: str,
    *,
    model_type: str = "ridge",
    horizon: int = 5,
    reg: float = 1.0,
) -> AlphaModelResult:
    """Train a cross-sectional alpha model on the given panel.

    Args:
        panel: DataFrame with feature_cols + target_col (one row per date×symbol).
        feature_cols: Feature columns (already rank-normalized cross-sectionally).
        target_col: Residualized forward return column to predict.
        model_type: "ridge" or "lgbm".
        horizon: Prediction horizon in trading days (for metadata).
        reg: Regularization strength (alpha for Ridge, lambda for LGBM).

    Returns:
        AlphaModelResult with the fitted model and metadata.
    """
    valid = panel[feature_cols + [target_col]].dropna()
    if len(valid) < 10:
        raise ValueError(
            f"Too few valid training samples after dropping NaN: {len(valid)}"
        )
    X = valid[feature_cols].values.astype(float)
    y = valid[target_col].values.astype(float)

    if model_type == "ridge":
        model = fit_ridge(X, y, alpha=reg)
    elif model_type == "lgbm":
        model = fit_lgbm(X, y, reg_lambda=reg)
    else:
        raise ValueError(f"Unknown model_type: {model_type!r}. Use 'ridge' or 'lgbm'.")

    return AlphaModelResult(
        model=model,
        feature_cols=feature_cols,
        model_type=model_type,
        horizon=horizon,
        reg=reg,
        n_train_samples=len(valid),
    )


def predict_alpha_scores(
    panel: pd.DataFrame,
    model_result: AlphaModelResult,
) -> pd.Series:
    """Predict cross-sectional alpha scores for each (date, symbol) row.

    Returns a Series with the same index as panel. Rows with any NaN feature
    receive score 0.0 (neutral).
    """
    feature_cols = model_result.feature_cols
    X_raw = panel[feature_cols]
    has_nan = X_raw.isna().any(axis=1)

    scores = pd.Series(0.0, index=panel.index)
    valid_mask = ~has_nan
    if valid_mask.any():
        X_valid = X_raw.loc[valid_mask].values.astype(float)
        scores.loc[valid_mask] = model_result.model.predict(X_valid)
    return scores
