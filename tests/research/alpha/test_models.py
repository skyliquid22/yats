"""Tests for research.alpha.models — Ridge and LightGBM alpha models."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.alpha.models import (
    AlphaModelResult,
    predict_alpha_scores,
    train_alpha_model,
)


def _make_training_panel(n_obs: int = 200, n_features: int = 5) -> pd.DataFrame:
    """Synthetic training panel with a planted signal."""
    rng = np.random.default_rng(42)
    X = rng.standard_normal((n_obs, n_features))
    true_coef = np.array([0.5, -0.3, 0.0, 0.2, -0.1])[:n_features]
    y = X @ true_coef + 0.1 * rng.standard_normal(n_obs)
    cols = [f"f{i}" for i in range(n_features)]
    df = pd.DataFrame(X, columns=cols)
    df["target"] = y
    df["date"] = pd.date_range("2024-01-01", periods=n_obs, freq="B").date
    df["symbol"] = "STOCK"
    return df


class TestTrainAlphaModelRidge:
    def test_returns_model_result(self):
        df = _make_training_panel()
        feat_cols = [f"f{i}" for i in range(5)]
        result = train_alpha_model(df, feat_cols, "target", model_type="ridge", horizon=5, reg=1.0)
        assert isinstance(result, AlphaModelResult)
        assert result.model_type == "ridge"
        assert result.horizon == 5
        assert result.n_train_samples == len(df)

    def test_predict_shape_matches_panel(self):
        df = _make_training_panel()
        feat_cols = [f"f{i}" for i in range(5)]
        result = train_alpha_model(df, feat_cols, "target", model_type="ridge", horizon=5, reg=1.0)
        scores = predict_alpha_scores(df, result)
        assert len(scores) == len(df)

    def test_predictions_not_all_zero(self):
        df = _make_training_panel()
        feat_cols = [f"f{i}" for i in range(5)]
        result = train_alpha_model(df, feat_cols, "target", model_type="ridge", horizon=5, reg=1.0)
        scores = predict_alpha_scores(df, result)
        assert scores.std() > 0.01

    def test_higher_reg_shrinks_predictions(self):
        """Higher regularization → predictions closer to zero (smaller variance)."""
        df = _make_training_panel()
        feat_cols = [f"f{i}" for i in range(5)]
        r_low = train_alpha_model(df, feat_cols, "target", model_type="ridge", horizon=5, reg=0.01)
        r_high = train_alpha_model(df, feat_cols, "target", model_type="ridge", horizon=5, reg=1000.0)
        s_low = predict_alpha_scores(df, r_low).std()
        s_high = predict_alpha_scores(df, r_high).std()
        assert s_low > s_high

    def test_nan_features_get_zero_score(self):
        df = _make_training_panel(n_obs=50)
        feat_cols = [f"f{i}" for i in range(5)]
        result = train_alpha_model(df, feat_cols, "target", model_type="ridge", horizon=5, reg=1.0)
        df_test = df.copy()
        df_test.loc[0, "f0"] = np.nan
        scores = predict_alpha_scores(df_test, result)
        assert scores.iloc[0] == pytest.approx(0.0)


class TestTrainAlphaModelLGBM:
    def test_returns_model_result(self):
        df = _make_training_panel()
        feat_cols = [f"f{i}" for i in range(5)]
        result = train_alpha_model(df, feat_cols, "target", model_type="lgbm", horizon=21, reg=0.1)
        assert isinstance(result, AlphaModelResult)
        assert result.model_type == "lgbm"
        assert result.horizon == 21

    def test_predictions_have_variance(self):
        df = _make_training_panel()
        feat_cols = [f"f{i}" for i in range(5)]
        result = train_alpha_model(df, feat_cols, "target", model_type="lgbm", horizon=5, reg=0.1)
        scores = predict_alpha_scores(df, result)
        assert scores.std() > 0.0

    def test_invalid_model_type_raises(self):
        df = _make_training_panel(n_obs=30)
        feat_cols = ["f0"]
        with pytest.raises(ValueError, match="Unknown model_type"):
            train_alpha_model(df, feat_cols, "target", model_type="knn", horizon=5, reg=1.0)

    def test_too_few_samples_raises(self):
        df = _make_training_panel(n_obs=5)
        feat_cols = [f"f{i}" for i in range(5)]
        df["target"] = np.nan  # all NaN → no valid rows
        with pytest.raises(ValueError, match="Too few valid training samples"):
            train_alpha_model(df, feat_cols, "target", model_type="ridge", horizon=5, reg=1.0)
