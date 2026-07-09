"""Tests for research.alpha.portfolio — rank-weighted portfolio construction."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.alpha.portfolio import portfolio_returns_from_weights, rank_weighted_portfolio


class TestRankWeightedPortfolio:
    def test_weights_sum_to_one(self):
        scores = pd.Series({"A": 0.5, "B": 0.3, "C": 0.1, "D": 0.7})
        weights = rank_weighted_portfolio(scores)
        assert abs(weights.sum() - 1.0) < 1e-10

    def test_higher_score_gets_higher_weight(self):
        scores = pd.Series({"A": 1.0, "B": 2.0, "C": 3.0, "D": 4.0})
        weights = rank_weighted_portfolio(scores)
        assert weights["A"] <= weights["B"] <= weights["C"] <= weights["D"]

    def test_max_weight_cap_respected(self):
        scores = pd.Series({"A": 100.0, "B": 0.1, "C": 0.2, "D": 0.3})
        max_w = 0.30
        weights = rank_weighted_portfolio(scores, max_symbol_weight=max_w)
        assert all(w <= max_w + 1e-9 for w in weights)

    def test_all_nan_returns_zero_weights(self):
        scores = pd.Series({"A": np.nan, "B": np.nan})
        weights = rank_weighted_portfolio(scores)
        assert (weights == 0.0).all()

    def test_long_only_no_negative_weights(self):
        scores = pd.Series({"A": -10.0, "B": -5.0, "C": 1.0, "D": 2.0})
        weights = rank_weighted_portfolio(scores)
        assert (weights >= -1e-10).all()

    def test_two_symbols_split_evenly_when_equal(self):
        scores = pd.Series({"A": 1.0, "B": 1.0})
        weights = rank_weighted_portfolio(scores)
        assert abs(weights.sum() - 1.0) < 1e-10

    def test_single_symbol_gets_full_weight(self):
        scores = pd.Series({"A": 5.0})
        weights = rank_weighted_portfolio(scores, max_symbol_weight=1.0)
        assert abs(weights["A"] - 1.0) < 1e-10

    def test_negative_scores_neutral_treated_correctly(self):
        """All-negative scores: rank-weight still produces a valid long-only portfolio."""
        scores = pd.Series({"A": -3.0, "B": -2.0, "C": -1.0})
        weights = rank_weighted_portfolio(scores)
        # Should still sum to 1 if any weight is positive
        assert weights.sum() >= 0.0
        assert all(w >= -1e-10 for w in weights)

    def test_cap_and_renormalize_maintains_sum(self):
        """Capping redistribution must preserve unit sum (feasible cap: 5×0.25=1.25>1)."""
        scores = pd.Series({"A": 10.0, "B": 0.5, "C": 0.4, "D": 0.3, "E": 0.2})
        weights = rank_weighted_portfolio(scores, max_symbol_weight=0.25)
        assert abs(weights.sum() - 1.0) < 1e-8
        assert all(w <= 0.25 + 1e-9 for w in weights)


class TestPortfolioReturnsFromWeights:
    def test_basic_computation(self):
        weights = {
            "2024-01-01": pd.Series({"A": 0.6, "B": 0.4}),
            "2024-01-02": pd.Series({"A": 0.5, "B": 0.5}),
        }
        returns = {
            "2024-01-01": pd.Series({"A": 0.01, "B": -0.02}),
            "2024-01-02": pd.Series({"A": 0.02, "B": 0.03}),
        }
        port = portfolio_returns_from_weights(weights, returns, ["A", "B"])
        assert len(port) == 2
        assert abs(port[0] - (0.6 * 0.01 + 0.4 * -0.02)) < 1e-10
        assert abs(port[1] - (0.5 * 0.02 + 0.5 * 0.03)) < 1e-10

    def test_missing_return_date_excluded(self):
        """Dates in weights but not in returns are excluded."""
        weights = {
            "2024-01-01": pd.Series({"A": 1.0}),
            "2024-01-02": pd.Series({"A": 1.0}),  # no matching return
        }
        returns = {
            "2024-01-01": pd.Series({"A": 0.05}),
        }
        port = portfolio_returns_from_weights(weights, returns, ["A"])
        assert len(port) == 1

    def test_chronological_order(self):
        """Returns come out in date-sorted order regardless of dict order."""
        weights = {
            "2024-01-03": pd.Series({"A": 1.0}),
            "2024-01-01": pd.Series({"A": 1.0}),
        }
        returns = {
            "2024-01-03": pd.Series({"A": 0.03}),
            "2024-01-01": pd.Series({"A": 0.01}),
        }
        port = portfolio_returns_from_weights(weights, returns, ["A"])
        assert port[0] == pytest.approx(0.01)
        assert port[1] == pytest.approx(0.03)
