"""Tests for SMAWeightPolicy."""

import numpy as np
import pytest

from research.policies.sma_weight_policy import SMAWeightPolicy


class TestSMAWeightPolicy:
    def test_insufficient_data_returns_equal_weight(self):
        """Before enough price history, fallback to equal weight."""
        policy = SMAWeightPolicy(n_symbols=3, short_window=2, long_window=5)
        # Only 1 step of data — not enough for long window
        weights = policy.act(np.array([100.0, 200.0, 300.0]))
        np.testing.assert_allclose(weights.sum(), 1.0)
        np.testing.assert_allclose(weights, 1.0 / 3)

    def test_bullish_signal_from_rising_prices(self):
        """Symbol with rising prices should get positive signal."""
        policy = SMAWeightPolicy(n_symbols=2, short_window=2, long_window=4)

        # Feed 4 steps of data: symbol 0 rising, symbol 1 flat
        for step in range(4):
            prices = np.array([100.0 + step * 10, 100.0])
            policy.act(prices)

        # Now the short SMA for symbol 0 should be above long SMA
        weights = policy.act(np.array([140.0, 100.0]))
        assert weights[0] > weights[1]

    def test_uses_context_close_prices(self):
        """Should prefer close_prices from context over obs."""
        policy = SMAWeightPolicy(n_symbols=2, short_window=2, long_window=3)
        obs = np.array([999.0, 999.0])  # misleading obs
        context = {"close_prices": [100.0, 200.0]}
        weights = policy.act(obs, context)
        assert weights.shape == (2,)
        np.testing.assert_allclose(weights.sum(), 1.0)

    def test_weights_sum_to_one(self):
        policy = SMAWeightPolicy(n_symbols=3, short_window=2, long_window=4)
        for _ in range(10):
            weights = policy.act(np.random.rand(3) * 100 + 50)
            np.testing.assert_allclose(weights.sum(), 1.0, atol=1e-10)

    def test_reset_clears_history(self):
        policy = SMAWeightPolicy(n_symbols=2, short_window=2, long_window=3)
        for _ in range(5):
            policy.act(np.array([100.0, 200.0]))
        policy.reset()
        # After reset, insufficient data again
        weights = policy.act(np.array([100.0, 200.0]))
        np.testing.assert_allclose(weights, 0.5)

    def test_invalid_params(self):
        with pytest.raises(ValueError, match="n_symbols must be > 0"):
            SMAWeightPolicy(n_symbols=0)
        with pytest.raises(ValueError, match="short_window must be > 0"):
            SMAWeightPolicy(n_symbols=2, short_window=0)
        with pytest.raises(ValueError, match="long_window.*must be > short_window"):
            SMAWeightPolicy(n_symbols=2, short_window=5, long_window=5)

    def test_no_bullish_signals_gives_equal_weight(self):
        """When no symbols are bullish, fallback to equal weight."""
        policy = SMAWeightPolicy(n_symbols=3, short_window=2, long_window=4)
        # All prices declining
        for step in range(5):
            prices = np.array([100.0 - step * 5, 100.0 - step * 5, 100.0 - step * 5])
            policy.act(prices)
        weights = policy.act(np.array([75.0, 75.0, 75.0]))
        np.testing.assert_allclose(weights, 1.0 / 3)
