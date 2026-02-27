"""Tests for EqualWeightPolicy."""

import numpy as np
import pytest

from research.policies.equal_weight_policy import EqualWeightPolicy


class TestEqualWeightPolicy:
    def test_produces_uniform_weights(self):
        policy = EqualWeightPolicy(n_symbols=4)
        obs = np.zeros(10)
        weights = policy.act(obs)
        assert weights.shape == (4,)
        np.testing.assert_allclose(weights, 0.25)

    def test_weights_sum_to_one(self):
        policy = EqualWeightPolicy(n_symbols=7)
        weights = policy.act(np.zeros(10))
        np.testing.assert_allclose(weights.sum(), 1.0)

    def test_single_symbol(self):
        policy = EqualWeightPolicy(n_symbols=1)
        weights = policy.act(np.zeros(5))
        np.testing.assert_allclose(weights, [1.0])

    def test_ignores_obs_and_context(self):
        policy = EqualWeightPolicy(n_symbols=3)
        w1 = policy.act(np.ones(10))
        w2 = policy.act(np.zeros(10), context={"foo": "bar"})
        np.testing.assert_array_equal(w1, w2)

    def test_invalid_n_symbols(self):
        with pytest.raises(ValueError, match="n_symbols must be > 0"):
            EqualWeightPolicy(n_symbols=0)
        with pytest.raises(ValueError, match="n_symbols must be > 0"):
            EqualWeightPolicy(n_symbols=-1)
