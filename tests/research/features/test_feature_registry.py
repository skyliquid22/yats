"""Tests for feature_registry — @feature registration, load_feature_set, config_hash."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
import yaml

from research.features.feature_registry import FeatureRegistry, FeatureSet


class TestFeatureRegistration:
    def test_register_and_get(self):
        reg = FeatureRegistry()
        fn = lambda df: df["close"]
        reg.register("my_feat", fn)
        assert reg.get("my_feat") is fn

    def test_get_unknown_raises(self):
        reg = FeatureRegistry()
        with pytest.raises(KeyError, match="not registered"):
            reg.get("nonexistent")

    def test_registered_features_sorted(self):
        reg = FeatureRegistry()
        reg.register("zebra", lambda df: None)
        reg.register("alpha", lambda df: None)
        reg.register("beta", lambda df: None)
        assert reg.registered_features == ["alpha", "beta", "zebra"]

    def test_register_overwrites(self):
        reg = FeatureRegistry()
        fn1 = lambda df: 1
        fn2 = lambda df: 2
        reg.register("feat", fn1)
        reg.register("feat", fn2)
        assert reg.get("feat") is fn2


class TestLoadFeatureSet:
    def _make_registry_with_config(self, config_data: dict) -> FeatureRegistry:
        tmpdir = tempfile.mkdtemp()
        config_dir = Path(tmpdir)
        config_file = config_dir / f"{config_data['name']}.yml"
        with open(config_file, "w") as f:
            yaml.dump(config_data, f)
        reg = FeatureRegistry()
        reg._configs_dir = config_dir
        return reg

    def test_load_valid_set(self):
        config = {
            "name": "valid_set",
            "description": "Valid test set",
            "ohlcv": ["ret_1d"],
            "cross_sectional": ["size_rank"],
        }
        reg = self._make_registry_with_config(config)
        reg.register("ret_1d", lambda df: df["close"])
        reg.register("size_rank", lambda df: df["log_mkt_cap"].rank(pct=True))

        fs = reg.load_feature_set("valid_set")

        assert fs.name == "valid_set"
        assert fs.ohlcv == ["ret_1d"]
        assert fs.cross_sectional == ["size_rank"]
        assert fs.fundamental == []
        assert fs.regime == []

    def test_load_missing_config_raises(self):
        reg = FeatureRegistry()
        with pytest.raises(FileNotFoundError):
            reg.load_feature_set("does_not_exist")

    def test_unregistered_features_raises(self):
        config = {
            "name": "bad_set",
            "description": "Bad test set",
            "ohlcv": ["missing_feature"],
        }
        reg = self._make_registry_with_config(config)
        with pytest.raises(ValueError, match="unregistered features"):
            reg.load_feature_set("bad_set")

    def test_all_features_aggregation(self):
        config = {
            "name": "full_set",
            "description": "Full set",
            "ohlcv": ["ret_1d", "ret_5d"],
            "cross_sectional": ["size_rank"],
            "fundamental": ["pe_ttm"],
            "regime": ["market_vol_20d"],
        }
        reg = self._make_registry_with_config(config)
        for name in ["ret_1d", "ret_5d", "size_rank", "pe_ttm", "market_vol_20d"]:
            reg.register(name, lambda df: None)

        fs = reg.load_feature_set("full_set")
        assert set(fs.all_features) == {"ret_1d", "ret_5d", "size_rank", "pe_ttm", "market_vol_20d"}


class TestConfigHash:
    def test_hash_is_deterministic(self):
        fs = FeatureSet(
            name="hash_test",
            description="Test",
            ohlcv=["ret_1d", "ret_5d"],
            cross_sectional=["size_rank"],
            fundamental=["pe_ttm"],
            regime=["market_vol_20d"],
        )
        h1 = fs.config_hash()
        h2 = fs.config_hash()
        assert h1 == h2

    def test_hash_changes_on_feature_change(self):
        fs1 = FeatureSet(name="s", description="", ohlcv=["ret_1d"])
        fs2 = FeatureSet(name="s", description="", ohlcv=["ret_5d"])
        assert fs1.config_hash() != fs2.config_hash()

    def test_hash_length(self):
        fs = FeatureSet(name="s", description="", ohlcv=["ret_1d"])
        assert len(fs.config_hash()) == 12

    def test_hash_ignores_description(self):
        # description is excluded from the hash payload
        fs1 = FeatureSet(name="s", description="desc A", ohlcv=["ret_1d"])
        fs2 = FeatureSet(name="s", description="desc B", ohlcv=["ret_1d"])
        assert fs1.config_hash() == fs2.config_hash()

    def test_hash_order_stable(self):
        fs1 = FeatureSet(name="s", description="", ohlcv=["ret_1d", "ret_5d"])
        fs2 = FeatureSet(name="s", description="", ohlcv=["ret_1d", "ret_5d"])
        assert fs1.config_hash() == fs2.config_hash()
