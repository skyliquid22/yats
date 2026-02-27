"""Tests for research.shadow.data_source — ReplayMarketDataSource."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from research.shadow.data_source import (
    ReplayMarketDataSource,
    Snapshot,
    _parse_date,
    _safe_float,
    _to_date_key,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _configs_dir() -> Path:
    """Resolve real configs dir from project root."""
    return Path(__file__).resolve().parents[3] / "configs"


def _make_source(**overrides) -> ReplayMarketDataSource:
    defaults = dict(
        symbols=("AAPL", "MSFT"),
        start_date=date(2023, 1, 1),
        end_date=date(2023, 12, 31),
        feature_set="core_v1",
        configs_dir=_configs_dir(),
    )
    defaults.update(overrides)
    return ReplayMarketDataSource(**defaults)


# ---------------------------------------------------------------------------
# Unit tests for helpers
# ---------------------------------------------------------------------------

class TestSafeFloat:
    def test_normal(self):
        assert _safe_float(3.14) == 3.14

    def test_int(self):
        assert _safe_float(42) == 42.0

    def test_none(self):
        assert _safe_float(None) == 0.0

    def test_nan(self):
        assert _safe_float(float("nan")) == 0.0

    def test_string_number(self):
        assert _safe_float("1.5") == 1.5

    def test_invalid_string(self):
        assert _safe_float("abc") == 0.0


class TestToDateKey:
    def test_datetime(self):
        assert _to_date_key(datetime(2023, 6, 15, 10, 30)) == "2023-06-15"

    def test_date(self):
        assert _to_date_key(date(2023, 6, 15)) == "2023-06-15"

    def test_string(self):
        assert _to_date_key("2023-06-15 00:00:00") == "2023-06-15"


class TestParseDate:
    def test_basic(self):
        result = _parse_date("2023-06-15")
        assert result == datetime(2023, 6, 15)


# ---------------------------------------------------------------------------
# Construction tests
# ---------------------------------------------------------------------------

class TestConstruction:
    def test_basic_construction(self):
        src = _make_source()
        assert src.symbols == ("AAPL", "MSFT")
        assert src.start_date == date(2023, 1, 1)
        assert src.end_date == date(2023, 12, 31)
        assert src.feature_set == "core_v1"

    def test_symbols_sorted_deduped(self):
        src = _make_source(symbols=("MSFT", "AAPL", "MSFT"))
        assert src.symbols == ("AAPL", "MSFT")

    def test_observation_columns_include_close(self):
        src = _make_source()
        assert "close" in src._observation_columns
        assert src._observation_columns[0] == "close"

    def test_no_regime_columns_without_regime_feature_set(self):
        src = _make_source(regime_feature_set=None)
        assert src._regime_columns == ()

    def test_regime_columns_with_regime_feature_set(self):
        src = _make_source(regime_feature_set="core_v1")
        assert "market_vol_20d" in src._regime_columns
        assert "market_trend_20d" in src._regime_columns

    def test_from_experiment_spec(self):
        mock_spec = MagicMock()
        mock_spec.symbols = ("AAPL", "GOOG")
        mock_spec.start_date = date(2023, 1, 1)
        mock_spec.end_date = date(2023, 12, 31)
        mock_spec.feature_set = "core_v1"
        mock_spec.regime_feature_set = None

        src = ReplayMarketDataSource.from_experiment_spec(
            mock_spec, configs_dir=_configs_dir(),
        )
        assert src.symbols == ("AAPL", "GOOG")
        assert src.feature_set == "core_v1"


# ---------------------------------------------------------------------------
# Snapshot building tests (no DB needed)
# ---------------------------------------------------------------------------

class TestBuildSnapshots:
    def test_aligned_dates(self):
        src = _make_source()
        canonical = {
            "2023-01-03": {
                "AAPL": {"open": 130.0, "high": 132.0, "low": 129.0, "close": 131.0, "volume": 1e6, "vwap": 130.5},
                "MSFT": {"open": 240.0, "high": 242.0, "low": 239.0, "close": 241.0, "volume": 2e6, "vwap": 240.5},
            },
            "2023-01-04": {
                "AAPL": {"open": 131.0, "high": 133.0, "low": 130.0, "close": 132.0, "volume": 1.1e6, "vwap": 131.5},
                "MSFT": {"open": 241.0, "high": 243.0, "low": 240.0, "close": 242.0, "volume": 2.1e6, "vwap": 241.5},
            },
        }
        features = {
            "2023-01-03": {
                "AAPL": {"close": 131.0, "ret_1d": 0.01, "rv_21d": 0.15},
                "MSFT": {"close": 241.0, "ret_1d": -0.005, "rv_21d": 0.12},
            },
            "2023-01-04": {
                "AAPL": {"close": 132.0, "ret_1d": 0.008, "rv_21d": 0.14},
                "MSFT": {"close": 242.0, "ret_1d": 0.004, "rv_21d": 0.11},
            },
        }

        snapshots = src._build_snapshots(canonical, features)
        assert len(snapshots) == 2
        assert snapshots[0].as_of == datetime(2023, 1, 3)
        assert snapshots[1].as_of == datetime(2023, 1, 4)

    def test_date_intersection(self):
        """Only dates present in both canonical and features are included."""
        src = _make_source()
        canonical = {
            "2023-01-03": {
                "AAPL": {"close": 131.0},
                "MSFT": {"close": 241.0},
            },
            "2023-01-04": {
                "AAPL": {"close": 132.0},
                "MSFT": {"close": 242.0},
            },
        }
        features = {
            "2023-01-03": {
                "AAPL": {"close": 131.0, "ret_1d": 0.01},
                "MSFT": {"close": 241.0, "ret_1d": -0.005},
            },
            # 2023-01-04 missing from features
            "2023-01-05": {
                "AAPL": {"close": 133.0, "ret_1d": 0.007},
                "MSFT": {"close": 243.0, "ret_1d": 0.003},
            },
        }

        snapshots = src._build_snapshots(canonical, features)
        assert len(snapshots) == 1
        assert snapshots[0].as_of == datetime(2023, 1, 3)

    def test_empty_intersection(self):
        src = _make_source()
        canonical = {"2023-01-03": {"AAPL": {"close": 131.0}}}
        features = {"2023-01-04": {"AAPL": {"close": 132.0}}}

        snapshots = src._build_snapshots(canonical, features)
        assert len(snapshots) == 0

    def test_snapshot_schema(self):
        src = _make_source(regime_feature_set="core_v1")
        canonical = {
            "2023-01-03": {
                "AAPL": {"close": 131.0},
                "MSFT": {"close": 241.0},
            },
        }
        features = {
            "2023-01-03": {
                "AAPL": {"close": 130.5, "ret_1d": 0.01},
                "MSFT": {"close": 240.5, "ret_1d": -0.005},
                "__regime__": {
                    "market_vol_20d": 0.15,
                    "market_trend_20d": 0.02,
                    "dispersion_20d": 0.08,
                    "corr_mean_20d": 0.45,
                },
            },
        }

        snapshots = src._build_snapshots(canonical, features)
        assert len(snapshots) == 1
        snap = snapshots[0]

        # Check all schema fields
        assert isinstance(snap.as_of, datetime)
        assert snap.symbols == ("AAPL", "MSFT")
        assert isinstance(snap.panel, dict)
        assert "AAPL" in snap.panel
        assert "MSFT" in snap.panel
        assert isinstance(snap.regime_features, tuple)
        assert isinstance(snap.regime_feature_names, tuple)
        assert isinstance(snap.observation_columns, tuple)

    def test_canonical_close_overrides_feature_close(self):
        """Canonical close is authoritative — must override feature close."""
        src = _make_source()
        canonical = {
            "2023-01-03": {
                "AAPL": {"close": 131.0},
                "MSFT": {"close": 241.0},
            },
        }
        features = {
            "2023-01-03": {
                "AAPL": {"close": 999.0, "ret_1d": 0.01},
                "MSFT": {"close": 888.0, "ret_1d": -0.005},
            },
        }

        snapshots = src._build_snapshots(canonical, features)
        assert snapshots[0].panel["AAPL"]["close"] == 131.0
        assert snapshots[0].panel["MSFT"]["close"] == 241.0

    def test_regime_features_tuple(self):
        src = _make_source(regime_feature_set="core_v1")
        canonical = {
            "2023-01-03": {
                "AAPL": {"close": 131.0},
                "MSFT": {"close": 241.0},
            },
        }
        features = {
            "2023-01-03": {
                "AAPL": {"close": 131.0},
                "MSFT": {"close": 241.0},
                "__regime__": {
                    "market_vol_20d": 0.15,
                    "market_trend_20d": 0.02,
                    "dispersion_20d": 0.08,
                    "corr_mean_20d": 0.45,
                },
            },
        }

        snapshots = src._build_snapshots(canonical, features)
        snap = snapshots[0]
        assert snap.regime_features == (0.15, 0.02, 0.08, 0.45)
        assert snap.regime_feature_names == (
            "market_vol_20d", "market_trend_20d",
            "dispersion_20d", "corr_mean_20d",
        )

    def test_no_regime_features(self):
        src = _make_source(regime_feature_set=None)
        canonical = {
            "2023-01-03": {"AAPL": {"close": 131.0}, "MSFT": {"close": 241.0}},
        }
        features = {
            "2023-01-03": {"AAPL": {"close": 131.0}, "MSFT": {"close": 241.0}},
        }

        snapshots = src._build_snapshots(canonical, features)
        assert snapshots[0].regime_features == ()
        assert snapshots[0].regime_feature_names == ()

    def test_missing_symbol_in_features(self):
        """Symbols missing from features get empty feature dicts."""
        src = _make_source()
        canonical = {
            "2023-01-03": {
                "AAPL": {"close": 131.0},
                "MSFT": {"close": 241.0},
            },
        }
        features = {
            "2023-01-03": {
                "AAPL": {"close": 131.0, "ret_1d": 0.01},
                # MSFT missing from features
            },
        }

        snapshots = src._build_snapshots(canonical, features)
        assert len(snapshots) == 1
        # MSFT should still be present with canonical close
        assert snapshots[0].panel["MSFT"]["close"] == 241.0

    def test_snapshots_ordered_by_date(self):
        src = _make_source()
        canonical = {
            "2023-01-05": {"AAPL": {"close": 133.0}, "MSFT": {"close": 243.0}},
            "2023-01-03": {"AAPL": {"close": 131.0}, "MSFT": {"close": 241.0}},
            "2023-01-04": {"AAPL": {"close": 132.0}, "MSFT": {"close": 242.0}},
        }
        features = {
            "2023-01-05": {"AAPL": {"close": 133.0}, "MSFT": {"close": 243.0}},
            "2023-01-03": {"AAPL": {"close": 131.0}, "MSFT": {"close": 241.0}},
            "2023-01-04": {"AAPL": {"close": 132.0}, "MSFT": {"close": 242.0}},
        }

        snapshots = src._build_snapshots(canonical, features)
        dates = [s.as_of for s in snapshots]
        assert dates == sorted(dates)


# ---------------------------------------------------------------------------
# Snapshot.to_dict
# ---------------------------------------------------------------------------

class TestSnapshotToDict:
    def test_roundtrip(self):
        snap = Snapshot(
            as_of=datetime(2023, 1, 3),
            symbols=("AAPL", "MSFT"),
            panel={"AAPL": {"close": 131.0}, "MSFT": {"close": 241.0}},
            regime_features=(0.15,),
            regime_feature_names=("market_vol_20d",),
            observation_columns=("close",),
        )
        d = snap.to_dict()
        assert d["as_of"] == datetime(2023, 1, 3)
        assert d["symbols"] == ("AAPL", "MSFT")
        assert d["panel"]["AAPL"]["close"] == 131.0
        assert d["regime_features"] == (0.15,)


# ---------------------------------------------------------------------------
# SQL generation tests
# ---------------------------------------------------------------------------

class TestWhereClause:
    def test_basic(self):
        src = _make_source()
        clause = src._where_clause()
        assert "'AAPL'" in clause
        assert "'MSFT'" in clause
        assert "2023-01-01" in clause
        assert "2023-12-31" in clause

    def test_extra(self):
        src = _make_source()
        clause = src._where_clause("reconcile_method = 'batch'")
        assert "reconcile_method = 'batch'" in clause
