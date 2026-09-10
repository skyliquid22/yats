"""Membership loader tests — boundary semantics, backfill union, validation."""
from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd
import pytest

from research.universe.membership import (
    DEFAULT_MEMBERSHIP_PATH,
    all_symbols,
    is_member,
    load_membership,
    symbols_as_of,
)


@pytest.fixture
def membership() -> pd.DataFrame:
    """Two rebalances: {AAA, BBB} at 2020-01-02, {BBB, CCC} at 2020-04-01.

    AAA exits at the second rebalance (delisted); CCC enters.
    """
    return pd.DataFrame(
        {
            "rebalance_date": pd.to_datetime(
                ["2020-01-02", "2020-01-02", "2020-04-01", "2020-04-01"]
            ),
            "symbol": ["AAA", "BBB", "BBB", "CCC"],
            "rank": [1, 2, 1, 2],
            "median_dollar_volume": [400.0, 300.0, 350.0, 200.0],
        }
    )


class TestSymbolsAsOf:
    def test_exactly_on_rebalance_date_uses_new_membership(self, membership):
        assert symbols_as_of("2020-04-01", membership) == ["BBB", "CCC"]

    def test_between_rebalances_uses_latest_prior(self, membership):
        assert symbols_as_of("2020-02-15", membership) == ["AAA", "BBB"]
        assert symbols_as_of(date(2020, 3, 31), membership) == ["AAA", "BBB"]

    def test_before_first_rebalance_is_empty(self, membership):
        assert symbols_as_of("2019-12-31", membership) == []

    def test_on_first_rebalance_date(self, membership):
        assert symbols_as_of("2020-01-02", membership) == ["AAA", "BBB"]

    def test_after_last_rebalance_persists(self, membership):
        assert symbols_as_of("2026-01-01", membership) == ["BBB", "CCC"]

    def test_rank_order_preserved(self, membership):
        # BBB outranks CCC at the April rebalance.
        assert symbols_as_of("2020-06-01", membership)[0] == "BBB"

    def test_accepts_date_datetime_and_tz_aware(self, membership):
        expected = ["AAA", "BBB"]
        assert symbols_as_of(date(2020, 2, 1), membership) == expected
        assert symbols_as_of(datetime(2020, 2, 1, 15, 30), membership) == expected
        assert symbols_as_of(
            pd.Timestamp("2020-02-01", tz=timezone.utc), membership
        ) == expected

    def test_empty_membership(self):
        empty = pd.DataFrame(
            columns=["rebalance_date", "symbol", "rank", "median_dollar_volume"]
        )
        assert symbols_as_of("2020-02-01", empty) == []


class TestAllSymbols:
    def test_union_includes_exited_names_sorted(self, membership):
        # AAA delisted after Q1 but must remain in the backfill union.
        assert all_symbols(membership) == ["AAA", "BBB", "CCC"]


class TestIsMember:
    def test_member_and_non_member(self, membership):
        assert is_member("AAA", "2020-02-01", membership)
        assert not is_member("CCC", "2020-02-01", membership)

    def test_delisted_name_no_longer_member(self, membership):
        assert not is_member("AAA", "2020-04-01", membership)

    def test_before_first_rebalance(self, membership):
        assert not is_member("AAA", "2019-06-01", membership)


class TestLoadMembership:
    def test_parquet_roundtrip_sorted(self, membership, tmp_path):
        path = tmp_path / "m.parquet"
        # Shuffle rows: loader must restore (rebalance_date, rank) order.
        membership.sample(frac=1, random_state=7).to_parquet(path, index=False)
        loaded = load_membership(path)
        pd.testing.assert_frame_equal(loaded, membership)

    def test_missing_file_names_regeneration_command(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="build_pit_universe.py"):
            load_membership(tmp_path / "nope.parquet")

    def test_missing_columns_rejected(self, membership, tmp_path):
        path = tmp_path / "bad.parquet"
        membership.drop(columns=["rank"]).to_parquet(path, index=False)
        with pytest.raises(ValueError, match="rank"):
            load_membership(path)

    def test_default_path_location(self):
        assert DEFAULT_MEMBERSHIP_PATH.name == "pit250_membership.parquet"
        assert DEFAULT_MEMBERSHIP_PATH.parent.name == "universes"
