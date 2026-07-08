"""Regression tests for canonical table DDL + idempotency migrations.

Guards the ya-n4bhm fix: canonical_equity_ohlcv must be born (and migrated)
with QuestDB DEDUP UPSERT KEYS(timestamp, symbol) so that re-running
canonicalize UPSERTs bars in place instead of appending duplicates.

Guards the ya-6e7ok fix: canonical_options_chain must be born (and migrated)
with QuestDB DEDUP UPSERT KEYS(quote_date, underlying, expiry, strike, right,
source_vendor) so that re-running option_eod canonicalize UPSERTs contracts in
place instead of appending duplicates. source_vendor is in the key so live
(thetadata) and EOD (thetadata_eod) rows coexist.
"""

from __future__ import annotations

import re

from yats_pipelines.utils.create_tables import (
    CANONICAL_EQUITY_OHLCV,
    CANONICAL_OPTIONS_CHAIN,
    FEATURES,
    MIGRATIONS,
)


def _norm(sql: str) -> str:
    """Collapse whitespace so DDL assertions ignore formatting."""
    return re.sub(r"\s+", " ", sql).strip().upper()


class TestCanonicalEquityDedupDDL:
    def test_declares_dedup_upsert_keys_on_symbol_and_timestamp(self):
        ddl = _norm(CANONICAL_EQUITY_OHLCV)
        assert "DEDUP UPSERT KEYS(TIMESTAMP, SYMBOL)" in ddl, (
            "canonical_equity_ohlcv must declare DEDUP UPSERT KEYS(timestamp, symbol) "
            "so a fresh DB is idempotent across canonicalize reruns"
        )

    def test_is_a_wal_table(self):
        assert " WAL" in _norm(CANONICAL_EQUITY_OHLCV)

    def test_dedup_key_includes_designated_timestamp(self):
        ddl = _norm(CANONICAL_EQUITY_OHLCV)
        assert "TIMESTAMP(TIMESTAMP)" in ddl
        keys = ddl.split("UPSERT KEYS(")[1].split(")")[0]
        assert "TIMESTAMP" in keys


class TestCanonicalOptionsDedupDDL:
    def test_declares_dedup_upsert_keys(self):
        ddl = _norm(CANONICAL_OPTIONS_CHAIN)
        assert "DEDUP UPSERT KEYS(QUOTE_DATE, UNDERLYING, EXPIRY, STRIKE, RIGHT, SOURCE_VENDOR)" in ddl, (
            "canonical_options_chain must declare the 6-column dedup key so a fresh DB "
            "is idempotent across canonicalize reruns"
        )

    def test_is_a_wal_table(self):
        assert " WAL" in _norm(CANONICAL_OPTIONS_CHAIN)

    def test_dedup_key_includes_designated_timestamp(self):
        ddl = _norm(CANONICAL_OPTIONS_CHAIN)
        assert "TIMESTAMP(QUOTE_DATE)" in ddl
        keys = ddl.split("UPSERT KEYS(")[1].split(")")[0]
        assert "QUOTE_DATE" in keys

    def test_source_vendor_in_dedup_key(self):
        # source_vendor distinguishes live (thetadata) from EOD (thetadata_eod) rows.
        ddl = _norm(CANONICAL_OPTIONS_CHAIN)
        keys = ddl.split("UPSERT KEYS(")[1].split(")")[0]
        assert "SOURCE_VENDOR" in keys


class TestMigrations:
    def test_enables_dedup_on_existing_equity_table(self):
        """Existing tables predate the DDL change; a migration must enable DEDUP
        on them (CREATE TABLE IF NOT EXISTS never alters an existing table)."""
        joined = " ".join(_norm(m) for m in MIGRATIONS)
        assert "ALTER TABLE CANONICAL_EQUITY_OHLCV DEDUP ENABLE UPSERT KEYS(TIMESTAMP, SYMBOL)" in joined

    def test_enables_dedup_on_existing_options_table(self):
        """canonical_options_chain migration must use the full 6-column key."""
        joined = " ".join(_norm(m) for m in MIGRATIONS)
        assert "ALTER TABLE CANONICAL_OPTIONS_CHAIN DEDUP ENABLE UPSERT KEYS(QUOTE_DATE, UNDERLYING, EXPIRY, STRIKE, RIGHT, SOURCE_VENDOR)" in joined

    def test_enables_dedup_on_existing_features_table(self):
        """Feature reruns must upsert: fetch_features reads ALL rows for a
        (timestamp, symbol, feature_set) without filtering by computed_at, so
        stale rows from prior runs would contaminate training samples."""
        joined = " ".join(_norm(m) for m in MIGRATIONS)
        assert "ALTER TABLE FEATURES DEDUP ENABLE UPSERT KEYS(TIMESTAMP, SYMBOL, FEATURE_SET, FEATURE_SET_VERSION)" in joined


class TestFeaturesDedupDDL:
    def test_declares_dedup_upsert_keys(self):
        ddl = _norm(FEATURES)
        assert "DEDUP UPSERT KEYS(TIMESTAMP, SYMBOL, FEATURE_SET, FEATURE_SET_VERSION)" in ddl

    def test_is_a_wal_table(self):
        assert " WAL" in _norm(FEATURES)
