"""Regression tests for canonical table DDL + idempotency migrations.

Guards the ya-n4bhm fix: canonical_equity_ohlcv must be born (and migrated)
with QuestDB DEDUP UPSERT KEYS(timestamp, symbol) so that re-running
canonicalize UPSERTs bars in place instead of appending duplicates.
"""

from __future__ import annotations

import re

from yats_pipelines.utils.create_tables import (
    CANONICAL_EQUITY_OHLCV,
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
        # DEDUP is only available on WAL tables.
        assert " WAL" in _norm(CANONICAL_EQUITY_OHLCV)

    def test_dedup_key_includes_designated_timestamp(self):
        # QuestDB requires the designated timestamp to be part of the dedup key.
        ddl = _norm(CANONICAL_EQUITY_OHLCV)
        assert "TIMESTAMP(TIMESTAMP)" in ddl
        keys = ddl.split("UPSERT KEYS(")[1].split(")")[0]
        assert "TIMESTAMP" in keys


class TestMigrations:
    def test_enables_dedup_on_existing_equity_table(self):
        """Existing tables predate the DDL change; a migration must enable DEDUP
        on them (CREATE TABLE IF NOT EXISTS never alters an existing table)."""
        joined = " ".join(_norm(m) for m in MIGRATIONS)
        assert "ALTER TABLE CANONICAL_EQUITY_OHLCV DEDUP ENABLE UPSERT KEYS(TIMESTAMP, SYMBOL)" in joined
