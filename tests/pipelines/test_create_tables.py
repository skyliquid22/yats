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
    CANONICAL_INST_OWNERSHIP,
    CANONICAL_INSIDER_TRADES,
    CANONICAL_INSTITUTIONAL_HOLDINGS,
    CANONICAL_OPTIONS_CHAIN,
    FEATURES,
    JOB_RUNS,
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


class TestCanonicalInsiderTradesDDL:
    def test_declares_dedup_upsert_keys(self):
        ddl = _norm(CANONICAL_INSIDER_TRADES)
        assert "DEDUP UPSERT KEYS(FILING_DATE, SYMBOL, INSIDER_NAME, TRANSACTION_DATE, TRANSACTION_TYPE, SHARES)" in ddl, (
            "canonical_insider_trades must declare the 6-column dedup key"
        )

    def test_is_a_wal_table(self):
        assert " WAL" in _norm(CANONICAL_INSIDER_TRADES)

    def test_designated_timestamp_is_filing_date(self):
        ddl = _norm(CANONICAL_INSIDER_TRADES)
        assert "TIMESTAMP(FILING_DATE)" in ddl

    def test_dedup_key_includes_designated_timestamp(self):
        ddl = _norm(CANONICAL_INSIDER_TRADES)
        keys = ddl.split("UPSERT KEYS(")[1].split(")")[0]
        assert "FILING_DATE" in keys

    def test_partition_by_year(self):
        assert "PARTITION BY YEAR" in _norm(CANONICAL_INSIDER_TRADES)


class TestCanonicalInstitutionalHoldingsDDL:
    def test_declares_dedup_upsert_keys(self):
        ddl = _norm(CANONICAL_INSTITUTIONAL_HOLDINGS)
        assert "DEDUP UPSERT KEYS(FILING_DATE, SYMBOL, FILER_CIK, REPORT_PERIOD)" in ddl, (
            "canonical_institutional_holdings must declare the 4-column dedup key"
        )

    def test_is_a_wal_table(self):
        assert " WAL" in _norm(CANONICAL_INSTITUTIONAL_HOLDINGS)

    def test_designated_timestamp_is_filing_date(self):
        ddl = _norm(CANONICAL_INSTITUTIONAL_HOLDINGS)
        assert "TIMESTAMP(FILING_DATE)" in ddl

    def test_accession_number_is_a_column(self):
        assert "ACCESSION_NUMBER" in _norm(CANONICAL_INSTITUTIONAL_HOLDINGS)

    def test_partition_by_year(self):
        assert "PARTITION BY YEAR" in _norm(CANONICAL_INSTITUTIONAL_HOLDINGS)


class TestCanonicalInstOwnershipDDL:
    def test_declares_dedup_upsert_keys(self):
        ddl = _norm(CANONICAL_INST_OWNERSHIP)
        assert "DEDUP UPSERT KEYS(FILING_DATE, SYMBOL, REPORT_PERIOD)" in ddl, (
            "canonical_inst_ownership must declare the 3-column dedup key"
        )

    def test_is_a_wal_table(self):
        assert " WAL" in _norm(CANONICAL_INST_OWNERSHIP)

    def test_designated_timestamp_is_filing_date(self):
        ddl = _norm(CANONICAL_INST_OWNERSHIP)
        assert "TIMESTAMP(FILING_DATE)" in ddl

    def test_has_aggregate_columns(self):
        ddl = _norm(CANONICAL_INST_OWNERSHIP)
        assert "TOTAL_SHARES" in ddl
        assert "TOTAL_VALUE_USD" in ddl
        assert "FILER_COUNT" in ddl

    def test_filer_count_is_long(self):
        ddl = _norm(CANONICAL_INST_OWNERSHIP)
        assert "FILER_COUNT LONG" in ddl

    def test_partition_by_year(self):
        assert "PARTITION BY YEAR" in _norm(CANONICAL_INST_OWNERSHIP)


class TestMigrationsStage3b:
    def test_enables_dedup_on_canonical_insider_trades(self):
        joined = " ".join(_norm(m) for m in MIGRATIONS)
        assert "ALTER TABLE CANONICAL_INSIDER_TRADES DEDUP ENABLE UPSERT KEYS(FILING_DATE, SYMBOL, INSIDER_NAME, TRANSACTION_DATE, TRANSACTION_TYPE, SHARES)" in joined

    def test_enables_dedup_on_canonical_institutional_holdings(self):
        joined = " ".join(_norm(m) for m in MIGRATIONS)
        assert "ALTER TABLE CANONICAL_INSTITUTIONAL_HOLDINGS DEDUP ENABLE UPSERT KEYS(FILING_DATE, SYMBOL, FILER_CIK, REPORT_PERIOD)" in joined

    def test_enables_dedup_on_canonical_inst_ownership(self):
        joined = " ".join(_norm(m) for m in MIGRATIONS)
        assert "ALTER TABLE CANONICAL_INST_OWNERSHIP DEDUP ENABLE UPSERT KEYS(FILING_DATE, SYMBOL, REPORT_PERIOD)" in joined


class TestJobRunsDDL:
    """Guards ya-vs9a1: job_runs instrumentation table DDL and migration."""

    def test_declares_dedup_upsert_keys(self):
        ddl = _norm(JOB_RUNS)
        assert "DEDUP UPSERT KEYS(STARTED_AT, JOB_NAME, DAGSTER_RUN_ID)" in ddl, (
            "job_runs must declare DEDUP UPSERT KEYS so start-write is replaced by finish-write"
        )

    def test_is_a_wal_table(self):
        assert " WAL" in _norm(JOB_RUNS)

    def test_designated_timestamp_is_started_at(self):
        ddl = _norm(JOB_RUNS)
        assert "TIMESTAMP(STARTED_AT)" in ddl

    def test_partition_by_month(self):
        assert "PARTITION BY MONTH" in _norm(JOB_RUNS)

    def test_has_required_columns(self):
        ddl = _norm(JOB_RUNS)
        for col in ("JOB_NAME", "DAGSTER_RUN_ID", "STATUS", "FINISHED_AT",
                    "DURATION_S", "ROWS_WRITTEN", "DETAIL", "FAILURE_CAUSE"):
            assert col in ddl, f"job_runs DDL missing column: {col}"

    def test_status_is_symbol(self):
        ddl = _norm(JOB_RUNS)
        assert "STATUS SYMBOL" in ddl

    def test_rows_written_is_long(self):
        ddl = _norm(JOB_RUNS)
        assert "ROWS_WRITTEN LONG" in ddl


class TestMigrationsD0:
    def test_enables_dedup_on_job_runs(self):
        joined = " ".join(_norm(m) for m in MIGRATIONS)
        assert "ALTER TABLE JOB_RUNS DEDUP ENABLE UPSERT KEYS(STARTED_AT, JOB_NAME, DAGSTER_RUN_ID)" in joined
