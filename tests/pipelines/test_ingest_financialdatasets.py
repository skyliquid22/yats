"""Tests for financialdatasets.ai adapter and ingest job.

Guards ya-2gqv7:
- Insider trades keyed at filing_date (point-in-time); fallback to transaction_date logged.
- All 6 new insider_trade columns land in the ILP row.
- Institutional holdings row mapping and pagination exhaustion.
- DDL: raw_fd_institutional_holdings has expected columns + PARTITION BY YEAR WAL.
- Migrations: ALTER TABLE adds the 6 new columns to raw_fd_insider_trades.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest

from yats_pipelines.jobs.ingest_financialdatasets import (
    ALL_DOMAINS,
    _ingest_insider_trades,
    _ingest_institutional_holdings,
    _ts,
)
from yats_pipelines.resources.financialdatasets import FinancialDatasetsResource
from yats_pipelines.utils.create_tables import MIGRATIONS, RAW_FD_INSTITUTIONAL_HOLDINGS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip().upper()


def _make_sender():
    sender = MagicMock()
    sender.row = MagicMock()
    return sender


_NOW = datetime(2026, 7, 8, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Insider trades — point-in-time keying
# ---------------------------------------------------------------------------


class TestIngestInsiderTradesPointInTime:
    """filing_date is the designated timestamp; transaction_date is a data column."""

    def _run(self, records):
        fd = MagicMock(spec=FinancialDatasetsResource)
        fd.get_insider_trades.return_value = records
        sender = _make_sender()
        _ingest_insider_trades(fd, sender, ["AAPL"], _NOW)
        return sender

    def test_uses_filing_date_as_designated_timestamp(self):
        sender = self._run([{
            "filing_date": "2024-03-15",
            "transaction_date": "2024-03-13",
            "transaction_type": "P",
            "name": "John Doe",
            "title": "CEO",
            "transaction_shares": 1000.0,
            "transaction_price_per_share": 175.0,
            "transaction_value": 175000.0,
            "shares_owned_after_transaction": 50000.0,
            "is_board_director": True,
            "shares_owned_before_transaction": 49000.0,
            "security_title": "Common Stock",
            "issuer": "Apple Inc.",
        }])
        assert sender.row.call_count == 1
        _, kwargs = sender.row.call_args
        # Designated timestamp must be filing_date, not transaction_date
        assert kwargs["at"] == datetime(2024, 3, 15, tzinfo=timezone.utc)

    def test_transaction_date_preserved_as_column(self):
        sender = self._run([{
            "filing_date": "2024-03-15",
            "transaction_date": "2024-03-13",
            "transaction_type": "P",
        }])
        _, kwargs = sender.row.call_args
        assert kwargs["columns"]["transaction_date"] == datetime(2024, 3, 13, tzinfo=timezone.utc)

    def test_filing_date_preserved_as_column(self):
        sender = self._run([{
            "filing_date": "2024-03-15",
            "transaction_date": "2024-03-13",
            "transaction_type": "P",
        }])
        _, kwargs = sender.row.call_args
        assert kwargs["columns"]["filing_date"] == datetime(2024, 3, 15, tzinfo=timezone.utc)

    def test_fallback_to_transaction_date_when_filing_date_missing(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING):
            sender = self._run([{
                "filing_date": None,
                "transaction_date": "2024-03-13",
                "transaction_type": "S",
            }])
        assert sender.row.call_count == 1
        _, kwargs = sender.row.call_args
        assert kwargs["at"] == datetime(2024, 3, 13, tzinfo=timezone.utc)
        assert "falling back to transaction_date" in caplog.text

    def test_skips_row_when_both_dates_missing(self):
        sender = self._run([{
            "filing_date": None,
            "transaction_date": None,
            "transaction_type": "P",
        }])
        assert sender.row.call_count == 0

    def test_new_columns_land(self):
        sender = self._run([{
            "filing_date": "2024-03-15",
            "transaction_date": "2024-03-13",
            "transaction_type": "P",
            "name": "Jane Smith",
            "title": "CFO",
            "transaction_shares": 500.0,
            "transaction_price_per_share": 180.0,
            "transaction_value": 90000.0,
            "shares_owned_after_transaction": 20000.0,
            "is_board_director": False,
            "shares_owned_before_transaction": 19500.0,
            "security_title": "Common Stock",
            "issuer": "Apple Inc.",
        }])
        _, kwargs = sender.row.call_args
        cols = kwargs["columns"]
        assert cols["is_board_director"] is False
        assert cols["shares_owned_before"] == 19500.0
        assert cols["security_title"] == "Common Stock"
        assert cols["issuer"] == "Apple Inc."

    def test_returns_row_count(self):
        fd = MagicMock(spec=FinancialDatasetsResource)
        fd.get_insider_trades.return_value = [
            {"filing_date": "2024-03-15", "transaction_date": "2024-03-13", "transaction_type": "P"},
            {"filing_date": "2024-03-16", "transaction_date": "2024-03-14", "transaction_type": "S"},
        ]
        sender = _make_sender()
        count = _ingest_insider_trades(fd, sender, ["AAPL"], _NOW)
        assert count == 2

    def test_multiple_tickers(self):
        fd = MagicMock(spec=FinancialDatasetsResource)
        fd.get_insider_trades.return_value = [
            {"filing_date": "2024-03-15", "transaction_date": "2024-03-13", "transaction_type": "P"},
        ]
        sender = _make_sender()
        _ingest_insider_trades(fd, sender, ["AAPL", "MSFT"], _NOW)
        assert fd.get_insider_trades.call_count == 2
        assert sender.row.call_count == 2


# ---------------------------------------------------------------------------
# Institutional holdings — row mapping + pagination
# ---------------------------------------------------------------------------


class TestIngestInstitutionalHoldings:
    def _run(self, records, run_id="run-abc"):
        fd = MagicMock(spec=FinancialDatasetsResource)
        fd.get_institutional_holdings.return_value = records
        sender = _make_sender()
        count = _ingest_institutional_holdings(fd, sender, ["AAPL"], _NOW, run_id=run_id)
        return sender, count

    def _sample_record(self, overrides=None):
        rec = {
            "filing_date": "2024-03-31",
            "report_period": "2023-12-31",
            "accession_number": "0001234567-24-000001",
            "filer_cik": "0001234567",
            "filer_name": "Vanguard Group",
            "cusip": "037833100",
            "title_of_class": "COM",
            "shares": 1234567.0,
            "value_usd": 215000000.0,
            "reported_price": 174.18,
        }
        if overrides:
            rec.update(overrides)
        return rec

    def test_row_mapped_to_correct_table(self):
        sender, _ = self._run([self._sample_record()])
        assert sender.row.call_count == 1
        args, _ = sender.row.call_args
        assert args[0] == "raw_fd_institutional_holdings"

    def test_filing_date_is_designated_timestamp(self):
        sender, _ = self._run([self._sample_record()])
        _, kwargs = sender.row.call_args
        assert kwargs["at"] == datetime(2024, 3, 31, tzinfo=timezone.utc)

    def test_symbol_in_symbols(self):
        sender, _ = self._run([self._sample_record()])
        _, kwargs = sender.row.call_args
        assert kwargs["symbols"]["symbol"] == "AAPL"

    def test_filer_cik_in_symbols(self):
        sender, _ = self._run([self._sample_record()])
        _, kwargs = sender.row.call_args
        assert kwargs["symbols"]["filer_cik"] == "0001234567"

    def test_cusip_in_symbols(self):
        sender, _ = self._run([self._sample_record()])
        _, kwargs = sender.row.call_args
        assert kwargs["symbols"]["cusip"] == "037833100"

    def test_columns_mapped_correctly(self):
        sender, _ = self._run([self._sample_record()])
        _, kwargs = sender.row.call_args
        cols = kwargs["columns"]
        assert cols["accession_number"] == "0001234567-24-000001"
        assert cols["filer_name"] == "Vanguard Group"
        assert cols["shares"] == 1234567.0
        assert cols["value_usd"] == 215000000.0
        assert cols["reported_price"] == 174.18
        assert cols["report_period"] == datetime(2023, 12, 31, tzinfo=timezone.utc)

    def test_dagster_run_id_in_columns(self):
        sender, _ = self._run([self._sample_record()], run_id="run-xyz")
        _, kwargs = sender.row.call_args
        assert kwargs["columns"]["dagster_run_id"] == "run-xyz"

    def test_skips_row_when_filing_date_missing(self):
        sender, count = self._run([self._sample_record({"filing_date": None})])
        assert sender.row.call_count == 0
        assert count == 0

    def test_returns_row_count(self):
        _, count = self._run([self._sample_record(), self._sample_record()])
        assert count == 2

    def test_adapter_exception_handled_gracefully(self):
        fd = MagicMock(spec=FinancialDatasetsResource)
        fd.get_institutional_holdings.side_effect = RuntimeError("network error")
        sender = _make_sender()
        count = _ingest_institutional_holdings(fd, sender, ["AAPL"], _NOW)
        assert count == 0
        assert sender.row.call_count == 0


# ---------------------------------------------------------------------------
# Adapter pagination
# ---------------------------------------------------------------------------


class TestGetInstitutionalHoldingsPagination:
    def test_single_page_returned_when_below_limit(self):
        fd = FinancialDatasetsResource(api_key="test")
        page = [{"ticker": "AAPL", "filing_date": "2024-03-31"}] * 3
        with patch.object(fd, "_get", return_value={"institutional_holdings": page}) as mock_get:
            result = fd.get_institutional_holdings("AAPL", limit=100)
        assert len(result) == 3
        mock_get.assert_called_once_with("/institutional-holdings", {"ticker": "AAPL", "limit": 100, "offset": 0})

    def test_paginates_until_exhaustion(self):
        fd = FinancialDatasetsResource(api_key="test")
        full_page = [{"ticker": "AAPL", "filing_date": "2024-03-31"}] * 2
        partial_page = [{"ticker": "AAPL", "filing_date": "2024-06-30"}] * 1
        responses = [
            {"institutional_holdings": full_page},
            {"institutional_holdings": partial_page},
        ]
        with patch.object(fd, "_get", side_effect=responses) as mock_get:
            result = fd.get_institutional_holdings("AAPL", limit=2)
        assert len(result) == 3
        assert mock_get.call_count == 2
        mock_get.assert_any_call("/institutional-holdings", {"ticker": "AAPL", "limit": 2, "offset": 0})
        mock_get.assert_any_call("/institutional-holdings", {"ticker": "AAPL", "limit": 2, "offset": 2})

    def test_empty_response_returns_empty_list(self):
        fd = FinancialDatasetsResource(api_key="test")
        with patch.object(fd, "_get", return_value={"institutional_holdings": []}):
            result = fd.get_institutional_holdings("AAPL")
        assert result == []


# ---------------------------------------------------------------------------
# DDL: raw_fd_institutional_holdings
# ---------------------------------------------------------------------------


class TestRawFdInstitutionalHoldingsDDL:
    def test_table_name(self):
        assert "raw_fd_institutional_holdings" in RAW_FD_INSTITUTIONAL_HOLDINGS.lower()

    def test_filing_date_is_designated_timestamp(self):
        ddl = _norm(RAW_FD_INSTITUTIONAL_HOLDINGS)
        assert "TIMESTAMP(FILING_DATE)" in ddl

    def test_partition_by_year_wal(self):
        ddl = _norm(RAW_FD_INSTITUTIONAL_HOLDINGS)
        assert "PARTITION BY YEAR WAL" in ddl

    def test_required_columns_present(self):
        ddl = _norm(RAW_FD_INSTITUTIONAL_HOLDINGS)
        for col in ("SYMBOL", "REPORT_PERIOD", "ACCESSION_NUMBER", "FILER_CIK",
                    "FILER_NAME", "CUSIP", "TITLE_OF_CLASS", "SHARES", "VALUE_USD",
                    "REPORTED_PRICE", "INGESTED_AT", "DAGSTER_RUN_ID"):
            assert col in ddl, f"Missing column: {col}"


# ---------------------------------------------------------------------------
# Migrations: ALTER TABLE for raw_fd_insider_trades new columns
# ---------------------------------------------------------------------------


class TestInsiderTradesMigrations:
    def _migrations_text(self):
        return " ".join(_norm(m) for m in MIGRATIONS)

    def test_filing_date_column_added(self):
        assert "ALTER TABLE RAW_FD_INSIDER_TRADES ADD COLUMN FILING_DATE TIMESTAMP" in self._migrations_text()

    def test_transaction_date_column_added(self):
        assert "ALTER TABLE RAW_FD_INSIDER_TRADES ADD COLUMN TRANSACTION_DATE TIMESTAMP" in self._migrations_text()

    def test_is_board_director_column_added(self):
        assert "ALTER TABLE RAW_FD_INSIDER_TRADES ADD COLUMN IS_BOARD_DIRECTOR BOOLEAN" in self._migrations_text()

    def test_shares_owned_before_column_added(self):
        assert "ALTER TABLE RAW_FD_INSIDER_TRADES ADD COLUMN SHARES_OWNED_BEFORE DOUBLE" in self._migrations_text()

    def test_security_title_column_added(self):
        assert "ALTER TABLE RAW_FD_INSIDER_TRADES ADD COLUMN SECURITY_TITLE STRING" in self._migrations_text()

    def test_issuer_column_added(self):
        assert "ALTER TABLE RAW_FD_INSIDER_TRADES ADD COLUMN ISSUER STRING" in self._migrations_text()


# ---------------------------------------------------------------------------
# ALL_DOMAINS includes institutional_holdings
# ---------------------------------------------------------------------------


class TestAllDomains:
    def test_institutional_holdings_in_all_domains(self):
        assert "institutional_holdings" in ALL_DOMAINS
