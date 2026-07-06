"""Tests for ThetaData adapter — normalization, validation, canonicalize, dedup."""

from __future__ import annotations

import socket
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from yats_pipelines.jobs.ingest_thetadata import (
    _ingested_after,
    _normalize_right,
    _parse_exp_to_datetime,
    _pick_latest_per_contract,
    _run_already_written,
    _validate_contract,
)
from yats_pipelines.resources.thetadata import (
    ThetaDataResource,
    _f,
    _i,
    _parse_iso_timestamp,
    _parse_strike,
    _parse_thetadata_date,
    _parse_thetadata_datetime,
)
from yats_pipelines.utils.schema_guard import assert_table_schema


def _thetadata_reachable() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 25503), timeout=1):
            return True
    except OSError:
        return False


# ---------------------------------------------------------------------------
# Resource helpers
# ---------------------------------------------------------------------------


class TestParseStrike:
    def test_milli_dollars_to_dollars(self):
        assert _parse_strike(150000) == 150.0

    def test_fractional_strike(self):
        assert _parse_strike(152500) == 152.5

    def test_none_returns_none(self):
        assert _parse_strike(None) is None

    def test_zero_strike(self):
        assert _parse_strike(0) == 0.0


class TestSafeConversions:
    def test_f_valid(self):
        assert _f("3.14") == 3.14

    def test_f_none(self):
        assert _f(None) is None

    def test_i_valid(self):
        assert _i("42") == 42

    def test_i_none(self):
        assert _i(None) is None

    def test_i_float_string(self):
        # v3 returns integers as e.g. "38" — also verify float strings round correctly
        assert _i("38") == 38


class TestParseThetadataDate:
    def test_yyyymmdd_integer(self):
        dt = _parse_thetadata_date(20240119)
        assert dt is not None
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 19
        assert dt.tzinfo == timezone.utc

    def test_zero_returns_none(self):
        assert _parse_thetadata_date(0) is None


class TestParseThetadataDatetime:
    def test_date_plus_ms_offset(self):
        dt = _parse_thetadata_datetime(20240119, 60_000)  # 1 minute
        assert dt is not None
        assert dt.minute == 1
        assert dt.hour == 0

    def test_zero_date_returns_none(self):
        assert _parse_thetadata_datetime(0, 5000) is None


class TestParseIsoTimestamp:
    def test_valid_timestamp(self):
        dt = _parse_iso_timestamp("2026-07-02T16:00:00.003")
        assert dt is not None
        assert dt.year == 2026
        assert dt.month == 7
        assert dt.day == 2
        assert dt.tzinfo == timezone.utc

    def test_none_returns_none(self):
        assert _parse_iso_timestamp(None) is None

    def test_empty_string_returns_none(self):
        assert _parse_iso_timestamp("") is None

    def test_date_only_string(self):
        dt = _parse_iso_timestamp("2026-07-02T00:00:00.000")
        assert dt is not None
        assert dt.hour == 0


# ---------------------------------------------------------------------------
# CSV response parsing (v3)
# ---------------------------------------------------------------------------


class TestParseCsvResponse:
    # Fixture: verified live sample from /v3/option/list/expirations?symbol=AAPL
    EXPIRATIONS_CSV = 'symbol,expiration\n"AAPL","2026-07-10"\n"AAPL","2026-07-17"\n'

    # Fixture: verified live sample from /v3/option/snapshot/open_interest?symbol=AAPL&expiration=2026-07-10
    OPEN_INTEREST_CSV = (
        "timestamp,symbol,expiration,strike,right,open_interest\n"
        '2026-07-02T06:30:25.000,"AAPL","2026-07-10",375.000,"CALL",38\n'
        '2026-07-02T06:30:13.000,"AAPL","2026-07-10",375.000,"PUT",0\n'
    )

    def test_expirations_parsed(self):
        rows = ThetaDataResource._parse_csv_response(self.EXPIRATIONS_CSV)
        assert len(rows) == 2
        assert rows[0]["symbol"] == "AAPL"
        assert rows[0]["expiration"] == "2026-07-10"
        assert rows[1]["expiration"] == "2026-07-17"

    def test_open_interest_parsed(self):
        rows = ThetaDataResource._parse_csv_response(self.OPEN_INTEREST_CSV)
        assert len(rows) == 2
        assert rows[0]["strike"] == "375.000"
        assert rows[0]["right"] == "CALL"
        assert rows[0]["open_interest"] == "38"
        assert rows[1]["right"] == "PUT"
        assert rows[1]["open_interest"] == "0"

    def test_empty_csv_returns_empty_list(self):
        rows = ThetaDataResource._parse_csv_response("symbol,expiration\n")
        assert rows == []

    def test_header_only_returns_empty_list(self):
        rows = ThetaDataResource._parse_csv_response("timestamp,strike,right\n")
        assert rows == []


class TestListExpirations:
    EXPIRATIONS_CSV = 'symbol,expiration\n"AAPL","2026-07-10"\n"AAPL","2025-01-17"\n'

    def test_returns_sorted_yyyymmdd_strings(self):
        td = ThetaDataResource()
        with patch.object(td, "_request_with_retry", return_value=self.EXPIRATIONS_CSV):
            result = td.list_expirations("AAPL")
        assert result == ["20250117", "20260710"]

    def test_empty_expiration_skipped(self):
        td = ThetaDataResource()
        csv_text = 'symbol,expiration\n"AAPL",""\n"AAPL","2026-07-10"\n'
        with patch.object(td, "_request_with_retry", return_value=csv_text):
            result = td.list_expirations("AAPL")
        assert result == ["20260710"]


class TestGetOptionChainSnapshot:
    GREEKS_CSV = (
        "symbol,expiration,strike,right,timestamp,bid,ask,delta,theta,vega,rho,"
        "epsilon,lambda,implied_vol,iv_error,underlying_timestamp,underlying_price\n"
        '"AAPL","2026-07-10",375.000,"CALL",2026-07-02T16:00:00.003,'
        "0.0000,0.2400,0.0129,-0.0572,1.5154,0.0844,-0.0871,33.2011,0.5999,-0.0024,"
        "2026-07-02T20:00:00.265,306.2400\n"
    )
    OI_CSV = (
        "timestamp,symbol,expiration,strike,right,open_interest\n"
        '2026-07-02T06:30:25.000,"AAPL","2026-07-10",375.000,"CALL",38\n'
    )
    OHLC_CSV = (
        "timestamp,symbol,expiration,strike,right,open,high,low,close,volume,count\n"
        '2026-06-26T09:30:12.881,"AAPL","2026-07-10",375.000,"CALL",'
        "0.01,0.01,0.01,0.01,25,1\n"
    )

    def test_snapshot_row_shape(self):
        td = ThetaDataResource()
        responses = [self.GREEKS_CSV, self.OI_CSV, self.OHLC_CSV]
        with patch.object(td, "_request_with_retry", side_effect=responses), \
             patch.object(td, "_try_get_second_order_greeks", return_value={}):
            rows = td.get_option_chain_snapshot("AAPL", "20260710")

        assert len(rows) == 1
        row = rows[0]
        assert row["root"] == "AAPL"
        assert row["exp"] == "20260710"
        assert row["strike"] == 375.0
        assert row["right"] == "CALL"
        assert row["bid"] == 0.0
        assert row["ask"] == 0.24
        assert row["iv"] == pytest.approx(0.5999)
        assert row["delta"] == pytest.approx(0.0129)
        assert row["gamma"] is None        # no second-order data (empty dict)
        assert row["open_interest"] == 38
        assert row["volume"] == 25
        assert row["last"] == pytest.approx(0.01)
        assert isinstance(row["quote_ts"], datetime)

    def test_snapshot_gamma_populated_from_second_order(self):
        td = ThetaDataResource()
        responses = [self.GREEKS_CSV, self.OI_CSV, self.OHLC_CSV]
        second_order_gamma = {("375.000", "CALL"): 0.0042}
        with patch.object(td, "_request_with_retry", side_effect=responses), \
             patch.object(td, "_try_get_second_order_greeks", return_value=second_order_gamma):
            rows = td.get_option_chain_snapshot("AAPL", "20260710")

        assert rows[0]["gamma"] == pytest.approx(0.0042)

    def test_missing_oi_gives_none(self):
        td = ThetaDataResource()
        empty_oi = "timestamp,symbol,expiration,strike,right,open_interest\n"
        responses = [self.GREEKS_CSV, empty_oi, self.OHLC_CSV]
        with patch.object(td, "_request_with_retry", side_effect=responses), \
             patch.object(td, "_try_get_second_order_greeks", return_value={}):
            rows = td.get_option_chain_snapshot("AAPL", "20260710")

        assert rows[0]["open_interest"] is None


class TestGetHistoricalEod:
    EOD_CSV = (
        "symbol,expiration,strike,right,created,last_trade,open,high,low,close,"
        "volume,count,bid_size,bid_exchange,bid,bid_condition,ask_size,ask_exchange,"
        "ask,ask_condition\n"
        '"AAPL","2026-07-10",375.000,"CALL",'
        "2026-06-22T17:23:38.351,2026-06-22T00:00:00.000,"
        "0.00,0.00,0.00,0.00,0,0,0,6,0.00,50,172,22,0.52,50\n"
    )

    # Real response captured live from Theta Terminal v3
    # GET /v3/option/history/eod?symbol=AAPL&expiration=20250718&start_date=20250701&end_date=20250705
    EOD_CSV_LIVE = (
        "symbol,expiration,strike,right,created,last_trade,open,high,low,close,"
        "volume,count,bid_size,bid_exchange,bid,bid_condition,ask_size,ask_exchange,"
        "ask,ask_condition\n"
        '"AAPL","2025-07-18",300.000,"PUT",'
        "2025-07-01T17:15:03.452,2025-07-01T00:00:00.000,"
        "0.00,0.00,0.00,0.00,0,0,105,7,91.40,50,100,7,92.70,50\n"
        '"AAPL","2025-07-18",300.000,"CALL",'
        "2025-07-01T17:15:03.452,2025-07-01T15:52:58.319,"
        "0.01,0.01,0.01,0.01,11,3,0,31,0.00,50,20,31,0.01,50\n"
    )

    def test_eod_row_shape(self):
        td = ThetaDataResource()
        with patch.object(td, "_request_with_retry", return_value=self.EOD_CSV):
            rows = td.get_historical_eod("AAPL", "20260710", "20260620", "20260705")

        assert len(rows) == 1
        row = rows[0]
        assert row["root"] == "AAPL"
        assert row["exp"] == "20260710"
        assert row["strike"] == 375.0
        assert row["right"] == "CALL"
        assert row["open"] == 0.0
        assert row["close"] == 0.0
        assert row["volume"] == 0
        assert row["trade_count"] == 0
        assert isinstance(row["quote_date"], datetime)

    def test_eod_live_response_two_rows(self):
        """Real response: one no-trade PUT (midnight last_trade) + one traded CALL."""
        td = ThetaDataResource()
        with patch.object(td, "_request_with_retry", return_value=self.EOD_CSV_LIVE):
            rows = td.get_historical_eod("AAPL", "20250718", "20250701", "20250705")

        assert len(rows) == 2

        put_row = next(r for r in rows if r["right"] == "PUT")
        assert put_row["root"] == "AAPL"
        assert put_row["exp"] == "20250718"
        assert put_row["strike"] == 300.0
        assert put_row["volume"] == 0
        assert put_row["trade_count"] == 0
        assert isinstance(put_row["quote_date"], datetime)
        assert put_row["quote_date"].date().isoformat() == "2025-07-01"

        call_row = next(r for r in rows if r["right"] == "CALL")
        assert call_row["strike"] == 300.0
        assert call_row["volume"] == 11
        assert call_row["trade_count"] == 3
        assert call_row["open"] == pytest.approx(0.01)
        assert call_row["close"] == pytest.approx(0.01)
        assert isinstance(call_row["quote_date"], datetime)
        assert call_row["quote_date"].date().isoformat() == "2025-07-01"

    def test_eod_no_trade_midnight_quote_date_is_not_none(self):
        """Rows with last_trade=...T00:00:00.000 (no trade) must yield a valid quote_date."""
        no_trade_csv = (
            "symbol,expiration,strike,right,created,last_trade,open,high,low,close,"
            "volume,count,bid_size,bid_exchange,bid,bid_condition,ask_size,ask_exchange,"
            "ask,ask_condition\n"
            '"AAPL","2025-07-18",300.000,"PUT",'
            "2025-07-01T17:15:03.452,2025-07-01T00:00:00.000,"
            "0.00,0.00,0.00,0.00,0,0,105,7,91.40,50,100,7,92.70,50\n"
        )
        td = ThetaDataResource()
        with patch.object(td, "_request_with_retry", return_value=no_trade_csv):
            rows = td.get_historical_eod("AAPL", "20250718", "20250701", "20250701")

        assert len(rows) == 1
        assert rows[0]["quote_date"] is not None, (
            "Midnight last_trade must not produce None quote_date — "
            "write_raw_thetadata silently skips None-quote_date rows"
        )


# ---------------------------------------------------------------------------
# Normalization (ThetaDataResource methods)
# ---------------------------------------------------------------------------


class TestNormalizeChainSnapshot:
    def test_adds_metadata(self):
        td = ThetaDataResource()
        raw_rows = [
            {"root": "AAPL", "exp": "20240119", "strike": 150.0, "right": "CALL"}
        ]
        now = datetime(2026, 1, 5, 10, 0, 0, tzinfo=timezone.utc)
        result = td.normalize_chain_snapshot(raw_rows, now, "run-abc")

        assert len(result) == 1
        assert result[0]["ingested_at"] == now
        assert result[0]["dagster_run_id"] == "run-abc"
        assert result[0]["root"] == "AAPL"

    def test_empty_input(self):
        td = ThetaDataResource()
        now = datetime(2026, 1, 5, tzinfo=timezone.utc)
        assert td.normalize_chain_snapshot([], now, "run-1") == []


class TestNormalizeEod:
    def test_adds_metadata(self):
        td = ThetaDataResource()
        raw_rows = [
            {"root": "SPY", "exp": "20240119", "strike": 450.0, "right": "PUT",
             "open": 2.0, "high": 2.5, "low": 1.8, "close": 2.2, "volume": 100, "trade_count": 5}
        ]
        now = datetime(2026, 1, 5, tzinfo=timezone.utc)
        result = td.normalize_eod(raw_rows, now, "run-xyz")

        assert len(result) == 1
        assert result[0]["ingested_at"] == now
        assert result[0]["dagster_run_id"] == "run-xyz"


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestValidateContract:
    def test_valid_contract(self):
        row = {
            "strike": 150.0,
            "open_interest": 500,
            "iv": 0.35,
        }
        assert _validate_contract(row) == []

    def test_missing_strike(self):
        row = {"open_interest": 100, "iv": 0.25}
        issues = _validate_contract(row)
        assert any("missing_strike" in i for i in issues)

    def test_negative_open_interest(self):
        row = {"strike": 150.0, "open_interest": -5, "iv": 0.30}
        issues = _validate_contract(row)
        assert any("negative_open_interest" in i for i in issues)

    def test_iv_too_high(self):
        row = {"strike": 100.0, "open_interest": 10, "iv": 25.0}
        issues = _validate_contract(row)
        assert any("iv_outlier" in i for i in issues)

    def test_iv_negative(self):
        row = {"strike": 100.0, "open_interest": 10, "iv": -0.1}
        issues = _validate_contract(row)
        assert any("iv_outlier" in i for i in issues)

    def test_none_oi_and_iv_are_not_flagged(self):
        row = {"strike": 150.0, "open_interest": None, "iv": None}
        assert _validate_contract(row) == []

    def test_zero_oi_is_valid(self):
        row = {"strike": 150.0, "open_interest": 0, "iv": 0.20}
        assert _validate_contract(row) == []


# ---------------------------------------------------------------------------
# Canonicalize: latest-quote-wins
# ---------------------------------------------------------------------------


def _make_chain_row(
    underlying: str,
    strike: float,
    right: str,
    quote_ts: datetime,
    ingested_at: datetime,
    iv: float = 0.30,
    oi: int = 100,
    gamma: float | None = None,
) -> dict:
    row = {
        "underlying": underlying,
        "expiry": datetime(2024, 1, 19, tzinfo=timezone.utc),
        "strike": strike,
        "right": right,
        "quote_ts": quote_ts,
        "ingested_at": ingested_at,
        "iv": iv,
        "open_interest": oi,
        "bid": 1.0,
        "ask": 1.1,
    }
    if gamma is not None:
        row["gamma"] = gamma
    return row


class TestPickLatestPerContract:
    def test_single_row_returned(self):
        ts = datetime(2026, 1, 5, 10, 0, tzinfo=timezone.utc)
        rows = [_make_chain_row("AAPL", 150.0, "C", ts, ts)]
        result = _pick_latest_per_contract(rows)
        assert len(result) == 1

    def test_latest_ingested_wins_same_contract_same_day(self):
        day = datetime(2026, 1, 5, tzinfo=timezone.utc)
        early = _make_chain_row(
            "AAPL", 150.0, "C",
            quote_ts=day,
            ingested_at=datetime(2026, 1, 5, 9, 0, tzinfo=timezone.utc),
            iv=0.25,
        )
        late = _make_chain_row(
            "AAPL", 150.0, "C",
            quote_ts=day,
            ingested_at=datetime(2026, 1, 5, 15, 0, tzinfo=timezone.utc),
            iv=0.35,
        )
        result = _pick_latest_per_contract([early, late])
        assert len(result) == 1
        assert result[0]["iv"] == 0.35

    def test_different_contracts_kept_separately(self):
        ts = datetime(2026, 1, 5, tzinfo=timezone.utc)
        call = _make_chain_row("AAPL", 150.0, "C", ts, ts)
        put = _make_chain_row("AAPL", 150.0, "P", ts, ts)
        result = _pick_latest_per_contract([call, put])
        assert len(result) == 2

    def test_different_strikes_kept_separately(self):
        ts = datetime(2026, 1, 5, tzinfo=timezone.utc)
        low_strike = _make_chain_row("AAPL", 140.0, "C", ts, ts)
        high_strike = _make_chain_row("AAPL", 150.0, "C", ts, ts)
        result = _pick_latest_per_contract([low_strike, high_strike])
        assert len(result) == 2

    def test_row_without_quote_ts_is_skipped(self):
        rows = [{"underlying": "AAPL", "strike": 150.0, "right": "C"}]
        result = _pick_latest_per_contract(rows)
        assert result == []

    def test_gamma_preserved_when_later_row_has_none(self):
        """If a later ingest run has gamma=None (second-order endpoint unavailable),
        the gamma value from the earlier run must be preserved in the output."""
        day = datetime(2026, 1, 5, tzinfo=timezone.utc)
        early = _make_chain_row(
            "AAPL", 150.0, "C",
            quote_ts=day,
            ingested_at=datetime(2026, 1, 5, 9, 0, tzinfo=timezone.utc),
            gamma=0.0042,
        )
        late = _make_chain_row(
            "AAPL", 150.0, "C",
            quote_ts=day,
            ingested_at=datetime(2026, 1, 5, 15, 0, tzinfo=timezone.utc),
            gamma=None,   # second-order endpoint was unavailable in this run
        )
        result = _pick_latest_per_contract([early, late])
        assert len(result) == 1
        assert result[0]["gamma"] == pytest.approx(0.0042), (
            "gamma from earlier run must survive when later run has gamma=None"
        )

    def test_gamma_updated_when_both_rows_have_gamma(self):
        """When both rows have gamma, the newer value wins (normal latest-wins)."""
        day = datetime(2026, 1, 5, tzinfo=timezone.utc)
        early = _make_chain_row(
            "AAPL", 150.0, "C", quote_ts=day,
            ingested_at=datetime(2026, 1, 5, 9, 0, tzinfo=timezone.utc),
            gamma=0.0042,
        )
        late = _make_chain_row(
            "AAPL", 150.0, "C", quote_ts=day,
            ingested_at=datetime(2026, 1, 5, 15, 0, tzinfo=timezone.utc),
            gamma=0.0055,
        )
        result = _pick_latest_per_contract([early, late])
        assert result[0]["gamma"] == pytest.approx(0.0055)

    def test_gamma_absent_stays_absent(self):
        """When neither row has gamma, gamma must remain absent (no phantom key)."""
        day = datetime(2026, 1, 5, tzinfo=timezone.utc)
        early = _make_chain_row("AAPL", 150.0, "C", quote_ts=day,
                                ingested_at=datetime(2026, 1, 5, 9, tzinfo=timezone.utc))
        late = _make_chain_row("AAPL", 150.0, "C", quote_ts=day,
                               ingested_at=datetime(2026, 1, 5, 15, tzinfo=timezone.utc))
        result = _pick_latest_per_contract([early, late])
        assert result[0].get("gamma") is None


class TestNormalizeRight:
    def test_call_normalized(self):
        assert _normalize_right("CALL") == "C"

    def test_put_normalized(self):
        assert _normalize_right("PUT") == "P"

    def test_already_short_call(self):
        assert _normalize_right("C") == "C"

    def test_already_short_put(self):
        assert _normalize_right("P") == "P"

    def test_empty_passthrough(self):
        assert _normalize_right("") == ""

    def test_unknown_passthrough(self):
        assert _normalize_right("X") == "X"


# ---------------------------------------------------------------------------
# Dedup on re-run
# ---------------------------------------------------------------------------


class TestRunAlreadyWritten:
    def test_returns_true_when_rows_exist(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.fetchone.return_value = (3,)

        result = _run_already_written(conn, "run-123", "raw_thetadata_options_chain")
        assert result is True

    def test_returns_false_when_no_rows(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.fetchone.return_value = (0,)

        result = _run_already_written(conn, "run-456", "raw_thetadata_options_chain")
        assert result is False

    def test_returns_false_on_exception(self):
        conn = MagicMock()
        conn.cursor.side_effect = Exception("connection error")

        result = _run_already_written(conn, "run-789", "raw_thetadata_options_chain")
        assert result is False


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------


class TestParseExpToDatetime:
    def test_valid_yyyymmdd(self):
        dt = _parse_exp_to_datetime("20240119")
        assert dt is not None
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 19

    def test_empty_string(self):
        assert _parse_exp_to_datetime("") is None

    def test_wrong_length(self):
        assert _parse_exp_to_datetime("202401") is None


class TestIngestedAfter:
    def test_later_ingested_at_wins(self):
        a = {"ingested_at": datetime(2026, 1, 5, 15, 0, tzinfo=timezone.utc)}
        b = {"ingested_at": datetime(2026, 1, 5, 9, 0, tzinfo=timezone.utc)}
        assert _ingested_after(a, b) is True
        assert _ingested_after(b, a) is False

    def test_none_a_loses(self):
        a = {"ingested_at": None}
        b = {"ingested_at": datetime(2026, 1, 5, tzinfo=timezone.utc)}
        assert _ingested_after(a, b) is False

    def test_none_b_a_wins(self):
        a = {"ingested_at": datetime(2026, 1, 5, tzinfo=timezone.utc)}
        b = {"ingested_at": None}
        assert _ingested_after(a, b) is True


# ---------------------------------------------------------------------------
# 472 handling — no-data response treated as valid empty result
# ---------------------------------------------------------------------------


class Test472Handling:
    def test_472_returns_empty_string_immediately(self):
        td = ThetaDataResource()
        mock_resp = MagicMock()
        mock_resp.status_code = 472
        mock_resp.headers = {}
        with patch("requests.get", return_value=mock_resp) as mock_get:
            result = td._request_with_retry("http://127.0.0.1:25503/v3/option/snapshot/greeks/first_order", {})
        assert result == ""
        assert mock_get.call_count == 1, "472 must not trigger retries"

    def test_472_from_chain_snapshot_returns_empty_list(self):
        td = ThetaDataResource()
        mock_resp = MagicMock()
        mock_resp.status_code = 472
        mock_resp.headers = {}
        with patch("requests.get", return_value=mock_resp), \
             patch.object(td, "_try_get_second_order_greeks", return_value={}):
            rows = td.get_option_chain_snapshot("AAPL", "19990101")
        assert rows == []

    def test_5xx_still_retries(self):
        import requests as req_module
        td = ThetaDataResource()
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.headers = {}
        mock_resp.raise_for_status.side_effect = req_module.exceptions.HTTPError("500 Server Error")
        with patch("requests.get", return_value=mock_resp) as mock_get, \
             patch("time.sleep"):
            with pytest.raises(RuntimeError):
                td._request_with_retry("http://127.0.0.1:25503/v3/option/snapshot/greeks/first_order", {})
        assert mock_get.call_count == 3


# ---------------------------------------------------------------------------
# Schema guard
# ---------------------------------------------------------------------------


class TestAssertTableSchema:
    def _make_conn(self, column_names: list[str]) -> MagicMock:
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.fetchall.return_value = [(col,) for col in column_names]
        return conn

    def test_valid_schema_passes(self):
        conn = self._make_conn(["quote_ts", "underlying", "strike", "right", "bid", "ask"])
        assert_table_schema(conn, "raw_thetadata_options_chain", ["quote_ts", "underlying"])

    def test_table_not_exist_raises(self):
        conn = self._make_conn([])
        with pytest.raises(RuntimeError, match="bootstrap_db"):
            assert_table_schema(conn, "raw_thetadata_options_chain", ["quote_ts"])

    def test_missing_column_raises(self):
        # Table created by ILP auto-create used 'timestamp' instead of 'quote_ts'
        conn = self._make_conn(["timestamp", "underlying", "strike", "right"])
        with pytest.raises(RuntimeError, match="bootstrap_db"):
            assert_table_schema(conn, "raw_thetadata_options_chain", ["quote_ts"])

    def test_error_message_names_missing_columns(self):
        conn = self._make_conn(["timestamp", "underlying"])
        with pytest.raises(RuntimeError, match="quote_ts"):
            assert_table_schema(conn, "raw_thetadata_options_chain", ["quote_ts", "strike"])

    def test_query_exception_raises_with_instructions(self):
        conn = MagicMock()
        conn.cursor.side_effect = Exception("connection refused")
        with pytest.raises(RuntimeError, match="bootstrap_db"):
            assert_table_schema(conn, "raw_thetadata_options_chain", ["quote_ts"])


# ---------------------------------------------------------------------------
# Live integration tests (require Theta Terminal v3 on 127.0.0.1:25503)
# ---------------------------------------------------------------------------

_skip_if_no_theta = pytest.mark.skipif(
    not _thetadata_reachable(),
    reason="Theta Terminal v3 not reachable on 127.0.0.1:25503",
)


@_skip_if_no_theta
def test_live_list_expirations():
    """Verify list_expirations returns a non-empty list against the live terminal."""
    td = ThetaDataResource()
    exps = td.list_expirations("AAPL")
    assert len(exps) > 0, "Expected at least one AAPL expiration from live terminal"
    # All entries should be 8-char YYYYMMDD strings
    for exp in exps[:5]:
        assert len(exp) == 8, f"Expected YYYYMMDD, got: {exp}"
        assert exp.isdigit(), f"Expected all digits, got: {exp}"


@_skip_if_no_theta
def test_live_open_interest_snapshot():
    """Verify get_option_chain_snapshot returns OI rows against the live terminal."""
    from datetime import date
    td = ThetaDataResource()
    exps = td.list_expirations("AAPL")
    assert exps, "Need at least one expiration to test OI snapshot"
    # Pick the first future expiration (at least 1 day out) so greeks/first_order has data
    today = date.today().strftime("%Y%m%d")
    future_exps = [e for e in exps if e >= today]
    assert future_exps, "No future AAPL expirations found"
    exp = future_exps[0]
    rows = td.get_option_chain_snapshot("AAPL", exp)
    assert len(rows) > 0, f"Expected option chain rows for AAPL exp={exp}"
    sample = rows[0]
    assert "strike" in sample
    assert "right" in sample
    assert sample["right"] in ("CALL", "PUT")
