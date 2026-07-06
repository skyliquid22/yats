"""Tests for ThetaData adapter — normalization, validation, canonicalize, dedup."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from yats_pipelines.jobs.ingest_thetadata import (
    _ingested_after,
    _parse_exp_to_datetime,
    _pick_latest_per_contract,
    _run_already_written,
    _validate_contract,
)
from yats_pipelines.resources.thetadata import (
    ThetaDataResource,
    _f,
    _i,
    _parse_strike,
    _parse_thetadata_date,
    _parse_thetadata_datetime,
)


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


# ---------------------------------------------------------------------------
# Normalization (ThetaDataResource methods)
# ---------------------------------------------------------------------------


class TestNormalizeChainSnapshot:
    def test_adds_metadata(self):
        td = ThetaDataResource()
        raw_rows = [
            {"root": "AAPL", "exp": "20240119", "strike": 150.0, "right": "C"}
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
            {"root": "SPY", "exp": "20240119", "strike": 450.0, "right": "P",
             "open": 2.0, "high": 2.5, "low": 1.8, "close": 2.2, "volume": 100, "trade_count": 5}
        ]
        now = datetime(2026, 1, 5, tzinfo=timezone.utc)
        result = td.normalize_eod(raw_rows, now, "run-xyz")

        assert len(result) == 1
        assert result[0]["ingested_at"] == now
        assert result[0]["dagster_run_id"] == "run-xyz"


class TestParseResponse:
    def test_columnar_response_to_dicts(self):
        data = {
            "header": {"format": ["strike", "right", "implied_vol"]},
            "response": [[150000, "C", 0.25], [155000, "P", 0.30]],
        }
        rows = ThetaDataResource._parse_response(data)
        assert len(rows) == 2
        assert rows[0] == {"strike": 150000, "right": "C", "implied_vol": 0.25}
        assert rows[1] == {"strike": 155000, "right": "P", "implied_vol": 0.30}

    def test_empty_response(self):
        data = {"header": {"format": ["strike"]}, "response": []}
        assert ThetaDataResource._parse_response(data) == []


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
        # Missing optional fields are not validation failures
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
) -> dict:
    return {
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
