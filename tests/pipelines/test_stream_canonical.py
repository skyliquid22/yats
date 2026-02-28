"""Tests for stream_canonical — validation, normalization, and ILP write."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, call

from yats_pipelines.jobs.stream_canonical import (
    _normalize_ws_bar,
    _validate_bar,
    _validation_status,
    _write_canonical_bar,
)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestValidateBar:
    def test_valid_bar(self):
        bar = {"open": 150.0, "high": 155.0, "low": 149.0, "close": 153.0, "volume": 1000}
        assert _validate_bar(bar) == []

    def test_missing_fields(self):
        bar = {"open": 150.0, "high": 155.0}
        warnings = _validate_bar(bar)
        assert "missing_low" in warnings
        assert "missing_close" in warnings
        assert "missing_volume" in warnings

    def test_non_positive_price(self):
        bar = {"open": 0, "high": 155.0, "low": -1.0, "close": 153.0, "volume": 1000}
        warnings = _validate_bar(bar)
        assert any("non_positive_open" in w for w in warnings)
        assert any("non_positive_low" in w for w in warnings)

    def test_empty_bar(self):
        warnings = _validate_bar({})
        assert len(warnings) == 5  # all 5 fields missing


class TestValidationStatus:
    def test_passed(self):
        assert _validation_status([]) == "passed"

    def test_failed_missing(self):
        assert _validation_status(["missing_open"]) == "failed"

    def test_failed_non_positive(self):
        assert _validation_status(["non_positive_close=0.0"]) == "failed"

    def test_warning_other(self):
        assert _validation_status(["some_other_warning"]) == "warning"


# ---------------------------------------------------------------------------
# Wire format normalization
# ---------------------------------------------------------------------------


class TestNormalizeWsBar:
    def test_full_bar(self):
        msg = {
            "T": "b",
            "S": "AAPL",
            "t": "2026-02-28T14:30:00Z",
            "o": 150.0,
            "h": 155.0,
            "l": 149.0,
            "c": 153.0,
            "v": 1000,
            "vw": 152.5,
            "n": 42,
        }
        bar = _normalize_ws_bar(msg)
        assert bar["symbol"] == "AAPL"
        assert bar["timestamp"] == "2026-02-28T14:30:00Z"
        assert bar["open"] == 150.0
        assert bar["high"] == 155.0
        assert bar["low"] == 149.0
        assert bar["close"] == 153.0
        assert bar["volume"] == 1000
        assert bar["vwap"] == 152.5
        assert bar["trade_count"] == 42

    def test_missing_optional_fields(self):
        msg = {
            "T": "b",
            "S": "MSFT",
            "t": "2026-02-28T14:30:00Z",
            "o": 400.0,
            "h": 405.0,
            "l": 399.0,
            "c": 402.0,
            "v": 500,
        }
        bar = _normalize_ws_bar(msg)
        assert bar["vwap"] == 0  # default when vw missing
        assert bar["trade_count"] == 0  # default when n missing


# ---------------------------------------------------------------------------
# ILP write
# ---------------------------------------------------------------------------


class TestWriteCanonicalBar:
    def test_writes_canonical_and_reconciliation(self):
        sender = MagicMock()
        bar = {
            "symbol": "AAPL",
            "timestamp": "2026-02-28T14:30:00Z",
            "open": 150.0,
            "high": 155.0,
            "low": 149.0,
            "close": 153.0,
            "volume": 1000,
            "vwap": 152.5,
            "trade_count": 42,
        }
        now = datetime(2026, 2, 28, 14, 30, 5, tzinfo=timezone.utc)
        status = _write_canonical_bar(sender, bar, now)

        assert status == "passed"
        assert sender.row.call_count == 2

        # First call: canonical_equity_ohlcv
        first_call = sender.row.call_args_list[0]
        assert first_call[0][0] == "canonical_equity_ohlcv"
        symbols = first_call[1]["symbols"]
        assert symbols["reconcile_method"] == "streaming"
        assert symbols["source_vendor"] == "alpaca"
        assert symbols["validation_status"] == "passed"

        # Second call: reconciliation_log
        second_call = sender.row.call_args_list[1]
        assert second_call[0][0] == "reconciliation_log"
        recon_symbols = second_call[1]["symbols"]
        assert recon_symbols["domain"] == "equity_ohlcv"
        assert recon_symbols["primary_vendor"] == "alpaca"

    def test_failed_bar_returns_failed_status(self):
        sender = MagicMock()
        bar = {
            "symbol": "BAD",
            "timestamp": "2026-02-28T14:30:00Z",
            "open": None,
            "high": 155.0,
            "low": 149.0,
            "close": 0,
            "volume": 1000,
            "vwap": 152.5,
            "trade_count": 42,
        }
        now = datetime(2026, 2, 28, 14, 30, 5, tzinfo=timezone.utc)
        status = _write_canonical_bar(sender, bar, now)
        assert status == "failed"

        first_call = sender.row.call_args_list[0]
        symbols = first_call[1]["symbols"]
        assert symbols["validation_status"] == "failed"
