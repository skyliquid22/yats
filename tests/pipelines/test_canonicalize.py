"""Tests for canonicalize — dedup guards, validation, and fundamentals logic."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, call

import pytest

from yats_pipelines.jobs.canonicalize import (
    CanonicalizeConfig,
    _build_rolling_stats,
    _canonicalize_equity_ohlcv,
    _canonicalize_financial_metrics,
    _canonicalize_fundamentals,
    _canonicalize_option_eod,
    _load_fundamentals_weighted_shares,
    _pick_latest_eod_per_contract,
    _run_already_written,
    _validate_ohlcv_row,
)


# ---------------------------------------------------------------------------
# _run_already_written
# ---------------------------------------------------------------------------


def _make_conn_dedup(count: int):
    """Mock connection returning count for SELECT count(*) dedup check."""
    cur = MagicMock()
    cur.fetchone.return_value = (count,)
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


def test_run_already_written_true():
    conn = _make_conn_dedup(3)
    assert _run_already_written(conn, "run-123", "equity_ohlcv") is True


def test_run_already_written_false():
    conn = _make_conn_dedup(0)
    assert _run_already_written(conn, "run-456", "equity_ohlcv") is False


def test_run_already_written_query_error_returns_false():
    conn = MagicMock()
    conn.cursor.side_effect = Exception("DB error")
    assert _run_already_written(conn, "run-789", "fundamentals") is False


def test_equity_ohlcv_dedup_skips_on_second_run():
    """When run_id already in reconciliation_log, no ILP rows are written."""
    now = datetime(2024, 1, 5, tzinfo=timezone.utc)
    config = CanonicalizeConfig()
    sender = MagicMock()
    log = MagicMock()

    conn = _make_conn_dedup(1)  # dedup check returns 1 → skip
    count = _canonicalize_equity_ohlcv(conn, sender, config, now, "run-001", log)

    assert count == 0
    sender.row.assert_not_called()


def test_equity_ohlcv_writes_on_first_run():
    """When run_id is new, rows should be written and count > 0."""
    now = datetime(2024, 1, 5, tzinfo=timezone.utc)
    config = CanonicalizeConfig(domains=["equity_ohlcv"])
    sender = MagicMock()
    log = MagicMock()

    raw_row = (
        datetime(2024, 1, 4, 10, 0, 0, tzinfo=timezone.utc),
        "AAPL", 150.0, 155.0, 149.0, 153.0, 10000, 151.0, 500, None,
    )
    raw_columns = ["timestamp", "symbol", "open", "high", "low", "close",
                   "volume", "vwap", "trade_count", "ingested_at"]

    call_count = [0]

    def cursor_factory():
        call_count[0] += 1
        cur = MagicMock()
        if call_count[0] == 1:
            cur.fetchone.return_value = (0,)  # dedup: not written
        else:
            cur.description = [(col,) for col in raw_columns]
            cur.fetchall.return_value = [raw_row]
        return cur

    conn = MagicMock()
    conn.cursor.side_effect = cursor_factory

    count = _canonicalize_equity_ohlcv(conn, sender, config, now, "run-NEW", log)

    assert count == 1
    assert sender.row.call_count == 2  # canonical row + reconciliation_log row


def test_fundamentals_dedup_skips_on_second_run():
    now = datetime(2024, 1, 5, tzinfo=timezone.utc)
    config = CanonicalizeConfig()
    sender = MagicMock()
    log = MagicMock()

    conn = _make_conn_dedup(1)
    count = _canonicalize_fundamentals(conn, sender, config, now, "run-001", log)

    assert count == 0
    sender.row.assert_not_called()


def test_financial_metrics_dedup_skips_on_second_run():
    now = datetime(2024, 1, 5, tzinfo=timezone.utc)
    config = CanonicalizeConfig()
    sender = MagicMock()
    log = MagicMock()

    conn = _make_conn_dedup(1)
    count = _canonicalize_financial_metrics(conn, sender, config, now, "run-001", log)

    assert count == 0
    sender.row.assert_not_called()


# ---------------------------------------------------------------------------
# _validate_ohlcv_row
# ---------------------------------------------------------------------------


def _good_row(symbol: str = "AAPL", close: float = 150.0) -> dict:
    return {
        "symbol": symbol,
        "open": 149.0,
        "high": 151.0,
        "low": 148.0,
        "close": close,
        "volume": 1000,
    }


class TestValidateOhlcvRow:
    def test_valid_row_no_warnings(self):
        row = _good_row()
        warnings = _validate_ohlcv_row(row, rolling_stats={}, std_threshold=5.0)
        assert warnings == []

    def test_missing_fields(self):
        row = {"symbol": "AAPL", "open": 150.0, "high": 155.0}
        warnings = _validate_ohlcv_row(row, rolling_stats={}, std_threshold=5.0)
        assert "missing_low" in warnings
        assert "missing_close" in warnings
        assert "missing_volume" in warnings
        assert "missing_open" not in warnings

    def test_all_fields_missing(self):
        row = {"symbol": "AAPL"}
        warnings = _validate_ohlcv_row(row, rolling_stats={}, std_threshold=5.0)
        assert len([w for w in warnings if w.startswith("missing_")]) == 5

    def test_non_positive_open(self):
        row = _good_row()
        row["open"] = 0
        warnings = _validate_ohlcv_row(row, rolling_stats={}, std_threshold=5.0)
        assert any("non_positive_open=0" in w for w in warnings)

    def test_non_positive_close(self):
        row = _good_row()
        row["close"] = -5.0
        warnings = _validate_ohlcv_row(row, rolling_stats={}, std_threshold=5.0)
        assert any("non_positive_close" in w for w in warnings)

    def test_outlier_detection_triggers(self):
        rolling_stats = {"AAPL": {"mean": 100.0, "stdev": 10.0, "count": 20}}
        row = _good_row(close=155.0)  # z = (155-100)/10 = 5.5 > 5.0
        warnings = _validate_ohlcv_row(row, rolling_stats=rolling_stats, std_threshold=5.0)
        assert any("outlier_close_z" in w for w in warnings)
        assert any("5.50" in w for w in warnings)

    def test_outlier_not_triggered_below_threshold(self):
        rolling_stats = {"AAPL": {"mean": 100.0, "stdev": 10.0, "count": 20}}
        row = _good_row(close=140.0)  # z = 4.0 < 5.0
        warnings = _validate_ohlcv_row(row, rolling_stats=rolling_stats, std_threshold=5.0)
        assert not any("outlier" in w for w in warnings)

    def test_outlier_requires_count_20(self):
        rolling_stats = {"AAPL": {"mean": 100.0, "stdev": 1.0, "count": 19}}
        row = _good_row(close=200.0)
        warnings = _validate_ohlcv_row(row, rolling_stats=rolling_stats, std_threshold=5.0)
        assert not any("outlier" in w for w in warnings)

    def test_missing_and_non_positive_both_reported(self):
        row = {"symbol": "AAPL", "open": 0, "high": 150.0}
        warnings = _validate_ohlcv_row(row, rolling_stats={}, std_threshold=5.0)
        assert any("non_positive_open" in w for w in warnings)
        assert "missing_low" in warnings
        assert "missing_close" in warnings
        assert "missing_volume" in warnings


class TestBuildRollingStats:
    def test_basic_stats(self):
        rows = [{"symbol": "AAPL", "close": float(100 + i)} for i in range(25)]
        stats = _build_rolling_stats(rows, window=60)
        assert "AAPL" in stats
        assert stats["AAPL"]["count"] == 25
        assert stats["AAPL"]["stdev"] > 0

    def test_excludes_nonpositive_close(self):
        rows = [
            {"symbol": "AAPL", "close": 0.0},
            {"symbol": "AAPL", "close": -1.0},
        ] + [{"symbol": "AAPL", "close": 100.0} for _ in range(10)]
        stats = _build_rolling_stats(rows, window=60)
        assert stats["AAPL"]["count"] == 10

    def test_requires_at_least_2_points(self):
        rows = [{"symbol": "AAPL", "close": 100.0}]
        stats = _build_rolling_stats(rows, window=60)
        assert "AAPL" not in stats

    def test_uses_last_window_rows(self):
        rows = [{"symbol": "AAPL", "close": float(i)} for i in range(1, 101)]
        stats = _build_rolling_stats(rows, window=10)
        import statistics
        expected_mean = statistics.mean(range(91, 101))
        assert pytest.approx(stats["AAPL"]["mean"], rel=1e-6) == expected_mean


# ---------------------------------------------------------------------------
# _canonicalize_fundamentals — latest-report-date-wins logic
# ---------------------------------------------------------------------------


def _make_conn_with_rows(rows: list[dict]):
    """Build a mock psycopg2 connection that returns the given rows.

    The first cursor call is the dedup check (_run_already_written) — it returns
    fetchone=(0,) meaning not yet written. The second call returns the raw rows.
    """
    if not rows:
        columns = []
        raw_rows = []
    else:
        columns = list(rows[0].keys())
        raw_rows = [tuple(r[c] for c in columns) for r in rows]

    call_count = [0]

    def cursor_factory():
        call_count[0] += 1
        cur = MagicMock()
        if call_count[0] == 1:
            # Dedup check cursor: return count=0 (not already written)
            cur.fetchone.return_value = (0,)
        else:
            cur.description = [(col, None, None, None, None, None, None) for col in columns]
            cur.fetchall.return_value = raw_rows
        return cur

    mock_conn = MagicMock()
    mock_conn.cursor.side_effect = cursor_factory
    return mock_conn, None  # second element kept for API compat


class TestCanonicalizeFundamentals:
    def _config(self) -> CanonicalizeConfig:
        return CanonicalizeConfig()

    def test_latest_report_date_wins(self):
        """When two rows share (symbol, fiscal_period, period), latest report_date wins."""
        t1 = datetime(2023, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2023, 1, 2, tzinfo=timezone.utc)

        rows = [
            {
                "symbol": "AAPL", "report_date": t1,
                "fiscal_period": "Q4", "period": "2022Q4",
                "revenue": 1_000_000, "cost_of_revenue": 600_000,
                "gross_profit": 400_000, "operating_expense": 200_000,
                "operating_income": 200_000, "net_income": 150_000,
                "eps": 1.5, "eps_diluted": 1.45,
                "weighted_average_shares": 100_000_000,
                "weighted_average_shares_diluted": 103_000_000,
            },
            {
                "symbol": "AAPL", "report_date": t2,
                "fiscal_period": "Q4", "period": "2022Q4",
                "revenue": 1_100_000, "cost_of_revenue": 650_000,
                "gross_profit": 450_000, "operating_expense": 210_000,
                "operating_income": 240_000, "net_income": 180_000,
                "eps": 1.8, "eps_diluted": 1.75,
                "weighted_average_shares": 100_000_000,
                "weighted_average_shares_diluted": 103_000_000,
            },
        ]
        conn, _ = _make_conn_with_rows(rows)
        sender = MagicMock()
        now = datetime(2023, 1, 10, tzinfo=timezone.utc)
        log = MagicMock()

        count = _canonicalize_fundamentals(conn, sender, self._config(), now, "run-1", log)

        assert count == 1
        canonical_call = sender.row.call_args_list[0]
        written_ts_arg = canonical_call[1]["at"]
        from yats_pipelines.jobs.canonicalize import _ts_nanos
        assert str(written_ts_arg) == str(_ts_nanos(t2))

    def test_multiple_symbols_separate_keys(self):
        """Different symbols are independent — each gets its own canonical row."""
        t1 = datetime(2023, 1, 1, tzinfo=timezone.utc)
        base_row = {
            "report_date": t1, "fiscal_period": "Q4", "period": "2022Q4",
            "revenue": 1_000_000, "cost_of_revenue": 600_000,
            "gross_profit": 400_000, "operating_expense": 200_000,
            "operating_income": 200_000, "net_income": 150_000,
            "eps": 1.5, "eps_diluted": 1.45,
            "weighted_average_shares": 100_000_000,
            "weighted_average_shares_diluted": 103_000_000,
        }
        rows = [
            {**base_row, "symbol": "AAPL"},
            {**base_row, "symbol": "MSFT"},
        ]
        conn, _ = _make_conn_with_rows(rows)
        sender = MagicMock()
        now = datetime(2023, 1, 10, tzinfo=timezone.utc)
        log = MagicMock()

        count = _canonicalize_fundamentals(conn, sender, self._config(), now, "run-2", log)

        assert count == 2
        assert sender.row.call_count == 4  # 2 canonical + 2 reconciliation_log

    def test_empty_rows_returns_zero(self):
        conn, _ = _make_conn_with_rows([])
        sender = MagicMock()
        now = datetime(2023, 1, 10, tzinfo=timezone.utc)
        log = MagicMock()

        count = _canonicalize_fundamentals(conn, sender, self._config(), now, "run-3", log)

        assert count == 0
        sender.row.assert_not_called()

    def test_earlier_report_does_not_win(self):
        """Ordering: later row supersedes earlier for same key."""
        t_early = datetime(2023, 1, 1, tzinfo=timezone.utc)
        t_late = datetime(2023, 1, 5, tzinfo=timezone.utc)
        base = {
            "symbol": "AAPL", "fiscal_period": "Q4", "period": "2022Q4",
            "revenue": None, "cost_of_revenue": None,
            "gross_profit": None, "operating_expense": None,
            "operating_income": None, "net_income": None,
            "eps": None, "eps_diluted": None,
            "weighted_average_shares": None,
            "weighted_average_shares_diluted": None,
        }
        rows = [
            {**base, "report_date": t_early, "revenue": 100},
            {**base, "report_date": t_late, "revenue": 999},
        ]
        conn, _ = _make_conn_with_rows(rows)
        sender = MagicMock()
        now = datetime(2023, 1, 10, tzinfo=timezone.utc)
        log = MagicMock()

        _canonicalize_fundamentals(conn, sender, self._config(), now, "run-4", log)

        canonical_call = sender.row.call_args_list[0]
        assert canonical_call[1]["columns"]["revenue"] == 999


# ---------------------------------------------------------------------------
# _load_fundamentals_weighted_shares — shares_outstanding data gap fix (ya-balr8)
# ---------------------------------------------------------------------------


def _make_fund_shares_conn(rows: list[tuple] | None = None, fail: bool = False):
    """Mock conn for _load_fundamentals_weighted_shares.

    rows: list of (symbol, timestamp, weighted_average_shares) tuples.
    fail=True simulates a DB error.
    """
    cur = MagicMock()
    if fail:
        cur.execute.side_effect = Exception("DB error")
    else:
        cur.fetchall.return_value = rows or []
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


class TestLoadFundamentalsWeightedShares:
    """Unit tests for the _load_fundamentals_weighted_shares helper."""

    def test_returns_sorted_timeline_per_symbol(self):
        from datetime import date

        t1 = datetime(2023, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2023, 6, 1, tzinfo=timezone.utc)
        rows = [
            ("AAPL", t1, 1_000_000.0),
            ("AAPL", t2, 1_100_000.0),
        ]
        conn = _make_fund_shares_conn(rows)
        result = _load_fundamentals_weighted_shares(conn)

        assert "AAPL" in result
        assert len(result["AAPL"]) == 2
        assert result["AAPL"][0][0] == date(2023, 1, 1)
        assert result["AAPL"][1][1] == pytest.approx(1_100_000.0)

    def test_skips_none_shares(self):
        t1 = datetime(2023, 1, 1, tzinfo=timezone.utc)
        rows = [("AAPL", t1, None), ("MSFT", t1, 500_000.0)]
        conn = _make_fund_shares_conn(rows)
        result = _load_fundamentals_weighted_shares(conn)

        assert "AAPL" not in result
        assert "MSFT" in result

    def test_returns_empty_on_db_error(self):
        conn = _make_fund_shares_conn(fail=True)
        result = _load_fundamentals_weighted_shares(conn)
        assert result == {}

    def test_deduplicates_same_date_keeps_last(self):
        """Two rows for same symbol+date — later row's value is kept."""
        from datetime import date

        t = datetime(2023, 3, 1, tzinfo=timezone.utc)
        rows = [("AAPL", t, 900_000.0), ("AAPL", t, 1_000_000.0)]
        conn = _make_fund_shares_conn(rows)
        result = _load_fundamentals_weighted_shares(conn)

        assert len(result["AAPL"]) == 1
        assert result["AAPL"][0][1] == pytest.approx(1_000_000.0)


class TestSharesOutstandingEnrichment:
    """_canonicalize_financial_metrics must enrich shares_outstanding from raw_fd_fundamentals."""

    def _make_metrics_conn(self, metrics_rows: list[dict], fund_rows: list[tuple]):
        """Mock conn with 3 cursor calls: dedup / metrics data / fundamentals shares."""
        metrics_columns = list(metrics_rows[0].keys()) if metrics_rows else []
        metrics_raw = [tuple(r[c] for c in metrics_columns) for r in metrics_rows]

        call_count = [0]

        def cursor_factory():
            call_count[0] += 1
            cur = MagicMock()
            if call_count[0] == 1:
                cur.fetchone.return_value = (0,)  # dedup: not yet written
            elif call_count[0] == 2:
                cur.description = [(col, None, None, None, None, None, None) for col in metrics_columns]
                cur.fetchall.return_value = metrics_raw
            else:
                cur.fetchall.return_value = fund_rows  # fundamentals shares
            return cur

        conn = MagicMock()
        conn.cursor.side_effect = cursor_factory
        return conn

    def test_shares_outstanding_filled_from_fundamentals(self):
        """When metrics lacks shares_outstanding, it is sourced from raw_fd_fundamentals."""
        t_metric = datetime(2023, 6, 1, tzinfo=timezone.utc)
        t_fund = datetime(2023, 1, 1, tzinfo=timezone.utc)

        metrics_rows = [{
            "symbol": "AAPL",
            "timestamp": t_metric,
            "market_cap": None,
            "pe_ratio": 20.0,
            "ps_ratio": None,
            "pb_ratio": None,
            "ev_ebitda": None,
            "roe": 0.15,
            "gross_margin": 0.40,
            "operating_margin": None,
            "net_margin": None,
            "fcf_margin": None,
            "debt_to_equity": None,
            "revenue_growth_yoy": None,
            "eps_growth_yoy": None,
            "shares_outstanding": None,  # missing from metrics API
            "ingested_at": t_metric,
        }]
        fund_rows = [("AAPL", t_fund, 1_000_000.0)]

        conn = self._make_metrics_conn(metrics_rows, fund_rows)
        sender = MagicMock()
        config = CanonicalizeConfig(start_date="2023-06-01", end_date="2023-06-01")
        now = datetime(2023, 6, 5, tzinfo=timezone.utc)
        log = MagicMock()

        count = _canonicalize_financial_metrics(conn, sender, config, now, "run-enrich", log)

        assert count == 1
        # Find the canonical_financial_metrics row call
        metrics_calls = [
            c for c in sender.row.call_args_list
            if c[1]["symbols"].get("symbol") == "AAPL"
            and "shares_outstanding" in c[1].get("columns", {})
        ]
        assert metrics_calls, "Expected shares_outstanding to be written"
        assert metrics_calls[0][1]["columns"]["shares_outstanding"] == pytest.approx(1_000_000.0)

    def test_metrics_shares_takes_precedence_over_fundamentals(self):
        """When metrics API provides shares_outstanding, it is not overwritten by fundamentals."""
        t_metric = datetime(2023, 6, 1, tzinfo=timezone.utc)
        t_fund = datetime(2023, 1, 1, tzinfo=timezone.utc)

        metrics_rows = [{
            "symbol": "AAPL",
            "timestamp": t_metric,
            "market_cap": None,
            "pe_ratio": 20.0,
            "ps_ratio": None,
            "pb_ratio": None,
            "ev_ebitda": None,
            "roe": None,
            "gross_margin": None,
            "operating_margin": None,
            "net_margin": None,
            "fcf_margin": None,
            "debt_to_equity": None,
            "revenue_growth_yoy": None,
            "eps_growth_yoy": None,
            "shares_outstanding": 2_000_000.0,  # API does provide it
            "ingested_at": t_metric,
        }]
        fund_rows = [("AAPL", t_fund, 1_000_000.0)]  # different value

        conn = self._make_metrics_conn(metrics_rows, fund_rows)
        sender = MagicMock()
        config = CanonicalizeConfig(start_date="2023-06-01", end_date="2023-06-01")
        now = datetime(2023, 6, 5, tzinfo=timezone.utc)
        log = MagicMock()

        _canonicalize_financial_metrics(conn, sender, config, now, "run-no-overwrite", log)

        metrics_calls = [
            c for c in sender.row.call_args_list
            if c[1]["symbols"].get("symbol") == "AAPL"
            and "shares_outstanding" in c[1].get("columns", {})
        ]
        assert metrics_calls
        assert metrics_calls[0][1]["columns"]["shares_outstanding"] == pytest.approx(2_000_000.0)


# ---------------------------------------------------------------------------
# _pick_latest_eod_per_contract
# ---------------------------------------------------------------------------


def _make_eod_row(
    underlying: str,
    strike: float,
    right: str,
    quote_date: datetime,
    ingested_at: datetime,
    iv: float | None = None,
    open_interest: int | None = None,
    gamma: float | None = None,
) -> dict:
    return {
        "underlying": underlying,
        "expiry": datetime(2026, 8, 15, tzinfo=timezone.utc),
        "strike": strike,
        "right": right,
        "quote_date": quote_date,
        "ingested_at": ingested_at,
        "open": 1.0,
        "high": 1.5,
        "low": 0.9,
        "close": 1.1,
        "bid": 1.0,
        "ask": 1.2,
        "volume": 100,
        "trade_count": 5,
        "iv": iv,
        "delta": 0.5 if iv is not None else None,
        "theta": -0.01 if iv is not None else None,
        "vega": 0.1 if iv is not None else None,
        "rho": 0.05 if iv is not None else None,
        "gamma": gamma,
        "open_interest": open_interest,
    }


class TestPickLatestEodPerContract:
    def test_single_row_returned(self):
        qd = datetime(2026, 6, 15, tzinfo=timezone.utc)
        rows = [_make_eod_row("AAPL", 200.0, "C", qd, qd)]
        result = _pick_latest_eod_per_contract(rows)
        assert len(result) == 1

    def test_latest_ingested_wins(self):
        qd = datetime(2026, 6, 15, tzinfo=timezone.utc)
        early = _make_eod_row(
            "AAPL", 200.0, "C", qd,
            ingested_at=datetime(2026, 6, 15, 9, tzinfo=timezone.utc),
            iv=0.25,
        )
        late = _make_eod_row(
            "AAPL", 200.0, "C", qd,
            ingested_at=datetime(2026, 6, 15, 18, tzinfo=timezone.utc),
            iv=0.30,
        )
        result = _pick_latest_eod_per_contract([early, late])
        assert len(result) == 1
        assert result[0]["iv"] == pytest.approx(0.30)

    def test_different_strike_kept_separately(self):
        qd = datetime(2026, 6, 15, tzinfo=timezone.utc)
        rows = [
            _make_eod_row("AAPL", 190.0, "C", qd, qd),
            _make_eod_row("AAPL", 200.0, "C", qd, qd),
        ]
        result = _pick_latest_eod_per_contract(rows)
        assert len(result) == 2

    def test_different_dates_kept_separately(self):
        rows = [
            _make_eod_row("AAPL", 200.0, "C",
                          datetime(2026, 6, 15, tzinfo=timezone.utc),
                          datetime(2026, 6, 15, tzinfo=timezone.utc)),
            _make_eod_row("AAPL", 200.0, "C",
                          datetime(2026, 6, 16, tzinfo=timezone.utc),
                          datetime(2026, 6, 16, tzinfo=timezone.utc)),
        ]
        result = _pick_latest_eod_per_contract(rows)
        assert len(result) == 2

    def test_greek_preserved_when_later_row_has_none(self):
        """iv from an earlier ingest survives when a later ingest has iv=None."""
        qd = datetime(2026, 6, 15, tzinfo=timezone.utc)
        early = _make_eod_row(
            "AAPL", 200.0, "C", qd,
            ingested_at=datetime(2026, 6, 15, 9, tzinfo=timezone.utc),
            iv=0.28,
        )
        late = _make_eod_row(
            "AAPL", 200.0, "C", qd,
            ingested_at=datetime(2026, 6, 15, 18, tzinfo=timezone.utc),
            iv=None,
        )
        result = _pick_latest_eod_per_contract([early, late])
        assert result[0]["iv"] == pytest.approx(0.28)

    def test_none_quote_date_row_skipped(self):
        row = {"underlying": "AAPL", "strike": 200.0, "right": "C", "quote_date": None}
        result = _pick_latest_eod_per_contract([row])
        assert result == []


# ---------------------------------------------------------------------------
# _canonicalize_option_eod
# ---------------------------------------------------------------------------


def _make_eod_conn(rows: list[dict]):
    """Mock connection for _canonicalize_option_eod.

    First cursor: dedup check returns 0 (not yet written).
    Second cursor: returns the raw EOD rows.
    """
    if not rows:
        columns = []
        raw_rows = []
    else:
        columns = list(rows[0].keys())
        raw_rows = [tuple(r.get(c) for c in columns) for r in rows]

    call_count = [0]

    def cursor_factory():
        call_count[0] += 1
        cur = MagicMock()
        if call_count[0] == 1:
            cur.fetchone.return_value = (0,)
        else:
            cur.description = [(col, None, None, None, None, None, None) for col in columns]
            cur.fetchall.return_value = raw_rows
        return cur

    conn = MagicMock()
    conn.cursor.side_effect = cursor_factory
    return conn


class TestCanonicalizeOptionEod:
    def _config(self, **kwargs) -> CanonicalizeConfig:
        return CanonicalizeConfig(domains=["option_eod"], **kwargs)

    def test_empty_rows_returns_zero(self):
        conn = _make_eod_conn([])
        sender = MagicMock()
        now = datetime(2026, 6, 20, tzinfo=timezone.utc)
        log = MagicMock()

        count = _canonicalize_option_eod(conn, sender, self._config(), now, "run-1", log)

        assert count == 0
        sender.row.assert_not_called()

    def test_dedup_skips_on_second_run(self):
        cur = MagicMock()
        cur.fetchone.return_value = (1,)
        conn = MagicMock()
        conn.cursor.return_value = cur
        sender = MagicMock()
        now = datetime(2026, 6, 20, tzinfo=timezone.utc)
        log = MagicMock()

        count = _canonicalize_option_eod(conn, sender, self._config(), now, "run-dup", log)

        assert count == 0
        sender.row.assert_not_called()

    def test_writes_canonical_and_reconciliation_rows(self):
        qd = datetime(2026, 6, 15, tzinfo=timezone.utc)
        rows = [_make_eod_row("AAPL", 200.0, "C", qd, qd, iv=0.25, open_interest=500)]
        conn = _make_eod_conn(rows)
        sender = MagicMock()
        now = datetime(2026, 6, 20, tzinfo=timezone.utc)
        log = MagicMock()

        count = _canonicalize_option_eod(conn, sender, self._config(), now, "run-2", log)

        assert count == 1
        assert sender.row.call_count == 2  # canonical + reconciliation_log

    def test_source_vendor_is_thetadata_eod(self):
        qd = datetime(2026, 6, 15, tzinfo=timezone.utc)
        rows = [_make_eod_row("AAPL", 200.0, "C", qd, qd)]
        conn = _make_eod_conn(rows)
        sender = MagicMock()
        now = datetime(2026, 6, 20, tzinfo=timezone.utc)
        log = MagicMock()

        _canonicalize_option_eod(conn, sender, self._config(), now, "run-3", log)

        canonical_call = sender.row.call_args_list[0]
        assert canonical_call[1]["symbols"]["source_vendor"] == "thetadata_eod"
        assert canonical_call[1]["symbols"]["reconcile_method"] == "eod_latest"

    def test_close_mapped_to_last(self):
        qd = datetime(2026, 6, 15, tzinfo=timezone.utc)
        rows = [_make_eod_row("AAPL", 200.0, "C", qd, qd)]
        rows[0]["close"] = 2.5
        conn = _make_eod_conn(rows)
        sender = MagicMock()
        now = datetime(2026, 6, 20, tzinfo=timezone.utc)
        log = MagicMock()

        _canonicalize_option_eod(conn, sender, self._config(), now, "run-4", log)

        canonical_call = sender.row.call_args_list[0]
        assert canonical_call[1]["columns"].get("last") == pytest.approx(2.5)

    def test_right_normalized_to_short_form(self):
        qd = datetime(2026, 6, 15, tzinfo=timezone.utc)
        row = _make_eod_row("AAPL", 200.0, "CALL", qd, qd)
        conn = _make_eod_conn([row])
        sender = MagicMock()
        now = datetime(2026, 6, 20, tzinfo=timezone.utc)
        log = MagicMock()

        _canonicalize_option_eod(conn, sender, self._config(), now, "run-5", log)

        canonical_call = sender.row.call_args_list[0]
        assert canonical_call[1]["symbols"]["right"] == "C"

    def test_null_greeks_not_written(self):
        """When iv=None (historical endpoint unavailable), the column is omitted."""
        qd = datetime(2026, 6, 15, tzinfo=timezone.utc)
        rows = [_make_eod_row("AAPL", 200.0, "C", qd, qd, iv=None)]
        conn = _make_eod_conn(rows)
        sender = MagicMock()
        now = datetime(2026, 6, 20, tzinfo=timezone.utc)
        log = MagicMock()

        _canonicalize_option_eod(conn, sender, self._config(), now, "run-6", log)

        canonical_call = sender.row.call_args_list[0]
        assert "iv" not in canonical_call[1]["columns"]

    def test_greeks_written_when_available(self):
        qd = datetime(2026, 6, 15, tzinfo=timezone.utc)
        rows = [_make_eod_row("AAPL", 200.0, "C", qd, qd, iv=0.30, open_interest=1000, gamma=0.005)]
        conn = _make_eod_conn(rows)
        sender = MagicMock()
        now = datetime(2026, 6, 20, tzinfo=timezone.utc)
        log = MagicMock()

        _canonicalize_option_eod(conn, sender, self._config(), now, "run-7", log)

        canonical_call = sender.row.call_args_list[0]
        cols = canonical_call[1]["columns"]
        assert cols.get("iv") == pytest.approx(0.30)
        assert cols.get("open_interest") == 1000
        assert cols.get("gamma") == pytest.approx(0.005)
