"""Tests for canonicalize — _validate_ohlcv_row and _canonicalize_fundamentals."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from yats_pipelines.jobs.canonicalize import (
    CanonicalizeConfig,
    _validate_ohlcv_row,
    _build_rolling_stats,
    _canonicalize_fundamentals,
)


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
        assert "missing_open" not in warnings  # open is present

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
        # Build rolling stats where close=150 is far from mean=100
        rolling_stats = {
            "AAPL": {"mean": 100.0, "stdev": 10.0, "count": 20},
        }
        row = _good_row(close=155.0)  # z = (155-100)/10 = 5.5 > 5.0
        warnings = _validate_ohlcv_row(row, rolling_stats=rolling_stats, std_threshold=5.0)
        assert any("outlier_close_z" in w for w in warnings)
        # z value should be in the warning string
        assert any("5.50" in w for w in warnings)

    def test_outlier_not_triggered_below_threshold(self):
        rolling_stats = {
            "AAPL": {"mean": 100.0, "stdev": 10.0, "count": 20},
        }
        row = _good_row(close=140.0)  # z = (140-100)/10 = 4.0 < 5.0
        warnings = _validate_ohlcv_row(row, rolling_stats=rolling_stats, std_threshold=5.0)
        assert not any("outlier" in w for w in warnings)

    def test_outlier_requires_count_20(self):
        rolling_stats = {
            "AAPL": {"mean": 100.0, "stdev": 1.0, "count": 19},  # < 20
        }
        row = _good_row(close=200.0)  # Would be extreme outlier with count >= 20
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
        # 100 rows, window=10 — should only use last 10
        rows = [{"symbol": "AAPL", "close": float(i)} for i in range(1, 101)]
        stats = _build_rolling_stats(rows, window=10)
        # Last 10 values: 91..100, mean = 95.5
        import statistics
        expected_mean = statistics.mean(range(91, 101))
        assert pytest.approx(stats["AAPL"]["mean"], rel=1e-6) == expected_mean


# ---------------------------------------------------------------------------
# _canonicalize_fundamentals — latest-report-date-wins logic
# ---------------------------------------------------------------------------


def _make_conn_with_rows(rows: list[dict]):
    """Build a mock psycopg2 connection that returns the given rows."""
    if not rows:
        columns = []
        raw_rows = []
    else:
        columns = list(rows[0].keys())
        raw_rows = [tuple(r[c] for c in columns) for r in rows]

    mock_cur = MagicMock()
    mock_cur.description = [(col, None, None, None, None, None, None) for col in columns]
    mock_cur.fetchall.return_value = raw_rows

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    return mock_conn, mock_cur


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
                "symbol": "AAPL", "report_date": t2,  # same key, later date
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

        # Only 1 canonical row (deduplicated by key), plus 1 reconciliation log
        assert count == 1
        # The row written should use t2 (latest) as timestamp
        canonical_call = sender.row.call_args_list[0]
        written_ts_arg = canonical_call[1]["at"]
        # TimestampNanos doesn't implement __eq__ — compare via string representation
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

        # 2 symbols → 2 canonical rows + 2 reconciliation log rows = 4 sender.row calls
        assert count == 2
        assert sender.row.call_count == 4

    def test_empty_rows_returns_zero(self):
        conn, _ = _make_conn_with_rows([])
        sender = MagicMock()
        now = datetime(2023, 1, 10, tzinfo=timezone.utc)
        log = MagicMock()

        count = _canonicalize_fundamentals(conn, sender, self._config(), now, "run-3", log)

        assert count == 0
        sender.row.assert_not_called()

    def test_earlier_report_does_not_win(self):
        """Ordering: rows come in time order; later row must supersede earlier."""
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

        # First sender.row call is the canonical write — check revenue=999 (from t_late)
        canonical_call = sender.row.call_args_list[0]
        assert canonical_call[1]["columns"]["revenue"] == 999
