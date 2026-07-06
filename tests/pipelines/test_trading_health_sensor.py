"""Tests for trading health sensor bug fixes (ya-1vwwc).

Covers:
A - naive UTC normalization: QuestDB timestamps arrive tz-naive; arithmetic
    with datetime.now(timezone.utc) (tz-aware) must not raise TypeError.
B - DST-aware market hours: _is_market_hours must honour EDT (UTC-4) vs
    EST (UTC-5) rather than using a hardcoded UTC-5 offset.
C - exception surfacing: sensor wraps errors into SkipReason so Dagster
    sees a clean evaluation rather than a silent crash.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from dagster import build_sensor_context


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_conn(hb_rows, livelock_rows=None):
    """Mock psycopg2 connection returning hb_rows then livelock_rows."""
    cur = MagicMock()
    cur.fetchall.side_effect = [hb_rows, livelock_rows or []]
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


# ---------------------------------------------------------------------------
# B — _is_market_hours: DST awareness
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("module", [
    "yats_pipelines.jobs.paper_trading",
    "yats_pipelines.jobs.live_trading",
])
def test_is_market_hours_edt_open(module):
    """14:00 UTC in June → 10:00 EDT (summer) → market open."""
    from importlib import import_module
    fn = import_module(module)._is_market_hours
    dt = datetime(2026, 6, 15, 14, 0, tzinfo=timezone.utc)
    assert fn(dt) is True


@pytest.mark.parametrize("module", [
    "yats_pipelines.jobs.paper_trading",
    "yats_pipelines.jobs.live_trading",
])
def test_is_market_hours_old_utc5_bug(module):
    """Regression: old UTC-5 code saw 14:00 UTC as 9:00 'ET' (before open).
    Correct answer with zoneinfo: 14:00 UTC in June is 10:00 EDT → open.
    """
    from importlib import import_module
    fn = import_module(module)._is_market_hours
    dt = datetime(2026, 7, 1, 14, 0, tzinfo=timezone.utc)
    assert fn(dt) is True  # old code returned False (hardcoded UTC-5 saw 9:00)


@pytest.mark.parametrize("module", [
    "yats_pipelines.jobs.paper_trading",
    "yats_pipelines.jobs.live_trading",
])
def test_is_market_hours_est_before_open(module):
    """14:00 UTC in January → 9:00 EST (winter) → before 9:30 open."""
    from importlib import import_module
    fn = import_module(module)._is_market_hours
    dt = datetime(2026, 1, 15, 14, 0, tzinfo=timezone.utc)
    assert fn(dt) is False


@pytest.mark.parametrize("module", [
    "yats_pipelines.jobs.paper_trading",
    "yats_pipelines.jobs.live_trading",
])
def test_is_market_hours_weekend(module):
    from importlib import import_module
    fn = import_module(module)._is_market_hours
    dt = datetime(2026, 7, 4, 14, 0, tzinfo=timezone.utc)  # Saturday
    assert fn(dt) is False


# ---------------------------------------------------------------------------
# A — naive datetime normalization in _check_heartbeat_health
# ---------------------------------------------------------------------------

def _naive_utc(delta: timedelta) -> datetime:
    """Return a tz-naive datetime representing UTC time offset by delta."""
    return datetime.now(timezone.utc).replace(tzinfo=None) + delta


def test_paper_check_heartbeat_naive_hb_no_typeerror():
    """Naive last_hb (QuestDB format) must not raise TypeError."""
    from yats_pipelines.jobs.paper_trading import _check_heartbeat_health

    stale_naive = _naive_utc(-timedelta(minutes=5))
    conn = _make_conn([(
        "exp-001", "paper",
        stale_naive,  # last_heartbeat — naive, 5 min ago → triggers alert
        10, 0, None,
    )])
    ctx = MagicMock()

    alerts = _check_heartbeat_health(conn, ctx)
    assert any(a["type"] == "no_heartbeat" for a in alerts)


def test_paper_check_heartbeat_naive_bar_no_typeerror():
    """Naive last_bar (QuestDB format) must not raise TypeError."""
    from yats_pipelines.jobs.paper_trading import _check_heartbeat_health

    fresh_naive = _naive_utc(-timedelta(seconds=10))
    stale_bar_naive = _naive_utc(-timedelta(minutes=5))
    conn = _make_conn([(
        "exp-001", "paper",
        fresh_naive,     # last_heartbeat — fresh, no heartbeat alert
        10, 0,
        stale_bar_naive, # last_bar — stale, triggers websocket_stale
    )])
    ctx = MagicMock()

    with patch("yats_pipelines.jobs.paper_trading._is_market_hours", return_value=True):
        alerts = _check_heartbeat_health(conn, ctx)

    assert any(a["type"] == "websocket_stale" for a in alerts)


def test_live_check_heartbeat_naive_hb_no_typeerror():
    """Naive last_hb in live_trading._check_live_heartbeat_health must not raise TypeError."""
    from yats_pipelines.jobs.live_trading import _check_live_heartbeat_health

    stale_naive = _naive_utc(-timedelta(minutes=5))
    conn = _make_conn([(
        "exp-002",
        stale_naive,  # last_heartbeat — naive
        10, 0, None,
    )])
    ctx = MagicMock()

    alerts = _check_live_heartbeat_health(conn, ctx)
    assert any(a["type"] == "no_heartbeat" for a in alerts)


# ---------------------------------------------------------------------------
# C — sensor surfaces exceptions as SkipReason
# ---------------------------------------------------------------------------

def test_paper_sensor_db_error_surfaces_as_skip():
    """DB connection failure → SkipReason, not unhandled exception."""
    from dagster import SkipReason
    from yats_pipelines.jobs.paper_trading import paper_trading_health_sensor

    ctx = build_sensor_context()
    with patch("psycopg2.connect", side_effect=Exception("connection refused")):
        result = paper_trading_health_sensor(ctx)

    assert isinstance(result, SkipReason)
    assert "connection refused" in str(result.skip_message)


def test_live_sensor_db_error_surfaces_as_skip():
    """DB connection failure → SkipReason in live sensor too."""
    from dagster import SkipReason
    from yats_pipelines.jobs.live_trading import live_trading_health_sensor

    ctx = build_sensor_context()
    with patch("psycopg2.connect", side_effect=Exception("timeout")):
        result = live_trading_health_sensor(ctx)

    assert isinstance(result, SkipReason)
    assert "timeout" in str(result.skip_message)


# ---------------------------------------------------------------------------
# C — stale heartbeat → RunRequest (end-to-end sensor path)
# ---------------------------------------------------------------------------

def test_paper_sensor_stale_heartbeat_yields_run_request():
    """Stale heartbeat (>3 min) causes sensor to return SensorResult with RunRequest."""
    from dagster import RunRequest, SensorResult
    from yats_pipelines.jobs.paper_trading import paper_trading_health_sensor

    stale_naive = _naive_utc(-timedelta(minutes=5))
    mock_conn = _make_conn([(
        "exp-stale", "paper",
        stale_naive, 10, 0, None,
    )])
    ctx = build_sensor_context()

    with patch("psycopg2.connect", return_value=mock_conn), \
         patch("yats_pipelines.jobs.paper_trading._write_alerts"):
        result = paper_trading_health_sensor(ctx)

    assert isinstance(result, SensorResult)
    assert len(result.run_requests) == 1
    assert isinstance(result.run_requests[0], RunRequest)
    assert "exp-stale" in result.run_requests[0].run_key
