"""Tests for research.governance.explosion_guard — PRD §23.5."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from research.governance.explosion_guard import (
    EXPLOSION_THRESHOLD_DEFAULT,
    EXPLOSION_WINDOW_DAYS_DEFAULT,
    ExplosionGuardError,
    check_explosion_guard,
    count_recent_experiments,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_conn(count: int) -> MagicMock:
    """Return a mock psycopg2 connection that returns ``count`` from SELECT."""
    cur = MagicMock()
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = (count,)

    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


# ---------------------------------------------------------------------------
# count_recent_experiments
# ---------------------------------------------------------------------------


class TestCountRecentExperiments:
    def test_returns_count_from_query(self):
        conn = _make_conn(42)
        result = count_recent_experiments(conn)
        assert result == 42

    def test_uses_default_window(self):
        conn = _make_conn(0)
        count_recent_experiments(conn)
        query_text = conn.cursor.return_value.execute.call_args[0][0]
        assert str(EXPLOSION_WINDOW_DAYS_DEFAULT) in query_text

    def test_custom_window(self):
        conn = _make_conn(0)
        count_recent_experiments(conn, days=7)
        query_text = conn.cursor.return_value.execute.call_args[0][0]
        assert "7" in query_text

    def test_empty_result_returns_zero(self):
        conn = _make_conn(0)
        conn.cursor.return_value.fetchone.return_value = None
        result = count_recent_experiments(conn)
        assert result == 0

    def test_large_count(self):
        conn = _make_conn(9999)
        result = count_recent_experiments(conn)
        assert result == 9999


# ---------------------------------------------------------------------------
# check_explosion_guard
# ---------------------------------------------------------------------------


class TestCheckExplosionGuard:
    def test_passes_when_below_threshold(self):
        conn = _make_conn(500)
        # Should not raise
        check_explosion_guard(conn=conn, threshold=1000)

    def test_passes_when_at_threshold(self):
        conn = _make_conn(1000)
        # count == threshold is allowed (> not >=)
        check_explosion_guard(conn=conn, threshold=1000)

    def test_raises_when_above_threshold(self):
        conn = _make_conn(1001)
        with pytest.raises(ExplosionGuardError) as exc_info:
            check_explosion_guard(conn=conn, threshold=1000)
        assert "1001" in str(exc_info.value)
        assert "1000" in str(exc_info.value)

    def test_raises_with_custom_threshold(self):
        conn = _make_conn(51)
        with pytest.raises(ExplosionGuardError):
            check_explosion_guard(conn=conn, threshold=50)

    def test_ack_bypasses_guard(self):
        conn = _make_conn(9999)
        # Should not raise even with absurdly high count when ack provided
        check_explosion_guard(conn=conn, threshold=1000, managing_partner_ack=True)
        # Connection should not be used when ack=True (guard returns early)
        conn.cursor.assert_not_called()

    def test_error_message_includes_window_days(self):
        conn = _make_conn(2000)
        with pytest.raises(ExplosionGuardError) as exc_info:
            check_explosion_guard(conn=conn, threshold=1000, days=30)
        assert "30" in str(exc_info.value)

    def test_error_message_mentions_managing_partner(self):
        conn = _make_conn(1500)
        with pytest.raises(ExplosionGuardError) as exc_info:
            check_explosion_guard(conn=conn, threshold=1000)
        assert "managing_partner_ack" in str(exc_info.value)

    def test_default_threshold_is_1000(self):
        assert EXPLOSION_THRESHOLD_DEFAULT == 1000

    def test_default_window_is_30_days(self):
        assert EXPLOSION_WINDOW_DAYS_DEFAULT == 30
