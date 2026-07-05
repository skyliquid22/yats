"""
Python-side tests for reconcile logic — verifying dedup guards and that
experiment_index (not the nonexistent 'experiments' table) is queried.

These tests exercise the Python components (registry dedup, canonicalize dedup)
that feed into the reconciliation story. The TypeScript reconcile tool itself
is tested at the integration level.
"""
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Canonicalize dedup: reconciliation_log is the sentinel table
# ---------------------------------------------------------------------------

def test_canonicalize_dedup_checks_reconciliation_log_domain():
    """_run_already_written queries reconciliation_log with domain param."""
    from yats_pipelines.jobs.canonicalize import _run_already_written

    cur = MagicMock()
    cur.fetchone.return_value = (1,)
    conn = MagicMock()
    conn.cursor.return_value = cur

    result = _run_already_written(conn, "run-abc", "equity_ohlcv")

    assert result is True
    query_text = cur.execute.call_args[0][0]
    assert "reconciliation_log" in query_text
    assert "dagster_run_id" in query_text
    assert "domain" in query_text


# ---------------------------------------------------------------------------
# Registry: experiment_index (not 'experiments') is the target table
# ---------------------------------------------------------------------------

def test_index_run_exists_queries_experiment_index():
    """_index_run_exists must query experiment_index, not a nonexistent 'experiments' table."""
    from research.experiments.registry import _index_run_exists, QuestDBWriter

    writer = QuestDBWriter()

    with patch("psycopg2.connect") as mock_connect:
        conn = MagicMock()
        mock_connect.return_value = conn
        cur = MagicMock()
        cur.fetchone.return_value = (0,)
        conn.cursor.return_value = cur

        _index_run_exists(writer, "run-check")

    query_text = cur.execute.call_args[0][0]
    assert "experiment_index" in query_text
    assert "experiments" not in query_text.replace("experiment_index", "")


# ---------------------------------------------------------------------------
# Seeded inconsistency: reconcile detects a missing row
# ---------------------------------------------------------------------------

def test_index_run_exists_detects_missing():
    """When count=0, _index_run_exists returns False (used as inconsistency signal)."""
    from research.experiments.registry import _index_run_exists, QuestDBWriter

    writer = QuestDBWriter()

    with patch("psycopg2.connect") as mock_connect:
        conn = MagicMock()
        mock_connect.return_value = conn
        cur = MagicMock()
        cur.fetchone.return_value = (0,)
        conn.cursor.return_value = cur

        missing = not _index_run_exists(writer, "run-missing")

    assert missing is True


def test_index_run_exists_detects_present():
    """When count>0, _index_run_exists returns True (consistent state)."""
    from research.experiments.registry import _index_run_exists, QuestDBWriter

    writer = QuestDBWriter()

    with patch("psycopg2.connect") as mock_connect:
        conn = MagicMock()
        mock_connect.return_value = conn
        cur = MagicMock()
        cur.fetchone.return_value = (1,)
        conn.cursor.return_value = cur

        present = _index_run_exists(writer, "run-present")

    assert present is True
