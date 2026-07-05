"""Tests for experiment_index dedup in write_index_row (dagster_run_id check)."""
from unittest.mock import MagicMock, patch

import pytest

from research.experiments.registry import (
    QuestDBWriter,
    _index_run_exists,
    write_index_row,
)


def _mock_writer():
    return QuestDBWriter(
        ilp_host="localhost", ilp_port=9009,
        pg_host="localhost", pg_port=8812,
        pg_user="admin", pg_password="quest", pg_database="qdb",
    )


# ---------------------------------------------------------------------------
# _index_run_exists
# ---------------------------------------------------------------------------


def test_index_run_exists_true():
    with patch("research.experiments.registry.psycopg2") as mock_pg:
        conn = MagicMock()
        mock_pg.connect.return_value = conn
        cur = MagicMock()
        cur.fetchone.return_value = (2,)
        conn.cursor.return_value = cur

        result = _index_run_exists(_mock_writer(), "run-exists")

    assert result is True


def test_index_run_exists_false():
    with patch("research.experiments.registry.psycopg2") as mock_pg:
        conn = MagicMock()
        mock_pg.connect.return_value = conn
        cur = MagicMock()
        cur.fetchone.return_value = (0,)
        conn.cursor.return_value = cur

        result = _index_run_exists(_mock_writer(), "run-new")

    assert result is False


def test_index_run_exists_db_error_returns_false():
    with patch("research.experiments.registry.psycopg2") as mock_pg:
        mock_pg.connect.side_effect = Exception("connection refused")
        result = _index_run_exists(_mock_writer(), "run-err")

    assert result is False


# ---------------------------------------------------------------------------
# write_index_row dedup: skips when run already written
# ---------------------------------------------------------------------------


def _make_spec():
    """Minimal ExperimentSpec for tests."""
    from datetime import date
    from research.experiments.spec import CostConfig, ExperimentSpec

    return ExperimentSpec(
        experiment_name="test_exp",
        symbols=("AAPL", "MSFT"),
        start_date=date(2023, 1, 1),
        end_date=date(2023, 12, 31),
        interval="daily",
        feature_set="core_v1",
        policy="equal_weight",
        policy_params={},
        cost_config=CostConfig(transaction_cost_bp=1.0),
        seed=42,
    )


def test_write_index_row_skips_when_run_already_exists():
    """write_index_row must not write if dagster_run_id already in experiment_index."""
    spec = _make_spec()

    with patch("research.experiments.registry._index_run_exists", return_value=True) as mock_check, \
         patch("research.experiments.registry.QuestDBWriter.from_resource"), \
         patch("questdb.ingress.Sender"):

        write_index_row(spec, dagster_run_id="run-dup")

    mock_check.assert_called_once()


def test_write_index_row_writes_when_run_is_new():
    """write_index_row must proceed normally when run_id is new."""
    spec = _make_spec()

    sender_mock = MagicMock()
    sender_mock.__enter__ = MagicMock(return_value=sender_mock)
    sender_mock.__exit__ = MagicMock(return_value=False)

    with patch("research.experiments.registry._index_run_exists", return_value=False), \
         patch("questdb.ingress.Sender", return_value=sender_mock), \
         patch("research.experiments.registry.QuestDBWriter.from_resource", return_value=_mock_writer()):

        write_index_row(spec, dagster_run_id="run-fresh")

    sender_mock.row.assert_called_once()


def test_write_index_row_no_run_id_always_writes():
    """When dagster_run_id is None, dedup check is skipped and write always proceeds."""
    spec = _make_spec()

    sender_mock = MagicMock()
    sender_mock.__enter__ = MagicMock(return_value=sender_mock)
    sender_mock.__exit__ = MagicMock(return_value=False)

    with patch("research.experiments.registry._index_run_exists") as mock_check, \
         patch("questdb.ingress.Sender", return_value=sender_mock), \
         patch("research.experiments.registry.QuestDBWriter.from_resource", return_value=_mock_writer()):

        write_index_row(spec, dagster_run_id=None)

    mock_check.assert_not_called()
    sender_mock.row.assert_called_once()
