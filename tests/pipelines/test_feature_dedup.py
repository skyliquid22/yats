"""Tests for feature pipeline dedup (check-before-write on dagster_run_id)."""
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from yats_pipelines.jobs.feature_pipeline_incremental import (
    _features_run_already_written,
    _write_features,
)


def _make_conn(count: int):
    cur = MagicMock()
    cur.fetchone.return_value = (count,)
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


def test_features_run_already_written_true():
    conn = _make_conn(5)
    assert _features_run_already_written(conn, "run-abc") is True


def test_features_run_already_written_false():
    conn = _make_conn(0)
    assert _features_run_already_written(conn, "run-new") is False


def test_features_run_already_written_error_returns_false():
    conn = MagicMock()
    conn.cursor.side_effect = Exception("db offline")
    assert _features_run_already_written(conn, "run-err") is False


def test_write_features_includes_dagster_run_id():
    """_write_features passes dagster_run_id as a column when provided."""
    from datetime import datetime, timezone

    sender = MagicMock()
    now = datetime(2024, 1, 5, tzinfo=timezone.utc)
    ts = pd.Timestamp("2024-01-04", tz="UTC")
    timestamps = pd.Series([ts])
    features = {"ret_1d": pd.Series([0.01])}

    _write_features(
        sender, "AAPL", timestamps, features,
        "core_v1", "1.0", now, run_id="run-xyz",
    )

    assert sender.row.call_count == 1
    call_kwargs = sender.row.call_args
    columns = call_kwargs[1]["columns"] if call_kwargs[1] else call_kwargs[0][2]
    # dagster_run_id should appear in columns
    assert "dagster_run_id" in columns
    assert columns["dagster_run_id"] == "run-xyz"


def test_write_features_omits_dagster_run_id_when_empty():
    """When run_id is empty string, dagster_run_id column is not written."""
    from datetime import datetime, timezone

    sender = MagicMock()
    now = datetime(2024, 1, 5, tzinfo=timezone.utc)
    ts = pd.Timestamp("2024-01-04", tz="UTC")
    timestamps = pd.Series([ts])
    features = {"ret_1d": pd.Series([0.01])}

    _write_features(
        sender, "AAPL", timestamps, features,
        "core_v1", "1.0", now, run_id="",
    )

    assert sender.row.call_count == 1
    call_kwargs = sender.row.call_args
    columns = call_kwargs[1]["columns"] if call_kwargs[1] else call_kwargs[0][2]
    assert "dagster_run_id" not in columns


def test_write_features_skips_nan_rows():
    """Rows where all feature values are NaN are not written."""
    from datetime import datetime, timezone

    sender = MagicMock()
    now = datetime(2024, 1, 5, tzinfo=timezone.utc)
    timestamps = pd.Series([
        pd.Timestamp("2024-01-03", tz="UTC"),
        pd.Timestamp("2024-01-04", tz="UTC"),
    ])
    features = {"ret_1d": pd.Series([float("nan"), 0.02])}

    written = _write_features(
        sender, "AAPL", timestamps, features,
        "core_v1", "1.0", now, run_id="run-1",
    )

    assert written == 1  # NaN row skipped
    assert sender.row.call_count == 1
