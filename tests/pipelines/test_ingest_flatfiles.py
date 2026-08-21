"""Tests for the daily option flat-file capture job."""

from __future__ import annotations

import os
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_eod_df() -> pd.DataFrame:
    return pd.DataFrame({
        "root": ["AAPL", "AAPL"],
        "expiration": ["2026-08-15", "2026-08-15"],
        "strike": [200.0, 200.0],
        "right": ["CALL", "PUT"],
        "close": [5.5, 3.2],
        "volume": [1000, 800],
        "open_interest": [5000, 4200],
    })


def _make_oi_df() -> pd.DataFrame:
    return pd.DataFrame({
        "root": ["AAPL"],
        "expiration": ["2026-08-15"],
        "strike": [200.0],
        "right": ["CALL"],
        "open_interest": [5000],
    })


# ---------------------------------------------------------------------------
# _already_captured
# ---------------------------------------------------------------------------

class TestAlreadyCaptured:
    def test_false_when_files_missing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        from yats_pipelines.jobs.ingest_flatfiles import _already_captured
        assert _already_captured("20260802") is False

    def test_true_when_both_files_exist(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        d = tmp_path / ".yats_data" / "flatfiles" / "20260802"
        d.mkdir(parents=True)
        (d / "option_eod.parquet").write_bytes(b"")
        (d / "option_oi.parquet").write_bytes(b"")
        from yats_pipelines.jobs.ingest_flatfiles import _already_captured
        assert _already_captured("20260802") is True

    def test_false_when_only_eod_exists(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        d = tmp_path / ".yats_data" / "flatfiles" / "20260802"
        d.mkdir(parents=True)
        (d / "option_eod.parquet").write_bytes(b"")
        from yats_pipelines.jobs.ingest_flatfiles import _already_captured
        assert _already_captured("20260802") is False


# ---------------------------------------------------------------------------
# fetch_and_write_flatfiles op
# ---------------------------------------------------------------------------

class TestFetchAndWriteFlatfiles:

    def _run_op(self, config_kwargs: dict, mock_client: MagicMock, tmp_path: Path, monkeypatch) -> dict:
        """Run the op with a mocked ThetaClient, returning the result dict."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("THETADATA_API_KEY", "test-key")

        from dagster import build_op_context
        from yats_pipelines.jobs.ingest_flatfiles import (
            IngestFlatfilesConfig,
            fetch_and_write_flatfiles,
        )

        with patch("thetadata.ThetaClient", return_value=mock_client):
            ctx = build_op_context()
            result = fetch_and_write_flatfiles(ctx, IngestFlatfilesConfig(**config_kwargs))
        return result

    def test_writes_parquet_files(self, tmp_path, monkeypatch):
        mock_client = MagicMock()
        mock_client.option_flat_file_eod.return_value = _make_eod_df()
        mock_client.option_flat_file_open_interest.return_value = _make_oi_df()

        result = self._run_op({"date": "20260802"}, mock_client, tmp_path, monkeypatch)

        assert result["date"] == "20260802"
        assert result["eod_rows"] == 2
        assert result["oi_rows"] == 1
        assert result["skipped"] is False

        eod_path = tmp_path / ".yats_data" / "flatfiles" / "20260802" / "option_eod.parquet"
        oi_path = tmp_path / ".yats_data" / "flatfiles" / "20260802" / "option_oi.parquet"
        assert eod_path.exists()
        assert oi_path.exists()

        read_back = pd.read_parquet(eod_path)
        assert len(read_back) == 2
        assert list(read_back["root"]) == ["AAPL", "AAPL"]

    def test_skips_before_first_access_date(self, tmp_path, monkeypatch):
        mock_client = MagicMock()
        result = self._run_op({"date": "20260701"}, mock_client, tmp_path, monkeypatch)

        assert result["skipped"] is True
        assert result["eod_rows"] == 0
        mock_client.option_flat_file_eod.assert_not_called()

    def test_force_bypasses_first_access_gate(self, tmp_path, monkeypatch):
        mock_client = MagicMock()
        mock_client.option_flat_file_eod.return_value = _make_eod_df()
        mock_client.option_flat_file_open_interest.return_value = _make_oi_df()

        result = self._run_op({"date": "20260701", "force": True}, mock_client, tmp_path, monkeypatch)

        mock_client.option_flat_file_eod.assert_called_once()
        assert result["eod_rows"] == 2

    def test_skips_when_already_captured(self, tmp_path, monkeypatch):
        d = tmp_path / ".yats_data" / "flatfiles" / "20260802"
        d.mkdir(parents=True)
        (d / "option_eod.parquet").write_bytes(b"x")
        (d / "option_oi.parquet").write_bytes(b"x")

        mock_client = MagicMock()
        result = self._run_op({"date": "20260802"}, mock_client, tmp_path, monkeypatch)

        assert result["skipped"] is True
        mock_client.option_flat_file_eod.assert_not_called()

    def test_force_overwrites_existing(self, tmp_path, monkeypatch):
        d = tmp_path / ".yats_data" / "flatfiles" / "20260802"
        d.mkdir(parents=True)
        (d / "option_eod.parquet").write_bytes(b"old")
        (d / "option_oi.parquet").write_bytes(b"old")

        mock_client = MagicMock()
        mock_client.option_flat_file_eod.return_value = _make_eod_df()
        mock_client.option_flat_file_open_interest.return_value = _make_oi_df()

        result = self._run_op({"date": "20260802", "force": True}, mock_client, tmp_path, monkeypatch)

        assert result["eod_rows"] == 2
        mock_client.option_flat_file_eod.assert_called_once()

    def test_permission_denied_eod_is_warned_not_raised(self, tmp_path, monkeypatch):
        import grpc

        mock_client = MagicMock()
        err = grpc.RpcError()
        err.code = lambda: grpc.StatusCode.PERMISSION_DENIED
        mock_client.option_flat_file_eod.side_effect = err
        mock_client.option_flat_file_open_interest.return_value = _make_oi_df()

        result = self._run_op({"date": "20260802"}, mock_client, tmp_path, monkeypatch)

        assert result["eod_rows"] == 0
        assert result["oi_rows"] == 1
        assert result["skipped"] is False

    def test_permission_denied_oi_is_warned_not_raised(self, tmp_path, monkeypatch):
        import grpc

        mock_client = MagicMock()
        mock_client.option_flat_file_eod.return_value = _make_eod_df()
        err = grpc.RpcError()
        err.code = lambda: grpc.StatusCode.PERMISSION_DENIED
        mock_client.option_flat_file_open_interest.side_effect = err

        result = self._run_op({"date": "20260802"}, mock_client, tmp_path, monkeypatch)

        assert result["eod_rows"] == 2
        assert result["oi_rows"] == 0

    def test_no_data_found_treated_as_empty(self, tmp_path, monkeypatch):
        from thetadata.errors import NoDataFoundError

        mock_client = MagicMock()
        mock_client.option_flat_file_eod.side_effect = NoDataFoundError("No data")
        mock_client.option_flat_file_open_interest.side_effect = NoDataFoundError("No data")

        result = self._run_op({"date": "20260802"}, mock_client, tmp_path, monkeypatch)

        assert result["eod_rows"] == 0
        assert result["oi_rows"] == 0
        assert result["skipped"] is False

        out_dir = tmp_path / ".yats_data" / "flatfiles" / "20260802"
        assert not out_dir.exists()

    def test_empty_dataframe_not_written(self, tmp_path, monkeypatch):
        mock_client = MagicMock()
        mock_client.option_flat_file_eod.return_value = pd.DataFrame()
        mock_client.option_flat_file_open_interest.return_value = pd.DataFrame()

        result = self._run_op({"date": "20260802"}, mock_client, tmp_path, monkeypatch)

        assert result["eod_rows"] == 0
        assert result["oi_rows"] == 0
        eod_path = tmp_path / ".yats_data" / "flatfiles" / "20260802" / "option_eod.parquet"
        assert not eod_path.exists()

    def test_date_defaults_to_today_utc(self, tmp_path, monkeypatch):
        mock_client = MagicMock()
        mock_client.option_flat_file_eod.return_value = _make_eod_df()
        mock_client.option_flat_file_open_interest.return_value = _make_oi_df()

        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("THETADATA_API_KEY", "test-key")

        from dagster import build_op_context
        from yats_pipelines.jobs.ingest_flatfiles import (
            IngestFlatfilesConfig,
            fetch_and_write_flatfiles,
        )

        today_utc = datetime.now(timezone.utc).strftime("%Y%m%d")

        with patch("thetadata.ThetaClient", return_value=mock_client):
            ctx = build_op_context()
            result = fetch_and_write_flatfiles(ctx, IngestFlatfilesConfig(date=""))

        assert result["date"] == today_utc

    def test_client_called_with_date_object(self, tmp_path, monkeypatch):
        mock_client = MagicMock()
        mock_client.option_flat_file_eod.return_value = _make_eod_df()
        mock_client.option_flat_file_open_interest.return_value = _make_oi_df()

        self._run_op({"date": "20260802"}, mock_client, tmp_path, monkeypatch)

        call_arg = mock_client.option_flat_file_eod.call_args[0][0]
        assert call_arg == date(2026, 8, 2)

    def test_other_grpc_errors_are_reraised(self, tmp_path, monkeypatch):
        import grpc

        mock_client = MagicMock()
        err = grpc.RpcError()
        err.code = lambda: grpc.StatusCode.UNAVAILABLE
        mock_client.option_flat_file_eod.side_effect = err
        mock_client.option_flat_file_open_interest.return_value = _make_oi_df()

        with pytest.raises(Exception):
            self._run_op({"date": "20260802"}, mock_client, tmp_path, monkeypatch)
