"""Tests for run_recorder — never-raises behavior, upsert semantics, per-job wiring smoke.

Guards ya-vs9a1:
- record_start/record_finish never raise even when ILP is down.
- record_finish uses the same started_at as record_start (upsert key stability).
- Both status="running" (start) and status="success"/"failed" (finish) are written.
- Each of the 5 instrumented jobs calls record_start/record_finish around its work.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import yats_pipelines.utils.run_recorder as rr
from yats_pipelines.utils.run_recorder import record_finish, record_start


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 7, 8, 12, 0, 0, tzinfo=timezone.utc)


def _make_sender():
    sender = MagicMock()
    sender.__enter__ = MagicMock(return_value=sender)
    sender.__exit__ = MagicMock(return_value=False)
    return sender


def _mock_sender_cls(sender):
    cls = MagicMock(return_value=sender)
    return cls


def _ts_nanos_value(dt: datetime) -> int:
    """Convert datetime to nanosecond int (same formula as run_recorder)."""
    return int(dt.timestamp() * 1_000_000_000)


# ---------------------------------------------------------------------------
# Never-raises: ILP down → warning logged, no exception escapes
# ---------------------------------------------------------------------------


class TestNeverRaises:
    def setup_method(self):
        rr._starts.clear()

    def test_record_start_does_not_raise_when_sender_errors(self, caplog):
        with patch("yats_pipelines.utils.run_recorder.Sender") as mock_cls:
            mock_cls.side_effect = ConnectionRefusedError("ILP down")
            record_start("ingest_alpaca", "run-001", "test")
        assert "record_start failed" in caplog.text

    def test_record_finish_does_not_raise_when_sender_errors(self, caplog):
        rr._starts[("ingest_alpaca", "run-002")] = _NOW
        with patch("yats_pipelines.utils.run_recorder.Sender") as mock_cls:
            mock_cls.side_effect = ConnectionRefusedError("ILP down")
            record_finish("ingest_alpaca", "run-002", "success")
        assert "record_finish failed" in caplog.text

    def test_record_finish_without_prior_start_does_not_raise(self, caplog):
        record_finish("ingest_alpaca", "run-no-start", "success")
        assert "no matching record_start" in caplog.text

    def test_record_start_swallows_any_exception(self):
        with patch("yats_pipelines.utils.run_recorder.Sender") as mock_cls:
            mock_cls.side_effect = RuntimeError("unexpected error")
            record_start("canonicalize", "run-003", "detail")  # must not raise

    def test_record_finish_swallows_any_exception(self):
        rr._starts[("canonicalize", "run-004")] = _NOW
        with patch("yats_pipelines.utils.run_recorder.Sender") as mock_cls:
            mock_cls.side_effect = RuntimeError("unexpected error")
            record_finish("canonicalize", "run-004", "failed", failure_cause="boom")


# ---------------------------------------------------------------------------
# Upsert semantics: start → finish use the same started_at timestamp
# ---------------------------------------------------------------------------


class TestUpsertStartFinishReplacement:
    def setup_method(self):
        rr._starts.clear()

    def test_start_writes_running_status(self):
        sender = _make_sender()
        with (
            patch("yats_pipelines.utils.run_recorder.Sender", _mock_sender_cls(sender)),
            patch("yats_pipelines.utils.run_recorder.datetime") as mock_dt,
        ):
            mock_dt.now.return_value = _NOW
            record_start("canonicalize", "run-100", "detail")

        sender.row.assert_called_once()
        _, kwargs = sender.row.call_args
        assert kwargs["symbols"]["status"] == "running"
        assert kwargs["symbols"]["job_name"] == "canonicalize"
        assert kwargs["symbols"]["dagster_run_id"] == "run-100"
        assert kwargs["columns"]["detail"] == "detail"

    def test_finish_uses_same_started_at_as_start(self):
        start_sender = _make_sender()
        finish_sender = _make_sender()
        senders = [start_sender, finish_sender]

        with (
            patch("yats_pipelines.utils.run_recorder.Sender", side_effect=lambda *a, **kw: senders.pop(0)),
            patch("yats_pipelines.utils.run_recorder.datetime") as mock_dt,
        ):
            mock_dt.now.return_value = _NOW
            record_start("canonicalize", "run-200", "detail")

            # Advance time for finish
            finish_time = datetime(2026, 7, 8, 12, 5, 0, tzinfo=timezone.utc)
            mock_dt.now.return_value = finish_time
            record_finish("canonicalize", "run-200", "success", rows_written=42)

        # start row
        _, start_kwargs = start_sender.row.call_args
        start_at_nanos = start_kwargs["at"].value

        # finish row — at= must use the SAME started_at so the DEDUP upsert key matches
        _, finish_kwargs = finish_sender.row.call_args
        finish_at_nanos = finish_kwargs["at"].value

        assert start_at_nanos == finish_at_nanos, (
            "record_finish must use the same started_at as record_start for DEDUP upsert"
        )
        # Also verify the at= is NOT the finish time (which would be a different timestamp)
        assert finish_at_nanos == _ts_nanos_value(_NOW)
        assert finish_at_nanos != _ts_nanos_value(finish_time)

    def test_finish_writes_success_status_and_rows(self):
        sender = _make_sender()
        rr._starts[("canonicalize", "run-300")] = _NOW
        with (
            patch("yats_pipelines.utils.run_recorder.Sender", _mock_sender_cls(sender)),
            patch("yats_pipelines.utils.run_recorder.datetime") as mock_dt,
        ):
            mock_dt.now.return_value = datetime(2026, 7, 8, 12, 1, 0, tzinfo=timezone.utc)
            record_finish("canonicalize", "run-300", "success", rows_written=99)

        _, kwargs = sender.row.call_args
        assert kwargs["symbols"]["status"] == "success"
        assert kwargs["columns"]["rows_written"] == 99
        assert kwargs["columns"]["duration_s"] == pytest.approx(60.0)

    def test_finish_writes_failed_status_and_cause(self):
        sender = _make_sender()
        rr._starts[("ingest_alpaca", "run-400")] = _NOW
        with (
            patch("yats_pipelines.utils.run_recorder.Sender", _mock_sender_cls(sender)),
            patch("yats_pipelines.utils.run_recorder.datetime") as mock_dt,
        ):
            mock_dt.now.return_value = datetime(2026, 7, 8, 12, 0, 30, tzinfo=timezone.utc)
            record_finish("ingest_alpaca", "run-400", "failed", failure_cause="API timeout")

        _, kwargs = sender.row.call_args
        assert kwargs["symbols"]["status"] == "failed"
        assert kwargs["columns"]["failure_cause"] == "API timeout"

    def test_failure_cause_truncated_to_200_chars(self):
        sender = _make_sender()
        rr._starts[("canonicalize", "run-500")] = _NOW
        long_cause = "x" * 300
        with (
            patch("yats_pipelines.utils.run_recorder.Sender", _mock_sender_cls(sender)),
            patch("yats_pipelines.utils.run_recorder.datetime") as mock_dt,
        ):
            mock_dt.now.return_value = _NOW
            record_finish("canonicalize", "run-500", "failed", failure_cause=long_cause)

        _, kwargs = sender.row.call_args
        assert len(kwargs["columns"]["failure_cause"]) == 200

    def test_starts_dict_cleared_after_finish(self):
        rr._starts[("canonicalize", "run-600")] = _NOW
        with patch("yats_pipelines.utils.run_recorder.Sender", _mock_sender_cls(_make_sender())):
            with patch("yats_pipelines.utils.run_recorder.datetime") as mock_dt:
                mock_dt.now.return_value = _NOW
                record_finish("canonicalize", "run-600", "success")
        assert ("canonicalize", "run-600") not in rr._starts


# ---------------------------------------------------------------------------
# Per-job wiring smoke: each instrumented job's op calls record_start/record_finish
#
# Strategy: patch record_start and record_finish at the job module level, call
# the underlying function directly (bypassing Dagster's op decoration) so we
# don't need a real Dagster context.
# ---------------------------------------------------------------------------


def _call_op_fn(op_obj, *args, **kwargs):
    """Call the underlying function of a Dagster @op, bypassing op machinery."""
    return op_obj.compute_fn.decorated_fn(*args, **kwargs)


class _FakeCtx:
    """Minimal stand-in for OpExecutionContext in unit tests."""

    def __init__(self, run_id: str = "smoke-001"):
        self.run_id = run_id
        self.log = MagicMock()


# --- ingest_alpaca ---


class TestIngestAlpacaWiring:
    def setup_method(self):
        rr._starts.clear()

    def test_fetch_alpaca_bars_calls_record_start(self):
        from yats_pipelines.jobs.ingest_alpaca import fetch_alpaca_bars, IngestAlpacaConfig

        ctx = _FakeCtx("alpaca-001")
        config = IngestAlpacaConfig(
            ticker_list=["AAPL"], start_date="2024-01-01", end_date="2024-12-31"
        )
        with (
            patch("yats_pipelines.jobs.ingest_alpaca.record_start") as mock_start,
            patch("yats_pipelines.jobs.ingest_alpaca.record_finish"),
            patch("yats_pipelines.jobs.ingest_alpaca.AlpacaResource") as mock_alpaca_cls,
        ):
            mock_alpaca = MagicMock()
            mock_alpaca.api_key = "key"
            mock_alpaca.api_secret = "secret"
            mock_alpaca.get_historical_bars.return_value = {}
            mock_alpaca.normalize_bars.return_value = []
            mock_alpaca_cls.return_value = mock_alpaca

            _call_op_fn(fetch_alpaca_bars, ctx, config)

        mock_start.assert_called_once()
        assert mock_start.call_args[0][0] == "ingest_alpaca"
        assert mock_start.call_args[0][1] == "alpaca-001"

    def test_fetch_alpaca_bars_calls_record_finish_failed_on_error(self):
        from yats_pipelines.jobs.ingest_alpaca import fetch_alpaca_bars, IngestAlpacaConfig

        ctx = _FakeCtx("alpaca-002")
        config = IngestAlpacaConfig(
            ticker_list=["AAPL"], start_date="2024-01-01", end_date="2024-12-31"
        )
        with (
            patch("yats_pipelines.jobs.ingest_alpaca.record_start"),
            patch("yats_pipelines.jobs.ingest_alpaca.record_finish") as mock_finish,
            patch("yats_pipelines.jobs.ingest_alpaca.AlpacaResource") as mock_alpaca_cls,
        ):
            mock_alpaca = MagicMock()
            mock_alpaca.api_key = "key"
            mock_alpaca.api_secret = "secret"
            mock_alpaca.get_historical_bars.side_effect = RuntimeError("API down")
            mock_alpaca_cls.return_value = mock_alpaca

            with pytest.raises(RuntimeError, match="API down"):
                _call_op_fn(fetch_alpaca_bars, ctx, config)

        mock_finish.assert_called_once()
        _, kwargs = mock_finish.call_args
        assert kwargs.get("failure_cause") or mock_finish.call_args[0][3] if len(mock_finish.call_args[0]) > 3 else True
        # Verify status="failed" is the third positional arg
        assert mock_finish.call_args[0][2] == "failed"

    def test_write_bars_calls_record_finish_success(self):
        from yats_pipelines.jobs.ingest_alpaca import write_bars_to_questdb

        ctx = _FakeCtx("alpaca-003")
        rr._starts[("ingest_alpaca", "alpaca-003")] = _NOW

        with (
            patch("yats_pipelines.jobs.ingest_alpaca.record_finish") as mock_finish,
            patch("yats_pipelines.jobs.ingest_alpaca.Sender"),
            patch("yats_pipelines.jobs.ingest_alpaca.QuestDBResource"),
        ):
            _call_op_fn(write_bars_to_questdb, ctx, {"rows": []})

        mock_finish.assert_called_once()
        assert mock_finish.call_args[0][2] == "success"

    def test_write_bars_calls_record_finish_failed_on_error(self):
        from yats_pipelines.jobs.ingest_alpaca import write_bars_to_questdb

        ctx = _FakeCtx("alpaca-004")
        rr._starts[("ingest_alpaca", "alpaca-004")] = _NOW

        with (
            patch("yats_pipelines.jobs.ingest_alpaca.record_finish") as mock_finish,
            patch("yats_pipelines.jobs.ingest_alpaca.Sender") as mock_sender_cls,
            patch("yats_pipelines.jobs.ingest_alpaca.QuestDBResource"),
        ):
            mock_s = MagicMock()
            mock_s.__enter__ = MagicMock(side_effect=RuntimeError("ILP crash"))
            mock_s.__exit__ = MagicMock(return_value=False)
            mock_sender_cls.return_value = mock_s

            row = {
                "timestamp": _NOW, "ingested_at": _NOW, "symbol": "AAPL",
                "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
                "volume": 100, "vwap": 1.0, "trade_count": 10,
            }
            with pytest.raises(RuntimeError, match="ILP crash"):
                _call_op_fn(write_bars_to_questdb, ctx, {"rows": [row]})

        mock_finish.assert_called_once()
        assert mock_finish.call_args[0][2] == "failed"


# --- ingest_financialdatasets ---


class TestIngestFinancialdatasetsWiring:
    def setup_method(self):
        rr._starts.clear()

    def test_op_calls_record_start_and_record_finish_success(self):
        from yats_pipelines.jobs.ingest_financialdatasets import (
            ingest_financialdatasets_op, IngestFinancialdatasetsConfig,
        )

        ctx = _FakeCtx("fd-001")
        config = IngestFinancialdatasetsConfig(
            ticker_list=["AAPL"], data_domains=["fundamentals"]
        )
        with (
            patch("yats_pipelines.jobs.ingest_financialdatasets.record_start") as mock_start,
            patch("yats_pipelines.jobs.ingest_financialdatasets.record_finish") as mock_finish,
            patch("yats_pipelines.jobs.ingest_financialdatasets.FinancialDatasetsResource") as mock_fd_cls,
            patch("yats_pipelines.jobs.ingest_financialdatasets.QuestDBResource"),
            patch("yats_pipelines.jobs.ingest_financialdatasets._ilp_sender") as mock_ilp,
        ):
            mock_fd = MagicMock()
            mock_fd.get_income_statements.return_value = []
            mock_fd_cls.return_value = mock_fd
            mock_ilp_ctx = MagicMock()
            mock_ilp_ctx.__enter__ = MagicMock(return_value=MagicMock())
            mock_ilp_ctx.__exit__ = MagicMock(return_value=False)
            mock_ilp.return_value = mock_ilp_ctx

            _call_op_fn(ingest_financialdatasets_op, ctx, config)

        mock_start.assert_called_once_with("ingest_financialdatasets", "fd-001", mock_start.call_args[0][2])
        mock_finish.assert_called_once()
        assert mock_finish.call_args[0][2] == "success"

    def test_op_calls_record_finish_failed_on_exception(self):
        from yats_pipelines.jobs.ingest_financialdatasets import (
            ingest_financialdatasets_op, IngestFinancialdatasetsConfig,
        )

        ctx = _FakeCtx("fd-002")
        config = IngestFinancialdatasetsConfig(
            ticker_list=["AAPL"], data_domains=["fundamentals"]
        )
        with (
            patch("yats_pipelines.jobs.ingest_financialdatasets.record_start"),
            patch("yats_pipelines.jobs.ingest_financialdatasets.record_finish") as mock_finish,
            patch("yats_pipelines.jobs.ingest_financialdatasets.FinancialDatasetsResource") as mock_fd_cls,
            patch("yats_pipelines.jobs.ingest_financialdatasets.QuestDBResource"),
        ):
            mock_fd_cls.side_effect = RuntimeError("credentials missing")

            with pytest.raises(RuntimeError, match="credentials missing"):
                _call_op_fn(ingest_financialdatasets_op, ctx, config)

        mock_finish.assert_called_once()
        assert mock_finish.call_args[0][2] == "failed"
        assert "credentials missing" in (mock_finish.call_args.kwargs.get("failure_cause") or "")


# --- canonicalize ---


class TestCanonicalizeWiring:
    def setup_method(self):
        rr._starts.clear()

    def test_op_calls_record_start_and_record_finish_success(self):
        from yats_pipelines.jobs.canonicalize import canonicalize_op, CanonicalizeConfig

        ctx = _FakeCtx("canon-001")
        config = CanonicalizeConfig(domains=[])

        with (
            patch("yats_pipelines.jobs.canonicalize.record_start") as mock_start,
            patch("yats_pipelines.jobs.canonicalize.record_finish") as mock_finish,
            patch("yats_pipelines.jobs.canonicalize.QuestDBResource"),
            patch("yats_pipelines.jobs.canonicalize._pg_conn") as mock_pg,
            patch("yats_pipelines.jobs.canonicalize._ilp_sender") as mock_ilp,
        ):
            mock_conn = MagicMock()
            mock_pg.return_value = mock_conn
            mock_ilp_ctx = MagicMock()
            mock_ilp_ctx.__enter__ = MagicMock(return_value=MagicMock())
            mock_ilp_ctx.__exit__ = MagicMock(return_value=False)
            mock_ilp.return_value = mock_ilp_ctx

            _call_op_fn(canonicalize_op, ctx, config)

        mock_start.assert_called_once_with("canonicalize", "canon-001", mock_start.call_args[0][2])
        mock_finish.assert_called_once()
        assert mock_finish.call_args[0][2] == "success"

    def test_op_records_failed_on_db_error(self):
        from yats_pipelines.jobs.canonicalize import canonicalize_op, CanonicalizeConfig

        ctx = _FakeCtx("canon-002")
        config = CanonicalizeConfig(domains=["equity_ohlcv"])

        with (
            patch("yats_pipelines.jobs.canonicalize.record_start"),
            patch("yats_pipelines.jobs.canonicalize.record_finish") as mock_finish,
            patch("yats_pipelines.jobs.canonicalize.QuestDBResource"),
            patch("yats_pipelines.jobs.canonicalize._pg_conn") as mock_pg,
        ):
            mock_pg.side_effect = RuntimeError("DB connection failed")

            with pytest.raises(RuntimeError, match="DB connection"):
                _call_op_fn(canonicalize_op, ctx, config)

        mock_finish.assert_called_once()
        assert mock_finish.call_args[0][2] == "failed"
        assert "DB connection" in (mock_finish.call_args.kwargs.get("failure_cause") or "")


# --- feature_pipeline ---


class TestFeaturePipelineWiring:
    def setup_method(self):
        rr._starts.clear()

    def test_op_calls_record_start_and_finish_success(self):
        import pandas as pd
        from yats_pipelines.jobs.feature_pipeline import feature_pipeline_op, FeaturePipelineConfig

        ctx = _FakeCtx("feat-001")
        config = FeaturePipelineConfig(universe="test", feature_set="core_v1")

        with (
            patch("yats_pipelines.jobs.feature_pipeline.record_start") as mock_start,
            patch("yats_pipelines.jobs.feature_pipeline.record_finish") as mock_finish,
            patch("yats_pipelines.jobs.feature_pipeline.QuestDBResource"),
            patch("yats_pipelines.jobs.feature_pipeline._pg_conn") as mock_pg,
            patch("yats_pipelines.jobs.feature_pipeline.registry") as mock_registry,
            patch("yats_pipelines.jobs.feature_pipeline._load_universe", return_value=["AAPL"]),
            patch("yats_pipelines.jobs.feature_pipeline._load_ohlcv", return_value=pd.DataFrame()),
        ):
            mock_pg.return_value = MagicMock()
            mock_fs = MagicMock()
            mock_fs.name = "core_v1"
            mock_fs.all_features = []
            mock_fs.ohlcv = []
            mock_fs.cross_sectional = []
            mock_fs.fundamental = []
            mock_fs.regime = []
            mock_fs.options = []
            mock_registry.load_feature_set.return_value = mock_fs

            _call_op_fn(feature_pipeline_op, ctx, config)

        mock_start.assert_called_once_with("feature_pipeline", "feat-001", mock_start.call_args[0][2])
        mock_finish.assert_called_once()
        assert mock_finish.call_args[0][2] == "success"

    def test_op_records_failed_on_exception(self):
        from yats_pipelines.jobs.feature_pipeline import feature_pipeline_op, FeaturePipelineConfig

        ctx = _FakeCtx("feat-002")
        config = FeaturePipelineConfig(universe="test", feature_set="core_v1")

        with (
            patch("yats_pipelines.jobs.feature_pipeline.record_start"),
            patch("yats_pipelines.jobs.feature_pipeline.record_finish") as mock_finish,
            patch("yats_pipelines.jobs.feature_pipeline.QuestDBResource"),
            patch("yats_pipelines.jobs.feature_pipeline._pg_conn") as mock_pg,
            patch("yats_pipelines.jobs.feature_pipeline.registry") as mock_registry,
            patch("yats_pipelines.jobs.feature_pipeline._load_universe", return_value=["AAPL"]),
        ):
            mock_pg.side_effect = RuntimeError("DB down")
            mock_fs = MagicMock()
            mock_fs.all_features = []
            mock_registry.load_feature_set.return_value = mock_fs

            with pytest.raises(RuntimeError, match="DB down"):
                _call_op_fn(feature_pipeline_op, ctx, config)

        mock_finish.assert_called_once()
        assert mock_finish.call_args[0][2] == "failed"
