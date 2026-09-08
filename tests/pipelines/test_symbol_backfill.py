"""Tests for the one-command symbol backfill (gRPC-3).

Covers: CLI arg parsing, stage plan / job chaining order (mocked execute),
per-domain config plumbing (equity / option_eod / fundamentals + feature sets),
--force behavior, and default-end-date behavior.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from yats_pipelines.backfill.__main__ import build_parser, main
from yats_pipelines.jobs.backfill import symbol_backfill as sb
from yats_pipelines.jobs.backfill.symbol_backfill import (
    BACKFILL_DOMAINS,
    DEFAULT_FEATURE_SETS,
    build_stage_plan,
    default_max_concurrent,
    resolve_end_date,
    run_symbol_backfill,
    validate_params,
)


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# CLI arg parsing
# ---------------------------------------------------------------------------


class TestCliParsing:
    def test_symbols_split_stripped_uppercased(self):
        with patch("yats_pipelines.backfill.__main__.run_symbol_backfill",
                   return_value=True) as run:
            code = main(["--symbols", " nflx, amd ,", "--start", "2020-01-01"])
        assert code == 0
        assert run.call_args[0][0] == ["NFLX", "AMD"]

    def test_start_and_end_passed_through(self):
        with patch("yats_pipelines.backfill.__main__.run_symbol_backfill",
                   return_value=True) as run:
            main(["--symbols", "AMD", "--start", "2020-01-01", "--end", "2021-06-30"])
        args = run.call_args
        assert args[0][1] == "2020-01-01"
        assert args[0][2] == "2021-06-30"

    def test_end_defaults_to_empty_string(self):
        """CLI leaves end empty — run_symbol_backfill resolves it to today."""
        with patch("yats_pipelines.backfill.__main__.run_symbol_backfill",
                   return_value=True) as run:
            main(["--symbols", "AMD", "--start", "2020-01-01"])
        assert run.call_args[0][2] == ""

    def test_force_default_false(self):
        with patch("yats_pipelines.backfill.__main__.run_symbol_backfill",
                   return_value=True) as run:
            main(["--symbols", "AMD", "--start", "2020-01-01"])
        assert run.call_args[1]["force"] is False

    def test_force_flag_passed(self):
        with patch("yats_pipelines.backfill.__main__.run_symbol_backfill",
                   return_value=True) as run:
            main(["--symbols", "AMD", "--start", "2020-01-01", "--force"])
        assert run.call_args[1]["force"] is True

    def test_default_feature_sets(self):
        with patch("yats_pipelines.backfill.__main__.run_symbol_backfill",
                   return_value=True) as run:
            main(["--symbols", "AMD", "--start", "2020-01-01"])
        assert run.call_args[1]["feature_sets"] == list(DEFAULT_FEATURE_SETS)

    def test_feature_sets_override(self):
        with patch("yats_pipelines.backfill.__main__.run_symbol_backfill",
                   return_value=True) as run:
            main(["--symbols", "AMD", "--start", "2020-01-01",
                  "--feature-sets", "core_v1, options_v1"])
        assert run.call_args[1]["feature_sets"] == ["core_v1", "options_v1"]

    def test_max_concurrent_default_8(self, monkeypatch):
        monkeypatch.delenv("THETADATA_MAX_CONCURRENT", raising=False)
        args = build_parser().parse_args(["--symbols", "AMD", "--start", "2020-01-01"])
        assert args.max_concurrent == 8

    def test_max_concurrent_env_override(self, monkeypatch):
        monkeypatch.setenv("THETADATA_MAX_CONCURRENT", "4")
        args = build_parser().parse_args(["--symbols", "AMD", "--start", "2020-01-01"])
        assert args.max_concurrent == 4

    def test_max_concurrent_flag_wins(self, monkeypatch):
        monkeypatch.setenv("THETADATA_MAX_CONCURRENT", "4")
        with patch("yats_pipelines.backfill.__main__.run_symbol_backfill",
                   return_value=True) as run:
            main(["--symbols", "AMD", "--start", "2020-01-01", "--max-concurrent", "2"])
        assert run.call_args[1]["max_concurrent"] == 2

    def test_empty_symbols_exits_2(self):
        with patch("yats_pipelines.backfill.__main__.run_symbol_backfill") as run:
            code = main(["--symbols", " , ", "--start", "2020-01-01"])
        assert code == 2
        run.assert_not_called()

    def test_missing_required_args_raise_system_exit(self):
        with pytest.raises(SystemExit):
            main(["--symbols", "AMD"])  # no --start

    def test_invalid_date_exits_2(self):
        # run_symbol_backfill raises ValueError on bad dates → exit code 2
        code = main(["--symbols", "AMD", "--start", "01/01/2020"])
        assert code == 2

    def test_failed_backfill_exits_1(self):
        with patch("yats_pipelines.backfill.__main__.run_symbol_backfill",
                   return_value=False):
            code = main(["--symbols", "AMD", "--start", "2020-01-01"])
        assert code == 1


# ---------------------------------------------------------------------------
# Parameter validation + default end date
# ---------------------------------------------------------------------------


class TestValidateParams:
    def test_valid_params_pass(self):
        validate_params(["NFLX"], "2020-01-01", "2021-01-01")

    def test_empty_symbols_raise(self):
        with pytest.raises(ValueError, match="No symbols"):
            validate_params([], "2020-01-01", "2021-01-01")

    def test_bad_start_format_raises(self):
        with pytest.raises(ValueError, match="Invalid date format"):
            validate_params(["NFLX"], "20200101", "2021-01-01")

    def test_bad_end_format_raises(self):
        with pytest.raises(ValueError, match="Invalid date format"):
            validate_params(["NFLX"], "2020-01-01", "tomorrow")

    def test_start_after_end_raises(self):
        with pytest.raises(ValueError, match="cannot be after"):
            validate_params(["NFLX"], "2022-01-01", "2021-01-01")


class TestResolveEndDate:
    def test_empty_resolves_to_today_utc(self):
        assert resolve_end_date("") == _today()

    def test_explicit_end_passthrough(self):
        assert resolve_end_date("2024-06-30") == "2024-06-30"


class TestDefaultMaxConcurrent:
    def test_default_8(self, monkeypatch):
        monkeypatch.delenv("THETADATA_MAX_CONCURRENT", raising=False)
        assert default_max_concurrent() == 8

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("THETADATA_MAX_CONCURRENT", "3")
        assert default_max_concurrent() == 3


# ---------------------------------------------------------------------------
# Stage plan — chaining order and per-domain config plumbing
# ---------------------------------------------------------------------------


class TestBuildStagePlan:
    def _plan(self, **kwargs):
        return build_stage_plan(
            ["NFLX", "AMD"], "2020-01-01", "2024-12-31",
            max_concurrent=kwargs.pop("max_concurrent", 8), **kwargs,
        )

    def test_stage_order(self):
        names = [name for name, _, _ in self._plan()]
        assert names == [
            "ingest_thetadata",
            "ingest_alpaca",
            "ingest_financialdatasets",
            "canonicalize",
            "feature_pipeline:core_v1",
            "feature_pipeline:options_v1",
            "feature_pipeline:insider_v1",
        ]

    def test_ingest_before_canonicalize_before_features(self):
        names = [name for name, _, _ in self._plan()]
        canon_idx = names.index("canonicalize")
        assert all(names.index(n) < canon_idx for n in names if n.startswith("ingest_"))
        assert all(names.index(n) > canon_idx for n in names if n.startswith("feature_"))

    def test_thetadata_config(self):
        _, job_def, run_config = self._plan()[0]
        assert job_def is sb.ingest_thetadata
        cfg = run_config["ops"]["fetch_thetadata_options"]["config"]
        assert cfg["underlyings"] == ["NFLX", "AMD"]
        assert cfg["start_date"] == "2020-01-01"
        assert cfg["end_date"] == "2024-12-31"
        assert cfg["eod_by_date"] is True, "backfills must use by-date bulk mode"
        assert cfg["max_concurrent"] == 8
        assert cfg["force"] is False

    def test_thetadata_force_passed(self):
        _, _, run_config = self._plan(force=True)[0]
        assert run_config["ops"]["fetch_thetadata_options"]["config"]["force"] is True

    def test_thetadata_max_concurrent_env_default(self, monkeypatch):
        monkeypatch.setenv("THETADATA_MAX_CONCURRENT", "5")
        plan = build_stage_plan(["NFLX"], "2020-01-01", "2020-06-30")
        cfg = plan[0][2]["ops"]["fetch_thetadata_options"]["config"]
        assert cfg["max_concurrent"] == 5

    def test_alpaca_config(self):
        _, job_def, run_config = self._plan()[1]
        assert job_def is sb.ingest_alpaca
        cfg = run_config["ops"]["fetch_alpaca_bars"]["config"]
        assert cfg["ticker_list"] == ["NFLX", "AMD"]
        assert cfg["start_date"] == "2020-01-01"
        assert cfg["end_date"] == "2024-12-31"

    def test_financialdatasets_config(self):
        _, job_def, run_config = self._plan()[2]
        assert job_def is sb.ingest_financialdatasets
        cfg = run_config["ops"]["ingest_financialdatasets_op"]["config"]
        assert cfg["ticker_list"] == ["NFLX", "AMD"]

    def test_canonicalize_domains(self):
        _, job_def, run_config = self._plan()[3]
        assert job_def is sb.canonicalize
        cfg = run_config["ops"]["canonicalize_op"]["config"]
        for domain in ("equity_ohlcv", "option_eod", "fundamentals",
                       "financial_metrics", "insider_trades", "institutional_holdings"):
            assert domain in cfg["domains"], f"missing canonicalize domain {domain}"
        assert cfg["domains"] == list(BACKFILL_DOMAINS)
        assert cfg["start_date"] == "2020-01-01"
        assert cfg["end_date"] == "2024-12-31"

    def test_feature_pipeline_config_per_feature_set(self):
        plan = self._plan(feature_sets=["core_v1", "options_v1"])
        feature_stages = [(n, j, rc) for n, j, rc in plan if n.startswith("feature_pipeline")]
        assert [n for n, _, _ in feature_stages] == [
            "feature_pipeline:core_v1", "feature_pipeline:options_v1",
        ]
        for (name, job_def, run_config), fs in zip(feature_stages, ["core_v1", "options_v1"]):
            assert job_def is sb.feature_pipeline
            cfg = run_config["ops"]["feature_pipeline_op"]["config"]
            assert cfg["feature_set"] == fs
            assert cfg["tickers"] == ["NFLX", "AMD"], (
                "feature pipeline must be restricted to the backfilled symbols"
            )
            assert cfg["start_date"] == "2020-01-01"
            assert cfg["end_date"] == "2024-12-31"

    def test_run_configs_are_valid_for_the_jobs(self):
        """Every stage run_config must pass the target job's config schema."""
        from dagster import validate_run_config
        for name, job_def, run_config in self._plan():
            result = validate_run_config(job_def, run_config)
            assert result is not None, f"stage {name} run_config invalid"


# ---------------------------------------------------------------------------
# run_symbol_backfill — chaining with mocked execute
# ---------------------------------------------------------------------------


def _mock_job(name: str, calls: list[str], success: bool = True) -> MagicMock:
    job = MagicMock(name=name)

    def _execute(run_config=None, raise_on_error=True):
        calls.append(name)
        return MagicMock(success=success)

    job.execute_in_process.side_effect = _execute
    return job


class TestRunSymbolBackfill:
    def _patched(self, calls: list[str], failing: str | None = None):
        """Patch all five stage jobs; the `failing` one returns success=False."""
        jobs = {}
        for name in ("ingest_thetadata", "ingest_alpaca", "ingest_financialdatasets",
                     "canonicalize", "feature_pipeline"):
            jobs[name] = _mock_job(name, calls, success=(name != failing))
        return patch.multiple(
            "yats_pipelines.jobs.backfill.symbol_backfill",
            record_start=MagicMock(), record_finish=MagicMock(), **jobs,
        )

    def test_all_stages_run_in_order_on_success(self):
        calls: list[str] = []
        with self._patched(calls):
            ok = run_symbol_backfill(
                ["NFLX"], "2020-01-01", "2024-12-31", feature_sets=["core_v1"]
            )
        assert ok is True
        assert calls == [
            "ingest_thetadata", "ingest_alpaca", "ingest_financialdatasets",
            "canonicalize", "feature_pipeline",
        ]

    def test_one_feature_pipeline_run_per_feature_set(self):
        calls: list[str] = []
        with self._patched(calls):
            run_symbol_backfill(
                ["NFLX"], "2020-01-01", "2024-12-31",
                feature_sets=["core_v1", "options_v1", "insider_v1"],
            )
        assert calls.count("feature_pipeline") == 3

    def test_failed_stage_aborts_remaining(self):
        calls: list[str] = []
        with self._patched(calls, failing="ingest_alpaca"):
            ok = run_symbol_backfill(["NFLX"], "2020-01-01", "2024-12-31")
        assert ok is False
        assert calls == ["ingest_thetadata", "ingest_alpaca"], (
            "stages after a failure must not run on partial raw data"
        )

    def test_stage_exception_aborts_and_returns_false(self):
        calls: list[str] = []
        cm = self._patched(calls)
        with cm:
            sb.canonicalize.execute_in_process.side_effect = RuntimeError("boom")
            ok = run_symbol_backfill(["NFLX"], "2020-01-01", "2024-12-31")
        assert ok is False
        assert "feature_pipeline" not in calls

    def test_symbols_normalized(self):
        calls: list[str] = []
        with self._patched(calls):
            run_symbol_backfill([" nflx ", "amd", ""], "2020-01-01", "2024-12-31")
            cfg = (sb.ingest_thetadata.execute_in_process.call_args
                   [1]["run_config"]["ops"]["fetch_thetadata_options"]["config"])
        assert cfg["underlyings"] == ["NFLX", "AMD"]

    def test_default_end_date_is_today(self):
        calls: list[str] = []
        with self._patched(calls):
            run_symbol_backfill(["NFLX"], "2020-01-01")  # no end date
            cfg = (sb.ingest_thetadata.execute_in_process.call_args
                   [1]["run_config"]["ops"]["fetch_thetadata_options"]["config"])
        assert cfg["end_date"] == _today()

    def test_invalid_dates_raise_before_any_stage_runs(self):
        calls: list[str] = []
        with self._patched(calls):
            with pytest.raises(ValueError):
                run_symbol_backfill(["NFLX"], "2025-01-01", "2020-01-01")
        assert calls == []

    def test_record_finish_failure_names_failed_stage(self):
        calls: list[str] = []
        finish = MagicMock()
        with self._patched(calls, failing="canonicalize"), \
             patch("yats_pipelines.jobs.backfill.symbol_backfill.record_finish", finish):
            run_symbol_backfill(["NFLX"], "2020-01-01", "2024-12-31")
        assert finish.call_args[0][2] == "failed"
        assert "canonicalize" in finish.call_args[1]["failure_cause"]


# ---------------------------------------------------------------------------
# --force behavior in the thetadata ingest (resume disabled)
# ---------------------------------------------------------------------------


class TestForceDisablesResume:
    def _run_fetch(self, force: bool) -> MagicMock:
        from dagster import build_op_context
        from yats_pipelines.jobs.ingest_thetadata import (
            IngestThetadataConfig, fetch_thetadata_options,
        )

        config = IngestThetadataConfig(
            underlyings=["AAPL"],
            start_date="20240101",
            end_date="20240103",
            eod_by_date=True,
            max_concurrent=2,
            force=force,
        )
        get_days = MagicMock(return_value={("AAPL", "20240102")})
        with build_op_context() as context, \
             patch("yats_pipelines.jobs.ingest_thetadata._get_ingested_days", get_days), \
             patch("yats_pipelines.jobs.ingest_thetadata._fetch_eod_day", return_value=[]), \
             patch("yats_pipelines.jobs.ingest_thetadata._pg_conn", return_value=MagicMock()), \
             patch("yats_pipelines.jobs.ingest_thetadata.QuestDBResource", return_value=MagicMock()), \
             patch("yats_pipelines.jobs.ingest_thetadata.ThetaDataResource") as mock_td_cls, \
             patch("yats_pipelines.jobs.ingest_thetadata.record_start"), \
             patch("yats_pipelines.jobs.ingest_thetadata.record_finish"):
            mock_td = MagicMock()
            mock_td.list_expirations.return_value = []
            mock_td.normalize_eod.return_value = []
            mock_td_cls.return_value = mock_td
            fetch_thetadata_options(context, config)
        return get_days

    def test_force_skips_resume_query(self):
        get_days = self._run_fetch(force=True)
        get_days.assert_not_called()

    def test_default_uses_resume_query(self):
        get_days = self._run_fetch(force=False)
        get_days.assert_called_once()

    def test_force_defaults_false(self):
        from yats_pipelines.jobs.ingest_thetadata import IngestThetadataConfig
        assert IngestThetadataConfig(underlyings=["AAPL"]).force is False


# ---------------------------------------------------------------------------
# feature_pipeline tickers override (backfill runs on symbols, not universes)
# ---------------------------------------------------------------------------


class TestFeaturePipelineTickersOverride:
    def _run_op(self, config) -> MagicMock:
        from dagster import build_op_context
        from yats_pipelines.jobs.feature_pipeline import feature_pipeline_op

        empty_ohlcv = pd.DataFrame(
            columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"]
        )
        load_universe = MagicMock(return_value=["AAPL"])
        with build_op_context() as context, \
             patch("yats_pipelines.jobs.feature_pipeline._load_universe", load_universe), \
             patch("yats_pipelines.jobs.feature_pipeline._pg_conn", return_value=MagicMock()), \
             patch("yats_pipelines.jobs.feature_pipeline._load_ohlcv", return_value=empty_ohlcv), \
             patch("yats_pipelines.jobs.feature_pipeline.record_start"), \
             patch("yats_pipelines.jobs.feature_pipeline.record_finish"):
            feature_pipeline_op(context, config)
        return load_universe

    def test_explicit_tickers_bypass_universe(self):
        from yats_pipelines.jobs.feature_pipeline import FeaturePipelineConfig
        load_universe = self._run_op(
            FeaturePipelineConfig(tickers=["NFLX", "AMD"], feature_set="core_v1")
        )
        load_universe.assert_not_called()

    def test_empty_tickers_fall_back_to_universe(self):
        from yats_pipelines.jobs.feature_pipeline import FeaturePipelineConfig
        load_universe = self._run_op(FeaturePipelineConfig(feature_set="core_v1"))
        load_universe.assert_called_once_with("sp500")
