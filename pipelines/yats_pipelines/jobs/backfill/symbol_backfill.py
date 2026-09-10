"""One-command symbol backfill — ingest → canonicalize → feature pipeline.

Entry point:
    python -m yats_pipelines.backfill --symbols NFLX,AMD --start 2020-01-01

Each stage is an existing Dagster job executed in-process, in this order:

1. ingest_thetadata          — options EOD history via gRPC (by-date bulk mode)
2. ingest_alpaca             — equity OHLCV daily bars
3. ingest_financialdatasets  — fundamentals / financial metrics / insider / 13F
4. canonicalize              — raw → canonical for all backfill domains
5. feature_pipeline          — one run per feature set, restricted to the
                               backfilled symbols

A stage failure aborts the remaining stages: downstream stages would otherwise
compute canonical/feature rows from incomplete raw data. Re-running the same
command is safe — the options ingest resumes past already-fetched (symbol, day)
pairs and canonicalization dedups on rerun. Pass force=True (CLI --force) to
re-fetch option days that are already in QuestDB.
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

from yats_pipelines.jobs.canonicalize import canonicalize
from yats_pipelines.jobs.feature_pipeline import feature_pipeline
from yats_pipelines.jobs.ingest_alpaca import ingest_alpaca
from yats_pipelines.jobs.ingest_financialdatasets import ingest_financialdatasets
from yats_pipelines.jobs.ingest_thetadata import ingest_thetadata
from yats_pipelines.utils.run_recorder import record_finish, record_start

logger = logging.getLogger(__name__)

# Feature sets computed for freshly backfilled symbols. Together these cover
# every canonical domain the backfill ingests: core_v1 (OHLCV + fundamentals +
# cross-sectional + regime), options_v1 (EOD chain), insider_v1 (insider + 13F).
DEFAULT_FEATURE_SETS: tuple[str, ...] = ("core_v1", "options_v1", "insider_v1")

# Canonicalize domains covered by the backfill's three ingest stages.
BACKFILL_DOMAINS: tuple[str, ...] = (
    "equity_ohlcv",
    "fundamentals",
    "financial_metrics",
    "option_eod",
    "insider_trades",
    "institutional_holdings",
)


def default_max_concurrent() -> int:
    """Concurrent gRPC request cap — PRO plan allows 8."""
    return int(os.environ.get("THETADATA_MAX_CONCURRENT", "8"))


def resolve_end_date(end_date: str) -> str:
    """Empty end date defaults to YESTERDAY (UTC): free-tier Alpaca returns
    403 Forbidden for current-day SIP data, which would abort the whole chain."""
    return end_date or (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")


def validate_params(symbols: list[str], start_date: str, end_date: str) -> None:
    """Validate backfill parameters. Raises ValueError on bad input."""
    if not symbols:
        raise ValueError("No symbols specified for backfill")
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"Invalid date format (expected YYYY-MM-DD): {exc}") from exc
    if start > end:
        raise ValueError(
            f"start_date {start_date} cannot be after end_date {end_date}"
        )


def build_stage_plan(
    symbols: list[str],
    start_date: str,
    end_date: str,
    *,
    force: bool = False,
    max_concurrent: int | None = None,
    feature_sets: tuple[str, ...] | list[str] = DEFAULT_FEATURE_SETS,
    skip_stages: tuple[str, ...] | list[str] = (),
) -> list[tuple[str, object, dict]]:
    """Build the ordered (stage_name, job_def, run_config) execution plan.

    skip_stages: stage names to omit (e.g. ("ingest_thetadata",) when a
    universe needs no options data — per-symbol options history is the slow
    stage). Canonicalize domains are trimmed to match skipped ingests.

    Dates are ISO YYYY-MM-DD throughout; the thetadata ingest op strips the
    dashes itself for the vendor's YYYYMMDD format.
    """
    if max_concurrent is None:
        max_concurrent = default_max_concurrent()

    plan: list[tuple[str, object, dict]] = [
        (
            "ingest_thetadata",
            ingest_thetadata,
            {
                "ops": {
                    "fetch_thetadata_options": {
                        "config": {
                            "underlyings": symbols,
                            "start_date": start_date,
                            "end_date": end_date,
                            # By-date bulk mode: one greeks-eod call per
                            # (symbol, trading day) — the only mode that scales
                            # to multi-year backfills.
                            "eod_by_date": True,
                            "max_concurrent": max_concurrent,
                            "force": force,
                        }
                    }
                }
            },
        ),
        (
            "ingest_alpaca",
            ingest_alpaca,
            {
                "ops": {
                    "fetch_alpaca_bars": {
                        "config": {
                            "ticker_list": symbols,
                            "start_date": start_date,
                            "end_date": end_date,
                        }
                    }
                }
            },
        ),
        (
            "ingest_financialdatasets",
            ingest_financialdatasets,
            {
                "ops": {
                    "ingest_financialdatasets_op": {
                        "config": {
                            "ticker_list": symbols,
                            # data_domains defaults to all six FD domains
                        }
                    }
                }
            },
        ),
        (
            "canonicalize",
            canonicalize,
            {
                "ops": {
                    "canonicalize_op": {
                        "config": {
                            "domains": list(BACKFILL_DOMAINS),
                            "start_date": start_date,
                            "end_date": end_date,
                        }
                    }
                }
            },
        ),
    ]

    for fs in feature_sets:
        plan.append(
            (
                f"feature_pipeline:{fs}",
                feature_pipeline,
                {
                    "ops": {
                        "feature_pipeline_op": {
                            "config": {
                                "tickers": symbols,
                                "feature_set": fs,
                                "start_date": start_date,
                                "end_date": end_date,
                            }
                        }
                    }
                },
            )
        )

    if skip_stages:
        skip = set(skip_stages)
        plan = [(name, job, cfg) for (name, job, cfg) in plan if name not in skip]
        # trim canonicalize domains matching skipped ingests
        if "ingest_thetadata" in skip:
            for name, _job, cfg in plan:
                if name == "canonicalize":
                    doms = cfg["ops"]["canonicalize_op"]["config"]["domains"]
                    cfg["ops"]["canonicalize_op"]["config"]["domains"] = [
                        d for d in doms if d != "option_eod"
                    ]
    return plan


def run_symbol_backfill(
    symbols: list[str],
    start_date: str,
    end_date: str = "",
    *,
    force: bool = False,
    max_concurrent: int | None = None,
    feature_sets: tuple[str, ...] | list[str] = DEFAULT_FEATURE_SETS,
    skip_stages: tuple[str, ...] | list[str] = (),
) -> bool:
    """Run the full backfill chain for the given symbols.

    Returns True when every stage succeeded. Stops at the first failed stage
    and returns False — downstream stages must not run on partial raw data.
    """
    symbols = [s.strip().upper() for s in symbols if s.strip()]
    end_date = resolve_end_date(end_date)
    validate_params(symbols, start_date, end_date)

    run_id = f"backfill-{uuid.uuid4().hex[:12]}"
    detail = f"{','.join(symbols)} {start_date}..{end_date}"
    record_start("symbol_backfill", run_id, detail)

    plan = build_stage_plan(
        symbols,
        start_date,
        end_date,
        force=force,
        max_concurrent=max_concurrent,
        feature_sets=feature_sets,
        skip_stages=skip_stages,
    )

    for stage_name, job_def, run_config in plan:
        logger.info("symbol_backfill %s: running stage %s", run_id, stage_name)
        try:
            result = job_def.execute_in_process(
                run_config=run_config, raise_on_error=False
            )
        except Exception as exc:
            logger.error(
                "symbol_backfill %s: stage %s raised: %s", run_id, stage_name, exc
            )
            record_finish(
                "symbol_backfill", run_id, "failed",
                failure_cause=f"{stage_name}: {exc}"[:200],
            )
            return False

        if not result.success:
            logger.error(
                "symbol_backfill %s: stage %s failed — aborting remaining stages",
                run_id, stage_name,
            )
            record_finish(
                "symbol_backfill", run_id, "failed",
                failure_cause=f"stage {stage_name} failed",
            )
            return False

        logger.info("symbol_backfill %s: stage %s succeeded", run_id, stage_name)

    record_finish("symbol_backfill", run_id, "success")
    return True
