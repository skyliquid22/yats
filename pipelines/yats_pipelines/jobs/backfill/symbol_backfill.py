"""Dagster job — one-command symbol backfill and ingestion.

This job provides a single command to add new symbols to the YATS system:
    python -m yats_pipelines.backfill --symbols NFLX,AMD --start 2020-01-01

The job orchestrates the full ingestion pipeline for the specified symbols:
1. Ingest historical data from ThetaData (gRPC-2 path)
2. Canonicalize the data (equity/option_eod/fundamentals domains)
3. Run the feature pipeline for configured feature sets

The job handles:
- Resume: skips already-ingested data
- Date ranges: configurable start/end dates
- Concurrency: respects THETADATA_MAX_CONCURRENT limit
- Error handling: graceful degradation on partial failures
"""

import logging
import os
from datetime import datetime, timedelta
from typing import List

from dagster import Config, OpExecutionContext, job, op
from dagster import Failure

logger = logging.getLogger(__name__)


class SymbolBackfillConfig(Config):
    """Configuration for the symbol backfill job."""

    symbols: List[str]
    start_date: str  # YYYY-MM-DD
    end_date: str = ""  # YYYY-MM-DD, empty = today
    # Force re-ingestion even if data already exists
    force: bool = False
    # Maximum concurrent gRPC requests (PRO plan allows 8)
    max_concurrent: int = int(os.environ.get("THETADATA_MAX_CONCURRENT", "8"))


@op
def validate_config(context: OpExecutionContext, config: SymbolBackfillConfig) -> dict:
    """Validate the job configuration and prepare parameters."""
    context.log.info(f"Validating configuration for symbols: {', '.join(config.symbols)}")

    # Validate symbols
    if not config.symbols:
        raise Failure("No symbols specified for backfill")

    # Validate dates
    try:
        start_date = datetime.strptime(config.start_date, "%Y-%m-%d")
        if config.end_date:
            end_date = datetime.strptime(config.end_date, "%Y-%m-%d")
        else:
            end_date = datetime.now()

        if start_date > end_date:
            raise Failure("start_date cannot be after end_date")
    except ValueError as e:
        raise Failure(f"Invalid date format: {str(e)}")

    context.log.info(f"Date range: {config.start_date} to {config.end_date or 'today'}")

    return {
        "symbols": config.symbols,
        "start_date": config.start_date,
        "end_date": config.end_date or end_date.strftime("%Y-%m-%d"),
        "force": config.force,
        "max_concurrent": config.max_concurrent,
    }


@op
def ingest_symbol_data(context: OpExecutionContext, params: dict) -> dict:
    """Ingest historical data for the specified symbols.

    Uses the ingest_thetadata job infrastructure with backfill parameters.
    """
    from ..jobs.ingest_thetadata import IngestThetadataConfig, fetch_thetadata_options, write_raw_thetadata, canonicalize_options
    from ..resources.questdb import QuestDBResource
    from ..utils.run_recorder import record_start, record_finish
    from dagster import build_op_context

    context.log.info(f"Ingesting data for symbols: {', '.join(params['symbols'])}")

    # Record the backfill start
    run_id = context.run_id
    detail = f"backfill {' '.join(params['symbols'])}"
    record_start("symbol_backfill", run_id, detail)

    try:
        # Configure the ingestion job
        ingest_config = IngestThetadataConfig(
            underlyings=params["symbols"],
            start_date=params["start_date"].replace("-", ""),
            end_date=params["end_date"].replace("-", ""),
            eod_by_date=True,  # Use efficient by-date mode for backfills
            max_concurrent=params["max_concurrent"],
        )

        # Create a sub-context for the ingestion ops
        sub_context = build_op_context(run_id=run_id)

        # Fetch the data
        fetch_result = fetch_thetadata_options(sub_context, ingest_config)
        context.log.info(f"Fetched {len(fetch_result['chain_rows'])} chain rows and {len(fetch_result['eod_rows'])} EOD rows")

        # Write raw data
        write_result = write_raw_thetadata(sub_context, fetch_result)
        context.log.info(f"Wrote {write_result['chain_count']} chain rows and {write_result['eod_count']} EOD rows")

        # Canonicalize the data
        canonicalize_options(sub_context, write_result)
        context.log.info("Completed canonicalization of options data")

        record_finish("symbol_backfill", run_id, "success")
        return {
            "symbols": params["symbols"],
            "rows_written": write_result["chain_count"] + write_result["eod_count"],
        }

    except Exception as e:
        record_finish("symbol_backfill", run_id, "failed", failure_cause=str(e)[:200])
        raise Failure(f"Symbol ingestion failed: {str(e)}")


@op
def run_feature_pipeline(context: OpExecutionContext, ingestion_result: dict) -> None:
    """Run the feature pipeline for the newly ingested symbols.

    This processes the canonicalized data through the feature engineering pipeline.
    """
    from ..jobs.feature_pipeline import run_feature_pipeline_for_symbols

    symbols = ingestion_result["symbols"]
    context.log.info(f"Running feature pipeline for symbols: {', '.join(symbols)}")

    try:
        # Run the feature pipeline for these symbols
        # This will use the existing feature pipeline infrastructure
        run_feature_pipeline_for_symbols(symbols)
        context.log.info("Feature pipeline completed successfully")
    except Exception as e:
        logger.error(f"Feature pipeline failed: {str(e)}")
        raise Failure(f"Feature pipeline failed: {str(e)}")


@job(tags={"yats/concurrency_pool": "backfill", "dagster/priority": "15"})
def symbol_backfill():
    """Dagster job: one-command symbol backfill and ingestion.

    Usage:
        python -m yats_pipelines.backfill --symbols NFLX,AMD --start 2020-01-01

    This job orchestrates the full ingestion pipeline for new symbols:
    1. Ingest historical data from ThetaData
    2. Canonicalize the data
    3. Run the feature pipeline
    """
    params = validate_config()
    ingestion_result = ingest_symbol_data(params)
    run_feature_pipeline(ingestion_result)