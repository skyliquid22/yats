"""Dagster job — daily whole-market option flat-file capture.

Fetches option_flat_file_eod and option_flat_file_open_interest for the given
date via gRPC and writes them as Parquet files to:

    {YATS_DATA_DIR}/flatfiles/YYYYMMDD/option_eod.parquet
    {YATS_DATA_DIR}/flatfiles/YYYYMMDD/option_oi.parquet

YATS_DATA_DIR defaults to .yats_data/ relative to the working directory.

Access notes:
- Flat files are available from the account's first-access date (2026-07-02).
- Dates before that return PERMISSION_DENIED — the job logs a warning and
  succeeds with no data written. See docs/ingestion.md for the historical
  add-on purchase decision.
- Weekends and market holidays return NoDataFoundError — treated as empty.

Run daily after market close (4:00 PM ET, ~21:00 UTC).
"""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from dagster import Config, OpExecutionContext, job, op

logger = logging.getLogger(__name__)

YATS_DATA_DIR = os.environ.get("YATS_DATA_DIR", ".yats_data")
# First date flat files are available on this account.
_FIRST_ACCESS_DATE = "20260702"


def _flatfiles_dir(date_ymd: str) -> Path:
    return Path(YATS_DATA_DIR) / "flatfiles" / date_ymd


def _already_captured(date_ymd: str) -> bool:
    d = _flatfiles_dir(date_ymd)
    return (d / "option_eod.parquet").exists() and (d / "option_oi.parquet").exists()


class IngestFlatfilesConfig(Config):
    """Run config for the ingest_flatfiles job."""

    date: str = ""   # YYYYMMDD; empty = today (UTC)
    force: bool = False  # overwrite if files already exist


@op
def fetch_and_write_flatfiles(context: OpExecutionContext, config: IngestFlatfilesConfig) -> dict:
    """Fetch whole-market flat files and write to Parquet.

    Returns dict with keys: date, eod_rows, oi_rows, skipped.
    """
    import grpc
    import pandas as pd
    from thetadata import ThetaClient
    from thetadata.errors import NoDataFoundError

    date_ymd = config.date.strip() if config.date.strip() else datetime.now(timezone.utc).strftime("%Y%m%d")
    context.log.info("ingest_flatfiles: target date %s", date_ymd)

    if date_ymd < _FIRST_ACCESS_DATE and not config.force:
        context.log.warning(
            "ingest_flatfiles: %s is before first-access date %s — "
            "historical flat files require the paid add-on (see docs/ingestion.md). "
            "Skipping without error.",
            date_ymd, _FIRST_ACCESS_DATE,
        )
        return {"date": date_ymd, "eod_rows": 0, "oi_rows": 0, "skipped": True}

    if _already_captured(date_ymd) and not config.force:
        context.log.info(
            "ingest_flatfiles: %s already captured — skipping (idempotent). "
            "Pass force=true to overwrite.",
            date_ymd,
        )
        return {"date": date_ymd, "eod_rows": 0, "oi_rows": 0, "skipped": True}

    api_key = os.environ.get("THETADATA_API_KEY", "")
    client = ThetaClient(api_key=api_key, dataframe_type="pandas")

    d = datetime(int(date_ymd[:4]), int(date_ymd[4:6]), int(date_ymd[6:8]))
    date_obj = d.date()

    eod_df: pd.DataFrame | None = None
    oi_df: pd.DataFrame | None = None

    try:
        eod_df = client.option_flat_file_eod(date_obj)
        context.log.info("ingest_flatfiles: EOD rows=%d for %s", len(eod_df), date_ymd)
    except grpc.RpcError as exc:
        if exc.code() == grpc.StatusCode.PERMISSION_DENIED:
            context.log.warning(
                "ingest_flatfiles: PERMISSION_DENIED for EOD flat file on %s "
                "(date before first-access or historical add-on not purchased). "
                "See docs/ingestion.md.",
                date_ymd,
            )
        else:
            raise
    except NoDataFoundError:
        context.log.info(
            "ingest_flatfiles: no EOD flat file data for %s (holiday or non-trading day)",
            date_ymd,
        )

    try:
        oi_df = client.option_flat_file_open_interest(date_obj)
        context.log.info("ingest_flatfiles: OI rows=%d for %s", len(oi_df), date_ymd)
    except grpc.RpcError as exc:
        if exc.code() == grpc.StatusCode.PERMISSION_DENIED:
            context.log.warning(
                "ingest_flatfiles: PERMISSION_DENIED for OI flat file on %s. "
                "See docs/ingestion.md.",
                date_ymd,
            )
        else:
            raise
    except NoDataFoundError:
        context.log.info(
            "ingest_flatfiles: no OI flat file data for %s (holiday or non-trading day)",
            date_ymd,
        )

    if eod_df is None and oi_df is None:
        context.log.info("ingest_flatfiles: no data to write for %s", date_ymd)
        return {"date": date_ymd, "eod_rows": 0, "oi_rows": 0, "skipped": False}

    out_dir = _flatfiles_dir(date_ymd)
    out_dir.mkdir(parents=True, exist_ok=True)

    eod_rows = 0
    oi_rows = 0

    if eod_df is not None and not eod_df.empty:
        eod_path = out_dir / "option_eod.parquet"
        eod_df.to_parquet(eod_path, index=False, compression="snappy")
        eod_rows = len(eod_df)
        context.log.info("ingest_flatfiles: wrote %d EOD rows to %s", eod_rows, eod_path)

    if oi_df is not None and not oi_df.empty:
        oi_path = out_dir / "option_oi.parquet"
        oi_df.to_parquet(oi_path, index=False, compression="snappy")
        oi_rows = len(oi_df)
        context.log.info("ingest_flatfiles: wrote %d OI rows to %s", oi_rows, oi_path)

    return {"date": date_ymd, "eod_rows": eod_rows, "oi_rows": oi_rows, "skipped": False}


@job(tags={"yats/concurrency_pool": "ingest", "dagster/priority": "5"})
def ingest_flatfiles():
    """Dagster job: capture whole-market option flat files to Parquet (daily, after close)."""
    fetch_and_write_flatfiles()
