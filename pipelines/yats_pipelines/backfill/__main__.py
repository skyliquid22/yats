"""Main entry point for the YATS backfill command.

Allows running the symbol backfill job from the command line:
    python -m yats_pipelines.backfill --symbols NFLX,AMD --start 2020-01-01
"""

import argparse
import sys
from datetime import datetime
from typing import List

from dagster import execute_job, job, op
from dagster import DagsterInstance

from ..jobs.backfill.symbol_backfill import symbol_backfill


def main():
    parser = argparse.ArgumentParser(description="YATS Symbol Backfill")
    parser.add_argument(
        "--symbols",
        required=True,
        help="Comma-separated list of symbols to backfill (e.g., NFLX,AMD)",
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start date for backfill in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end",
        help="End date for backfill in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-ingestion even if data already exists",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=8,
        help="Maximum concurrent gRPC requests (default: 8)",
    )

    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        print("Error: No valid symbols provided")
        sys.exit(1)

    end_date = args.end or datetime.now().strftime("%Y-%m-%d")

    # Configure the job
    config = {
        "ops": {
            "validate_config": {
                "config": {
                    "symbols": symbols,
                    "start_date": args.start,
                    "end_date": end_date,
                    "force": args.force,
                    "max_concurrent": args.max_concurrent,
                }
            }
        }
    }

    # Execute the job
    instance = DagsterInstance.get()
    result = execute_job(
        job=symbol_backfill,
        run_config=config,
        instance=instance,
    )

    if result.success:
        print("Symbol backfill completed successfully!")
        sys.exit(0)
    else:
        print("Symbol backfill failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()