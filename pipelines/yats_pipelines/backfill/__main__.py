"""CLI entry point for the one-command symbol backfill.

    python -m yats_pipelines.backfill --symbols NFLX,AMD --start 2020-01-01

Chains ingest (ThetaData options EOD, Alpaca OHLCV, financialdatasets.ai
fundamentals/insider/13F) → canonicalize → feature pipeline for the given
symbols. See docs/ingestion.md ("Adding a symbol") for the full runbook.
"""

from __future__ import annotations

import argparse
import logging
import sys

from yats_pipelines.jobs.backfill.symbol_backfill import (
    DEFAULT_FEATURE_SETS,
    default_max_concurrent,
    run_symbol_backfill,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m yats_pipelines.backfill",
        description="YATS one-command symbol backfill",
    )
    parser.add_argument(
        "--symbols",
        required=True,
        help="Comma-separated list of symbols to backfill (e.g. NFLX,AMD)",
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end",
        default="",
        help="End date in YYYY-MM-DD format (default: yesterday, UTC — free-tier data providers block current-day)",
    )
    parser.add_argument(
        "--skip-stages",
        default="",
        help="Comma-separated stage names to skip (e.g. ingest_thetadata for universes without options data)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch option days even if already ingested (disables resume)",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=default_max_concurrent(),
        help="Max concurrent gRPC requests (default: THETADATA_MAX_CONCURRENT or 8)",
    )
    parser.add_argument(
        "--feature-sets",
        default=",".join(DEFAULT_FEATURE_SETS),
        help=f"Comma-separated feature sets to compute (default: {','.join(DEFAULT_FEATURE_SETS)})",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = build_parser().parse_args(argv)

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        print("Error: no valid symbols provided", file=sys.stderr)
        return 2

    feature_sets = [s.strip() for s in args.feature_sets.split(",") if s.strip()]

    try:
        ok = run_symbol_backfill(
            symbols,
            args.start,
            args.end,
            force=args.force,
            max_concurrent=args.max_concurrent,
            feature_sets=feature_sets,
            skip_stages=tuple(x.strip() for x in args.skip_stages.split(",") if x.strip()),
        )
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    if ok:
        print(f"Symbol backfill completed successfully for: {', '.join(symbols)}")
        return 0
    print("Symbol backfill failed — see logs above", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
