"""YATS Backfill Package.

This module provides tools for backfilling historical data for new symbols.

The main entry point is the symbol_backfill job, which can be run with:
    python -m yats_pipelines.backfill --symbols NFLX,AMD --start 2020-01-01

This job orchestrates the full ingestion pipeline:
1. Ingest historical data from ThetaData (gRPC-2 path)
2. Canonicalize the data (equity/option_eod/fundamentals domains)
3. Run the feature pipeline for configured feature sets
"""