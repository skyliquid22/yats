"""YATS one-command symbol backfill.

Entry point:
    python -m yats_pipelines.backfill --symbols NFLX,AMD --start 2020-01-01

Chains the existing Dagster jobs in-process, in order:
1. ingest_thetadata          — options EOD history via gRPC (by-date bulk mode)
2. ingest_alpaca             — equity OHLCV daily bars
3. ingest_financialdatasets  — fundamentals / metrics / insider trades / 13F
4. canonicalize              — raw → canonical (equity_ohlcv, fundamentals,
                               financial_metrics, option_eod, insider_trades,
                               institutional_holdings)
5. feature_pipeline          — one run per configured feature set

See docs/ingestion.md ("Adding a symbol") for the runbook.
"""
