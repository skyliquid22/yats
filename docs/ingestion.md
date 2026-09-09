# YATS Ingestion Architecture

## Adding a symbol (one-command backfill)

To add new symbols to the platform, run a single command from the repo root:

```bash
PYTHONPATH=.:pipelines uv run python -m yats_pipelines.backfill \
    --symbols NFLX,AMD --start 2020-01-01
```

This chains the existing Dagster jobs in-process, in order:

| # | Stage | Job | What it writes |
|---|-------|-----|----------------|
| 1 | Options EOD history | `ingest_thetadata` | `raw_thetadata_options_chain` (current-chain snapshot) + `raw_thetadata_options_eod` (historical greeks/OHLC via gRPC by-date bulk mode) |
| 2 | Equity OHLCV | `ingest_alpaca` | `raw_alpaca_equity_ohlcv` (daily bars) |
| 3 | Fundamentals & filings | `ingest_financialdatasets` | `raw_fd_fundamentals`, `raw_fd_financial_metrics`, `raw_fd_earnings`, `raw_fd_insider_trades`, `raw_fd_analyst_estimates`, `raw_fd_institutional_holdings` |
| 4 | Canonicalize | `canonicalize` | `canonical_*` tables for domains `equity_ohlcv`, `fundamentals`, `financial_metrics`, `option_eod`, `insider_trades`, `institutional_holdings` + `reconciliation_log` |
| 5 | Features | `feature_pipeline` | `features` table, one run per feature set (default: `core_v1`, `options_v1`, `insider_v1`), restricted to the new symbols via the `tickers` config override |

A stage failure aborts the remaining stages (canonical/feature rows must not be
computed from incomplete raw data) and the command exits non-zero. The overall
run is recorded in `job_runs` under job name `symbol_backfill`.

### Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `--symbols` | (required) | Comma-separated tickers, e.g. `NFLX,AMD` |
| `--start` | (required) | Backfill start date, `YYYY-MM-DD` |
| `--end` | today (UTC) | Backfill end date, `YYYY-MM-DD` |
| `--force` | off | Re-fetch option days already present in QuestDB (disables resume) |
| `--max-concurrent` | `$THETADATA_MAX_CONCURRENT` or 8 | Concurrent gRPC requests to ThetaData |
| `--feature-sets` | `core_v1,options_v1,insider_v1` | Feature sets to compute (comma-separated, from `configs/feature_sets/`) |

### Re-running / resume

The command is idempotent and safe to re-run after an interruption:

- The options ingest queries `raw_thetadata_options_eod` for already-fetched
  `(symbol, day)` pairs and skips them (`--force` disables this).
- Canonicalization dedups latest-ingested-wins per contract/bar/filing, so
  re-ingested raw rows do not duplicate canonical rows.

After the backfill, add the symbols to a universe YAML under
`configs/universes/` so scheduled feature-pipeline runs and experiments pick
them up; the backfill itself computes features via an explicit ticker list and
does not edit universe configs.

## Transport and environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `THETADATA_TRANSPORT` | `grpc` | ThetaData transport. `grpc` (default) uses the `thetadata` Python client against the cloud gRPC API; `terminal` uses the local Theta Terminal REST API at `THETADATA_BASE_URL` (default `http://127.0.0.1:25503/v3`). |
| `THETADATA_API_KEY` | (empty) | API key for the gRPC transport. Required when `THETADATA_TRANSPORT=grpc`. |
| `THETADATA_MAX_CONCURRENT` | `8` | Max concurrent gRPC requests (PRO plan allows 8). |
| `APCA_API_KEY_ID` / `APCA_API_SECRET_KEY` | (empty) | Alpaca credentials for the equity OHLCV ingest. |
| `FINANCIALDATASETS_API_KEY` | (empty) | financialdatasets.ai key for fundamentals/insider/13F. |
| `FD_13F_QUARTERS` | `10` | How many recent calendar quarter-ends to fetch for 13F institutional holdings. |

## Vendor caveats

Known limits of the data vendors. The ingest jobs already work around them,
but they define what data can and cannot exist:

- **13F institutional holdings (financialdatasets.ai)**: the endpoint
  hard-caps responses at **200 rows per quarter** regardless of `limit`, and
  **ignores the `offset` parameter** (an offset loop refetches the same page
  forever; verified live 2026-07-08). Pagination is done with a
  **quarter cursor** instead: one request per `report_period=YYYY-MM-DD`
  quarter-end (last `FD_13F_QUARTERS` quarters). The ≤200 filers returned per
  quarter cover roughly ~75% of market cap for large names: top-holder
  features are fine, exhaustive filer coverage is not available.
- **Insider trades (financialdatasets.ai)**: fetch caps at **500 rows per
  ticker** (single request, no pagination). For high-insider-activity names
  this truncates deep history.
- **13F `value_usd` unreliable before 2025-06**: financialdatasets.ai dollar
  values are inconsistent for report periods before ~June 2025. Ownership
  levels use **`shares`** (consistent across splits), never absolute
  `value_usd`. The only sanctioned `value_usd` use is within-quarter ratios
  (e.g. `inst_top10_share`), where the uniform scaling artifact cancels out.
- **ETFs (SPY, QQQ, …) have no insider or fundamentals data**: there is no
  Form 4 insider activity or income-statement data for funds. `insider_*` and
  fundamental features are **structurally null** for ETFs; this is expected,
  not an ingest failure. 13F-based `inst_*` features do cover ETFs.
- **Options historical greeks (ThetaData gRPC `greeks-eod`)**: historical
  EOD greeks come from the bulk `option_history_greeks_eod` endpoint: **one
  call per (symbol, trading day)** with `expiration=*`. A multi-year backfill
  is therefore ~252 calls/symbol/year, run through a worker pool capped at
  `THETADATA_MAX_CONCURRENT`. The per-expiry history endpoint does not scale
  and is only used as a fallback path.
- **Flat files start 2026-07-02**: whole-market option flat files are
  available **forward from 2026-07-02** on our subscription; earlier dates
  return `PERMISSION_DENIED`. Historical flat files are a **paid add-on**
  (purchase deferred; see the flat-files section below). Per-symbol backfill
  via `ingest_thetadata` is unaffected by this limit.

## ThetaData Option Flat Files

### What they are

`ThetaClient.option_flat_file_eod(date)` and `option_flat_file_open_interest(date)` return
**whole-market** per-date snapshots: every optionable symbol, every expiration, every strike.
This is the full market universe for a single calendar date.

### Storage choice: Parquet under `.yats_data/flatfiles/`

Flat files go to Parquet (not QuestDB) for these reasons:

| Factor | Decision |
|--------|----------|
| **Volume** | Whole-market flat files are millions of rows per day. QuestDB is optimized for repeated time-series queries over known symbols, not one-time bulk scans of the full universe. |
| **Access pattern** | Flat files are consumed as batch snapshots (universe expansion, backtests). Columnar Parquet is faster and cheaper for this than SQL row scans. |
| **Schema stability** | ThetaData may add columns over time. Parquet preserves the raw schema without schema migrations. |
| **Compression** | Snappy-compressed Parquet is ~5–10× smaller than raw CSV and ~2–3× smaller than QuestDB for wide option data. |

**Layout:**

```
.yats_data/flatfiles/
  YYYYMMDD/
    option_eod.parquet       # EOD greeks + OHLCV for all contracts
    option_oi.parquet        # Open interest for all contracts
```

### First-access date and historical coverage

Our ThetaData account has access to flat files starting **2026-07-02** (first-access date).

The `ingest_flatfiles` job runs daily after close and builds the archive **forward from
that date**. Requesting flat files for dates before 2026-07-02 returns a gRPC
`PERMISSION_DENIED` error; the job handles this gracefully (logs a warning, succeeds
with no data written).

### Historical flat-file add-on purchase

To backfill data before 2026-07-02, ThetaData offers a **historical flat-file add-on**
(a one-time or recurring purchase on the subscription). This enables flat-file access
for arbitrary historical dates.

**Decision: purchase deferred.**

Rationale:
1. The current universe (defined in `configs/`) is small enough that historical backfill
   via `ingest_thetadata` (per-symbol gRPC EOD) is cheaper and sufficient.
2. Flat files become worthwhile when the universe expands to ~1,000+ symbols; at that
   scale the per-symbol loop would take hours and the flat-file bulk pull (~seconds/day)
   wins decisively.
3. The forward archive built by `ingest_flatfiles` covers any future universe expansion
   from 2026-07-02 onward without needing the add-on.

**When to purchase:** At universe expansion time. The `ingest_flatfiles` job is already
wired and running; enabling historical access only requires purchasing the add-on and
optionally running the job with a backfill date range.
