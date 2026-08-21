# YATS Ingestion Architecture

## ThetaData Option Flat Files

### What they are

`ThetaClient.option_flat_file_eod(date)` and `option_flat_file_open_interest(date)` return
**whole-market** per-date snapshots — every optionable symbol, every expiration, every strike.
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
`PERMISSION_DENIED` error — the job handles this gracefully (logs a warning, succeeds
with no data written).

### Historical flat-file add-on purchase

To backfill data before 2026-07-02, ThetaData offers a **historical flat-file add-on**
(a one-time or recurring purchase on the subscription). This enables flat-file access
for arbitrary historical dates.

**Decision: purchase deferred.**

Rationale:
1. The current universe (defined in `configs/`) is small enough that historical backfill
   via `ingest_thetadata` (per-symbol gRPC EOD) is cheaper and sufficient.
2. Flat files become worthwhile when the universe expands to ~1,000+ symbols — at that
   scale the per-symbol loop would take hours and the flat-file bulk pull (~seconds/day)
   wins decisively.
3. The forward archive built by `ingest_flatfiles` covers any future universe expansion
   from 2026-07-02 onward without needing the add-on.

**When to purchase:** At universe expansion time. The `ingest_flatfiles` job is already
wired and running; enabling historical access only requires purchasing the add-on and
optionally running the job with a backfill date range.
