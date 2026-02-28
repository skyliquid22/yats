"""Dagster canonicalization job — raw → canonical with reconciliation.

Reads from raw_* tables via PG wire, applies validation and reconciliation
rules, writes to canonical_* tables and reconciliation_log via ILP.

Supports three domains:
- equity_ohlcv: Alpaca primary, validation (missing fields, non-positive
  prices, extreme outliers), single-vendor per symbol-day
- fundamentals: financialdatasets.ai sole source, latest filing supersedes,
  point-in-time semantics
- financial_metrics: financialdatasets.ai sole source, forward-filled to
  daily frequency with as-of semantics
"""

import json
import logging
import statistics
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import psycopg2
from dagster import Config, OpExecutionContext, job, op
from questdb.ingress import Protocol, Sender, TimestampNanos

from yats_pipelines.resources.questdb import QuestDBResource

logger = logging.getLogger(__name__)

ALL_DOMAINS = ("equity_ohlcv", "fundamentals", "financial_metrics")


class CanonicalizeConfig(Config):
    """Run config for the canonicalize job."""

    domains: list[str] = list(ALL_DOMAINS)
    start_date: str = ""  # ISO-8601, e.g. "2024-01-01"; empty = all
    end_date: str = ""  # ISO-8601; empty = all
    primary_vendor_ohlcv: str = "alpaca"
    outlier_std_threshold: float = 5.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pg_conn(qdb: QuestDBResource):
    """Open a PG wire connection to QuestDB."""
    return psycopg2.connect(
        host=qdb.pg_host,
        port=qdb.pg_port,
        user=qdb.pg_user,
        password=qdb.pg_password,
        database=qdb.pg_database,
    )


def _ilp_sender(qdb: QuestDBResource):
    """Create a QuestDB ILP Sender context manager."""
    return Sender(Protocol.Tcp, qdb.ilp_host, qdb.ilp_port)


def _ts_nanos(dt: datetime) -> TimestampNanos:
    """Convert datetime to ILP TimestampNanos."""
    return TimestampNanos(int(dt.timestamp() * 1_000_000_000))


def _row(sender, table: str, symbols: dict, columns: dict, at: datetime) -> None:
    """Write an ILP row, filtering out None values from columns."""
    sender.row(
        table,
        symbols=symbols,
        columns={k: v for k, v in columns.items() if v is not None},
        at=_ts_nanos(at),
    )


def _date_clause(start_date: str, end_date: str, ts_col: str = "timestamp") -> str:
    """Build a WHERE clause for date range filtering."""
    parts = []
    if start_date:
        parts.append(f"{ts_col} >= '{start_date}T00:00:00.000000Z'")
    if end_date:
        parts.append(f"{ts_col} <= '{end_date}T23:59:59.999999Z'")
    if parts:
        return " WHERE " + " AND ".join(parts)
    return ""


# ---------------------------------------------------------------------------
# Equity OHLCV canonicalization
# ---------------------------------------------------------------------------

_OHLCV_PRICE_FIELDS = ("open", "high", "low", "close")


def _validate_ohlcv_row(row: dict, rolling_stats: dict, std_threshold: float) -> list[str]:
    """Validate a single OHLCV row. Returns list of warning strings."""
    warnings = []
    symbol = row["symbol"]

    # Missing fields
    for field in ("open", "high", "low", "close", "volume"):
        if row.get(field) is None:
            warnings.append(f"missing_{field}")

    # Non-positive prices
    for field in _OHLCV_PRICE_FIELDS:
        val = row.get(field)
        if val is not None and val <= 0:
            warnings.append(f"non_positive_{field}={val}")

    # Extreme outliers: >N std from rolling mean
    stats = rolling_stats.get(symbol)
    if stats and stats["count"] >= 20:
        close = row.get("close")
        if close is not None and stats["stdev"] > 0:
            z = abs(close - stats["mean"]) / stats["stdev"]
            if z > std_threshold:
                warnings.append(f"outlier_close_z={z:.2f}")

    return warnings


def _build_rolling_stats(rows: list[dict], window: int = 60) -> dict[str, dict]:
    """Build per-symbol rolling stats from recent close prices."""
    by_symbol: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        close = row.get("close")
        if close is not None and close > 0:
            by_symbol[row["symbol"]].append(close)

    result = {}
    for symbol, closes in by_symbol.items():
        recent = closes[-window:]
        if len(recent) >= 2:
            result[symbol] = {
                "mean": statistics.mean(recent),
                "stdev": statistics.stdev(recent),
                "count": len(recent),
            }
    return result


def _canonicalize_equity_ohlcv(
    conn, sender, config: CanonicalizeConfig, now: datetime, run_id: str, log,
    *, canonical_rows_out: dict[str, list[dict]] | None = None,
) -> int:
    """Read raw OHLCV, validate, write canonical + reconciliation log.

    If canonical_rows_out is provided, appends {symbol: [row_dicts]} for
    downstream canonical hash computation.
    """
    where = _date_clause(config.start_date, config.end_date)
    query = f"SELECT * FROM raw_alpaca_equity_ohlcv{where} ORDER BY timestamp"

    cur = conn.cursor()
    cur.execute(query)
    columns = [desc[0] for desc in cur.description]
    raw_rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()

    if not raw_rows:
        log.info("equity_ohlcv: no raw rows found")
        return 0

    log.info("equity_ohlcv: read %d raw rows", len(raw_rows))

    # Build rolling stats for outlier detection
    rolling_stats = _build_rolling_stats(raw_rows, window=60)

    # Group by (symbol, date) — pick one bar per symbol-day
    by_key: dict[tuple, list[dict]] = defaultdict(list)
    for row in raw_rows:
        ts = row["timestamp"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        row["timestamp"] = ts
        day = ts.date()
        by_key[(row["symbol"], day)].append(row)

    written = 0
    for (symbol, day), bars in by_key.items():
        # Single vendor per symbol-day — take the latest ingested bar
        bar = bars[-1]

        warnings = _validate_ohlcv_row(bar, rolling_stats, config.outlier_std_threshold)
        if any(w.startswith("missing_") or w.startswith("non_positive_") for w in warnings):
            validation_status = "failed"
        elif warnings:
            validation_status = "warning"
        else:
            validation_status = "passed"

        ts = bar["timestamp"]

        _row(sender, "canonical_equity_ohlcv",
             symbols={
                 "symbol": symbol,
                 "source_vendor": config.primary_vendor_ohlcv,
                 "reconcile_method": "batch",
                 "validation_status": validation_status,
             },
             columns={
                 "open": bar.get("open"),
                 "high": bar.get("high"),
                 "low": bar.get("low"),
                 "close": bar.get("close"),
                 "volume": bar.get("volume"),
                 "vwap": bar.get("vwap"),
                 "trade_count": bar.get("trade_count"),
                 "canonicalized_at": _ts_nanos(now),
             },
             at=ts)

        # Collect canonical row for hash computation
        if canonical_rows_out is not None:
            canonical_rows_out.setdefault(symbol, []).append({
                "symbol": symbol,
                "timestamp": ts,
                "open": bar.get("open"),
                "high": bar.get("high"),
                "low": bar.get("low"),
                "close": bar.get("close"),
                "volume": bar.get("volume"),
                "vwap": bar.get("vwap"),
                "trade_count": bar.get("trade_count"),
            })

        # Reconciliation log
        _row(sender, "reconciliation_log",
             symbols={
                 "domain": "equity_ohlcv",
                 "symbol": symbol,
                 "primary_vendor": config.primary_vendor_ohlcv,
             },
             columns={
                 "fallback_used": False,
                 "validation_warnings": json.dumps(warnings) if warnings else "[]",
                 "dagster_run_id": run_id,
                 "reconciled_at": _ts_nanos(now),
             },
             at=ts)
        written += 1

    return written


# ---------------------------------------------------------------------------
# Fundamentals canonicalization
# ---------------------------------------------------------------------------


def _canonicalize_fundamentals(
    conn, sender, config: CanonicalizeConfig, now: datetime, run_id: str, log
) -> int:
    """Read raw fundamentals, apply latest-supersedes, write canonical.

    Point-in-time semantics: report_date = when filed, not fiscal period end.
    Latest filing supersedes prior values for the same (symbol, fiscal_period, period).
    """
    where = _date_clause(config.start_date, config.end_date, ts_col="report_date")
    query = f"SELECT * FROM raw_fd_fundamentals{where} ORDER BY report_date"

    cur = conn.cursor()
    cur.execute(query)
    columns = [desc[0] for desc in cur.description]
    raw_rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()

    if not raw_rows:
        log.info("fundamentals: no raw rows found")
        return 0

    log.info("fundamentals: read %d raw rows", len(raw_rows))

    # Latest filing supersedes: group by (symbol, fiscal_period, period),
    # take the row with the latest report_date
    by_key: dict[tuple, dict] = {}
    for row in raw_rows:
        rd = row["report_date"]
        if isinstance(rd, str):
            rd = datetime.fromisoformat(rd.replace("Z", "+00:00"))
        row["report_date"] = rd
        key = (row["symbol"], row.get("fiscal_period", ""), row.get("period", ""))
        # Later rows supersede earlier ones (ordered by report_date)
        by_key[key] = row

    written = 0
    vendor = "financialdatasets"
    for (symbol, fiscal_period, period), row in by_key.items():
        report_date = row["report_date"]

        _row(sender, "canonical_fundamentals",
             symbols={
                 "symbol": symbol,
                 "fiscal_period": fiscal_period,
                 "period": period,
                 "source_vendor": vendor,
                 "reconcile_method": "batch",
             },
             columns={
                 "revenue": row.get("revenue"),
                 "cost_of_revenue": row.get("cost_of_revenue"),
                 "gross_profit": row.get("gross_profit"),
                 "operating_expense": row.get("operating_expense"),
                 "operating_income": row.get("operating_income"),
                 "net_income": row.get("net_income"),
                 "eps": row.get("eps"),
                 "eps_diluted": row.get("eps_diluted"),
                 "shares_outstanding": row.get("weighted_average_shares"),
                 "shares_outstanding_diluted": row.get("weighted_average_shares_diluted"),
                 "canonicalized_at": _ts_nanos(now),
             },
             at=report_date)

        _row(sender, "reconciliation_log",
             symbols={
                 "domain": "fundamentals",
                 "symbol": symbol,
                 "primary_vendor": vendor,
             },
             columns={
                 "fallback_used": False,
                 "validation_warnings": "[]",
                 "dagster_run_id": run_id,
                 "reconciled_at": _ts_nanos(now),
             },
             at=report_date)
        written += 1

    return written


# ---------------------------------------------------------------------------
# Financial metrics canonicalization (with forward-fill)
# ---------------------------------------------------------------------------


def _canonicalize_financial_metrics(
    conn, sender, config: CanonicalizeConfig, now: datetime, run_id: str, log
) -> int:
    """Read raw financial metrics, forward-fill to daily, write canonical.

    Forward-fill with as-of semantics: each metric observation carries forward
    to every subsequent trading day until a newer observation arrives.
    No lookahead — values are only available from their observation date onward.
    """
    where = _date_clause(config.start_date, config.end_date)
    query = f"SELECT * FROM raw_fd_financial_metrics{where} ORDER BY timestamp"

    cur = conn.cursor()
    cur.execute(query)
    columns = [desc[0] for desc in cur.description]
    raw_rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()

    if not raw_rows:
        log.info("financial_metrics: no raw rows found")
        return 0

    log.info("financial_metrics: read %d raw rows", len(raw_rows))

    # Parse timestamps
    for row in raw_rows:
        ts = row["timestamp"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        row["timestamp"] = ts

    # Group by symbol
    by_symbol: dict[str, list[dict]] = defaultdict(list)
    for row in raw_rows:
        by_symbol[row["symbol"]].append(row)

    metric_fields = [
        "market_cap", "pe_ratio", "ps_ratio", "pb_ratio", "ev_ebitda",
        "roe", "gross_margin", "operating_margin", "net_margin", "fcf_margin",
        "debt_to_equity", "revenue_growth_yoy", "eps_growth_yoy",
        "shares_outstanding",
    ]

    vendor = "financialdatasets"
    written = 0

    for symbol, rows in by_symbol.items():
        rows.sort(key=lambda r: r["timestamp"])

        if not rows:
            continue

        # Determine date range for forward-fill
        first_date = rows[0]["timestamp"].date()
        if config.end_date:
            last_date = datetime.fromisoformat(config.end_date).date()
        else:
            last_date = rows[-1]["timestamp"].date()

        # Build observation timeline: date → metric values
        observations: dict = {}
        for row in rows:
            d = row["timestamp"].date()
            observations[d] = row

        # Forward-fill: iterate day by day
        current_values: dict | None = None
        d = first_date
        while d <= last_date:
            if d in observations:
                current_values = observations[d]

            if current_values is not None:
                ts = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
                cols = {}
                for f in metric_fields:
                    cols[f] = current_values.get(f)

                cols["canonicalized_at"] = _ts_nanos(now)

                _row(sender, "canonical_financial_metrics",
                     symbols={
                         "symbol": symbol,
                         "source_vendor": vendor,
                     },
                     columns=cols,
                     at=ts)
                written += 1

            d += timedelta(days=1)

        # One reconciliation log entry per symbol
        _row(sender, "reconciliation_log",
             symbols={
                 "domain": "financial_metrics",
                 "symbol": symbol,
                 "primary_vendor": vendor,
             },
             columns={
                 "fallback_used": False,
                 "validation_warnings": "[]",
                 "dagster_run_id": run_id,
                 "reconciled_at": _ts_nanos(now),
             },
             at=datetime(first_date.year, first_date.month, first_date.day, tzinfo=timezone.utc))

    return written


# ---------------------------------------------------------------------------
# Domain dispatch
# ---------------------------------------------------------------------------

_DOMAIN_FNS = {
    "equity_ohlcv": _canonicalize_equity_ohlcv,
    "fundamentals": _canonicalize_fundamentals,
    "financial_metrics": _canonicalize_financial_metrics,
}


# ---------------------------------------------------------------------------
# Dagster op + job
# ---------------------------------------------------------------------------


@op
def canonicalize_op(context: OpExecutionContext, config: CanonicalizeConfig):
    """Read raw tables, validate, reconcile, write canonical + reconciliation log.

    After canonicalization, computes canonical hashes, detects changes,
    invalidates feature watermarks, and marks stale experiments (PRD §24.5).
    """
    from research.data.canonical_integrity import process_canonical_integrity

    qdb = QuestDBResource()
    now = datetime.now(timezone.utc)
    run_id = context.run_id

    conn = _pg_conn(qdb)
    conn.autocommit = True

    # Collect canonical rows for hash computation
    canonical_rows: dict[str, list[dict]] = {}

    try:
        with _ilp_sender(qdb) as sender:
            for domain in config.domains:
                fn = _DOMAIN_FNS.get(domain)
                if fn is None:
                    context.log.warning("Unknown domain: %s — skipping", domain)
                    continue

                context.log.info("Canonicalizing domain: %s", domain)
                if domain == "equity_ohlcv":
                    count = fn(
                        conn, sender, config, now, run_id, context.log,
                        canonical_rows_out=canonical_rows,
                    )
                else:
                    count = fn(conn, sender, config, now, run_id, context.log)
                context.log.info("Domain %s: %d canonical rows written", domain, count)

            sender.flush()

        # --- Canonical integrity check (PRD §24.5) ---
        if canonical_rows:
            context.log.info("Running canonical integrity checks...")

            # Determine date range
            date_from = datetime(2020, 1, 1, tzinfo=timezone.utc)
            date_to = now
            if config.start_date:
                date_from = datetime.fromisoformat(config.start_date).replace(tzinfo=timezone.utc)
            if config.end_date:
                date_to = datetime.fromisoformat(config.end_date).replace(tzinfo=timezone.utc)

            with _ilp_sender(qdb) as sender:
                summary = process_canonical_integrity(
                    conn, sender,
                    canonical_rows_by_symbol=canonical_rows,
                    date_from=date_from,
                    date_to=date_to,
                    run_id=run_id,
                    now=now,
                    ts_nanos_fn=_ts_nanos,
                    log=context.log,
                )
                sender.flush()

            context.log.info(
                "Canonical integrity: %d symbols hashed, %d changed, "
                "%d stale experiments",
                summary["symbols_hashed"],
                summary["symbols_changed"],
                summary["stale_experiments"],
            )
    finally:
        conn.close()

    context.log.info("Canonicalization complete")


@job
def canonicalize():
    """Dagster job: canonicalize raw data into canonical tables with reconciliation."""
    canonicalize_op()
