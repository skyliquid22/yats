"""Dagster ingest job — ThetaData options chains and EOD data to QuestDB."""

import logging
from datetime import datetime, timedelta, timezone

import psycopg2
from dagster import Config, OpExecutionContext, job, op
from questdb.ingress import Protocol, Sender, TimestampNanos

from yats_pipelines.resources.questdb import QuestDBResource
from yats_pipelines.resources.thetadata import ThetaDataResource
from yats_pipelines.utils.run_recorder import record_finish, record_start
from yats_pipelines.utils.schema_guard import assert_table_schema

logger = logging.getLogger(__name__)

# IV values above this threshold are rejected as unreliable data
IV_MAX = 20.0

# Columns that must exist in each target table (designated-timestamp name is critical)
_CHAIN_REQUIRED_COLS = ["quote_ts", "underlying", "strike", "right"]
_EOD_REQUIRED_COLS = ["quote_date", "underlying", "strike", "right"]
_CANONICAL_REQUIRED_COLS = ["quote_date", "underlying", "strike", "right"]


class IngestThetadataConfig(Config):
    """Run config for the ThetaData ingest job."""

    underlyings: list[str]
    expiry_window_days: int = 90  # fetch chains for expirations within this many days
    start_date: str = ""  # YYYYMMDD for historical EOD fetch (empty = skip EOD)
    end_date: str = ""    # YYYYMMDD for historical EOD fetch
    # By-date bulk EOD mode: one expiration=* request per trading day instead of
    # one request per expiration-history. The vendor serves the by-date shape in
    # seconds; per-expiry history slices cold-fetch for minutes each and do not
    # scale to multi-year backfills. Filters are applied SERVER-side.
    eod_by_date: bool = False
    eod_max_dte: int = 0        # drop contracts with more days-to-expiry (0 = off)
    eod_strike_range: int = 0   # at most 2n+1 strikes around spot (0 = off)


# ---------------------------------------------------------------------------
# Pure helpers (exported for testing)
# ---------------------------------------------------------------------------


def _validate_contract(row: dict) -> list[str]:
    """Validate a chain snapshot row. Returns list of rejection reasons."""
    issues = []
    if row.get("strike") is None:
        issues.append("missing_strike")
    oi = row.get("open_interest")
    if oi is not None and oi < 0:
        issues.append(f"negative_open_interest={oi}")
    iv = row.get("iv")
    if iv is not None and (iv < 0 or iv > IV_MAX):
        issues.append(f"iv_outlier={iv:.4f}")
    return issues


def _normalize_right(val: str) -> str:
    """Normalize ThetaData v3 right values to canonical single-char form.

    v3 API returns "CALL" / "PUT"; canonical table and feature code expect "C" / "P".
    """
    if val in ("CALL", "CALLS"):
        return "C"
    if val in ("PUT", "PUTS"):
        return "P"
    return val


# Fields populated from the second-order greeks endpoint (PRO subscription only).
# When a later ingest run cannot reach that endpoint, these fields come back as None.
# Preserve the most-recent non-None value so a transient endpoint outage doesn't
# silently zero-out data that was fetched in an earlier run.
_PRESERVE_IF_NONE = frozenset({"gamma"})


def _pick_latest_per_contract(rows: list[dict]) -> list[dict]:
    """Group rows by contract+date key and keep the latest ingested per group.

    Latest is determined by ingested_at. Used to implement latest-quote-wins
    semantics for canonicalization.

    For fields in _PRESERVE_IF_NONE (e.g. gamma from PRO second-order greeks):
    if the newest row has None but an earlier row had a value, keep the earlier value.
    """
    by_key: dict[tuple, dict] = {}
    for row in rows:
        qt = row.get("quote_ts")
        if qt is None:
            continue
        quote_date = qt.date() if hasattr(qt, "date") else None
        if quote_date is None:
            continue
        key = (
            row.get("underlying", ""),
            str(row.get("expiry", "")),
            row.get("strike"),
            row.get("right", ""),
            quote_date,
        )
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = row
        elif _ingested_after(row, existing):
            merged = dict(row)
            for field in _PRESERVE_IF_NONE:
                if merged.get(field) is None and existing.get(field) is not None:
                    merged[field] = existing[field]
            by_key[key] = merged
    return list(by_key.values())


def _ingested_after(a: dict, b: dict) -> bool:
    """Return True if row a was ingested strictly after row b."""
    ia = a.get("ingested_at")
    ib = b.get("ingested_at")
    if ia is None:
        return False
    if ib is None:
        return True
    return ia > ib


def _parse_exp_to_datetime(exp: str) -> datetime | None:
    """Convert YYYYMMDD expiry string to UTC midnight datetime."""
    if not exp or len(exp) != 8:
        return None
    try:
        return datetime(int(exp[:4]), int(exp[4:6]), int(exp[6:8]), tzinfo=timezone.utc)
    except ValueError:
        return None


def _run_already_written(conn, run_id: str, table: str) -> bool:
    """Return True if table already has rows for this dagster_run_id."""
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"SELECT count(*) FROM {table} WHERE dagster_run_id = %s LIMIT 1",
                (run_id,),
            )
            row = cur.fetchone()
            return bool(row and row[0] > 0)
        finally:
            cur.close()
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Infrastructure helpers
# ---------------------------------------------------------------------------


def _ts_nanos(dt: datetime) -> TimestampNanos:
    return TimestampNanos(int(dt.timestamp() * 1_000_000_000))


def _ilp_sender(qdb: QuestDBResource):
    return Sender(Protocol.Tcp, qdb.ilp_host, qdb.ilp_port)


def _pg_conn(qdb: QuestDBResource):
    return psycopg2.connect(
        host=qdb.pg_host,
        port=qdb.pg_port,
        user=qdb.pg_user,
        password=qdb.pg_password,
        database=qdb.pg_database,
    )


# ---------------------------------------------------------------------------
# Ops
# ---------------------------------------------------------------------------


@op
def fetch_thetadata_options(
    context: OpExecutionContext, config: IngestThetadataConfig
) -> dict:
    """Fetch option chain snapshots (and optional EOD data) from ThetaData API."""
    detail = f"{','.join(config.underlyings[:5])} eod={bool(config.start_date)}"
    record_start("ingest_thetadata", context.run_id, detail)
    try:
        td = ThetaDataResource()

        now = datetime.now(timezone.utc)
        today = now.date()
        cutoff = today + timedelta(days=config.expiry_window_days)
        today_str = today.strftime("%Y%m%d")
        cutoff_str = cutoff.strftime("%Y%m%d")

        chain_rows: list[dict] = []
        eod_rows: list[dict] = []

        start_ymd = config.start_date.replace("-", "") if config.start_date else ""
        end_ymd = config.end_date.replace("-", "") if config.end_date else ""

        for underlying in config.underlyings:
            context.log.info("Fetching expirations for %s", underlying)
            try:
                exps = td.list_expirations(underlying)
            except Exception as exc:
                context.log.warning(
                    "Failed to list expirations for %s: %s", underlying, exc
                )
                continue

            # Chain snapshots: upcoming expirations only
            relevant_exps = [e for e in exps if today_str <= e <= cutoff_str]
            context.log.info(
                "%s: %d expirations within %d-day window",
                underlying,
                len(relevant_exps),
                config.expiry_window_days,
            )

            chain_count = 0
            for exp in relevant_exps:
                try:
                    raw_chain = td.get_option_chain_snapshot(underlying, exp)
                    chain_rows.extend(td.normalize_chain_snapshot(raw_chain, now, ""))
                    chain_count += len(raw_chain)
                except Exception as exc:
                    context.log.warning(
                        "Chain snapshot failed for %s exp=%s: %s", underlying, exp, exc
                    )
            context.log.info(
                "%s: fetched %d chain rows across %d expirations",
                underlying, chain_count, len(relevant_exps),
            )

            # EOD historical backfill: use ALL expirations active during [start_ymd, end_ymd].
            # relevant_exps only contains upcoming expirations — historical expirations (which
            # have the actual EOD data) are filtered out by the today_str <= e cutoff. The EOD
            # loop must use its own expiration set derived from start_ymd.
            if start_ymd and end_ymd and config.eod_by_date:
                # Bulk by-date mode: one expiration=* request per weekday.
                # Non-trading days return 472/empty and are skipped naturally.
                eod_count = 0
                day = datetime.strptime(start_ymd, "%Y%m%d")
                end_day = datetime.strptime(end_ymd, "%Y%m%d")
                n_days = 0
                while day <= end_day:
                    if day.weekday() < 5:  # skip Sat/Sun
                        n_days += 1
                        date_str = day.strftime("%Y%m%d")
                        try:
                            raw_eod = td.get_historical_eod_by_date(
                                underlying, date_str,
                                max_dte=config.eod_max_dte,
                                strike_range=config.eod_strike_range,
                            )
                            eod_rows.extend(td.normalize_eod(raw_eod, now, ""))
                            eod_count += len(raw_eod)
                        except Exception as exc:
                            context.log.warning(
                                "EOD by-date fetch failed for %s %s: %s",
                                underlying, date_str, exc,
                            )
                    day += timedelta(days=1)
                context.log.info(
                    "%s: EOD by-date backfill fetched %d rows across %d weekdays (%s to %s)",
                    underlying, eod_count, n_days, start_ymd, end_ymd,
                )
            elif start_ymd and end_ymd:
                eod_exps = [e for e in exps if e >= start_ymd]
                eod_count = 0
                for exp in eod_exps:
                    try:
                        raw_eod = td.get_historical_eod(underlying, exp, start_ymd, end_ymd)
                        eod_rows.extend(td.normalize_eod(raw_eod, now, ""))
                        eod_count += len(raw_eod)
                    except Exception as exc:
                        context.log.warning(
                            "EOD fetch failed for %s exp=%s: %s", underlying, exp, exc
                        )
                if eod_count == 0:
                    context.log.warning(
                        "%s: EOD backfill (%s to %s) queried %d expirations but fetched 0 rows"
                        " — verify terminal subscription covers historical options data",
                        underlying, start_ymd, end_ymd, len(eod_exps),
                    )
                else:
                    context.log.info(
                        "%s: EOD backfill fetched %d rows across %d expirations (%s to %s)",
                        underlying, eod_count, len(eod_exps), start_ymd, end_ymd,
                    )

            context.log.info(
                "Fetched %d chain rows, %d EOD rows for %d underlyings",
                len(chain_rows),
                len(eod_rows),
                len(config.underlyings),
            )
            return {"chain_rows": chain_rows, "eod_rows": eod_rows}
    except Exception as exc:
        record_finish("ingest_thetadata", context.run_id, "failed", failure_cause=str(exc)[:200])
        raise


@op
def write_raw_thetadata(
    context: OpExecutionContext, fetch_result: dict
) -> dict:
    """Write raw ThetaData option data to QuestDB with dedup-before-write."""
    chain_rows = fetch_result["chain_rows"]
    eod_rows = fetch_result["eod_rows"]
    run_id = context.run_id

    # Stamp final run_id onto all rows
    for row in chain_rows:
        row["dagster_run_id"] = run_id
    for row in eod_rows:
        row["dagster_run_id"] = run_id

    qdb = QuestDBResource()
    conn = _pg_conn(qdb)
    conn.autocommit = True

    chain_written = 0
    eod_written = 0

    try:
        if chain_rows:
            assert_table_schema(conn, "raw_thetadata_options_chain", _CHAIN_REQUIRED_COLS)
        if eod_rows:
            assert_table_schema(conn, "raw_thetadata_options_eod", _EOD_REQUIRED_COLS)

        # --- Chain snapshot ---
        if chain_rows and _run_already_written(conn, run_id, "raw_thetadata_options_chain"):
            context.log.info(
                "raw_thetadata_options_chain: run %s already written — skipping (idempotent)",
                run_id,
            )
        elif chain_rows:
            skipped_chain = 0
            with _ilp_sender(qdb) as sender:
                for row in chain_rows:
                    quote_ts = row.get("quote_ts")
                    if quote_ts is None:
                        skipped_chain += 1
                        continue
                    ingested_at = row["ingested_at"]
                    expiry_dt = _parse_exp_to_datetime(row.get("exp", ""))

                    cols = {
                        "strike": row.get("strike"),
                        "bid": row.get("bid"),
                        "ask": row.get("ask"),
                        "last": row.get("last"),
                        "iv": row.get("iv"),
                        "delta": row.get("delta"),
                        "gamma": row.get("gamma"),
                        "theta": row.get("theta"),
                        "vega": row.get("vega"),
                        "rho": row.get("rho"),
                        "open_interest": row.get("open_interest"),
                        "volume": row.get("volume"),
                        "ingested_at": _ts_nanos(ingested_at),
                        "dagster_run_id": run_id,
                    }
                    if expiry_dt is not None:
                        cols["expiry"] = _ts_nanos(expiry_dt)

                    sender.row(
                        "raw_thetadata_options_chain",
                        symbols={
                            "underlying": row["root"],
                            "right": row.get("right", ""),
                        },
                        columns={k: v for k, v in cols.items() if v is not None},
                        at=_ts_nanos(quote_ts),
                    )
                sender.flush()
            if skipped_chain:
                context.log.warning(
                    "Skipped %d/%d chain rows with missing quote_ts "
                    "(timestamp field missing or unparseable)",
                    skipped_chain, len(chain_rows),
                )
            chain_written = len(chain_rows) - skipped_chain
            context.log.info(
                "Wrote %d rows to raw_thetadata_options_chain", chain_written
            )

        # --- Historical EOD ---
        if eod_rows and _run_already_written(conn, run_id, "raw_thetadata_options_eod"):
            context.log.info(
                "raw_thetadata_options_eod: run %s already written — skipping (idempotent)",
                run_id,
            )
        elif eod_rows:
            skipped_eod = 0
            with _ilp_sender(qdb) as sender:
                for row in eod_rows:
                    quote_date = row.get("quote_date")
                    if quote_date is None:
                        skipped_eod += 1
                        continue
                    ingested_at = row["ingested_at"]
                    expiry_dt = _parse_exp_to_datetime(row.get("exp", ""))

                    cols = {
                        "strike": row.get("strike"),
                        "open": row.get("open"),
                        "high": row.get("high"),
                        "low": row.get("low"),
                        "close": row.get("close"),
                        "volume": row.get("volume"),
                        "trade_count": row.get("trade_count"),
                        "bid": row.get("bid"),
                        "ask": row.get("ask"),
                        "iv": row.get("iv"),
                        "delta": row.get("delta"),
                        "gamma": row.get("gamma"),
                        "theta": row.get("theta"),
                        "vega": row.get("vega"),
                        "rho": row.get("rho"),
                        "open_interest": row.get("open_interest"),
                        "ingested_at": _ts_nanos(ingested_at),
                        "dagster_run_id": run_id,
                    }
                    if expiry_dt is not None:
                        cols["expiry"] = _ts_nanos(expiry_dt)

                    sender.row(
                        "raw_thetadata_options_eod",
                        symbols={
                            "underlying": row["root"],
                            "right": _normalize_right(row.get("right", "")),
                        },
                        columns={k: v for k, v in cols.items() if v is not None},
                        at=_ts_nanos(quote_date),
                    )
                sender.flush()
            if skipped_eod:
                context.log.warning(
                    "Skipped %d/%d EOD rows with missing quote_date"
                    " (last_trade field missing or unparseable)",
                    skipped_eod, len(eod_rows),
                )
            eod_written = len(eod_rows) - skipped_eod
            context.log.info(
                "Wrote %d rows to raw_thetadata_options_eod", eod_written
            )

    except Exception as exc:
        record_finish("ingest_thetadata", context.run_id, "failed", failure_cause=str(exc)[:200])
        raise
    finally:
        conn.close()

    return {"run_id": run_id, "chain_count": chain_written, "eod_count": eod_written}


@op
def canonicalize_options(
    context: OpExecutionContext, write_result: dict
) -> None:
    """Canonicalize options chain: latest-quote-wins per contract per day.

    Reads all raw_thetadata_options_chain rows, groups by
    (underlying, expiry, strike, right, quote_date), takes the latest-ingested
    row per group, validates, and writes to canonical_options_chain.
    """
    run_id = write_result["run_id"]
    qdb = QuestDBResource()
    now = datetime.now(timezone.utc)

    conn = _pg_conn(qdb)
    conn.autocommit = True

    _exc: Exception | None = None
    _written = 0
    try:
        assert_table_schema(conn, "canonical_options_chain", _CANONICAL_REQUIRED_COLS)

        if _run_already_written(conn, run_id, "canonical_options_chain"):
            context.log.info(
                "canonical_options_chain: run %s already written — skipping (idempotent)",
                run_id,
            )
            return

        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM raw_thetadata_options_chain ORDER BY quote_ts"
        )
        columns = [desc[0] for desc in cur.description]
        raw_rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        cur.close()

        if not raw_rows:
            context.log.info("No raw options chain rows found — skipping canonicalize")
            return

        context.log.info("Read %d raw chain rows for canonicalization", len(raw_rows))

        canonical_rows = _pick_latest_per_contract(raw_rows)
        rejected = 0

        with _ilp_sender(qdb) as sender:
            for row in canonical_rows:
                issues = _validate_contract(row)
                if issues:
                    rejected += 1
                    context.log.debug(
                        "Rejected contract %s/%s/%s/%s: %s",
                        row.get("underlying"),
                        row.get("expiry"),
                        row.get("strike"),
                        row.get("right"),
                        issues,
                    )
                    continue

                qt = row.get("quote_ts")
                quote_date_dt = datetime(
                    qt.date().year, qt.date().month, qt.date().day,
                    tzinfo=timezone.utc,
                ) if qt is not None and hasattr(qt, "date") else now

                expiry_raw = row.get("expiry")
                expiry_dt = (
                    expiry_raw
                    if isinstance(expiry_raw, datetime)
                    else None
                )

                cols = {
                    "strike": row.get("strike"),
                    "bid": row.get("bid"),
                    "ask": row.get("ask"),
                    "last": row.get("last"),
                    "iv": row.get("iv"),
                    "delta": row.get("delta"),
                    "gamma": row.get("gamma"),
                    "theta": row.get("theta"),
                    "vega": row.get("vega"),
                    "rho": row.get("rho"),
                    "open_interest": row.get("open_interest"),
                    "volume": row.get("volume"),
                    "canonicalized_at": _ts_nanos(now),
                    "dagster_run_id": run_id,
                }
                if expiry_dt is not None:
                    cols["expiry"] = _ts_nanos(expiry_dt)

                sender.row(
                    "canonical_options_chain",
                    symbols={
                        "underlying": row.get("underlying", ""),
                        "right": _normalize_right(row.get("right", "")),
                        "source_vendor": "thetadata",
                        "reconcile_method": "latest_quote_wins",
                    },
                    columns={k: v for k, v in cols.items() if v is not None},
                    at=_ts_nanos(quote_date_dt),
                )
                _written += 1

            sender.flush()

        context.log.info(
            "Canonicalized %d contracts, rejected %d", _written, rejected
        )
    except Exception as exc:
        _exc = exc
        raise
    finally:
        conn.close()
        record_finish(
            "ingest_thetadata", context.run_id,
            "failed" if _exc else "success",
            rows_written=None if _exc else _written,
            failure_cause=str(_exc)[:200] if _exc else None,
        )


# ---------------------------------------------------------------------------
# Job definition
# ---------------------------------------------------------------------------


@job(tags={"yats/concurrency_pool": "ingest", "dagster/priority": "10"})
def ingest_thetadata():
    """Dagster job: ingest ThetaData options chains into QuestDB and canonicalize."""
    raw = write_raw_thetadata(fetch_thetadata_options())
    canonicalize_options(raw)
