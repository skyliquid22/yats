"""Canonical data integrity — hash computation, feature invalidation, staleness detection.

Implements:
- Canonical hash computation (per-symbol + universe-level) — PRD §24.5 lines 2227-2241
- Feature watermark invalidation on canonical change — PRD §24.5 lines 2242-2254
- Experiment staleness detection + alerts — PRD §24.5 lines 2256-2272
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Metadata fields excluded from hash computation — only data content matters
_METADATA_FIELDS = frozenset({
    "canonicalized_at", "source_vendor", "reconcile_method",
    "validation_status", "dagster_run_id",
})

# Sentinel symbols for watermark table
_CS_WATERMARK_SYMBOL = "__universe__"
_REGIME_WATERMARK_SYMBOL = "__regime__"


# ---------------------------------------------------------------------------
# Hash computation
# ---------------------------------------------------------------------------


def compute_symbol_hash(rows: list[dict]) -> str:
    """Compute SHA256 of canonical data rows for one symbol.

    Rows sorted by timestamp for determinism. Metadata fields excluded.
    """
    if not rows:
        return hashlib.sha256(b"[]").hexdigest()

    sorted_rows = sorted(rows, key=lambda r: str(r.get("timestamp", "")))
    canonical = json.dumps(
        [_normalize_row(r) for r in sorted_rows],
        sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def compute_universe_date_hash(all_rows_on_date: list[dict]) -> str:
    """Compute SHA256 of ALL symbols' canonical data on one date.

    Used for cross-sectional feature invalidation: if ANY symbol's data
    changes on a date, the hash changes.
    """
    if not all_rows_on_date:
        return hashlib.sha256(b"[]").hexdigest()

    sorted_rows = sorted(
        all_rows_on_date,
        key=lambda r: (r.get("symbol", ""), str(r.get("timestamp", ""))),
    )
    canonical = json.dumps(
        [_normalize_row(r) for r in sorted_rows],
        sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _normalize_row(row: dict) -> dict:
    """Normalize a canonical row for deterministic hashing."""
    result = {}
    for k in sorted(row.keys()):
        if k in _METADATA_FIELDS:
            continue
        val = row[k]
        if isinstance(val, datetime):
            result[k] = val.isoformat()
        elif isinstance(val, float):
            result[k] = round(val, 10)
        else:
            result[k] = val
    return result


# ---------------------------------------------------------------------------
# Hash storage — read/write canonical_hashes table
# ---------------------------------------------------------------------------


def read_stored_symbol_hashes(conn) -> dict[str, str]:
    """Read latest canonical_hash per symbol from canonical_hashes table.

    Returns {symbol: canonical_hash}.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT symbol, canonical_hash "
        "FROM canonical_hashes "
        "WHERE symbol != '__universe__' "
        "LATEST ON timestamp PARTITION BY symbol"
    )
    rows = cur.fetchall()
    cur.close()
    return {sym: h for sym, h in rows if h is not None}


def read_stored_universe_hash(conn) -> str | None:
    """Read the latest overall universe hash.

    The universe hash is stored with symbol='__universe__'.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT universe_date_hash "
        "FROM canonical_hashes "
        "WHERE symbol = '__universe__' "
        "ORDER BY timestamp DESC "
        "LIMIT 1"
    )
    row = cur.fetchone()
    cur.close()
    if row and row[0]:
        return row[0]
    return None


def write_canonical_hashes(
    sender,
    symbol_hashes: dict[str, str],
    universe_hash: str,
    date_from: datetime,
    date_to: datetime,
    run_id: str,
    now: datetime,
    ts_nanos_fn,
) -> None:
    """Write per-symbol + universe hashes to canonical_hashes table via ILP."""
    for symbol, h in symbol_hashes.items():
        sender.row(
            "canonical_hashes",
            symbols={"symbol": symbol},
            columns={
                "date_from": ts_nanos_fn(date_from),
                "date_to": ts_nanos_fn(date_to),
                "canonical_hash": h,
                "dagster_run_id": run_id,
            },
            at=ts_nanos_fn(now),
        )

    # Universe-level hash
    sender.row(
        "canonical_hashes",
        symbols={"symbol": "__universe__"},
        columns={
            "date_from": ts_nanos_fn(date_from),
            "date_to": ts_nanos_fn(date_to),
            "universe_date_hash": universe_hash,
            "dagster_run_id": run_id,
        },
        at=ts_nanos_fn(now),
    )


# ---------------------------------------------------------------------------
# Feature watermark invalidation
# ---------------------------------------------------------------------------


def invalidate_feature_watermarks(
    sender,
    changed_symbols: list[str],
    cross_sectional_changed: bool,
    feature_set: str,
    feature_set_version: str,
    ts_nanos_fn,
    now: datetime,
) -> None:
    """Invalidate feature watermarks for symbols whose canonical data changed.

    Per-symbol changes: Reset that symbol's watermark to epoch, forcing
    full recompute on next incremental pipeline run.

    Cross-sectional changes: Reset __universe__ and __regime__ watermarks,
    forcing full recompute of cross-sectional and regime features.
    """
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)

    for symbol in changed_symbols:
        sender.row(
            "feature_watermarks",
            symbols={
                "symbol": symbol,
                "feature_set": feature_set,
                "feature_set_version": feature_set_version,
            },
            columns={"last_computed_date": ts_nanos_fn(epoch)},
            at=ts_nanos_fn(now),
        )
        logger.info("Invalidated watermark for symbol=%s feature_set=%s", symbol, feature_set)

    if cross_sectional_changed:
        for sentinel in (_CS_WATERMARK_SYMBOL, _REGIME_WATERMARK_SYMBOL):
            sender.row(
                "feature_watermarks",
                symbols={
                    "symbol": sentinel,
                    "feature_set": feature_set,
                    "feature_set_version": feature_set_version,
                },
                columns={"last_computed_date": ts_nanos_fn(epoch)},
                at=ts_nanos_fn(now),
            )
        logger.info(
            "Invalidated cross-sectional + regime watermarks for feature_set=%s",
            feature_set,
        )


# ---------------------------------------------------------------------------
# Experiment staleness detection
# ---------------------------------------------------------------------------


def detect_stale_experiments(
    conn,
    old_hashes: dict[str, str],
    new_hashes: dict[str, str],
) -> list[dict[str, Any]]:
    """Find experiments whose canonical_hash matches an OLD hash that changed.

    Returns list of dicts with experiment_id, promotion_tier, canonical_hash.
    """
    # Find hashes that changed
    changed_hashes: set[str] = set()
    for symbol, old_hash in old_hashes.items():
        new_hash = new_hashes.get(symbol)
        if new_hash is not None and new_hash != old_hash:
            changed_hashes.add(old_hash)

    if not changed_hashes:
        return []

    # Query experiment_index for experiments with any of the old hashes.
    # canonical_hash is stored as a STRING column in experiment_index.
    placeholders = ",".join(f"'{h}'" for h in changed_hashes)
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT experiment_id, promotion_tier, canonical_hash "
            f"FROM experiment_index "
            f"WHERE canonical_hash IN ({placeholders})"
        )
        rows = cur.fetchall()
    except Exception:
        # canonical_hash column may not exist yet in older schemas
        logger.debug("canonical_hash column not found in experiment_index, no stale experiments")
        return []
    finally:
        cur.close()

    results = []
    seen = set()
    for exp_id, tier, ch in rows:
        if exp_id not in seen:
            seen.add(exp_id)
            results.append({
                "experiment_id": exp_id,
                "promotion_tier": tier,
                "canonical_hash": ch,
            })

    return results


def mark_experiments_stale(
    sender,
    stale_experiments: list[dict[str, Any]],
    ts_nanos_fn,
    now: datetime,
) -> None:
    """Write stale=true rows to experiment_index for affected experiments.

    Also writes audit_trail entries for alerts on promoted experiments.
    """
    for exp in stale_experiments:
        exp_id = exp["experiment_id"]
        tier = exp.get("promotion_tier")

        # Write experiment_index row marking stale
        sender.row(
            "experiment_index",
            symbols={"experiment_id": exp_id},
            columns={"stale": True},
            at=ts_nanos_fn(now),
        )
        logger.info("Marked experiment %s as stale", exp_id)

        # Emit alerts for promoted experiments (PRD lines 2264-2268)
        if tier and tier != "research":
            alert_mode = "staleness_alert"
            detail = (
                f"Experiment {exp_id} at tier={tier} is stale: "
                f"canonical data changed since evaluation"
            )
            sender.row(
                "audit_trail",
                symbols={
                    "tool_name": "canonical_integrity",
                    "invoker": "canonicalize",
                    "experiment_id": exp_id,
                    "mode": alert_mode,
                    "result_status": "alert",
                },
                columns={
                    "result_summary": detail,
                    "parameters": json.dumps({
                        "promotion_tier": tier,
                        "old_canonical_hash": exp.get("canonical_hash"),
                    }),
                    "duration_ms": 0,
                },
                at=ts_nanos_fn(now),
            )
            logger.warning(
                "STALENESS ALERT: experiment %s at tier=%s — canonical data changed",
                exp_id, tier,
            )


# ---------------------------------------------------------------------------
# Orchestrator — called from canonicalize job
# ---------------------------------------------------------------------------


def process_canonical_integrity(
    conn,
    sender,
    canonical_rows_by_symbol: dict[str, list[dict]],
    date_from: datetime,
    date_to: datetime,
    run_id: str,
    now: datetime,
    ts_nanos_fn,
    feature_set: str = "core_v1",
    feature_set_version: str = "1.0",
    log: Any = None,
) -> dict[str, Any]:
    """Full canonical integrity pipeline — called after canonicalization.

    1. Compute per-symbol + universe hashes
    2. Compare with stored hashes
    3. Write new hashes
    4. Invalidate feature watermarks for changed symbols
    5. Detect and mark stale experiments
    6. Emit alerts for promoted stale experiments

    Returns summary dict.
    """
    log = log or logger

    # 1. Compute per-symbol hashes
    new_symbol_hashes: dict[str, str] = {}
    for symbol, rows in canonical_rows_by_symbol.items():
        new_symbol_hashes[symbol] = compute_symbol_hash(rows)

    # Compute universe hash (hash of ALL rows across ALL symbols)
    all_rows = []
    for rows in canonical_rows_by_symbol.values():
        all_rows.extend(rows)
    new_universe_hash = compute_universe_date_hash(all_rows)

    log.info(
        "Computed hashes: %d symbols, universe_hash=%s…",
        len(new_symbol_hashes), new_universe_hash[:12],
    )

    # 2. Read stored hashes
    old_symbol_hashes = read_stored_symbol_hashes(conn)
    old_universe_hash = read_stored_universe_hash(conn)

    # 3. Identify changes
    changed_symbols = []
    for symbol, new_hash in new_symbol_hashes.items():
        old_hash = old_symbol_hashes.get(symbol)
        if old_hash is not None and old_hash != new_hash:
            changed_symbols.append(symbol)

    cross_sectional_changed = (
        old_universe_hash is not None
        and old_universe_hash != new_universe_hash
    )

    log.info(
        "Hash changes: %d symbols changed, cross-sectional=%s",
        len(changed_symbols), cross_sectional_changed,
    )

    # 4. Write new hashes
    write_canonical_hashes(
        sender, new_symbol_hashes, new_universe_hash,
        date_from, date_to, run_id, now, ts_nanos_fn,
    )

    # 5. Invalidate feature watermarks
    if changed_symbols or cross_sectional_changed:
        invalidate_feature_watermarks(
            sender, changed_symbols, cross_sectional_changed,
            feature_set, feature_set_version, ts_nanos_fn, now,
        )

    # 6. Detect and mark stale experiments
    stale_experiments = detect_stale_experiments(
        conn, old_symbol_hashes, new_symbol_hashes,
    )
    if stale_experiments:
        mark_experiments_stale(sender, stale_experiments, ts_nanos_fn, now)
        log.warning(
            "Marked %d experiments as stale: %s",
            len(stale_experiments),
            [e["experiment_id"] for e in stale_experiments],
        )

    return {
        "symbols_hashed": len(new_symbol_hashes),
        "symbols_changed": len(changed_symbols),
        "changed_symbols": changed_symbols,
        "cross_sectional_changed": cross_sectional_changed,
        "universe_hash": new_universe_hash,
        "stale_experiments": len(stale_experiments),
    }


# ---------------------------------------------------------------------------
# Qualification helper — canonical hash comparison
# ---------------------------------------------------------------------------


def get_current_canonical_hash(conn) -> str | None:
    """Get the current overall universe canonical hash."""
    return read_stored_universe_hash(conn)


def check_data_drift(
    candidate_hash: str | None,
    baseline_hash: str | None,
) -> bool:
    """Return True if candidate and baseline used different canonical data.

    PRD §24.3: if candidate.canonical_hash != baseline.canonical_hash → data_drift.
    """
    if candidate_hash is None or baseline_hash is None:
        return False
    return candidate_hash != baseline_hash


def is_experiment_stale(conn, experiment_id: str) -> bool:
    """Check if an experiment is marked stale in experiment_index.

    Queries the latest row for this experiment_id.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT stale FROM experiment_index "
            "WHERE experiment_id = %s "
            "ORDER BY created_at DESC "
            "LIMIT 1",
            (experiment_id,),
        )
        row = cur.fetchone()
        if row and row[0]:
            return True
        return False
    except Exception:
        logger.debug("stale column not found in experiment_index")
        return False
    finally:
        cur.close()
