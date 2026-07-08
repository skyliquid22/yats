"""Best-effort job run recorder — writes start/finish rows to job_runs via ILP.

Never raises: instrumentation must not break the jobs it monitors.
Uses a module-level dict keyed by (job_name, run_id) to carry started_at
from record_start to record_finish so the DEDUP upsert key is stable.
"""

from __future__ import annotations

import logging
import psycopg2
from datetime import datetime, timezone

from questdb.ingress import Protocol, Sender, TimestampNanos

from yats_pipelines.resources.questdb import QuestDBResource

logger = logging.getLogger(__name__)

# (job_name, run_id) -> started_at datetime; cleared by record_finish
_starts: dict[tuple[str, str], datetime] = {}


def _ts_nanos(dt: datetime) -> TimestampNanos:
    return TimestampNanos(int(dt.timestamp() * 1_000_000_000))


def record_start(job_name: str, run_id: str, detail: str) -> None:
    """Write a job_runs row with status='running'. Never raises."""
    started_at = datetime.now(timezone.utc)
    _starts[(job_name, run_id)] = started_at
    try:
        qdb = QuestDBResource()
        with Sender(Protocol.Tcp, qdb.ilp_host, qdb.ilp_port) as sender:
            sender.row(
                "job_runs",
                symbols={"job_name": job_name, "dagster_run_id": run_id, "status": "running"},
                columns={"detail": detail},
                at=_ts_nanos(started_at),
            )
            sender.flush()
    except Exception:
        logger.warning("record_start failed for %s run=%s", job_name, run_id, exc_info=True)


def record_finish(
    job_name: str,
    run_id: str,
    status: str,
    rows_written: int | None = None,
    failure_cause: str | None = None,
) -> None:
    """Upsert the job_runs row with final status, duration, and optional rows/cause. Never raises."""
    started_at = _starts.pop((job_name, run_id), None)
    finished_at = datetime.now(timezone.utc)
    try:
        if started_at is None:
            logger.warning(
                "record_finish called for %s run=%s but no matching record_start",
                job_name, run_id,
            )
            return
        duration_s = (finished_at - started_at).total_seconds()
        cols: dict = {
            "finished_at": _ts_nanos(finished_at),
            "duration_s": duration_s,
        }
        if rows_written is not None:
            cols["rows_written"] = int(rows_written)
        if failure_cause is not None:
            cols["failure_cause"] = str(failure_cause)[:200]
        qdb = QuestDBResource()
        with Sender(Protocol.Tcp, qdb.ilp_host, qdb.ilp_port) as sender:
            sender.row(
                "job_runs",
                symbols={"job_name": job_name, "dagster_run_id": run_id, "status": status},
                columns=cols,
                at=_ts_nanos(started_at),
            )
            sender.flush()
    except Exception:
        logger.warning("record_finish failed for %s run=%s", job_name, run_id, exc_info=True)


def backfill_from_reconciliation_log(qdb: QuestDBResource | None = None) -> int:
    """Reconstruct job_runs rows from reconciliation_log for historical canonicalize runs.

    Uses min(reconciled_at) as started_at, max(reconciled_at) as finished_at,
    count(*) as rows_written, status='success', detail='backfilled'.
    Returns the number of rows written.
    """
    if qdb is None:
        qdb = QuestDBResource()
    conn = psycopg2.connect(
        host=qdb.pg_host,
        port=qdb.pg_port,
        user=qdb.pg_user,
        password=qdb.pg_password,
        database=qdb.pg_database,
    )
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT dagster_run_id, "
            "       min(reconciled_at) AS started_at, "
            "       max(reconciled_at) AS finished_at, "
            "       count(*) AS rows_written "
            "FROM reconciliation_log "
            "WHERE dagster_run_id IS NOT NULL AND dagster_run_id != '' "
            "GROUP BY dagster_run_id"
        )
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    if not rows:
        return 0

    written = 0
    qdb2 = QuestDBResource()
    with Sender(Protocol.Tcp, qdb2.ilp_host, qdb2.ilp_port) as sender:
        for (run_id, started_at, finished_at, rows_written) in rows:
            if started_at is None:
                continue
            if isinstance(started_at, str):
                started_at = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
            if isinstance(finished_at, str):
                finished_at = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
            sender.row(
                "job_runs",
                symbols={
                    "job_name": "canonicalize",
                    "dagster_run_id": str(run_id),
                    "status": "success",
                },
                columns={
                    "detail": "backfilled",
                    "finished_at": _ts_nanos(finished_at),
                    "rows_written": int(rows_written),
                },
                at=_ts_nanos(started_at),
            )
            written += 1
        sender.flush()

    return written


if __name__ == "__main__":
    n = backfill_from_reconciliation_log()
    print(f"Backfilled {n} job_runs rows from reconciliation_log")
