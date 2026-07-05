"""Experiment explosion guard — PRD §23.5.

Blocks new sweep creation when >1000 experiments were created in the last
30 days (queried from experiment_index). Requires managing_partner_ack to
resume sweeps after the guard is tripped.

The guard is stateless: it re-evaluates the count on every call.
To resume blocked sweeps, set managing_partner_ack=True in the sweep job
run config.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

EXPLOSION_THRESHOLD_DEFAULT = 1000
EXPLOSION_WINDOW_DAYS_DEFAULT = 30


class ExplosionGuardError(Exception):
    """Raised when the experiment explosion guard is tripped."""


def count_recent_experiments(
    conn: Any,
    *,
    days: int = EXPLOSION_WINDOW_DAYS_DEFAULT,
) -> int:
    """Count experiments created in the last ``days`` days from experiment_index.

    Args:
        conn: A psycopg2-compatible connection to QuestDB.
        days: Look-back window in days (default 30).

    Returns:
        Number of experiments with created_at within the window.
    """
    query = """
        SELECT count() AS cnt
        FROM experiment_index
        WHERE created_at >= dateadd('d', -{days}, now())
    """.format(days=days)

    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()

    return int(row[0]) if row else 0


def check_explosion_guard(
    *,
    conn: Any | None = None,
    threshold: int = EXPLOSION_THRESHOLD_DEFAULT,
    days: int = EXPLOSION_WINDOW_DAYS_DEFAULT,
    managing_partner_ack: bool = False,
) -> None:
    """Enforce the experiment explosion guard.

    Raises ExplosionGuardError if count > threshold and no ack provided.
    When conn is None, connects to QuestDB using the default resource config.

    Args:
        conn: psycopg2-compatible connection. If None, connects automatically.
        threshold: Maximum experiments allowed in the window (default 1000).
        days: Look-back window in days (default 30).
        managing_partner_ack: If True, bypass the guard even when tripped.
    """
    if managing_partner_ack:
        logger.info(
            "Explosion guard: managing_partner_ack=True — guard bypassed"
        )
        return

    _conn = conn
    _owns_conn = False
    if _conn is None:
        import psycopg2
        from pipelines.yats_pipelines.resources.questdb import QuestDBResource

        qdb = QuestDBResource()
        _conn = psycopg2.connect(
            host=qdb.pg_host,
            port=qdb.pg_port,
            user=qdb.pg_user,
            password=qdb.pg_password,
            database=qdb.pg_database,
        )
        _conn.autocommit = True
        _owns_conn = True

    try:
        count = count_recent_experiments(_conn, days=days)
    finally:
        if _owns_conn:
            _conn.close()

    logger.info(
        "Explosion guard: %d experiments created in last %d days (threshold=%d)",
        count, days, threshold,
    )

    if count > threshold:
        raise ExplosionGuardError(
            f"Experiment explosion guard tripped: {count} experiments created "
            f"in the last {days} days exceeds threshold of {threshold}. "
            f"New sweep creation is blocked. "
            f"Set managing_partner_ack=True in the sweep job run config to resume."
        )
