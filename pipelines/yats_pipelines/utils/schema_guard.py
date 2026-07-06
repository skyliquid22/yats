"""Schema guard — verify QuestDB tables have expected columns before ILP writes.

Prevents silent schema corruption from ILP auto-create (which uses generic column
names like 'timestamp' instead of the designated timestamp column name from DDL).
"""
from __future__ import annotations


def assert_table_schema(conn, table_name: str, required_columns: list[str]) -> None:
    """Assert table exists and contains all required columns.

    Raises RuntimeError with actionable message if:
    - The table does not exist
    - The table exists but is missing one or more required columns

    Args:
        conn: psycopg2 connection to QuestDB (PG wire).
        table_name: Name of the target table.
        required_columns: Column names that must be present.
    """
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                (table_name,),
            )
            existing = {row[0] for row in cur.fetchall()}
        finally:
            cur.close()
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(
            f"Schema check for '{table_name}' failed: {exc}. "
            "Run scripts/bootstrap_db.py"
        ) from exc

    if not existing:
        raise RuntimeError(
            f"Table '{table_name}' does not exist. Run scripts/bootstrap_db.py"
        )

    missing = set(required_columns) - existing
    if missing:
        raise RuntimeError(
            f"Table '{table_name}' is missing columns {sorted(missing)} "
            "(ILP auto-create produces wrong designated-timestamp column names). "
            "Run scripts/bootstrap_db.py"
        )
