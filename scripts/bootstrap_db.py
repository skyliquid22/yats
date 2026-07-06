"""Bootstrap QuestDB tables for local development.

Creates all YATS tables via PG wire. Idempotent — safe to re-run.

Usage:
    python scripts/bootstrap_db.py
    python scripts/bootstrap_db.py --host localhost --port 8812
"""
from __future__ import annotations

import argparse
import sys

sys.path.insert(0, ".")
sys.path.insert(0, "pipelines")

from pipelines.yats_pipelines.resources.questdb import QuestDBResource
from pipelines.yats_pipelines.utils.create_tables import create_all_tables


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap QuestDB tables")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8812)
    parser.add_argument("--user", default="admin")
    parser.add_argument("--password", default="quest")
    parser.add_argument("--database", default="qdb")
    args = parser.parse_args()

    resource = QuestDBResource(
        pg_host=args.host,
        pg_port=args.port,
        pg_user=args.user,
        pg_password=args.password,
        pg_database=args.database,
    )

    print(f"Connecting to QuestDB at {args.host}:{args.port} ...")
    create_all_tables(resource)


if __name__ == "__main__":
    main()
