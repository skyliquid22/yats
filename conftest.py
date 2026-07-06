"""Root pytest configuration — live_db marker and QuestDB skip gating."""
from __future__ import annotations

import socket

import pytest


_QUESTDB_HOST = "localhost"
_QUESTDB_PG_PORT = 8812


def _questdb_reachable() -> bool:
    """Return True if QuestDB PG-wire port is accepting connections."""
    try:
        with socket.create_connection((_QUESTDB_HOST, _QUESTDB_PG_PORT), timeout=1):
            return True
    except OSError:
        return False


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "live_db: mark test as requiring a live QuestDB instance on localhost:8812",
    )


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Skip live_db tests when QuestDB is unreachable."""
    if _questdb_reachable():
        return

    skip_reason = pytest.mark.skip(
        reason="QuestDB not reachable on localhost:8812 — start the stack with "
               "'docker compose up -d' and run 'python scripts/bootstrap_db.py'"
    )
    for item in items:
        if item.get_closest_marker("live_db"):
            item.add_marker(skip_reason)
