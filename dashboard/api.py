"""YATS Dashboard FastAPI application.

Read-only backend serving 8 endpoints over QuestDB.
In-process TTL cache (15 s) per endpoint to avoid hammering the DB.
Binds to 127.0.0.1:8787 via __main__.py.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Callable

from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles

from dashboard import queries

app = FastAPI(title="YATS Dashboard", version="1")

# ---------------------------------------------------------------------------
# TTL cache — simple module-level dict, no external dependency
# ---------------------------------------------------------------------------

_TTL = 15.0  # seconds
_cache: dict[str, tuple[Any, float]] = {}


def _cached(key: str, fn: Callable[[], Any]) -> Any:
    """Return cached result if fresh; otherwise call fn(), cache, and return."""
    now = time.monotonic()
    if key in _cache:
        val, ts = _cache[key]
        if now - ts < _TTL:
            return val
    val = fn()
    _cache[key] = (val, now)
    return val


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/api/health")
def get_health():
    def _compute():
        with queries.pg_conn() as conn:
            services = queries.health_checks(conn)
            kill = queries.kill_switches_latest(conn)
        return {"services": services, "kill_switches": kill}
    return _cached("health", _compute)


@app.get("/api/coverage")
def get_coverage():
    def _compute():
        with queries.pg_conn() as conn:
            return queries.coverage(conn)
    return _cached("coverage", _compute)


@app.get("/api/storage")
def get_storage():
    def _compute():
        with queries.pg_conn() as conn:
            return queries.storage(conn)
    return _cached("storage", _compute)


@app.get("/api/storage/{domain}/tickers")
def get_storage_tickers(domain: str):
    def _compute():
        with queries.pg_conn() as conn:
            return queries.storage_tickers(conn, domain)
    return _cached(f"storage_tickers:{domain}", _compute)


@app.get("/api/runs")
def get_runs(hours: int = Query(default=24, ge=1, le=168)):
    def _compute():
        with queries.pg_conn() as conn:
            return queries.runs(conn, hours)
    return _cached(f"runs:{hours}", _compute)


@app.get("/api/experiments")
def get_experiments(q: str = Query(default="")):
    def _compute():
        with queries.pg_conn() as conn:
            return queries.experiments_search(conn, q)
    return _cached(f"experiments:{q}", _compute)


@app.get("/api/experiments/{experiment_id}")
def get_experiment(experiment_id: str):
    # Per-id; don't cache (varied ids, infrequent access)
    with queries.pg_conn() as conn:
        return queries.experiment_detail(conn, experiment_id)


@app.get("/api/trading")
def get_trading():
    def _compute():
        with queries.pg_conn() as conn:
            return queries.trading(conn)
    return _cached("trading", _compute)


# ---------------------------------------------------------------------------
# Static files — D2 populates dashboard/static/
# ---------------------------------------------------------------------------

_STATIC = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")


@app.get("/", include_in_schema=False)
def index():
    from fastapi.responses import FileResponse
    return FileResponse(_STATIC / "index.html")
