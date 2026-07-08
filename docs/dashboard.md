# YATS Operations Dashboard

Three pages — Data & System (read-only), Experiments, Trading — over a
FastAPI backend. Design spec: the v3 artifact (see ya-c85dk/ya-4xaxj).

## Run
    PYTHONPATH=.:pipelines uv run python -m dashboard
Serves http://127.0.0.1:8787 (localhost only; strictly read-only — no
mutating endpoint exists). Static page at `/`, API under `/api/*`, 30s poll.

## Endpoints
health · coverage · storage · storage/{domain}/tickers (lazy drill-down) ·
runs (job_runs table — jobs self-record; no persistent Dagster instance) ·
experiments?q= (experiment_index + wfo_sweeps summaries; deflation_clock =
len(configs) across all summaries) · experiments/{id} (artifact bundle) ·
trading (portfolio/orders/risk keyed by experiment_id).

## Adding a panel
One query fn in `dashboard/queries.py` (+unit test; monkeypatch
YATS_DATA_ROOT to tmp_path — real sweep summaries leak into counts
otherwise) + one render fn in `static/app.js`.

## Env
QDB_PG_* (default localhost:8812) · YATS_DATA_ROOT (default .yats_data).
