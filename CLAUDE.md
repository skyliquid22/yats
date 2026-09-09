# YATS — project conventions

## Stack
- Python 3.13 via uv (`pyproject.toml` + `uv.lock` are canonical; no requirements.txt)
- TypeScript MCP server in `src/` (strict mode, Node >= 22)
- QuestDB: PG wire for reads, ILP for writes; all timestamps UTC
- Dagster jobs in `pipelines/`, research code in `research/`, configs in `configs/` (YAML)

## Layout
- `pipelines/yats_pipelines/` — ingest / canonicalize / feature / experiment jobs
- `research/` — features, eval (WFO/DSR), training, portfolio, shadow, promotion
- `docs/research/` — verdict docs + `receipts/` (raw sweep outputs)
- `demo/` — no-keys synthetic demo (`make demo`)

## Tests
- `make test` (sets OMP_NUM_THREADS=1 — dual-libomp deadlock otherwise)
- Full command: `OMP_NUM_THREADS=1 PYTHONPATH=.:pipelines uv run --with pytest --with pytest-timeout pytest tests -q --timeout=120 -k "not live"`
- `npm test` for the MCP server

## Non-negotiable research rules (no lookahead)
- Data becomes visible at its filing/availability date, never its content date
- Evaluation fills at t+1 (`execution_lag_days=1`, `fill_timing`); same-bar fills are legacy-only
- Purge = max feature lookback between train and test; register lookbacks in the feature registry
- Every new metric claim must trace to a file in `docs/research/` (receipts included)
- Every trial counts toward the deflation clock — no pool-shopping
