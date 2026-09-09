# Contributing to YATS

## Dev setup

```bash
# Python (compute: pipelines, research, features, execution)
uv sync                      # or: pip install -e .

# TypeScript (MCP server)
npm ci
npm run build

# Optional local stack (only needed for live_db tests and real ingestion)
docker compose up -d         # QuestDB
python scripts/bootstrap_db.py
```

Copy `.env.example` to `.env` if you need vendor credentials; nothing in the unit test suite requires them.

## Tests

```bash
# Python: same command CI runs (live vendor/DB tests excluded)
PYTHONPATH=.:pipelines uv run --with pytest --with pytest-timeout \
  pytest tests -q --timeout=120 -k "not live"

# TypeScript
npm test
```

Tests marked `live_db` skip automatically when QuestDB isn't reachable on port 8812, so `pytest tests` is always safe to run. Bring the docker stack up to exercise them.

## PR expectations

1. **Tests accompany code.** New features and bug fixes come with tests; `tests/` mirrors the source tree (`tests/research/`, `tests/pipelines/`, `tests/src/`).
2. **The no-lookahead discipline is non-negotiable.** Anything touching data flow, features, labels, evaluation, or execution must preserve:
   - canonical tables are the only downstream input (never raw),
   - features at bar *t* use only data available at *t* (declared lookback memory, honored by the WFO purge/buffer),
   - fills happen at bar *t+1* (`fill_timing`), never same-bar,
   - purged walk-forward geometry stays matched when comparing arms.
   If your change could move a number in `docs/research/`, say so in the PR and count the trial against the deflation clock.
3. **The risk contract is static.** Changes to `configs/risk.yml` require explicit review; no code path may let a model override it.
4. **Keep claims sourced.** Numbers in docs/README must trace to a file in the repo (sweep summaries, verdict docs).

Fork, branch from `main`, and open a PR against `main`.

## Where things live

- [docs/REFERENCE.md](docs/REFERENCE.md): MCP tool catalog, QuestDB schemas, config reference
- [docs/ingestion.md](docs/ingestion.md): ingestion architecture, symbol backfill runbook
- [docs/dashboard.md](docs/dashboard.md): read-only ops dashboard
- [docs/research/](docs/research/): dated sweep verdicts (the lab notebook)
- `src/` TypeScript MCP server · `pipelines/` Dagster jobs · `research/` research modules · `compute/` stats/risk kernels · `configs/` YAML contracts
