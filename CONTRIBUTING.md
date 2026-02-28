# Contributing to YATS

## Language Split

YATS is a two-language system. Know which track you're working in:

| Track | Language | Scope |
|-------|----------|-------|
| **Interface** | TypeScript | MCP server, tool handlers, bridge layer, auth, types |
| **Compute** | Python | Dagster pipelines, research modules, features, training, execution |
| **Config** | YAML/JSON | Risk thresholds, feature sets, universes, experiment specs |

The TypeScript layer never runs compute directly — it triggers Dagster pipelines or Python subprocesses.

## Development Setup

```bash
# TypeScript
npm install
npm run build
npm run typecheck   # tsc --noEmit

# Python
pip install -e ".[dev]"
pytest tests/       # run test suite

# Both
# QuestDB must be running locally (port 8812 PG wire, 9009 ILP)
# Dagster: dagster dev -m pipelines.yats_pipelines.definitions
```

## Code Conventions

### TypeScript (src/)

- Tool handlers go in `src/tools/<domain>/<tool-name>.ts`
- Each tool domain has an `index.ts` that exports all tools for registry
- Tool inputs/outputs are typed in `src/types/`
- Bridge calls go through `src/bridge/` (never call Python or QuestDB directly from tools)
- Auth checks happen in `src/server.ts` dispatch, not in individual tools

### Python (research/, pipelines/, compute/)

- Type hints on all function signatures (`from __future__ import annotations`)
- Dataclasses for configs, not Pydantic
- Feature implementations use the `@feature("name")` decorator pattern
- Dagster jobs go in `pipelines/yats_pipelines/jobs/`
- Dagster resources go in `pipelines/yats_pipelines/resources/`
- QuestDB writes use ILP protocol for time-series, PG wire for queries

### Configs (configs/)

- `risk.yml` — risk policy thresholds (static contract, changes require review)
- `feature_sets/*.yml` — feature set definitions (hashable for reproducibility)
- `universes/*.yml` — ticker lists
- `regime_detectors/*.yml` — pluggable regime detection configs
- `regime_thresholds.yml` — regime bucketing thresholds

## Adding New Components

### Adding a new MCP tool

1. Create `src/tools/<domain>/<tool-name>.ts`
2. Export it from `src/tools/<domain>/index.ts`
3. The tool registry (`src/tools/registry.ts`) auto-discovers from domain index files
4. Define input/output types in `src/types/tools.ts`
5. Tool must specify its backend: Dagster pipeline (`run_id` return), Python subprocess, or direct QuestDB query

### Adding a new feature

1. Write the compute function in `research/features/` with `@feature("feature_name")` decorator
2. Add the feature name to the appropriate feature set YAML in `configs/feature_sets/`
3. Add the column to the `features` table DDL in `pipelines/yats_pipelines/utils/create_tables.py`
4. The feature pipeline automatically picks it up from the registry

### Adding a new policy type

1. Implement in `research/policies/` with the standard interface: `act(obs, context) -> weights`
2. Register the policy name in `ExperimentSpec` validation (`research/experiments/spec.py`)
3. Add loading logic in the experiment runner and shadow engine

### Adding a new regime detector

1. Create config in `configs/regime_detectors/<name>.yml`
2. Implement `compute()` and `label()` in a Python module referenced by the config
3. Reference by name in experiment specs via `regime_feature_set`

### Adding a new Dagster pipeline

1. Create job in `pipelines/yats_pipelines/jobs/<job_name>.py`
2. Register in `pipelines/yats_pipelines/definitions.py`
3. Create corresponding MCP tool handler in `src/tools/` that triggers it via `bridge/dagster-client.ts`

## Testing

```bash
# Run all Python tests
pytest tests/

# Run specific module tests
pytest tests/research/envs/
pytest tests/research/promotion/
pytest tests/research/shadow/

# Run with coverage
pytest --cov=research --cov=pipelines tests/
```

Tests live alongside the code they test:
- `tests/research/` mirrors `research/`
- `tests/pipelines/` mirrors `pipelines/`
- `tests/integration/` for end-to-end flows

### What to test

- **Feature implementations**: exact formula verification against known inputs
- **Risk constraints**: boundary cases for each of the 15 constraints
- **Qualification gates**: known pass/fail scenarios for hard and soft gates
- **Experiment lifecycle**: spec creation → ID derivation → artifact layout
- **Shadow engine**: deterministic replay producing identical results across runs

## Git Workflow

**Maintainers** push directly to `main`. No feature branches, no PRs.

```bash
git pull
# ... make changes ...
git add <files>
git commit -m "description of change"
git push
```

**External contributors** should fork and submit a PR against `main`.

## Key Invariants

These are system-level guarantees that must not be broken:

1. **Canonical is the only downstream input** — features, training, and evaluation read from canonical tables only, never raw
2. **ExperimentSpec is self-contained** — the materialized spec has no external references; changing a universe file doesn't affect existing experiments
3. **Experiment ID is deterministic** — same spec fields produce the same SHA256 ID
4. **Risk policy is static** — no model or strategy may override `configs/risk.yml` in paper/live mode
5. **Audit trail is append-only** — QuestDB's partitioned tables enforce this naturally
6. **Promotion records are immutable** — re-promoting with different content is an error
7. **Research uses batch data only** — research pipelines filter `reconcile_method='batch'`, never touch streaming data
