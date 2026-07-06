# YATS — Yet Another Trading System

An institutional-grade trading research and execution platform. YATS automates the full lifecycle: multi-vendor data ingestion, deterministic feature computation, reinforcement learning training, experiment evaluation, qualification gating, shadow execution, and paper/live trading under a static risk contract.

YATS is **MCP-native** — all capabilities are exposed as MCP tools callable by agents or notebooks. There is no CLI or dashboard.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Layer 1: Interface (TypeScript)                        │
│  MCP Server — @modelcontextprotocol/sdk                 │
│  76 tools across 12 domains                             │
├─────────────────────────────────────────────────────────┤
│  Layer 2: Orchestration (TypeScript → Python bridge)    │
│  Dagster GraphQL │ Python subprocess │ QuestDB client   │
├─────────────────────────────────────────────────────────┤
│  Layer 3: Compute + Storage (Python + QuestDB)          │
│  Dagster pipelines │ research/ modules │ QuestDB        │
│  .yats_data/ filesystem artifacts                       │
└─────────────────────────────────────────────────────────┘
```

**TypeScript** handles the MCP protocol layer and bridge.
**Python** handles all compute: RL training, evaluation, feature computation, execution.
**QuestDB** is the shared data plane for all time-series, metadata, and audit trails.
**Dagster** orchestrates pipelines with full observability.

## Prerequisites

- Node.js >= 20
- Python >= 3.11
- QuestDB (running on default port 8812 for PG wire, 9009 for ILP)
- Dagster (`pip install dagster dagster-webserver`)

### API Keys

| Variable | Source | Required for |
|----------|--------|-------------|
| `APCA_API_KEY_ID` | [Alpaca](https://alpaca.markets) | Market data + paper/live trading |
| `APCA_API_SECRET_KEY` | Alpaca | Market data + paper/live trading |
| `FD_API_KEY` | [financialdatasets.ai](https://financialdatasets.ai) | Fundamentals, metrics, earnings |

## Setup

```bash
# TypeScript (MCP server)
npm install
npm run build

# Python (pipelines + research)
pip install -e ".[dev]"

# QuestDB tables
python -c "from pipelines.yats_pipelines.utils.create_tables import create_all_tables; create_all_tables()"

# Dagster
dagster dev -m pipelines.yats_pipelines.definitions
```

## Local Development

### One-command stack

```bash
# 1. Start QuestDB (Docker Desktop must be running)
docker compose up -d

# 2. Wait for QuestDB to be ready, then create all tables
python scripts/bootstrap_db.py

# 3. Run the full test suite (13 live-DB promotion tests + 759 unit tests)
PYTHONPATH=.:pipelines uv run --with pytest pytest tests -q

# 4. Start Dagster UI (pipelines only — not containerized)
PYTHONPATH=.:pipelines uv run dagster dev -m pipelines.yats_pipelines.definitions
```

### Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 9000 | HTTP | QuestDB web console |
| 8812 | PG wire | psycopg2 reads |
| 9009 | ILP (TCP) | questdb.ingress writes |

### ThetaData terminal

ThetaData v2 REST is served by a local Java process (Theta Terminal) that proxies
requests upstream with your subscriber credentials. No auth headers are needed in
REST calls — authentication happens at terminal startup.

```bash
# Download ThetaTerminal.jar from https://thetadata.net/
# Run it with your ThetaData credentials:
java -jar ThetaTerminal.jar your@email.com yourpassword

# The terminal listens on port 25510 (default).
# Verify it's running:
curl http://127.0.0.1:25510/v2/list/roots/option
```

`THETADATA_API_KEY` in `.env` is your subscriber password used to launch the terminal,
not an API key sent in REST requests. Set `THETADATA_BASE_URL` only if the terminal
runs on a non-default address.

### Test gating

Tests that require a live QuestDB are marked `@pytest.mark.live_db`. They
**skip** automatically when port 8812 is unreachable — so `pytest tests` is
always safe to run without the stack. Bring the stack up to run them:

```bash
docker compose up -d && python scripts/bootstrap_db.py
PYTHONPATH=.:pipelines uv run --with pytest pytest tests -q
# → 772 passed when stack is up; 759 passed + 13 skipped when stack is down
```

## Quickstart

YATS tools are invoked via MCP. Here's the typical workflow:

```
1. Ingest data         →  data.ingest (Alpaca OHLCV + FD fundamentals)
2. Canonicalize        →  data.canonicalize (raw → canonical with lineage)
3. Compute features    →  features.compute (32 v1 features)
4. Create experiment   →  experiment.create (spec with policy, universe, params)
5. Train + evaluate    →  experiment.run (PPO/SAC training + deterministic eval)
6. Shadow replay       →  shadow.run (forward-only historical replay)
7. Qualify             →  qualify.run (candidate vs baseline, hard/soft gates)
8. Promote             →  promote.to_candidate → promote.to_production
9. Paper trade         →  execution.start_paper (Alpaca paper endpoint)
```

## Directory Structure

```
yats/
  src/                    # TypeScript MCP server
    server.ts             # Entry point (stdio transport)
    tools/                # 12 tool domains (data, features, experiment, ...)
    bridge/               # dagster-client, python-runner, questdb-client
    auth/                 # Role-based permissions, SQL safety, rate limiting
    types/                # TypeScript type definitions
    vendors/              # Alpaca + financialdatasets.ai API clients

  pipelines/              # Python Dagster pipelines
    yats_pipelines/
      jobs/               # 14 pipeline jobs (ingest, canonicalize, train, ...)
      resources/          # QuestDB, Alpaca, FD resources
      io/                 # QuestDB I/O manager

  research/               # Python research modules
    envs/                 # SignalWeightEnv (old Gym API)
    training/             # PPO + SAC trainers (SB3), reward shaping
    eval/                 # Deterministic evaluation, regime slicing
    experiments/          # ExperimentSpec, registry
    shadow/               # ShadowEngine, ReplayMarketDataSource
    execution/            # Paper/live trading, broker adapter, kill switches
    promotion/            # Qualification gates, promotion tiers
    features/             # Feature registry, OHLCV/fundamental/regime features
    hierarchy/            # ModeController, per-mode allocators
    policies/             # SMA, equal-weight policies
    risk/                 # Risk config, weight projection

  compute/                # Standalone compute modules
    stats/                # ADF, bootstrap, deflated Sharpe, PBO
    risk/                 # Stress test, tail analysis, correlation

  configs/                # Configuration files
    risk.yml              # Risk policy thresholds (15 constraints)
    feature_sets/         # Feature set definitions (YAML)
    universes/            # Ticker lists (sp500, sectors)
    regime_detectors/     # Pluggable regime detection configs
    regime_thresholds.yml # Regime bucketing thresholds
    vendors.yml           # Vendor configuration

  .yats_data/             # Runtime artifacts (not in git)
    experiments/          # Per-experiment specs, checkpoints, metrics
    promotions/           # Immutable promotion records
    shadow/               # Shadow execution logs

  tests/                  # Test suite
    research/             # Python module tests
    pipelines/            # Dagster pipeline tests
    integration/          # End-to-end tests
```

## Data Vendors

| Vendor | Data | Usage |
|--------|------|-------|
| **Alpaca** | OHLCV bars (daily), real-time WebSocket, paper/live trading | Primary market data + execution |
| **financialdatasets.ai** | Fundamentals, financial metrics, earnings, insider trades, analyst estimates | Research data |

All data flows through a two-layer model: **raw** (append-only, per-vendor) → **canonical** (reconciled, only input downstream). Canonical tables carry full lineage (source vendor, reconciliation method, validation status).

## Experiment Lifecycle

```
ExperimentSpec (canonical config)
    ↓
experiment.create → content-addressed ID (SHA256)
    ↓
experiment.run → train (PPO/SAC) + evaluate
    ↓
qualify.run → candidate vs baseline (hard/soft gates)
    ↓
promote.to_candidate → promote.to_production
    ↓
execution.start_paper → paper trading via Alpaca
```

**Policies**: PPO, SAC, SMA, equal-weight, hierarchical (ModeController + per-mode allocators)

**Reward versions**: v1 (identity log-return), v2 (shaped: turnover + drawdown + cost penalties)

## Risk Engine

Risk policy is a **static contract** — no strategy or model may override it. Enforced at runtime with 15 constraints across 6 groups:

1. **Kill switches**: daily loss limit, trailing drawdown
2. **Global limits**: gross/net exposure, leverage, daily turnover
3. **Per-symbol**: max weight, position count, concentration, ADV participation
4. **Volatility**: vol scaling, vol regime brakes
5. **Signal**: confidence gating, min holding period
6. **Cash floor**: minimum cash reserve

All risk decisions logged to QuestDB. Kill switch state machine: TRADING → HALTING → HALTED → RESUMING → TRADING.

## Security

Role-based access control with 5 tiers: `intern` (read-only) → `researcher` → `risk_officer` → `pm` → `managing_partner` (full control). SQL queries are parameterized with table whitelists per role. Python subprocesses run sandboxed (no network, restricted filesystem, memory limits).

## Documentation

- [Reference Guide](docs/REFERENCE.md) — MCP tool catalog, QuestDB schemas, config reference
- [Contributing](CONTRIBUTING.md) — Development workflow and conventions
- [PRD](yats-prd.md) — Full product requirements document

## License

[MIT](LICENSE.md)
