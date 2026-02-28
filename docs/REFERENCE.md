# YATS Reference Guide

## MCP Tool Catalog

All tools are versioned (`_v1` suffix). New parameters are always optional with defaults in minor versions. Breaking changes require a new major version.

Tools return either:
- **run_id** (Dagster pipeline tools) — poll status via `monitor.pipeline_status`
- **result** (Python subprocess / QuestDB query tools) — immediate response

### data.* — Data Ingestion and Query

| Tool | Description | Backend |
|------|-------------|---------|
| `data.ingest_v1` | Ingest historical data for ticker(s) from a vendor into raw tables | Dagster pipeline |
| `data.ingest_universe_v1` | Bulk ingest for an entire universe of tickers | Dagster pipeline |
| `data.refresh_v1` | Incremental update since last ingest high-water mark | Dagster pipeline |
| `data.canonicalize_v1` | Run raw → canonical reconciliation for data domains over a date range | Dagster pipeline |
| `data.verify_v1` | Data quality checks on canonical data | Python subprocess |
| `data.query_v1` | Parameterized SELECT against QuestDB (role-restricted table access, no DDL/DML) | Direct QuestDB |
| `data.coverage_v1` | Coverage report per symbol — gaps, fill ratios, date ranges | Direct QuestDB |
| `data.vendors_v1` | List available vendors and their configuration status | Static config |

### features.* — Feature Computation

| Tool | Description | Backend |
|------|-------------|---------|
| `features.compute_v1` | Full recompute of features for universe + date range | Dagster pipeline |
| `features.compute_incremental_v1` | Compute only new data since watermark | Dagster pipeline |
| `features.list_v1` | List registered feature sets, versions, and contained features | Python subprocess |
| `features.stats_v1` | Summary statistics (avg, std, min, max) for a feature column | Direct QuestDB |
| `features.correlations_v1` | Correlation matrix across features for a feature set | Python subprocess |
| `features.coverage_v1` | Feature coverage: missing values, date range, row counts | Direct QuestDB |
| `features.watermarks_v1` | High-water marks per symbol and feature set | Direct QuestDB |

### experiment.* — Experiment Management

| Tool | Description | Backend |
|------|-------------|---------|
| `experiment.create_v1` | Create experiment from spec (supports composable inheritance) | Python subprocess |
| `experiment.run_v1` | Train + evaluate an experiment | Dagster pipeline |
| `experiment.list_v1` | List experiments with optional filters (queries experiment_index) | Direct QuestDB |
| `experiment.get_v1` | Full experiment details: spec, metrics, artifact paths | Python subprocess |
| `experiment.compare_v1` | Side-by-side comparison of two experiments | Python subprocess |

### eval.* — Evaluation

| Tool | Description | Backend |
|------|-------------|---------|
| `eval.run_v1` | Run deterministic evaluation, produce metrics.json | Dagster pipeline |
| `eval.metrics_v1` | Fetch evaluation metrics (performance, trading, safety, regime) | Python subprocess |
| `eval.regime_slices_v1` | Per-regime performance breakdown | Python subprocess |

### shadow.* — Shadow Execution

| Tool | Description | Backend |
|------|-------------|---------|
| `shadow.run_v1` | Shadow replay with execution_mode=none (direct rebalance) | Dagster pipeline |
| `shadow.run_sim_v1` | Shadow replay with execution_mode=sim (simulated broker) | Dagster pipeline |
| `shadow.status_v1` | Poll shadow run progress | Dagster GraphQL |
| `shadow.results_v1` | Fetch shadow metrics + step-level logs | Python subprocess |
| `shadow.compare_modes_v1` | Compare shadow vs paper vs live for same experiment | Direct QuestDB |

### sweep.* — Experiment Sweeps

| Tool | Description | Backend |
|------|-------------|---------|
| `sweep.run_v1` | Run parameter sweep (base spec + override grid) | Dagster pipeline |
| `sweep.status_v1` | Per-experiment status within a sweep | Dagster GraphQL |
| `sweep.results_v1` | Aggregate results across sweep, sorted by metric | Direct QuestDB |

### qualify.* — Qualification

| Tool | Description | Backend |
|------|-------------|---------|
| `qualify.run_v1` | Run qualification (candidate vs baseline, hard/soft gates) | Dagster pipeline |
| `qualify.report_v1` | Fetch qualification_report.json | Python subprocess |
| `qualify.gates_v1` | List all gates and their threshold defaults | Python subprocess |

### promote.* — Promotion

| Tool | Description | Backend |
|------|-------------|---------|
| `promote.to_research_v1` | Promote to research tier (requires qualification passed) | Dagster pipeline |
| `promote.to_candidate_v1` | Promote to candidate tier (requires PM approval) | Dagster pipeline |
| `promote.to_production_v1` | Promote to production tier (requires managing_partner approval) | Dagster pipeline |
| `promote.list_v1` | List promotion records, optionally filter by tier | Direct QuestDB |
| `promote.history_v1` | Full promotion history for an experiment | Python subprocess |

### execution.* — Paper/Live Trading

| Tool | Description | Backend |
|------|-------------|---------|
| `execution.start_paper_v1` | Start paper trading for a promoted experiment | Dagster pipeline |
| `execution.stop_paper_v1` | Stop active paper trading run | Dagster (control) |
| `execution.paper_status_v1` | Paper trading status + live metrics | Dagster + QuestDB |
| `execution.promote_live_v1` | Promote to live trading (managing_partner gate) | Dagster pipeline |
| `execution.positions_v1` | Current positions for a trading run | Direct QuestDB |
| `execution.orders_v1` | Order history with fill details | Direct QuestDB |
| `execution.nav_v1` | NAV and portfolio snapshot | Direct QuestDB |

### monitor.* — System Health

| Tool | Description | Backend |
|------|-------------|---------|
| `monitor.health_v1` | QuestDB, Dagster, Alpaca connectivity + latency | HTTP health checks |
| `monitor.pipeline_status_v1` | Recent Dagster run statuses | Dagster GraphQL |
| `monitor.data_freshness_v1` | Most recent timestamp per canonical table | Direct QuestDB |
| `monitor.audit_log_v1` | Query audit trail, filter by action/actor/experiment | Direct QuestDB |
| `monitor.reconcile_v1` | Consistency check across QuestDB, filesystem, Dagster | QuestDB + filesystem |

### stats.* — Statistical Tools

| Tool | Description | Backend |
|------|-------------|---------|
| `stats.adf_test_v1` | Augmented Dickey-Fuller stationarity test | Python subprocess |
| `stats.bootstrap_sharpe_v1` | Bootstrap confidence interval for Sharpe ratio | Python subprocess |
| `stats.deflated_sharpe_v1` | Deflated Sharpe Ratio (multiple testing correction) | Python subprocess |
| `stats.pbo_v1` | Probability of Backtest Overfitting | Python subprocess |
| `stats.regime_detect_v1` | Regime detection using registered detector | Python subprocess |
| `stats.conditional_sharpe_v1` | Sharpe conditioned on regime bucket | Python subprocess |
| `stats.ic_analysis_v1` | Information coefficient and decay across horizons | Python subprocess |
| `stats.scm_leakage_v1` | Look-ahead bias detection via lead-lag correlation | Python subprocess |

### registry.* — Metadata

| Tool | Description | Backend |
|------|-------------|---------|
| `registry.universes_v1` | List available universe configs and ticker counts | Filesystem config |
| `registry.feature_sets_v1` | List registered feature sets and versions | Python subprocess |
| `registry.regime_detectors_v1` | List registered regime detectors | Python subprocess |
| `registry.policies_v1` | List available policy types and parameters | Static config |

---

## QuestDB Table Reference

26 tables organized by domain. All timestamps are UTC. Tables use QuestDB's designated timestamp and partitioning for query performance.

### Raw Tables (Per-Vendor, Append-Only)

| Table | Partition | Description |
|-------|-----------|-------------|
| `raw_alpaca_equity_ohlcv` | MONTH | Alpaca OHLCV bars (timestamp, symbol, OHLCV, vwap, trade_count, ingested_at) |
| `raw_fd_fundamentals` | YEAR | Income statement data (revenue, gross_profit, operating_income, net_income, EPS, shares) |
| `raw_fd_financial_metrics` | YEAR | 50+ pre-computed ratios (PE, ROE, margins, growth, shares_outstanding) |
| `raw_fd_insider_trades` | YEAR | Form 3/4/5 transactions (insider_name, transaction_type, shares, price, value) |
| `raw_fd_analyst_estimates` | YEAR | Consensus forecasts (EPS/revenue estimates, actuals, analyst count) |
| `raw_fd_earnings` | YEAR | Earnings results (EPS actual vs estimate, surprise, revenue) |

### Canonical Tables (Reconciled)

| Table | Partition | Description |
|-------|-----------|-------------|
| `canonical_equity_ohlcv` | MONTH | Reconciled OHLCV + lineage (source_vendor, reconcile_method, validation_status, canonicalized_at) |
| `canonical_fundamentals` | YEAR | Point-in-time fundamentals + lineage |
| `canonical_financial_metrics` | YEAR | Daily frequency metrics (forward-filled) + lineage |

### Feature Tables

| Table | Partition | Description |
|-------|-----------|-------------|
| `features` | MONTH | All v1 features: OHLCV (7), cross-sectional (5), fundamental (11), regime (4) + metadata (feature_set, feature_set_version, computed_at) |
| `feature_watermarks` | — | Incremental computation tracking (symbol, feature_set, last_computed_date) |

### Experiment Tables

| Table | Partition | Description |
|-------|-----------|-------------|
| `experiment_index` | YEAR | Experiment metadata + metrics summary (universe, policy, sharpe, max_drawdown, qualification_status, promotion_tier, stale flag) |

### Execution Tables (Unified: shadow/paper/live)

| Table | Partition | Description |
|-------|-----------|-------------|
| `execution_log` | MONTH | Per-step execution data (target_weight, realized_weight, fill_price, slippage, fees, rejected, regime_bucket) |
| `execution_metrics` | YEAR | Per-run summary (fill_rate, reject_rate, avg/p95 slippage, total fees, sharpe, max_drawdown) |
| `positions` | MONTH | Current positions snapshot (symbol, quantity, avg_entry_price, unrealized/realized PnL) |
| `orders` | MONTH | Order lifecycle (order_id, side, quantity, status, fill_price, risk_check_result, broker_order_id) |
| `portfolio_state` | MONTH | NAV snapshots (nav, cash, gross/net exposure, leverage, drawdown, daily_pnl) |

### Risk + Governance Tables

| Table | Partition | Description |
|-------|-----------|-------------|
| `risk_decisions` | MONTH | Every risk decision (rule_id, decision: pass/reject/halt/size_reduce, original_size, reduced_size) |
| `kill_switches` | YEAR | Kill switch events (trigger type, action, resolved status) |
| `promotions` | YEAR | Immutable promotion records (experiment_id, tier, qualification_passed, sharpe, promoted_by) |

### Operational Tables

| Table | Partition | Description |
|-------|-----------|-------------|
| `audit_trail` | MONTH | Every tool invocation, pipeline run, risk decision (tool_name, invoker, parameters, result_status, duration_ms) |
| `trading_heartbeat` | — | Liveness monitoring (loop_iteration, orders_pending, last_bar_received) |
| `reconciliation_log` | MONTH | Canonicalization diagnostics (domain, symbol, vendor selection, validation warnings) |
| `canonical_pins` | — | Snapshot pinning for reproducibility (experiment_id, canonicalized_at) |
| `canonical_hashes` | — | Per-symbol + universe-level hashes for feature invalidation |

---

## Config File Reference

### configs/risk.yml

Static risk policy thresholds. Paper/live trading always uses these values — no overrides allowed.

```yaml
# Kill switches
daily_loss_limit: -0.05           # -5% daily loss → halt
trailing_drawdown_limit: -0.20    # -20% drawdown → halt

# Global limits
max_gross_exposure: 1.0           # 100% of NAV
max_net_exposure: 1.0             # 100% of NAV
max_leverage: 1.0                 # No leverage in v1

# Per-symbol limits
max_symbol_weight: 0.20           # 20% max per symbol
max_active_positions: 50
min_cash: 0.02                    # 2% minimum cash

# Turnover
max_daily_turnover: 0.50          # 50% of NAV daily

# Concentration
max_top_n_concentration: 0.60     # Top 5 positions <= 60%
top_n: 5

# Liquidity
max_adv_pct: 0.05                 # 5% of 20-day ADV

# Volatility
target_vol: 0.15                  # 15% annualized target
vol_regime_threshold: 0.30        # 30% vol triggers brakes
vol_brake_position_reduction: 0.50
vol_brake_exposure_reduction: 0.50

# Signal constraints
min_confidence: 0.0               # No confidence gating by default
min_holding_period: 1             # 1 bar minimum hold

# Size reduce
minimum_order_threshold: 0.01    # 1% of NAV — below this, reject

# Operational
max_broker_errors: 5
data_staleness_threshold: 300     # Seconds
```

### configs/feature_sets/core_v1.yml

Defines which features are computed and their dependencies.

**OHLCV features (7):** ret_1d, ret_5d, ret_21d, rv_21d, rv_63d, dist_20d_high, dist_20d_low

**Cross-sectional features (5):** mom_3m, mom_12m_excl_1m, log_mkt_cap, size_rank, value_rank

**Fundamental features (11):** pe_ttm, ps_ttm, pb, ev_ebitda, roe, gross_margin, operating_margin, fcf_margin, debt_equity, eps_growth_1y, revenue_growth_1y

**Regime features (4):** market_vol_20d, market_trend_20d, dispersion_20d, corr_mean_20d

### configs/universes/

Ticker lists in YAML format. Example: `sp500.yml` contains 500 tickers.

The resolved ticker list is stored in the ExperimentSpec at experiment creation time — changing the universe file after creation does not affect existing experiments.

### configs/regime_thresholds.yml

Threshold-based regime bucketing (v2 labeling):

```yaml
vol_high: 0.20      # Above = high_vol
vol_low: 0.12       # Below = low_vol
trend_up: 0.02      # Above = trend_up
trend_down: -0.02   # Below = trend_down
```

Produces 6 buckets: `{high,low}_vol` × `{trend_up,trend_down,trend_flat}`

### configs/regime_detectors/

Pluggable regime detection configs. Each detector provides `compute()` (feature generation) and `label()` (evaluation bucketing).

```yaml
# regime_v1_1.yml
name: regime_v1_1
version: "1.1"
universe: [SPY, QQQ, IWM]
features: [market_vol_20d, market_trend_20d, dispersion_20d, corr_mean_20d]
labeling: v2
compute_module: research.features.regime_features_v1
label_module: research.eval.regime_slicing
```

---

## Dagster Pipeline Catalog

14 pipeline jobs available via `dagster dev -m pipelines.yats_pipelines.definitions`:

| Job | Trigger Tool | Description |
|-----|-------------|-------------|
| `ingest_alpaca` | data.ingest | Historical OHLCV from Alpaca → raw table |
| `ingest_financialdatasets` | data.ingest | Fundamentals/metrics from FD → raw tables |
| `canonicalize` | data.canonicalize | Raw → canonical with reconciliation + lineage |
| `stream_canonical` | (WebSocket) | Real-time Alpaca bars → canonical (streaming mode) |
| `feature_pipeline` | features.compute | Full recompute of all v1 features |
| `feature_pipeline_incremental` | features.compute_incremental | Watermark-based incremental feature computation |
| `experiment_run` | experiment.run | Load spec → train (if RL) → evaluate → write artifacts |
| `experiment_sweep` | sweep.run | Base spec + override grid → create + run each experiment |
| `shadow_run` | shadow.run / shadow.run_sim | Replay canonical data through policy, write execution tables |
| `qualify` | qualify.run | Candidate vs baseline, all hard/soft gates |
| `promote` | promote.to_* | Tier promotion with immutable records |
| `paper_trading` | execution.start_paper | Setup/teardown for paper trading process |
| `live_trading` | execution.promote_live | Setup/teardown for live trading process |
| `refresh` | data.refresh | Incremental data update from last high-water mark |

---

## Role Permissions

| Role | Access Level |
|------|-------------|
| `intern` | Read-only: query canonical data, features, experiment index, health checks |
| `researcher` | Read + write: ingest, canonicalize, compute features, create/run experiments, shadow, stats |
| `risk_officer` | Risk domain: risk checks, qualification, shadow replay, halt trading (emergency) |
| `pm` | Coordination: all researcher + promote to research/candidate, start/stop paper trading |
| `managing_partner` | Full control: promote to production, live trading, resume after halt, flatten positions |

---

## Filesystem Artifacts

Stored under `.yats_data/` (not tracked in git):

```
.yats_data/
  experiments/<EXPERIMENT_ID>/
    spec/experiment_spec.json          # Canonical config (source of truth)
    runs/<checkpoint files>            # Model checkpoints (PPO/SAC)
    runs/rollout.json                  # Training metadata
    evaluation/metrics.json            # Core metrics with regime breakdown
    evaluation/mode_timeline.json      # Mode transitions (hierarchical only)
    logs/run_summary.json              # Run pointers
    promotion/qualification_report.json

  promotions/
    research/<EXPERIMENT_ID>.json
    candidate/<EXPERIMENT_ID>.json
    production/<EXPERIMENT_ID>.json

  shadow/<EXPERIMENT_ID>/<RUN_ID>/
    logs/steps.jsonl                   # Per-step execution log
    state.json                         # Resume-safe state
    summary.json                       # Run summary
```
