# Product Requirements Document (PRD)
# YATS — Yet Another Trading System

Version: 1.0
Owner: Ahmed
Date: 2026-02-27
Status: Design Phase

Purpose: Define WHAT YATS must deliver. YATS is a successor to the Quanto trading
platform as an MCP-native system. The existing codebase is referred to as "Quanto" (the predecessor).

Audience: QuantTown agents, Claude Code instances, human developers.

**This document is standalone.** Appendices A-J at the end provide full
implementation specifications for all subsystems (env, rewards, experiment spec,
qualification gates, hierarchical policy, shadow engine, risk engine, regression
methodology, regime detection, feature computation). An implementer can build
YATS from this PRD alone without access to the Quanto codebase.

---

# 1. Vision

YATS is an institutional-grade, end-to-end trading research and execution platform.
It automates the full lifecycle: multi-vendor data ingestion and canonicalization,
deterministic feature computation, reinforcement learning training with shaped rewards,
experiment evaluation and regime-aware qualification, shadow execution with simulated
broker replay, tiered promotion, and paper/live execution under a static risk contract.

YATS rebuilds this system with three structural changes:

1. **MCP-native interface.** A TypeScript MCP server (official SDK) replaces the CLI
   and scripts. QuantTown agents invoke YATS tools via MCP. Humans debug via
   Python notebooks or ad-hoc scripts — no separate CLI layer.

2. **QuestDB as the queryable backbone.** Parquet-on-disk is replaced by QuestDB for
   all time-series and metadata that needs to be queried across experiments, symbols,
   or time ranges. Large artifacts (model checkpoints, rollout series) remain on
   the filesystem, indexed by QuestDB.

3. **Eight architectural improvements** over Quanto:
   - Streaming + batch canonicalization
   - Incremental feature computation with high-water marks
   - Qualification and promotion as Dagster pipelines (not scripts)
   - Unified artifact tables for shadow/paper/live (queryable by mode)
   - Composable experiment specs with inheritance
   - Pluggable regime detection via feature registry
   - Risk engine with simulation mode for research
   - Queryable experiment history, audit trails, and promotion records

Everything else — the experiment lifecycle, canonicalization philosophy, policy
abstractions, reward versioning, determinism guarantees, shadow execution model,
risk policy as static contract — carries over from Quanto because it is well-designed.

---

# 2. What Changed from Quanto (Predecessor)

| Aspect | Quanto | YATS |
|--------|--------|-----|
| Interface | Python CLI + scripts | TypeScript MCP server |
| Storage (time-series) | Parquet on disk | QuestDB |
| Storage (artifacts) | Filesystem (.yats_data/) | Filesystem (unchanged) + QuestDB index |
| Experiment queries | Walk directory tree | SQL against QuestDB tables |
| Canonicalization | Batch only | Batch + streaming append |
| Feature computation | Full recompute | Incremental with high-water marks |
| Qualification | scripts/qualify_experiment.py | Dagster pipeline |
| Promotion | scripts/promote_experiment.py | Dagster pipeline with approval gate |
| Shadow/paper/live artifacts | Separate formats | Unified QuestDB tables with mode column |
| Experiment specs | Standalone JSON | Composable with inheritance |
| Regime detection | Hardcoded v1/v2 | Pluggable via feature registry |
| Risk engine (research) | Static limits only | Simulation mode (overridable thresholds) |
| Data vendors | Polygon | Alpaca + financialdatasets.ai |
| Broker | Alpaca (placeholder) | Alpaca |
| Options | In scope (v1) | Deferred (no vendor) |
| Dashboard | Streamlit-style | Removed (Dagster UI + notebooks) |
| CLI | Python (cli/app.py) | Removed (MCP + notebooks) |

### What Carries Over Unchanged
- Experiment lifecycle: spec -> run -> eval -> qualify -> promote
- Canonicalization philosophy: raw -> canonical, canonical is only downstream input
- Risk policy as static contract (RISK_POLICY.md)
- Policy abstractions (PPO, SAC, SMA, equal-weight, hierarchical controller)
- SignalWeightEnv (old Gym API: reset/step, not Gymnasium)
- Reward versioning (reward_v1 = identity log-return, reward_v2 = multi-component shaped)
- Hierarchical policy (ModeController -> regime-based mode -> per-mode allocator)
- Shadow execution as separate system from backtesting
- Determinism and reproducibility guarantees
- .yats_data/ directory structure for artifacts

### What Gets Dropped from v1 Scope
- Options data ingestion (no vendor coverage — deferred)
- Options-implied features (IV ATM, risk reversals, butterflies, vol surface)
- Internal Greek computation
- Tenor/delta bucketing
- Polygon adapters
- IBKR adapter (was placeholder)

---

# 3. Goals and Non-Goals

## 3.1 Goals

YATS MUST:

1. Expose all platform capabilities as MCP tools callable by QuantTown agents
2. Ingest equity OHLCV from Alpaca and fundamentals/metrics/filings from financialdatasets.ai
3. Maintain raw -> canonical two-layer storage in QuestDB with full lineage
4. Support both batch reconciliation (historical) and streaming append (live)
5. Compute institutional-quality features incrementally with high-water mark tracking
6. Support pluggable regime detection registered through the feature registry
7. Train RL agents (PPO, SAC) with versioned reward shaping in SignalWeightEnv
8. Support all policy types: PPO, SAC, SMA, equal-weight, hierarchical controller
9. Evaluate experiments deterministically with regime-aware metrics
10. Qualify experiments against baselines with hard/soft gates (including regime + execution gates)
11. Promote experiments through research -> candidate -> production tiers with immutable records
12. Run shadow execution (both direct rebalance and sim broker modes) with unified QuestDB output
13. Execute paper trading via Alpaca under RISK_POLICY.md static constraints
14. Provide risk engine simulation mode for research (overridable thresholds)
15. Maintain full audit trail of every tool invocation, pipeline run, and risk decision
16. Orchestrate all pipelines via Dagster (including qualification and promotion)
17. Store experiment metadata and metrics in QuestDB for cross-experiment querying
18. Compose with QuantTown MEOW stack (molecules invoke YATS tools as beads)

## 3.2 Non-Goals

YATS WILL NOT:

- Include options data or options-derived features (no vendor — deferred to v2)
- Provide a GUI, dashboard, or CLI (MCP is the interface; notebooks for debugging)
- Implement portfolio optimization
- Implement AutoML or automated feature discovery
- Support tick-level data or HFT
- Replace QuantTown — YATS is the toolbox, QuantTown is the firm
- Implement dynamic regime-adaptive risk limits (risk is static; regimes affect policy, not limits)

---

# 4. System Architecture

## 4.1 Three-Layer Architecture

**Layer 1: Interface (TypeScript)**
MCP server built on the official @modelcontextprotocol/sdk. Single server, all tools.
Communicates with Layer 2 via Dagster GraphQL API and Python subprocess bridge.

**Layer 2: Orchestration (TypeScript -> Python bridge)**
- Dagster GraphQL client for pipeline triggering, status polling, result retrieval
- Python subprocess runner for ad-hoc computations (quick stats, one-off queries)
- QuestDB client (PG wire protocol for reads, ILP for writes) shared across layers

**Layer 3: Compute and Storage (Python + QuestDB + Filesystem)**
- Dagster pipelines for all orchestrated work
- research/ module tree (envs, training, eval, shadow, promotion, hierarchy, features, execution)
- QuestDB for time-series, metadata, indexes, audit trail
- Filesystem (.yats_data/) for large artifacts (checkpoints, rollout series, specs)

## 4.2 TypeScript to Python Bridge

The MCP server (TypeScript) never runs compute directly. It either:

1. Triggers a Dagster pipeline via GraphQL -> polls for completion -> reads results from QuestDB
2. Runs a Python subprocess for lightweight operations (stat tests, queries, small transforms)

QuestDB is the shared data plane. TypeScript writes audit trail entries and reads results.
Python writes compute outputs (features, metrics, experiment indexes) and reads canonical data.

## 4.3 Why This Split

- MCP official SDK is TypeScript — best protocol support and type safety
- All compute is already Python — RL, NumPy/Pandas, statsmodels, PyTorch
- Dagster is Python — its GraphQL API is the natural bridge
- No need to rewrite compute; just need a proper protocol layer on top

---

# 5. Data Ingestion and Canonicalization

## 5.1 Vendors

### Alpaca (Market Data + Execution)
- Historical OHLCV bars (daily, intraday) via Data API v2 REST
- Real-time bars via WebSocket (streaming to canonical)
- Account data (positions, orders, fills) for execution layer
- Paper + live trading via Trading API

### financialdatasets.ai (Fundamentals + Research Data)
- Financial statements: income, balance sheet, cash flow (annual, quarterly, TTM)
- Financial metrics: 50+ pre-computed ratios (PE, ROE, margins, growth, etc.)
- Earnings data: results, estimates, surprise
- SEC filings: 10-K, 10-Q, 8-K (metadata + section access)
- Insider trades: Form 3/4/5 transactions
- Analyst estimates: consensus forecasts
- Segmented financials: business segment breakdowns
- News: market news with ticker associations

financialdatasets.ai provides shares outstanding via the financial metrics endpoint.
Daily market cap is computed on our side as shares_outstanding * close_price (from
canonical equity OHLCV). This derived daily market cap feeds log_mkt_cap and size_rank.

## 5.2 Raw to Canonical Two-Layer Model (QuestDB)

### Raw Tables (Per-Vendor, Append-Only)

Raw equity OHLCV from Alpaca:
- timestamp (TIMESTAMP), symbol (SYMBOL), open, high, low, close (DOUBLE),
  volume (LONG), vwap (DOUBLE), trade_count (LONG), ingested_at (TIMESTAMP)
- Partitioned by MONTH

Raw fundamentals from financialdatasets.ai:
- report_date (TIMESTAMP), symbol (SYMBOL), fiscal_period (SYMBOL),
  period (SYMBOL), currency (SYMBOL), revenue, cost_of_revenue, gross_profit,
  operating_expense, operating_income, interest_expense, ebit, net_income,
  net_income_common_stock, eps, eps_diluted, weighted_average_shares,
  weighted_average_shares_diluted, dividends_per_share (all DOUBLE),
  ingested_at (TIMESTAMP)
- Partitioned by YEAR

Raw financial metrics from financialdatasets.ai:
- timestamp (TIMESTAMP), symbol (SYMBOL), market_cap, pe_ratio, ps_ratio,
  pb_ratio, ev_ebitda, roe, roa, gross_margin, operating_margin, net_margin,
  fcf_margin, debt_to_equity, current_ratio, revenue_growth_yoy,
  eps_growth_yoy, dividend_yield, shares_outstanding (all DOUBLE),
  ingested_at (TIMESTAMP)
- Partitioned by YEAR

Raw insider trades from financialdatasets.ai:
- filed_at (TIMESTAMP), symbol (SYMBOL), insider_name (STRING),
  insider_title (STRING), transaction_type (SYMBOL), shares, price_per_share,
  total_value, shares_owned_after (all DOUBLE), ingested_at (TIMESTAMP)
- Partitioned by YEAR

Raw analyst estimates from financialdatasets.ai:
- timestamp (TIMESTAMP), symbol (SYMBOL), period (SYMBOL),
  eps_estimate_mean, eps_estimate_high, eps_estimate_low, eps_actual,
  revenue_estimate_mean, revenue_actual (all DOUBLE),
  num_analysts (LONG), ingested_at (TIMESTAMP)
- Partitioned by YEAR

Raw earnings from financialdatasets.ai:
- report_date (TIMESTAMP), symbol (SYMBOL), fiscal_period (SYMBOL),
  eps_actual, eps_estimate, eps_surprise, eps_surprise_pct,
  revenue_actual, revenue_estimate (all DOUBLE), ingested_at (TIMESTAMP)
- Partitioned by YEAR

### Canonical Tables (Reconciled, Only Input Downstream)

Canonical equity OHLCV (the ONLY price input to features):
- timestamp (TIMESTAMP), symbol (SYMBOL), open, high, low, close (DOUBLE),
  volume (LONG), vwap (DOUBLE), trade_count (LONG)
- Lineage columns: source_vendor (SYMBOL), reconcile_method (SYMBOL),
  validation_status (SYMBOL), canonicalized_at (TIMESTAMP)
- Partitioned by MONTH

Canonical fundamentals (point-in-time):
- report_date (TIMESTAMP), symbol (SYMBOL), fiscal_period (SYMBOL),
  period (SYMBOL), revenue, cost_of_revenue, gross_profit, operating_expense,
  operating_income, net_income, eps, eps_diluted, shares_outstanding,
  shares_outstanding_diluted, free_cash_flow, total_assets, total_liabilities,
  shareholder_equity (all DOUBLE)
- Lineage columns: source_vendor, reconcile_method, canonicalized_at
- Partitioned by YEAR

Canonical financial metrics (daily time-series):
- timestamp (TIMESTAMP), symbol (SYMBOL), market_cap, pe_ratio, ps_ratio,
  pb_ratio, ev_ebitda, roe, gross_margin, operating_margin, net_margin,
  fcf_margin, debt_to_equity, revenue_growth_yoy, eps_growth_yoy,
  shares_outstanding (all DOUBLE)
- Lineage columns: source_vendor, canonicalized_at
- Partitioned by YEAR

### Reconciliation Rules (from DATA_SPEC.md, adapted)

Equity OHLCV:
- Primary vendor: Alpaca (configurable)
- Fallback: only if primary is missing or fails validation
- Validation: missing fields, non-positive prices, extreme outliers, timestamp gaps
- No averaging — canonical bar comes from a single vendor per symbol-day
- Vendor choice logged per row (source_vendor column)

Fundamentals:
- Primary vendor: financialdatasets.ai (sole source in v1)
- Latest filing supersedes prior values
- Point-in-time semantics preserved (report_date = when filed, not fiscal period end)

Financial Metrics:
- Primary vendor: financialdatasets.ai (sole source in v1)
- Forward-filled to daily frequency with as-of semantics (no lookahead)

## 5.3 Batch + Streaming Canonicalization (YATS Improvement)

**Batch mode** (historical backfill):
- Dagster pipeline reads from raw tables, applies reconciliation rules, writes to canonical
- Same deterministic reconciliation as Quanto, now against QuestDB instead of Parquet

**Streaming mode** (live data):
- Alpaca WebSocket bars arrive -> validate -> append directly to canonical
- Single vendor in streaming, so reconciliation is trivial (validate only)
- Streaming writes use QuestDB ILP (InfluxDB Line Protocol) for high throughput
- Feature pipeline triggered incrementally on new data arrival

## 5.4 Reconciliation Diagnostics

Reconciliation log table:
- timestamp (TIMESTAMP), domain (SYMBOL), symbol (SYMBOL),
  primary_vendor (SYMBOL), fallback_used (BOOLEAN), fallback_vendor (SYMBOL),
  validation_warnings (STRING as JSON array), dagster_run_id (STRING),
  reconciled_at (TIMESTAMP)
- Partitioned by MONTH

## 5.5 Vendor Failure Handling

**Alpaca (market data):**
- REST API failures: exponential backoff retry (3 attempts, 1s/5s/30s).
  After max retries, Dagster job fails with explicit error.
- WebSocket disconnection: automatic reconnect with exponential backoff.
  If reconnect fails for >5 minutes, streaming canonicalization pauses and
  an alert is emitted to audit trail. Paper/live trading continues with
  stale data guard (§12.1 data integrity rule halts trading if data is stale).
- Rate limits: Dagster pipeline respects Alpaca's rate limits via throttling.
  Ingest jobs for large universes use configurable concurrency limits.

**financialdatasets.ai (fundamentals):**
- REST API failures: same exponential backoff retry strategy.
- Rate limits: configurable request-per-second throttle in vendor adapter.
- Stale data: fundamental data changes slowly (quarterly). The ingestion
  pipeline records last_successful_ingest per domain in QuestDB. If a refresh
  fails, the system continues with existing data and logs a warning.
- No fallback vendor for fundamentals in v1 (sole source).

---

# 6. Feature Engine

## 6.1 Feature Registry

Features are organized into named feature sets registered in the feature registry.
Each feature set defines which features are computed and their dependencies.

**Full specification: Appendix J** (exact formulas for every feature, lookback
windows, cross-sectional computation, fundamental feature sources, label
definitions, global computation rules).

YATS replaces the Quanto hardcoded feature registry with a config-driven
feature registry:

- Feature sets are defined in YAML configs (configs/feature_sets/core_v1.yml,
  configs/feature_sets/core_v1_regime.yml) that declare which features are included,
  their dependencies (which canonical tables they need), lookback windows, and
  computation order.
- Each individual feature has a Python implementation registered by name via decorator
  (e.g. @feature("ret_1d") on the function that computes it).
- The registry loads YAML definitions at startup, resolves each feature name to its
  Python implementation, validates that all dependencies are satisfiable.
- Adding a new feature = write the Python function + add it to a YAML feature set.
  No touching registry code.
- YAML definitions are part of experiment spec lineage (hashable for reproducibility).

### v1 Feature Sets (Equities + Fundamentals)

**A. OHLCV Context Features**
- ret_1d, ret_5d, ret_21d — log returns (1-day, 5-day, 21-day)
- rv_21d, rv_63d — realized volatility (21-day, 63-day)
- dist_from_20d_high, dist_from_20d_low — distance from rolling extremes

**B. Cross-Sectional Factors**
- mom_3m, mom_12m_excl_1m — momentum signals
- log_mkt_cap — log market capitalization
- size_rank — cross-sectional size percentile
- value_rank — cross-sectional value percentile

**C. Fundamental Factors**
- Valuation: pe_ttm, ps_ttm, pb, ev_ebitda
- Quality: roe, gross_margin, operating_margin, fcf_margin, debt_equity
- Growth: eps_growth_1y, revenue_growth_1y

**D. Regime Features** (market-level, attached to each row when enabled)
- market_vol_20d — 20-day market volatility
- market_trend_20d — 20-day market trend
- dispersion_20d — 20-day cross-sectional dispersion
- corr_mean_20d — 20-day average correlation

Regime feature universe: configurable, default is fixed primary (SPY, QQQ, IWM)
for stability (regime_v1_1 behavior).

**Deferred to v2:** Options-implied sentiment (IV ATM, term slopes, risk reversals,
butterflies, IV-RV spread) and volatility surface features (surface_lv, term_curvature,
smile_asym).

### Global Computation Rules (from FEATURE_SPEC.md)
- UTC timestamps only
- No forward-fill for price data
- Fundamentals forward-filled until next report (point-in-time / as-of semantics)
- Winsorize 1st-99th percentile per symbol
- Cross-sectional features z-scored within each date

## 6.2 Feature Storage (QuestDB)

**Schema Evolution Strategy:**
The features table has explicit columns for all v1 features. When new features
are added via the pluggable registry, the schema must evolve. QuestDB supports
ALTER TABLE ADD COLUMN but not remove or rename. Strategy:

- Minor additions (new feature column): ALTER TABLE ADD COLUMN, backfill via
  recompute pipeline. Existing columns unaffected.
- Major changes (remove/rename/restructure): Table recreation. Create new table
  with updated schema, migrate data via recompute pipeline, swap table names.
  Old table retained as backup until validated.
- Feature set version tracks schema version. Queries always filter by
  feature_set_version to avoid mixing schema generations.

Features table:
- timestamp (TIMESTAMP), symbol (SYMBOL), feature_set (SYMBOL),
  feature_set_version (SYMBOL)
- OHLCV-derived: ret_1d, ret_5d, ret_21d, rv_21d, rv_63d,
  dist_20d_high, dist_20d_low (all DOUBLE)
- Cross-sectional: mom_3m, mom_12m_excl_1m, log_mkt_cap,
  size_rank, value_rank (all DOUBLE)
- Fundamental: pe_ttm, ps_ttm, pb, ev_ebitda, roe, gross_margin,
  operating_margin, fcf_margin, debt_equity, eps_growth_1y,
  revenue_growth_1y (all DOUBLE)
- Regime (NULL when not enabled): market_vol_20d, market_trend_20d,
  dispersion_20d, corr_mean_20d (all DOUBLE)
- Metadata: computed_at (TIMESTAMP)
- Partitioned by MONTH

Feature watermarks table (for incremental computation):
- updated_at (TIMESTAMP), symbol (SYMBOL), feature_set (SYMBOL),
  last_computed_date (TIMESTAMP), feature_set_version (SYMBOL)

## 6.3 Incremental Feature Computation (YATS Improvement)

Quanto recomputes features for entire date ranges. YATS computes incrementally:

1. Feature pipeline reads feature_watermarks for each symbol + feature set
2. Fetches canonical data from last_computed_date forward
3. Computes features only for new data
4. Appends to features table
5. Updates feature_watermarks

For features with lookback windows (e.g. rv_63d), the pipeline fetches
lookback_window + new_data from canonical but only emits features for the new dates.

**Cross-Sectional Feature Exception:**
Cross-sectional features (size_rank, value_rank, z-scored features) require the
entire universe on each date to compute rankings and z-scores. These CANNOT be
computed per-symbol incrementally. The incremental pipeline handles this by:

1. Per-symbol features (ret_1d, rv_21d, etc.) are computed incrementally per symbol.
2. Cross-sectional features are computed as a full-universe batch per date.
   The watermark for cross-sectional features is universe-level, not per-symbol.
3. If a new symbol is added to the universe, all cross-sectional features must be
   recomputed for all dates where the new symbol has data (full recompute triggered
   for cross-sectional features only; per-symbol features are unaffected).

Full recomputation is still available (ignores watermarks) for version bumps or
data corrections.

## 6.4 Pluggable Regime Detection (YATS Improvement)

Quanto hardcodes v1 (quantile-based) and v2 (threshold-based) regime labeling.
YATS makes regime detection pluggable through the feature registry:

- Regime detectors are registered by name (e.g. regime_v1, regime_v1_1, regime_v2)
- The experiment spec references the regime detector by name
- The feature registry resolves it at runtime

YATS unifies regime feature computation and regime labeling into a single
registerable regime detector. Regime detectors are registered in
configs/regime_detectors/ (same pattern as feature sets) and each detector
provides both compute() for feature generation and label() for evaluation bucketing.

**Full specification: Appendix I** (regime feature formulas, v1/v2 labeling
rules, threshold config, pluggable registration format).

---

# 7. RL Environment and Training

## 7.1 SignalWeightEnv

The environment is unchanged from Quanto. It is a minimal RL-style env
(old Gym API: reset() -> obs, step(action) -> (obs, reward, done, info)).

**Full specification: Appendix A** (observation vector construction, action
processing, step execution, info dict keys, ExecutionSimulator details).

**Observation vector construction:**
1. Per-symbol features from observation_columns (must include close)
2. Regime features appended (if enabled in experiment spec)
3. Previous weights appended (always)

**Action space:**
- Target portfolio weights (scalar for single asset, vector for multi-asset)
- Clipped to action_clip
- Projected with risk constraints via project_weights

**Reward:**
- Base: log(next_value / prev_value) after transaction costs (and execution sim if enabled)
- Wrapped by RewardAdapterEnv during PPO/SAC training for versioned reward shaping

**Execution sim (optional):**
- If execution_sim is enabled in config, a deterministic ExecutionSimulator
  resolves realized weights within the env step

The env receives pre-built rows (features already computed) from the runner/training
scripts. It does not read features directly from QuestDB. The runner pre-fetches
feature data from QuestDB and passes it into the env as arrays/DataFrames.

## 7.2 Training

### PPO Trainer
- Wraps SignalWeightEnv with RewardAdapterEnv
- Reward version controlled by policy_params.reward_version in experiment spec
- Saves model checkpoints to .yats_data/experiments/<ID>/runs/
- Config-driven hyperparameters (learning rate, batch size, epochs, etc.)

### SAC Trainer
- Same env wrapping and reward versioning as PPO
- Different algorithm implementation

### Reward Versions

**reward_v1:** Identity — base log-return passed through unchanged.

**reward_v2:** Multi-component shaped reward.
**Full specification: Appendix B** (exact formulas, default scaling factors,
clipping behavior, info dict extensions).
- Starts from base log-return
- Subtracts turnover penalty (L1 weight change, scaled)
- Subtracts drawdown increase penalty (scaled)
- Optional cost penalty (default 0.0, off unless configured)
- Optional clipping
- Emits reward_components in info dict

reward_v1 and reward_v2 are the complete set for YATS v1. Additional reward versions
will be added in future releases but are out of scope.

## 7.3 Policies

The system supports multiple policy types, all loadable by name during
shadow replay, evaluation, and live execution:

- PPO checkpoint: Trained PPO model loaded from checkpoint file
- SAC checkpoint: Trained SAC model loaded from checkpoint file
- SMA weight policy: Simple moving average-based weight generation
- Equal-weight: Uniform allocation across universe
- Hierarchical controller: ModeController + per-mode allocators

### Hierarchical Policy
- ModeController uses regime features to select a mode (e.g. defensive, risk_on)
- Each mode has its own allocator that produces target weights
- Mode transitions recorded in evaluation/mode_timeline.json

**Full specification: Appendix E** (mode definitions, decision logic, update
cadence, hold periods, allocator interface contract, transition handling).

---

# 8. Experiment Lifecycle

## 8.1 Experiment Spec (Canonical Configuration)

The ExperimentSpec is the single source of truth for what was run.
Serialized as spec/experiment_spec.json. The experiment ID is derived
from the canonical JSON (content-addressed).

**Full specification: Appendix C** (all field types, defaults, validation
rules, sub-configs, hierarchy validation, ID derivation procedure).

ExperimentSpec fields (from research/experiments/spec.py):

- experiment_name: human-readable name
- symbols: universe ticker list
- start_date, end_date: data window
- interval: bar frequency
- feature_set: registered feature set name
- policy: policy type (ppo, sac, sma, equal_weight, hierarchical)
- policy_params: policy-specific config (includes reward_version, learning_rate, etc.)
- cost_config: transaction_cost_bp, optional slippage_bp
- seed: for reproducibility
- evaluation_split: train_ratio, test_ratio, optional test_window_months
- risk_config: risk thresholds (overridable for simulation mode)
- execution_sim: ExecutionSimConfig (optional)
- notes: free-text
- regime_feature_set: registered regime detector name (optional)
- regime_labeling: labeling version (v1 or v2)
- hierarchy_enabled: boolean
- controller_config: ModeController config (optional)
- allocator_by_mode: per-mode allocator definitions (optional)

Additional frozen hashes (set at experiment creation time, see §24.4):
- regime_thresholds_hash: SHA256 of configs/regime_thresholds.yml
- regime_detector_version: version string of the registered detector
- regime_universe: explicit list of symbols used for regime computation
- feature_set_yaml_hash: SHA256 of the feature set YAML definition

The experiment ID is derived from the canonical JSON serialization of these fields.

The evaluation_split field, if set in the experiment spec, overrides the default
train-test split. This allows researchers to experiment with different split
configurations per experiment without modifying global defaults.

### Composable Specs (YATS Improvement)

Quanto experiment specs are standalone JSON. YATS adds spec inheritance
for sweep ergonomics:

A spec can include an "extends" field pointing to a base spec, plus "overrides"
that replace specific fields. The runner resolves inheritance at experiment creation
time and produces a fully-materialized experiment_spec.json (no extends reference
remaining). This preserves the Quanto guarantee that experiment_spec.json is
self-contained.

**Base Spec Content Hashing:**
At experiment creation time, the base spec's content is hashed (SHA256) and recorded
in the materialized spec as `_base_spec_hash`. This ensures that if the base spec
file is later modified, existing experiments are unaffected (their materialized spec
is already frozen), and any new experiments created from the modified base will
produce a different experiment ID (because the materialized content differs).

Sweep configs (configs/sweeps/) become a set of override dicts applied to a base spec.

## 8.2 Experiment Registry and Artifacts

Each experiment creates a directory under .yats_data/experiments/<EXPERIMENT_ID>/:

```
.yats_data/experiments/<EXPERIMENT_ID>/
  spec/
    experiment_spec.json         # Canonical config (source of truth)
  runs/
    rollout.json                 # Metadata, series, inputs_used, perf/trading/safety
    <checkpoint files>           # Model checkpoints (PPO/SAC)
  evaluation/
    metrics.json                 # Core metrics with regime metadata
    mode_timeline.json           # Mode transitions (hierarchical only)
    timeseries.json              # Optional: full time-series output
    regime_slices.json           # Optional: per-regime performance slices
  logs/
    run_summary.json             # Run pointers and metadata
  promotion/
    qualification_report.json    # Written after qualification runs
```

### QuestDB Experiment Index (YATS Improvement)

In addition to the filesystem artifacts, YATS writes an experiment index to QuestDB
for cross-experiment querying:

experiment_index table:
- created_at (TIMESTAMP), experiment_id (SYMBOL)
- From spec: universe (STRING), feature_set (SYMBOL), policy_type (SYMBOL),
  reward_version (SYMBOL), regime_feature_set (SYMBOL), regime_labeling (SYMBOL)
- From evaluation/metrics.json: sharpe, calmar, max_drawdown, total_return,
  annualized_return, win_rate, turnover_1d_mean (all DOUBLE)
- Validation: nan_inf_violations (LONG)
- Promotion status: qualification_status (SYMBOL), promotion_tier (SYMBOL)
- Pointers: spec_path, metrics_path, dagster_run_id (all STRING)
- Partitioned by YEAR

This enables queries like:
SELECT * FROM experiment_index
WHERE universe = 'sp500' AND sharpe > 1.0 AND qualification_status = 'passed'
ORDER BY sharpe DESC;

## 8.3 Evaluation

Deterministic evaluation producing evaluation/metrics.json with:

- metadata: experiment_id, spec path, data range, feature set version
- performance: sharpe, calmar, sortino, total_return, annualized_return,
  max_drawdown, max_drawdown_duration
- trading: turnover_1d_mean, turnover_1d_std, avg_holding_period, win_rate, profit_factor
- safety: nan_inf_violations, constraint violations
- performance_by_regime: per-regime-bucket performance metrics
- regime: regime metadata (labeling version, thresholds used)
- series: equity curve, drawdown series (or pointers to timeseries.json)
- inputs_used: data hash, feature versions, canonical data range
- config: embedded experiment spec summary

---

# 9. Shadow Execution

Shadow execution is a SEPARATE system from backtesting. It is a forward-only,
deterministic replay of canonical historical data through a policy and (optionally)
a simulated execution layer.

**Full specification: Appendix F** (snapshot schema, data loading, execution
flow for both modes, resume state format).

## 9.1 Shadow Architecture

ReplayMarketDataSource builds per-date snapshots from canonical equity data
and feature panels. ShadowEngine loads the policy from experiment spec and
produces target weights each step. Then one of two execution paths:

**execution_mode=none (default):**
- Direct portfolio rebalance at snapshot prices
- Applies transaction_cost_bp from the spec
- No order lifecycle, no broker, just deterministic reweighting

**execution_mode=sim (market simulation):**
- ExecutionController compiles orders from target weights (OrderCompiler)
- ExecutionRiskEngine runs risk checks
- SimBrokerAdapter fills deterministically at snapshot price with slippage/fees
- Execution metrics recorded: fill rate, reject rate, slippage, fees, turnover, halts, regime-bucket stats

## 9.2 Shadow Artifacts (Filesystem)

```
.yats_data/shadow/<EXPERIMENT_ID>/<RUN_ID>/
  logs/
    steps.jsonl                  # Per-step execution log
  state.json                     # Resume-safe state
  summary.json                   # Run summary
  execution_metrics.json         # Execution metrics (sim mode only)
  metrics_sim.json               # Simulated performance metrics
```

## 9.3 Unified Execution Tables (YATS Improvement)

In Quanto, shadow, paper, and live execution produce different artifact formats.
YATS writes all execution activity to unified QuestDB tables with a mode column:

execution_log table:
- timestamp (TIMESTAMP), experiment_id (SYMBOL), run_id (SYMBOL),
  mode (SYMBOL: shadow/paper/live), step (INT), symbol (SYMBOL),
  target_weight (DOUBLE), realized_weight (DOUBLE), fill_price (DOUBLE),
  slippage_bps (DOUBLE), fees (DOUBLE), rejected (BOOLEAN),
  reject_reason (STRING), portfolio_value (DOUBLE), cash (DOUBLE),
  regime_bucket (SYMBOL), dagster_run_id (STRING)
- Partitioned by MONTH

execution_metrics table (per-run summary):
- timestamp (TIMESTAMP), experiment_id (SYMBOL), run_id (SYMBOL),
  mode (SYMBOL), fill_rate (DOUBLE), reject_rate (DOUBLE),
  avg_slippage_bps (DOUBLE), p95_slippage_bps (DOUBLE),
  total_fees (DOUBLE), total_turnover (DOUBLE), execution_halts (LONG),
  sharpe (DOUBLE), max_drawdown (DOUBLE), total_return (DOUBLE),
  dagster_run_id (STRING)
- Partitioned by YEAR

This enables direct comparison:
SELECT mode, sharpe, max_drawdown, fill_rate, avg_slippage_bps
FROM execution_metrics WHERE experiment_id = 'exp_abc123' ORDER BY mode;

## 9.4 Promotion Gate on Shadow

Shadow execution is gated: only promoted or allowlisted experiments can run shadow.

**Qualification-Replay Bypass:**
The qualification pipeline (dagster.qualify) has implicit authority to trigger
shadow replay for ANY experiment being qualified, regardless of promotion status.
This resolves the chicken-and-egg problem: qualification requires shadow execution
evidence, but shadow requires promotion, and promotion requires qualification.

Mechanism:
- When dagster.qualify runs and finds no execution evidence for the candidate,
  it triggers dagster.shadow_run internally with a special flag
  (qualification_replay=true) that bypasses the promotion gate.
- The shadow run is tagged as qualification_replay in QuestDB execution tables.
- This bypass is ONLY available to the qualification pipeline — agents cannot
  invoke shadow.run with qualification_replay directly.
- The risk_officer role can also trigger shadow for non-promoted experiments
  via explicit allowlisting (adding experiment_id to qualification allowlist config).

## 9.5 Risk Controls During Simulation

The ExecutionRiskEngine enforces during execution_mode=sim:
- Max gross exposure
- Min cash
- Max symbol weight
- Max daily turnover
- Max active positions
- Daily loss limit
- Trailing drawdown
- Can halt execution (recorded as execution halt)

---

# 10. Qualification

Qualification is a deterministic gating step that compares a candidate experiment
against a baseline. It produces promotion/qualification_report.json.

**Full specification: Appendix D** (gate evaluation order, regression methodology,
all threshold defaults, Phase 1 baseline==candidate behavior).
**Appendix H** (regression gate comparison methodology in detail).

## 10.1 Qualification as Dagster Pipeline (YATS Improvement)

Quanto runs qualification via scripts/qualify_experiment.py.
YATS runs it as a Dagster job with full observability.

## 10.2 Gates

### Hard Gates (Fail = Block Promotion)

- Sharpe regression: must not degrade >5% vs baseline
- NaN/Inf violations: must be zero
- Safety constraint violations: must be zero
- Required artifacts present: spec, metrics.json, run_summary.json
- Execution evidence: must have execution metrics (or shadow replay attempted)
- No execution halts: zero halts during shadow replay
- Reject rate: <= 1%
- Fill rate: >= 99%
- P95 slippage: <= 25 bps
- High-vol drawdown regression: must not worsen vs baseline
- Execution drawdown guard: must not breach threshold

### Soft Gates (Fail = Warning, Does Not Block)

- Max drawdown regression: must not worsen >10% vs baseline
- Turnover regression: must not worsen >15% vs baseline
- Sharpe not improved: warning if no improvement
- Stability regressions: turnover std, mode churn, cost curve span
- High-vol turnover increase: warning
- Hierarchy sanity: warning if applicable
- Defensive exposure cap: if hierarchy enabled
- Baseline == candidate: delta gates skipped, flagged as warning

### Soft Execution Deltas vs Baseline
- Average slippage delta
- Total fees delta
- Turnover drift
- Execution metrics correlation check

## 10.3 Qualification Report

Contains: experiment_id, baseline_id, passed (boolean), hard_gates (array with
gate name, passed, value, baseline, threshold), soft_gates (same format),
execution_gates, regime_gates, warnings, timestamp, dagster_run_id.

---

# 11. Promotion

## 11.1 Tiered Promotion

research -> candidate -> production

Each tier is an explicit, immutable record written only if qualification passed.

## 11.2 Promotion as Dagster Pipeline (YATS Improvement)

Quanto runs promotion via scripts/promote_experiment.py.
YATS runs it as a Dagster job. The production tier requires
human approval (Managing Partner) via MCP tool with requires_approval flag.

## 11.3 Promotion Records

Filesystem: .yats_data/promotions/<tier>/<EXPERIMENT_ID>.json

Contains: experiment_id, tier, qualification_report_path, spec_path,
metrics_path, promotion_reason, promoted_at, promoted_by, dagster_run_id.

QuestDB promotions table:
- promoted_at (TIMESTAMP), experiment_id (SYMBOL), tier (SYMBOL),
  promoted_by (SYMBOL), qualification_passed (BOOLEAN),
  sharpe (DOUBLE), max_drawdown (DOUBLE), dagster_run_id (STRING)
- Partitioned by YEAR

Immutability: if a promotion record exists for the same experiment + tier with
different content, the system errors. No overwrites.

---

# 12. Risk Engine

## 12.1 Static Contract (RISK_POLICY.md — Unchanged)

The risk policy is model-agnostic, non-learnable, enforced at runtime.
No strategy, model, or RL agent may override it.

**Full specification: Appendix G** (exact constraint evaluation order,
all 15 constraints with size_reduce logic, evaluation interactions,
all default thresholds from configs/risk.yml).

Capital and Exposure: max gross/net exposure, max leverage
Position-Level: max position % NAV, concentration limits, ADV participation
Liquidity: max daily turnover
Drawdown: daily loss limit, rolling drawdown limit
Volatility: vol scaling (effective_size = base × TARGET_VOL / current_vol), vol regime brakes
Signal: confidence gating, min holding period
Operational: data integrity checks, broker failure limits, risk engine self-check
Kill Switches: automatic (drawdown, daily loss, data, broker) + manual (halt, flatten)
Size Reduce: when a constraint would reject, attempt to reduce order size to the
maximum that satisfies the constraint (if above minimum_order_threshold)

Enforcement layers: pre-signal validation -> pre-order checks -> post-fill verification.
Evaluation order: kill switches → vol brakes (adjust limits) → vol scaling (adjust sizes) →
global limits → per-symbol limits → signal constraints → cash floor.

YATS implements the FULL RISK_POLICY.md. Quanto only implemented kill switches,
gross exposure, turnover, symbol weight, active positions, and min cash. YATS
adds: net exposure, leverage, concentration, ADV, vol scaling, vol brakes, confidence
gating, min holding period, and size_reduce as a decision type.

Thresholds defined in configs/risk.yml. Risk engine refuses to start if config incomplete.

Every risk decision logged to QuestDB risk_decisions table:
- timestamp (TIMESTAMP), rule_id (SYMBOL), experiment_id (SYMBOL),
  mode (SYMBOL), input_metrics (STRING as JSON), decision (SYMBOL:
  pass/reject/halt/size_reduce), action_taken (STRING),
  original_size (DOUBLE or NULL), reduced_size (DOUBLE or NULL),
  dagster_run_id (STRING)
- Partitioned by MONTH

## 12.2 Risk Simulation Mode (YATS Improvement)

During research only (not paper or live), the experiment spec can override
risk thresholds to explore the risk-return frontier.

Rules:
- Risk overrides are ONLY applied when mode = shadow with execution_mode = sim
- Paper and live trading ALWAYS use configs/risk.yml (the static contract)
- Overrides are logged in the experiment spec and audit trail
- Qualification gates still apply against the baseline risk config

Risk simulation mode is available in BOTH training and shadow:

- During training: risk_config overrides in the experiment spec are passed to
  the env's project_weights, allowing agents to learn under different risk regimes
  (e.g. tighter drawdown limits produce more conservative policies).
- During shadow (execution_mode=sim): same overrides apply to ExecutionRiskEngine.
- Paper and live trading ALWAYS use configs/risk.yml. No exceptions.

---


# VERIFY ITEMS — ALL RESOLVED

1. financialdatasets.ai provides shares outstanding; daily market cap computed as shares * close price
2. financialdatasets.ai provides fundamentals/metrics only; all OHLCV from Alpaca exclusively
3. Feature registry upgraded to config-driven YAML + decorator-registered Python implementations
4. Regime feature computation and labeling unified into single registerable detector
5. Env receives pre-built feature rows from runner; no direct QuestDB access
6. reward_v1 and reward_v2 are the complete set for YATS v1
7. Full ExperimentSpec fields documented (19 fields including hierarchy and regime config)
8. Risk simulation mode available in both training (project_weights) and shadow (ExecutionRiskEngine)
9. Training always via experiment.run; standalone training.* tools removed

---

# 13. Execution (Paper and Live)

## 13.1 Signal Interface (from EXECUTION_SPEC.md)

Models output target position signals (percentage of NAV), not raw orders.

Signal schema:
- timestamp: bar-aligned UTC timestamp
- symbol: ticker
- target_position_pct: target as fraction of NAV (e.g. -0.3 to +0.3)
- confidence_score: optional, used for confidence gating
- model_version: policy identifier

Signals are bar-aligned, forward-looking, generated after bar close.

## 13.2 Order Lifecycle

```
Signal -> Order Translator -> Risk Engine -> Broker Adapter -> Fill -> Ledger
```

**Order Translator:**
- desired_notional = NAV * target_position_pct
- order_notional = desired_notional - current_position_notional
- Market orders only (v1)
- Executed at next bar open or broker market price
- Orders netted per symbol — one open position per symbol

**Risk Engine:** Full RISK_POLICY.md enforcement (see Section 12.1).
Every order passes through pre-order checks. Post-fill verification confirms
the resulting portfolio state doesn't breach any constraint.

**Broker Adapter (Alpaca):**
- Authentication: APCA-API-KEY-ID + APCA-API-SECRET-KEY from environment
- Paper endpoint: paper-api.alpaca.markets
- Live endpoint: api.alpaca.markets
- Paper/live separation enforced by config — the system refuses to use the live
  endpoint unless the experiment is promoted to production tier
- Responsibilities: order submission, status polling, fill handling, error normalization
- WebSocket connection for real-time bar streaming (feeds into streaming canonicalization)

## 13.3 Slippage and Cost Model

**Paper/Live:** Actual broker fills (real slippage from Alpaca).

**Shadow (execution_mode=sim):** Deterministic slippage model:
- SimBrokerAdapter fills at snapshot price + configurable slippage
- Slippage model configurable in experiment spec (flat bps or volume-scaled)
- Transaction costs applied as configurable basis points

**Training (env):** transaction_cost_bp applied directly to portfolio value changes.
If execution_sim enabled, ExecutionSimulator resolves within env step.

## 13.4 State Management

### QuestDB Ledger Tables

**State Reconstruction on Crash:**
Paper/live trading processes may crash and restart. State is reconstructed from
QuestDB using materialized view patterns:

- **Current positions**: Derived by aggregating all fills from the orders table:
  ```sql
  SELECT symbol, SUM(CASE WHEN side='buy' THEN fill_quantity ELSE -fill_quantity END) as quantity
  FROM orders
  WHERE experiment_id = '<ID>' AND mode = '<mode>' AND status = 'filled'
  LATEST ON timestamp PARTITION BY symbol
  ```
  A snapshot table (positions) is also maintained — updated after each fill —
  for fast reads. On restart, the snapshot is validated against the aggregated
  fills. If mismatch, the aggregation is authoritative.

- **NAV and portfolio state**: Reconstructed from the most recent portfolio_state
  row plus any fills that occurred after it.

- **Pending orders**: Queried from Alpaca's API on restart (source of truth for
  broker-side state). Reconciled against the orders table.

Positions table:
- timestamp (TIMESTAMP), experiment_id (SYMBOL), mode (SYMBOL: shadow/paper/live),
  symbol (SYMBOL), quantity (DOUBLE), avg_entry_price (DOUBLE),
  notional (DOUBLE), unrealized_pnl (DOUBLE), realized_pnl (DOUBLE)
- Partitioned by MONTH

Orders table:
- timestamp (TIMESTAMP), order_id (STRING), experiment_id (SYMBOL),
  mode (SYMBOL), symbol (SYMBOL), side (SYMBOL: buy/sell),
  quantity (DOUBLE), order_type (SYMBOL: market),
  status (SYMBOL: submitted/filled/rejected/cancelled),
  fill_price (DOUBLE), fill_quantity (DOUBLE),
  slippage_bps (DOUBLE), fees (DOUBLE),
  risk_check_result (SYMBOL: pass/reject/size_reduce),
  risk_check_details (STRING as JSON),
  broker_order_id (STRING),
  dagster_run_id (STRING)
- Partitioned by MONTH

Portfolio state table (NAV snapshots):
- timestamp (TIMESTAMP), experiment_id (SYMBOL), mode (SYMBOL),
  nav (DOUBLE), cash (DOUBLE), gross_exposure (DOUBLE),
  net_exposure (DOUBLE), leverage (DOUBLE),
  num_positions (INT), daily_pnl (DOUBLE),
  peak_nav (DOUBLE), drawdown (DOUBLE),
  dagster_run_id (STRING)
- Partitioned by MONTH

## 13.5 Kill Switches

**Automatic kill switches** (trigger trading halt immediately):
- Daily loss breach (daily_pnl < MAX_DAILY_LOSS)
- Rolling drawdown breach (drawdown > MAX_DRAWDOWN)
- Data integrity violation (stale data, missing bars, failed canonicalization)
- Broker instability (consecutive errors > MAX_BROKER_ERRORS)
- Risk engine failure (cannot initialize or validate state)

**Manual kill switches** (via MCP tools):
- risk.halt_trading: immediate halt, no new orders
- risk.flatten_positions: close all positions at market (emergency)

**Kill switch state** stored in QuestDB:

Kill switches table:
- timestamp (TIMESTAMP), experiment_id (SYMBOL), mode (SYMBOL),
  trigger (SYMBOL: drawdown/daily_loss/data_integrity/broker/manual),
  action (SYMBOL: halt/flatten), triggered_by (SYMBOL: system/agent_id),
  details (STRING as JSON), resolved (BOOLEAN),
  resolved_at (TIMESTAMP), resolved_by (SYMBOL)
- Partitioned by YEAR

Resuming after a kill switch requires explicit action via risk.resume_trading,
which is gated to managing_partner approval for production mode.

---

# 14. MCP Tool Layer

## 14.1 Design Principles

- Every tool maps to a real system capability (no wrappers around wrappers)
- Tools either trigger a Dagster pipeline (long-running) or run a Python subprocess/QuestDB query (fast)
- Long-running tools return a run_id for status polling
- All tool invocations are logged to the audit trail
- Tool inputs/outputs are typed (TypeScript interfaces in the MCP server)

**Tool Versioning:**
- Tools are versioned: yats.data.ingest_v1, yats.data.ingest_v2, etc.
- The MCP server exposes all supported versions simultaneously.
- New parameters are always optional with defaults in minor versions.
- Breaking changes (removed parameters, changed return types) require a new
  major version.
- Deprecated tool versions emit a warning in the audit trail but remain functional
  for at least one release cycle.
- QuantTown molecules reference tool versions explicitly in bead definitions
  to prevent silent breakage.

## 14.2 Tool Catalog

### yats.data.* (Data Ingestion and Query)

| Tool | Description | Backend | Returns |
|------|-------------|---------|---------|
| data.ingest | Ingest historical data for ticker(s) from vendor | Dagster pipeline | run_id |
| data.ingest_universe | Bulk ingest for entire universe | Dagster pipeline | run_id |
| data.refresh | Incremental update (latest data since last ingest) | Dagster pipeline | run_id |
| data.canonicalize | Run reconciliation for domain + date range | Dagster pipeline | run_id |
| data.verify | Data quality checks on canonical data | Python subprocess | validation report |
| data.query | Query canonical data (SQL passthrough to QuestDB) | Direct QuestDB | result rows |
| data.coverage | Report data coverage gaps per symbol | Direct QuestDB | coverage report |
| data.vendors | List available vendors and their capabilities | Static config | vendor list |

### yats.features.* (Feature Computation)

| Tool | Description | Backend | Returns |
|------|-------------|---------|---------|
| features.compute | Compute features for universe + date range | Dagster pipeline | run_id |
| features.compute_incremental | Compute only new data since watermark | Dagster pipeline | run_id |
| features.list | List registered feature sets and versions | Python subprocess | feature set list |
| features.stats | Summary statistics for a feature | Direct QuestDB | stats |
| features.correlations | Correlation matrix across features | Python subprocess | matrix |
| features.coverage | Coverage report (missing values, date range) | Direct QuestDB | coverage |
| features.watermarks | Show high-water marks per symbol/feature set | Direct QuestDB | watermark table |

### yats.experiment.* (Experiment Management)

| Tool | Description | Backend | Returns |
|------|-------------|---------|---------|
| experiment.create | Create experiment from spec (or base + overrides) | Python subprocess | experiment_id |
| experiment.run | Run experiment (train + evaluate) | Dagster pipeline | run_id |
| experiment.list | List experiments with optional filters | Direct QuestDB | experiment index rows |
| experiment.get | Get full experiment details (spec, metrics, status) | QuestDB + filesystem | experiment detail |
| experiment.compare | Side-by-side comparison of two experiments | Python subprocess | comparison report |

### yats.eval.* (Evaluation)

| Tool | Description | Backend | Returns |
|------|-------------|---------|---------|
| eval.run | Evaluate an experiment (deterministic) | Dagster pipeline | run_id |
| eval.metrics | Fetch evaluation metrics | QuestDB + filesystem | metrics.json content |
| eval.regime_slices | Generate regime slice artifacts | Python subprocess | regime slices |

### yats.shadow.* (Shadow Execution)

| Tool | Description | Backend | Returns |
|------|-------------|---------|---------|
| shadow.run | Run shadow execution for experiment | Dagster pipeline | run_id |
| shadow.run_sim | Run shadow with execution_mode=sim | Dagster pipeline | run_id |
| shadow.status | Check shadow run progress | Dagster GraphQL | status |
| shadow.results | Fetch shadow execution results | QuestDB + filesystem | metrics + logs |
| shadow.compare_modes | Compare shadow vs paper vs live for same experiment | Direct QuestDB | comparison |

### yats.qualify.* (Qualification)

| Tool | Description | Backend | Returns |
|------|-------------|---------|---------|
| qualify.run | Run qualification for experiment vs baseline | Dagster pipeline | run_id |
| qualify.report | Fetch qualification report | Filesystem | qualification_report.json |
| qualify.gates | List all gates and their current thresholds | Static config | gate definitions |

### yats.promote.* (Promotion)

| Tool | Description | Backend | Returns |
|------|-------------|---------|---------|
| promote.to_research | Promote experiment to research tier | Dagster pipeline | promotion record |
| promote.to_candidate | Promote to candidate tier | Dagster pipeline | promotion record |
| promote.to_production | Promote to production (requires approval) | Dagster pipeline + gate | promotion record |
| promote.list | List all promotions | Direct QuestDB | promotion records |
| promote.history | Full promotion history for an experiment | QuestDB + filesystem | history |

### yats.risk.* (Risk Management)

| Tool | Description | Backend | Returns |
|------|-------------|---------|---------|
| risk.check_order | Pre-trade risk check (does not submit) | Python subprocess | pass/reject + reasons |
| risk.portfolio_summary | Current portfolio risk snapshot | Direct QuestDB | risk metrics |
| risk.stress_test | Run stress test against historical scenarios | Python subprocess | stress report |
| risk.correlation | Strategy vs book correlation analysis | Python subprocess | correlation report |
| risk.tail_analysis | CVaR, worst drawdowns, bootstrap CIs | Python subprocess | tail report |
| risk.halt_trading | Emergency halt (no approval needed) | Direct (immediate) | confirmation |
| risk.flatten_positions | Emergency flatten all positions | Broker adapter | confirmation |
| risk.resume_trading | Resume after halt (managing_partner for prod) | Gated | confirmation |
| risk.decisions | Query risk decision log | Direct QuestDB | decision history |

### yats.execution.* (Paper/Live Execution)

| Tool | Description | Backend | Returns |
|------|-------------|---------|---------|
| execution.start_paper | Start paper trading for promoted experiment | Dagster pipeline | run_id |
| execution.stop_paper | Stop paper trading | Direct (immediate) | confirmation |
| execution.paper_status | Status of paper trading run | QuestDB + Dagster | status + metrics |
| execution.promote_live | Promote to live trading (managing_partner only) | Dagster pipeline + gate | confirmation |
| execution.positions | Current positions | Direct QuestDB | position list |
| execution.orders | Order history | Direct QuestDB | order list |
| execution.nav | NAV and portfolio state | Direct QuestDB | portfolio snapshot |

### yats.monitor.* (System Health)

| Tool | Description | Backend | Returns |
|------|-------------|---------|---------|
| monitor.health | System health (QuestDB, Dagster, Alpaca connectivity) | Direct checks | health report |
| monitor.pipeline_status | Status of Dagster pipeline runs | Dagster GraphQL | run statuses |
| monitor.data_freshness | Last ingestion timestamp per vendor/domain | Direct QuestDB | freshness report |
| monitor.audit_log | Query audit trail | Direct QuestDB | audit entries |
| monitor.reconcile | Check consistency across QuestDB, filesystem, Dagster | Dagster + QuestDB + filesystem | inconsistency report |

### yats.stats.* (Statistical Tools)

These are Python subprocess calls — lightweight, no pipelines:

| Tool | Description |
|------|-------------|
| stats.adf_test | Augmented Dickey-Fuller stationarity test |
| stats.bootstrap_sharpe | Bootstrap confidence interval for Sharpe ratio |
| stats.deflated_sharpe | Deflated Sharpe Ratio (multiple testing correction) |
| stats.pbo | Probability of Backtest Overfitting |
| stats.regime_detect | Regime detection using registered detector |
| stats.conditional_sharpe | Sharpe conditional on regime bucket |
| stats.ic_analysis | Information coefficient and decay |
| stats.scm_leakage | Structural Causal Model leakage detection |

### yats.sweep.* (Experiment Sweeps)

| Tool | Description | Backend | Returns |
|------|-------------|---------|---------|
| sweep.run | Run a sweep config (multiple experiments) | Dagster pipeline | run_ids |
| sweep.status | Status of sweep | Dagster GraphQL | per-experiment status |
| sweep.results | Aggregate results across sweep | Direct QuestDB | comparison table |

### yats.registry.* (Metadata)

| Tool | Description | Backend | Returns |
|------|-------------|---------|---------|
| registry.universes | List available universes | Static config | universe list |
| registry.feature_sets | List registered feature sets | Python subprocess | feature set list |
| registry.regime_detectors | List registered regime detectors | Python subprocess | detector list |
| registry.policies | List available policy types | Static config | policy type list |

---

# 15. Dagster Pipeline Catalog

## 15.1 Data Pipelines

**dagster.ingest_alpaca**
- Input: ticker list, date range, bar frequency
- Output: rows written to raw_alpaca_equity_ohlcv
- Trigger: yats.data.ingest, yats.data.ingest_universe

**dagster.ingest_financialdatasets**
- Input: ticker list, data domains (fundamentals, metrics, earnings, insider_trades, analyst_estimates)
- Output: rows written to raw_fd_* tables
- Trigger: yats.data.ingest, yats.data.ingest_universe

**dagster.canonicalize**
- Input: domain, date range, reconciliation config
- Output: rows written to canonical_* tables, reconciliation_log entries
- Dependencies: raw tables populated
- Trigger: yats.data.canonicalize (or chained after ingest)

**dagster.refresh**
- Input: universe, vendor
- Output: incremental raw + canonical updates
- Trigger: yats.data.refresh

## 15.2 Feature Pipelines

**dagster.feature_pipeline**
- Input: universe, feature_set, date range
- Output: rows written to features table
- Dependencies: canonical tables populated
- Trigger: yats.features.compute

**dagster.feature_pipeline_incremental**
- Input: universe, feature_set
- Output: rows written to features table (from watermark forward), watermarks updated
- Dependencies: canonical tables, feature_watermarks
- Trigger: yats.features.compute_incremental

## 15.3 Experiment Pipelines

**dagster.experiment_run**
- Input: experiment_id (spec must exist)
- Steps: load spec -> build env -> train (if RL policy) -> evaluate -> write artifacts -> update experiment_index
- Output: artifacts under .yats_data/experiments/<ID>/, experiment_index row in QuestDB
- Trigger: yats.experiment.run

**dagster.experiment_sweep**
- Input: sweep config (base spec + override grid)
- Steps: for each override combination -> create experiment -> run dagster.experiment_run
- Output: multiple experiment artifacts + index rows
- Trigger: yats.sweep.run

## 15.4 Shadow Pipelines

**dagster.shadow_run**
- Input: experiment_id, execution_mode (none or sim), date range
- Steps: load spec + policy -> build ReplayMarketDataSource -> run ShadowEngine -> write artifacts + unified execution tables
- Output: shadow artifacts under .yats_data/shadow/, execution_log and execution_metrics rows in QuestDB
- Gate: experiment must be promoted or allowlisted
- Trigger: yats.shadow.run, yats.shadow.run_sim

## 15.5 Qualification Pipeline

**dagster.qualify**
- Input: candidate_experiment_id, baseline_experiment_id
- Steps: load both experiments -> run regression gates -> run sanity/constraint checks -> run regime gates -> run execution gates (trigger shadow replay if needed) -> produce qualification_report.json -> update experiment_index
- Output: promotion/qualification_report.json, experiment_index updated
- Trigger: yats.qualify.run

## 15.6 Promotion Pipeline

**dagster.promote**
- Input: experiment_id, target_tier
- Steps: verify qualification passed -> verify tier ordering (research < candidate < production) -> write promotion record -> update experiment_index and promotions table
- Gate: production tier requires human approval (MCP tool blocks until approval received)
- Output: .yats_data/promotions/<tier>/<ID>.json, promotions row in QuestDB
- Trigger: yats.promote.to_research, yats.promote.to_candidate, yats.promote.to_production

## 15.7 Execution Pipelines

**dagster.paper_trading**
- Input: experiment_id (must be promoted to candidate or production)
- Architecture: Paper trading does NOT run as a Dagster long-running job.
  Dagster jobs are designed for finite runs; continuous streaming is an anti-pattern.
  Instead:
  - Dagster job handles SETUP: load spec + policy, validate promotion status,
    initialize state, connect Alpaca paper, write initial state to QuestDB.
  - An independent Python process (managed by systemd or equivalent) runs the
    execution loop: signal -> translate -> risk check -> submit -> fill -> ledger.
  - The process writes to QuestDB directly (execution_log, orders, positions,
    portfolio_state).
  - Dagster Sensor monitors the process health and QuestDB metrics; triggers
    alerts on anomalies.
  - TEARDOWN: Dagster job handles graceful shutdown (cancel orders, settle fills,
    write final state).
- On crash: the process restarts and reconstructs state from QuestDB
  materialized views (see §13.4).

**Livelock Detection (Heartbeat Monitoring):**
The paper/live trading process writes a heartbeat row to QuestDB every loop
iteration:

- trading_heartbeat table:
  - timestamp (TIMESTAMP), experiment_id (SYMBOL), mode (SYMBOL),
    loop_iteration (LONG), orders_pending (INT), last_bar_received (TIMESTAMP),
    last_fill_received (TIMESTAMP)

- Dagster Sensor monitors this table. Alerts triggered on:
  - No heartbeat for N bars (configurable, default 3) → kill switch
  - Order acknowledgment timeout: order submitted, no broker response in 60s → alert
  - Fill timeout: order acknowledged, no fill within expected window → alert
  - WebSocket staleness: no bar received for >2 minutes during market hours → alert
  - Silent livelock: heartbeats arriving but loop_iteration not incrementing → alert

- Each alert is logged to audit_trail and kill_switches table. Critical alerts
  (no heartbeat, WebSocket stale) auto-trigger the HALTING state machine (§24.6).
- Output: execution_log, orders, positions, portfolio_state rows in QuestDB
- Trigger: yats.execution.start_paper

**Dagster Availability:**
- If Dagster is unreachable, all pipeline-backed MCP tools return an explicit
  error: "Dagster unavailable — pipeline tools disabled."
- Direct QuestDB query tools (data.query, experiment.list, etc.) remain functional.
- Paper/live trading processes are independent of Dagster and continue running.
- In-flight Dagster runs that are interrupted by a Dagster restart are marked
  as failed. They must be manually re-triggered. Idempotent writes (§20.3)
  ensure safe retry.

**dagster.live_trading**
- Input: experiment_id (must be promoted to production)
- Architecture: Same as paper_trading — Dagster handles setup/teardown,
  independent Python process runs execution loop against live Alpaca endpoint.
- Gate: managing_partner approval
- Trigger: yats.execution.promote_live

---

# 16. Audit Trail

## 16.1 Audit Trail Table

Every tool invocation, pipeline run, and risk decision is logged:

audit_trail table:
- timestamp (TIMESTAMP), tool_name (SYMBOL), invoker (SYMBOL: agent_id or 'notebook'),
  experiment_id (SYMBOL or NULL), mode (SYMBOL or NULL),
  parameters (STRING as JSON), result_status (SYMBOL: success/failure/timeout),
  result_summary (STRING as JSON), duration_ms (LONG),
  dagster_run_id (STRING or NULL),
  quanttown_molecule_id (STRING or NULL),
  quanttown_bead_id (STRING or NULL)
- Partitioned by MONTH

## 16.2 What Gets Logged

- Every MCP tool invocation (tool name, parameters, result, duration)
- Every Dagster pipeline trigger and completion
- Every risk decision (pass, reject, halt, size_reduce) — also in risk_decisions table
- Every kill switch trigger and resolution
- Every promotion record creation
- Every qualification run
- Shadow, paper, and live execution events

## 16.3 Immutability

The audit_trail table is append-only. QuestDB does not support UPDATE or DELETE
on partitioned tables by default, which enforces immutability naturally.

## 16.4 Retention

**QuestDB:** No deletion policy in v1. QuestDB partitions by MONTH, so old partitions can
be detached to cold storage if space becomes an issue in the future.

**Filesystem artifacts (.yats_data/):** No automatic deletion in v1. With the
experiment explosion prevention (§23.5) limiting to ~1000 experiments/month, and
each experiment averaging ~50MB (spec + metrics + rollout + optional checkpoints),
growth is approximately 50GB/month. Manual archival of old experiments (not promoted
beyond research tier for >90 days) is recommended. A future `monitor.cleanup` tool
can automate this.

## 16.5 Universe Immutability

Universe files (configs/universes/*.yml) are mutable config files. However, the
ExperimentSpec stores the resolved `symbols` list (the actual ticker list at
experiment creation time), not a reference to the universe file. Changing a universe
file after experiment creation does NOT affect existing experiments. The ExperimentSpec
is self-contained.

---

# 17. Directory Structure

```
yats/
  src/                                # TypeScript MCP server
    server.ts                         # MCP server entry point (stdio/SSE)
    tools/
      registry.ts                     # Tool registry (single source of truth)
      data/
        ingest.ts
        canonicalize.ts
        query.ts
        verify.ts
        coverage.ts
      features/
        compute.ts
        list.ts
        stats.ts
        watermarks.ts
      experiment/
        create.ts
        run.ts
        list.ts
        get.ts
        compare.ts
      eval/
        run.ts
        metrics.ts
        regime_slices.ts
      shadow/
        run.ts
        results.ts
        compare_modes.ts
      qualify/
        run.ts
        report.ts
      promote/
        to_tier.ts
        list.ts
      risk/
        check_order.ts
        portfolio_summary.ts
        stress_test.ts
        halt.ts
        flatten.ts
        resume.ts
      execution/
        start_paper.ts
        stop_paper.ts
        promote_live.ts
        positions.ts
        orders.ts
        nav.ts
      monitor/
        health.ts
        pipeline_status.ts
        data_freshness.ts
        audit_log.ts
      stats/
        adf_test.ts
        bootstrap_sharpe.ts
        deflated_sharpe.ts
        pbo.ts
      sweep/
        run.ts
        status.ts
        results.ts
      registry_tools/
        universes.ts
        feature_sets.ts
        policies.ts
    bridge/
      dagster-client.ts               # Dagster GraphQL client
      python-runner.ts                 # Python subprocess execution
      questdb-client.ts               # QuestDB client (PG wire + ILP)
    vendors/
      alpaca.ts                        # Alpaca Data + Trading API client
      financial-datasets.ts            # financialdatasets.ai API client
    types/
      experiment.ts                    # ExperimentSpec, metrics types
      data.ts                          # Schema types for raw/canonical
      execution.ts                     # Order, position, signal types
      risk.ts                          # Risk config, decision types
      tools.ts                         # Tool input/output types
    config/
      loader.ts                        # Config file loading + validation

  pipelines/                           # Python Dagster pipelines
    yats_pipelines/
      definitions.py                   # Dagster Definitions entry point
      assets/
        canonical.py                   # Canonical table assets
      jobs/
        ingest_alpaca.py
        ingest_financialdatasets.py
        canonicalize.py
        refresh.py
        feature_pipeline.py
        feature_pipeline_incremental.py
        experiment_run.py
        experiment_sweep.py
        shadow_run.py
        qualify.py
        promote.py
        paper_trading.py
        live_trading.py
      io/
        questdb_io.py                  # QuestDB I/O manager (PG wire + ILP)
      resources/
        questdb.py                     # QuestDB resource
        alpaca.py                      # Alpaca resource
        financialdatasets.py           # financialdatasets.ai resource
      utils/
        reconciliation.py
        validation.py

  research/                            # Python research modules (from Quanto)
    envs/
      signal_weight_env.py             # SignalWeightEnv (old Gym API)
      execution_simulator.py
    training/
      ppo_trainer.py
      sac_trainer.py
      reward_registry.py
      rewards/
        reward_v1.py
        reward_v2.py
    eval/
      evaluate.py
      metrics.py
      regime_slicing.py
      timeseries.py
    experiments/
      spec.py                          # ExperimentSpec definition
      runner.py                        # Experiment runner (train + eval)
      registry.py                      # Filesystem experiment registry
      regression.py                    # Regression gates vs baseline
    shadow/
      engine.py                        # ShadowEngine
      data_source.py                   # ReplayMarketDataSource
      portfolio.py                     # Shadow portfolio tracking
      ppo_policy.py                    # PPO policy loader for shadow
      logging.py                       # Shadow step logging
      state_store.py                   # Resume-safe state
    execution/
      controller.py                    # ExecutionController
      compiler.py                      # OrderCompiler
      risk_engine.py                   # ExecutionRiskEngine
      sim_broker.py                    # SimBrokerAdapter
      metrics.py                       # Execution metrics
    promotion/
      qualify.py                       # Qualification logic
      criteria.py                      # Gate definitions
      regime_criteria.py               # Regime-specific gates
      execution_criteria.py            # Execution gates
      report.py                        # Report generation
    hierarchy/
      controller.py                    # ModeController
      policy_wrapper.py                # Hierarchical policy wrapper
      allocators/                      # Per-mode allocators
    features/
      feature_registry.py              # Feature set registration
      regime_features_v1.py            # Regime feature computation
      ohlcv_features.py
      fundamental_features.py
      cross_sectional_features.py
    policies/
      sma_weight_policy.py
      equal_weight_policy.py
    risk/
      risk_config.py                   # RiskConfig, ExecutionRiskConfig
      risk_engine.py                   # Production risk engine
    regime/
      universe.py                      # Regime universe management

  compute/                             # Standalone Python compute modules
    stats/
      adf.py
      bootstrap.py
      deflated_sharpe.py
      pbo.py
      scm_leakage.py
    risk/
      stress_test.py
      tail_analysis.py
      correlation.py

  configs/
    risk.yml                           # Risk policy thresholds (RISK_POLICY.md)
    vendors.yml                        # Vendor configuration
    regime_thresholds.yml              # Regime bucketing thresholds
    universes/
      sp500.yml                        # Ticker lists
      energy_sector.yml
      tech_sector.yml
    experiments/
      base_core_v1_regime.json         # Base experiment specs
    sweeps/
      core_v1_primary_regime_baselines.yml

  .yats_data/                        # Runtime data (filesystem artifacts)
    experiments/
      <EXPERIMENT_ID>/
        spec/
        runs/
        evaluation/
        logs/
        promotion/
    promotions/
      research/
      candidate/
      production/
    shadow/
      <EXPERIMENT_ID>/
        <RUN_ID>/

  tests/
    src/                               # TypeScript tests
      tools/
      bridge/
    pipelines/                         # Dagster pipeline tests
    research/                          # Python research tests
      envs/
      training/
      eval/
      execution/
      features/
      shadow/
      promotion/
    compute/                           # Compute module tests
    integration/                       # End-to-end integration tests

  package.json                         # TypeScript dependencies
  tsconfig.json
  pyproject.toml                       # Python dependencies
  requirements.txt
  README.md
  ARCHITECTURE.md
  PRD.md                               # This document (YATS PRD)
  DATA_SPEC.md
  FEATURE_SPEC.md
  EXECUTION_SPEC.md
  RISK_POLICY.md
```

---

# 18. Release Phases

## Phase 1 — Foundation: Data + Features + QuestDB (Weeks 1-3)

Deliverables:
- QuestDB setup with all table schemas (raw, canonical, features, watermarks, audit_trail)
- TypeScript MCP server scaffold (server.ts, tool registry, bridge layer)
- Alpaca vendor adapter (historical OHLCV ingestion)
- financialdatasets.ai vendor adapter (fundamentals, financial metrics)
- Dagster pipelines: ingest_alpaca, ingest_financialdatasets, canonicalize
- Reconciliation pipeline (raw -> canonical with lineage)
- yats.data.* tools (ingest, canonicalize, verify, query, coverage)
- Feature pipeline (full recompute mode) for v1 feature sets
- yats.features.* tools (compute, list, stats, coverage)
- Audit trail logging for all tool invocations
- yats.monitor.health tool

Acceptance:
- MCP server starts and responds to tool discovery
- Alpaca OHLCV ingests to QuestDB for 50+ symbols
- financialdatasets.ai fundamentals ingest for 50+ symbols
- Canonical tables populated with lineage metadata
- Features computed for full universe
- Audit trail captures all invocations

## Phase 2 — Research Loop: Experiments + Training + Evaluation (Weeks 4-6)

Deliverables:
- ExperimentSpec with composable inheritance
- Experiment registry (filesystem + QuestDB index)
- SignalWeightEnv integration (reading features from QuestDB)
- PPO and SAC trainers as Dagster jobs
- Reward versioning (reward_v1, reward_v2)
- Deterministic evaluation pipeline
- Regime feature computation and labeling
- yats.experiment.* tools
- yats.eval.* tools
- yats.stats.* tools
- yats.sweep.* tools
- Incremental feature computation with watermarks

Acceptance:
- Can create experiment from spec, train PPO, evaluate, view metrics
- Experiment index queryable in QuestDB
- Sweeps produce multiple experiments with comparable metrics
- Regime features included in observations when enabled
- Incremental feature computation only processes new data

## Phase 3 — Validation: Shadow + Qualification + Promotion (Weeks 7-9)

Deliverables:
- ShadowEngine with both execution modes (none and sim)
- ReplayMarketDataSource reading from canonical QuestDB tables
- ExecutionController, OrderCompiler, ExecutionRiskEngine, SimBrokerAdapter
- Unified execution tables (execution_log, execution_metrics)
- Qualification as Dagster pipeline with all hard/soft gates
- Promotion as Dagster pipeline with tiered records
- yats.shadow.* tools
- yats.qualify.* tools
- yats.promote.* tools
- Risk simulation mode (overridable thresholds in shadow)
- Pluggable regime detection via feature registry

Acceptance:
- Shadow replay produces execution metrics in unified QuestDB tables
- Qualification gates fire correctly (test with known pass/fail cases)
- Promotion records are immutable and queryable
- Can compare shadow vs baseline metrics via SQL
- Risk simulation mode allows different thresholds in shadow only

## Phase 4 — Execution: Risk + Paper Trading + Alpaca (Weeks 10-12)

Deliverables:
- Production risk engine enforcing RISK_POLICY.md
- Alpaca paper trading integration (order submission, fill handling)
- Streaming canonicalization (WebSocket bars -> canonical)
- Paper trading as Dagster long-running job
- Kill switches (automatic and manual)
- yats.risk.* tools
- yats.execution.* tools
- Ledger tables (positions, orders, portfolio_state)
- yats.monitor.* tools (full set)

Acceptance:
- Paper trading runs for 5+ days without intervention
- Risk engine correctly rejects orders that breach constraints
- Kill switches trigger on drawdown/loss breaches
- Streaming bars arrive and canonicalize in near-real-time
- All execution activity logged in unified tables

## Phase 5 — Integration: QuantTown + Production Hardening (Weeks 13-14)

Deliverables:
- QuantTown MEOW molecule -> YATS tool invocation mapping
- Gate evaluation integration (qualification gates as molecule gates)
- End-to-end formula: cook -> pour -> sling -> execute through all beads
- Production hardening (error handling, retry logic, graceful degradation)
- Live trading pathway (promote_live with managing_partner gate)

Acceptance:
- QuantTown agent can sling an alpha-hypothesis molecule that invokes YATS tools
- Qualification molecule produces valid qualification report
- Promotion molecule respects tier ordering and approval gates
- End-to-end: research -> qualify -> promote -> paper trade -> monitor

---

# 19. Acceptance Criteria (End-to-End)

The system is complete when:

1. **Data pipeline works end-to-end**: Alpaca OHLCV and financialdatasets.ai fundamentals
   ingest -> canonicalize -> features computed -> all queryable in QuestDB

2. **Experiment lifecycle works**: Create spec -> run training (PPO) -> evaluate ->
   view metrics in QuestDB experiment_index -> regime-aware metrics present

3. **Shadow execution works**: Promoted experiment -> shadow run (both modes) ->
   execution metrics in unified QuestDB tables -> comparable across modes

4. **Qualification works**: Candidate vs baseline -> all hard gates evaluated ->
   qualification_report.json produced -> experiment_index updated

5. **Promotion works**: research -> candidate -> production tiers -> immutable records ->
   production requires managing_partner approval

6. **Paper trading works**: Promoted experiment -> paper trading via Alpaca -> orders
   submitted and filled -> positions tracked -> risk engine enforces all constraints ->
   kill switches fire on breaches

7. **Incremental computation works**: New data arrives -> feature pipeline only
   computes from watermark forward -> correct results

8. **Audit trail is complete**: Every tool invocation, pipeline run, risk decision,
   and promotion logged in audit_trail table

9. **QuantTown integration works**: Agent slings molecule -> beads invoke YATS MCP
   tools -> results flow back to molecule -> gates evaluate

10. **Reproducibility holds**: Same experiment spec + same canonical data -> identical
    metrics across runs

---

# 20. Failure Recovery and Consistency

## 20.1 The Problem

YATS has a 7-system dependency graph: TypeScript MCP server, Dagster,
QuestDB, Python subprocess bridge, Alpaca APIs, financialdatasets.ai, and
the filesystem artifact store. A partial failure in any system can create
split-brain state between filesystem artifacts, QuestDB metadata, audit trail,
and Dagster run history.

## 20.2 Design Principles

1. No distributed transactions. The cure is worse than the disease.
2. Idempotency is the primary defense. Every operation can be safely retried.
3. The Dagster run ID is the universal correlation key across all systems.
4. Reconciliation detects and reports inconsistencies; humans decide resolution.

## 20.3 Idempotency Guarantees

QuestDB is append-only by design. It does NOT support upserts. Writing the same
(timestamp, symbol) row twice produces two rows. Idempotency is therefore enforced
at the application level:

**Application-Level Deduplication:**
- Every write includes a dagster_run_id column.
- Before writing, the pipeline queries for existing rows with the same dagster_run_id.
  If found, the write is skipped.
- All read queries use QuestDB's LATEST ON syntax or MAX(dagster_run_id) grouping
  to resolve to the most recent version of each logical row.
- A periodic dedup maintenance job (Dagster scheduled) scans for duplicate logical
  rows (same timestamp + symbol + different dagster_run_id) and removes stale entries
  via partition rewrite.

**Deterministic Run IDs:**
Dagster retries MUST use the same run_id as the original attempt. The MCP server
generates a deterministic run_id from a hash of the tool name + parameters + invoker.
If the same tool call is retried (same parameters), the same run_id is produced.
This prevents the edge case where a retry with a new run_id creates duplicate
logical rows that LATEST ON cannot resolve correctly (e.g., original partial write
has more rows than retry). Dagster's run_id parameter supports externally-provided IDs.

**Filesystem writes:** Overwrite. Writing the same experiment artifact with the same
content is safe. The experiment ID is content-addressed from the spec, so the
same spec always produces the same directory.

**Audit trail:** Append-only. Duplicate entries with the same dagster_run_id +
tool_name + timestamp are detectable and filterable in queries.

## 20.4 Failure Scenarios and Recovery

**Scenario 1: Dagster run succeeds, QuestDB write fails**
- Dagster run metadata records success
- experiment_index in QuestDB is missing the row
- Recovery: re-trigger the Dagster job with same run config. The job detects
  existing filesystem artifacts, skips compute, writes QuestDB metadata only.

**Scenario 2: QuestDB write succeeds, filesystem write fails**
- experiment_index has a row pointing to non-existent artifacts
- Recovery: re-trigger Dagster job. Recomputes and writes artifacts. QuestDB
  row is overwritten with same content.

**Scenario 3: Partial QuestDB write (some tables written, others not)**
- Example: features table updated but feature_watermarks not
- Recovery: re-trigger feature pipeline. Idempotent writes to features table
  (duplicate rows filtered), watermarks updated.

**Scenario 4: MCP returns success but downstream failed**
- MCP tool returned run_id, but Dagster run failed after MCP responded
- Recovery: client polls pipeline status via monitor.pipeline_status, discovers
  failure, re-triggers.

## 20.5 Reconciliation Tool

yats.monitor.reconcile performs consistency checks:

- For every Dagster run marked "success":
  - Verify expected QuestDB rows exist (experiment_index, execution_metrics, etc.)
  - Verify expected filesystem artifacts exist
  - Verify audit trail entries exist
- Report orphans (QuestDB rows without filesystem artifacts, or vice versa)
- Report mismatches (Dagster says success but QuestDB rows missing)

This tool is diagnostic only — it reports inconsistencies but does not auto-fix.
Human decides whether to re-trigger pipelines or manually correct.

## 20.6 Transaction Boundaries

Each Dagster job defines an atomic unit of work. Within a job:

1. Compute results (training, evaluation, etc.)
2. Write filesystem artifacts
3. Write QuestDB metadata
4. Write audit trail entry

If step 2 or 3 fails, the entire job is marked failed in Dagster and can be retried.
Steps are ordered so that filesystem (slower, more likely to fail) is written before
QuestDB (faster, less likely to fail). Audit trail is written last (least critical
for correctness; reconciliation tool can detect missing entries).

---

# 21. Security and Access Control

## 21.1 Role-Based Tool Permissions

Each QuantTown agent role has a defined permission set. The MCP server enforces
these based on the invoker identity passed with each tool call.

### intern (read-only + grunt work)
- data.query, data.coverage, data.vendors
- features.list, features.stats, features.correlations, features.coverage, features.watermarks
- experiment.list, experiment.get
- monitor.health, monitor.data_freshness
- registry.*

### researcher (read + research write)
- All intern permissions, plus:
- data.ingest, data.ingest_universe, data.refresh, data.canonicalize, data.verify
- features.compute, features.compute_incremental
- experiment.create, experiment.run, experiment.compare
- eval.run, eval.metrics, eval.regime_slices
- shadow.run, shadow.run_sim, shadow.results, shadow.compare_modes
- stats.*
- sweep.*

### risk_officer (risk domain)
- All intern permissions, plus:
- risk.check_order, risk.portfolio_summary, risk.stress_test,
  risk.correlation, risk.tail_analysis, risk.decisions
- qualify.run, qualify.report, qualify.gates
- shadow.run, shadow.run_sim, shadow.results, shadow.compare_modes
- risk.halt_trading (emergency — no approval needed)

### pm (coordination + promotion)
- All researcher permissions, plus:
- promote.to_research, promote.to_candidate, promote.list, promote.history
- execution.start_paper, execution.stop_paper, execution.paper_status
- execution.positions, execution.orders, execution.nav
- risk.halt_trading

### managing_partner (full control)
- All permissions, plus:
- promote.to_production
- execution.promote_live
- risk.resume_trading
- risk.flatten_positions

## 21.2 SQL Injection Prevention

yats.data.query is NOT a raw SQL passthrough. It is a parameterized query builder:

- Role-based table whitelist:
  - intern: canonical_*, features, experiment_index
  - researcher: all intern tables + reconciliation_log, audit_trail, execution_metrics,
    execution_log, risk_decisions, orders, portfolio_state, promotions
  - risk_officer: all researcher tables + kill_switches, raw_* tables
  - pm: all risk_officer tables
  - managing_partner: all tables
- No DDL (CREATE, DROP, ALTER)
- No DML (INSERT, UPDATE, DELETE)
- SELECT only, with parameterized WHERE clauses
- Symbol and experiment_id filters are parameterized, never interpolated
- Query timeout enforced (default 30 seconds)

## 21.3 Python Subprocess Sandboxing

Python subprocess tools (stats.*, features.correlations, etc.) run with:

- Working directory restricted to a temp directory
- Read access to .yats_data/ and QuestDB (via PG wire)
- Write access only to designated output paths
- No network access (subprocess does not inherit broker credentials)
- Execution timeout (default 300 seconds)
- Memory limit (configurable, default 4GB)

## 21.4 Rate Limits on Destructive Tools

| Tool | Rate Limit |
|------|------------|
| risk.halt_trading | 5 per hour (per agent) |
| risk.flatten_positions | 1 per hour |
| risk.resume_trading | 3 per hour |
| promote.to_production | 1 per day |
| execution.promote_live | 1 per day |

Rate limits are per-agent. Managing_partner is exempt from rate limits but
all invocations are logged to audit trail.

---

# 22. Determinism, Streaming, and Reproducibility

## 22.1 The Tension

YATS claims both deterministic reproducibility and streaming canonicalization.
These are reconciled by strict domain separation:

- Research (training, evaluation, qualification, shadow) uses BATCH canonicalization only.
  Batch canonical data is immutable once produced. Reproducibility is guaranteed.
- Live execution (paper, live) uses STREAMING canonicalization for real-time data.
  Streaming data feeds the execution loop, not research pipelines.

## 22.2 Batch Canonical Immutability

Batch canonicalization produces canonical rows tagged with:
- reconcile_method = 'batch'
- canonicalized_at = timestamp of the batch run
- dagster_run_id = the specific pipeline run

Once batch canonical rows are written, they are NOT overwritten by subsequent
batch runs. If source data is revised (Alpaca corrects historical data):

1. New raw data is ingested (appended to raw tables with new ingested_at)
2. A new batch canonicalization run produces NEW canonical rows with a later
   canonicalized_at timestamp
3. Old canonical rows remain (QuestDB append-only)
4. Experiments reference canonical data by canonicalized_at range or dagster_run_id
   to ensure reproducibility

**Canonical Version Resolution:**
All downstream consumers (feature pipelines, experiment runners, shadow engine)
resolve canonical data using QuestDB's LATEST ON syntax:

```sql
SELECT * FROM canonical_equity_ohlcv
LATEST ON timestamp PARTITION BY symbol
WHERE reconcile_method = 'batch'
```

This returns exactly one row per (timestamp, symbol) — the most recently
canonicalized version. Experiments that need to reproduce against an older
canonical snapshot filter explicitly:

```sql
WHERE reconcile_method = 'batch'
AND canonicalized_at <= <snapshot_timestamp>
```

The experiment's inputs_used.canonical_hash captures the exact canonical data used.
Reproducing the experiment requires the same canonical snapshot.

**Canonical Snapshot Pinning and Compaction:**
Append-only canonical tables grow with each re-canonicalization. To prevent
unbounded growth while preserving reproducibility:

- When an experiment is created, the `canonicalized_at` timestamp it used is
  recorded as a pin in the `canonical_pins` QuestDB table:
  - experiment_id (SYMBOL), canonicalized_at (TIMESTAMP), pinned_at (TIMESTAMP)
- Compaction (a periodic Dagster job) identifies canonical row versions with
  zero experiment references (no pin) and removes them via partition rewrite.
- Pinned versions are never compacted. Experiments must be archived/retired
  to release their pin.
- Archiving an experiment (manual action) removes its pin and sets
  `archived=true` in experiment_index. Archived experiments are excluded from
  qualification baselines.
- Expected growth without compaction: 500 symbols × 1250 days × 3 versions =
  ~1.8M rows. With compaction retaining only pinned versions, steady-state is
  proportional to active (non-archived) experiments.

## 22.3 Streaming Canonical Semantics

Streaming rows are tagged:
- reconcile_method = 'streaming'
- canonicalized_at = arrival timestamp

Streaming canonical data:
- Is used ONLY by live execution loops
- Is NOT used by research pipelines (training, eval, qualification, shadow)
- May contain late prints, revisions, or timing differences between runs
- Does NOT carry reproducibility guarantees

## 22.4 Research Pipeline Data Access Pattern

All research pipelines (experiment.run, shadow.run, qualify.run) query canonical
data with an explicit filter:

```sql
WHERE reconcile_method = 'batch'
AND canonicalized_at <= <snapshot_timestamp>
```

This ensures research never touches streaming data and always operates on a
deterministic snapshot.

---

# 23. Operational Limits and Governance

## 23.1 Concurrency Limits

| Resource | Limit | Rationale |
|----------|-------|-----------|
| Total concurrent Dagster runs | 10 (configurable) | QuestDB + CPU pressure |
| Concurrent shadow replays | 3 | IO-heavy, large QuestDB writes |
| Concurrent sweeps | 2 | Each sweep spawns multiple experiment runs |
| Paper trading jobs | 1 per experiment, 3 total | Broker rate limits + monitoring capacity |
| Live trading jobs | 1 total | Capital safety |
| Concurrent feature pipelines | 3 | QuestDB write throughput |

## 23.2 Priority Queue

When concurrency limits are hit, Dagster jobs queue with priority:

1. Live execution (highest)
2. Paper execution
3. Kill switch / halt actions
4. Qualification
5. Shadow replay
6. Experiment runs
7. Sweeps
8. Feature computation
9. Data ingestion (lowest)

## 23.3 Rate Limits (QuantTown Governance)

These limits prevent runaway automation from QuantTown agents:

| Action | Limit | Period |
|--------|-------|--------|
| Experiments created | 50 | per hour |
| Experiment runs triggered | 20 | per hour |
| Qualification runs | 10 | per hour |
| Promotions to research | 10 | per day |
| Promotions to candidate | 5 | per day |
| Promotions to production | 1 | per day |
| Shadow replays | 10 | per hour |

## 23.4 Auto-Promotion Block

Promotion beyond research tier requires explicit agent approval:

- research tier: any researcher or PM can promote (after qualification passes)
- candidate tier: PM approval required
- production tier: managing_partner approval required

Auto-promotion (molecule that auto-qualifies and auto-promotes without human
review) is blocked by default. The MCP tool for promote.to_candidate and
promote.to_production includes a requires_human_approval flag that cannot be
bypassed by agent configuration.

## 23.5 Experiment Explosion Prevention

If experiment_index exceeds configurable threshold (default 1000 experiments
in the last 30 days), the system:

1. Emits a warning to the audit trail
2. Blocks new sweep creation (individual experiments still allowed)
3. Requires PM acknowledgment to resume sweeps

---

# 24. Additional Hard Gates (Stress Test Resolutions)

## 24.1 Risk Override Promotion Block

Experiments with risk_overrides in their ExperimentSpec (i.e., experiments
that used risk simulation mode with non-production risk thresholds):

- CAN be promoted to research tier (for tracking and comparison)
- CANNOT be promoted to candidate or production tier
- This is a HARD block enforced in the promotion pipeline
- If a researcher wants to promote a risk-override experiment, they must retrain
  under production risk config (configs/risk.yml) and re-qualify

Qualification of risk-override experiments runs shadow replay under PRODUCTION
risk config (not the overrides). This ensures the qualification report reflects
real-world risk behavior, even if the training used relaxed constraints.

## 24.2 Baseline Integrity Gates

Before qualification runs:

- Baseline experiment artifacts MUST exist on filesystem (hard fail if missing)
- Baseline experiment MUST exist in QuestDB experiment_index (hard fail if missing)
- If baseline_id == candidate_id, promotion is limited to research tier only
  (hard block on candidate and production)

## 24.3 Canonical Data Hash Enforcement

The experiment's inputs_used includes canonical_hash (hash of the canonical data
used during evaluation). During qualification:

- If candidate.canonical_hash != baseline.canonical_hash, a hard warning is emitted
- The qualification report includes a data_drift flag
- Promotion to production with data_drift requires managing_partner acknowledgment

## 24.4 Regime Reproducibility Freeze

ExperimentSpec includes the following frozen hashes (also listed in §8.1):

- regime_thresholds_hash: SHA256 of configs/regime_thresholds.yml at experiment creation
- regime_detector_version: version string of the registered detector
- regime_universe: explicit list of symbols used for regime computation
- feature_set_yaml_hash: SHA256 of the feature set YAML definition

These are frozen at experiment creation time and serialized into experiment_spec.json.
Since the experiment ID is derived from the canonical spec JSON, any change to regime
config produces a different experiment ID. This prevents silent regime drift.

## 24.5 Feature Invalidation on Canonical Change

When batch canonicalization runs:

- The pipeline computes canonical hashes at TWO granularities:
  - **Per-symbol hash**: hash of canonical data for one symbol over a date range.
    Used for per-symbol feature watermark invalidation.
  - **Universe-level hash per date**: hash of ALL symbols' canonical data on each
    date. Used for cross-sectional feature watermark invalidation.

- Hashes stored in canonical_hashes QuestDB table:
  - timestamp (TIMESTAMP), symbol (SYMBOL), date_from (TIMESTAMP),
    date_to (TIMESTAMP), canonical_hash (STRING), universe_date_hash (STRING),
    dagster_run_id (STRING)

- Per-symbol feature invalidation:
  - If a symbol's canonical_hash changes, that symbol's per-symbol feature
    watermark is invalidated. Full recompute triggered for that symbol only.

- Cross-sectional feature invalidation:
  - If ANY symbol's canonical data changes on a date, the universe_date_hash
    changes for that date. Cross-sectional feature watermarks (which are
    universe-level, see §6.3) are invalidated for all affected dates.
    Full cross-sectional recompute triggered for those dates across ALL symbols.
  - This ensures that size_rank, value_rank, and z-scored features are
    recomputed when any constituent symbol's data changes.

- An alert is emitted to the audit trail with the affected experiments listed.

**Experiment Staleness Detection:**
When canonical data changes and features are invalidated, the system also
identifies affected experiments:

1. Query experiment_index for all experiments whose `inputs_used.canonical_hash`
   (stored at evaluation time) matches the OLD canonical hash.
2. Set `stale = true` on those experiment_index rows.
3. Stale experiments CANNOT be promoted (hard block at any tier).
4. Already-promoted experiments:
   - research tier: stale flag set, no further action
   - candidate tier: stale_warning flag set, alert emitted to PM
   - production tier: alert emitted to managing_partner with details of
     what canonical data changed and which features were affected
5. Stale experiments can be "refreshed" by re-running evaluation against the
   updated canonical data. This produces new metrics and clears the stale flag.
   The experiment ID does NOT change (spec is unchanged), but the evaluation
   artifacts are overwritten and the experiment_index metrics are updated.

## 24.6 Kill Switch State Machine

Trading execution follows a strict state machine:

```
TRADING ---(halt trigger)---> HALTING
  |                              |
  |                         [cancel open orders]
  |                         [settle partial fills]
  |                         [disconnect order submission]
  |                              |
  |                              v
  |                           HALTED
  |                              |
  |                    (managing_partner resume)
  |                              |
  |                              v
  |                          RESUMING
  |                              |
  |                    [verify all orders settled]
  |                    [verify no pending fills]
  |                    [verify risk state clean]
  |                    [reconnect order submission]
  |                              |
  +<-----------------------------+
                           TRADING
```

Rules:
- HALTING: no new orders submitted. Open orders cancelled via broker API.
  Partial fills accepted and recorded. WebSocket stays connected for data.
  Duplicate halt calls are idempotent (already in HALTING/HALTED).
- HALTED: resume requires managing_partner approval for production mode,
  PM approval for paper mode. State verification must pass before TRADING resumes.
- RESUMING: if verification fails (pending orders, unsettled fills), system
  remains HALTED and reports the issue.
- State transitions are logged to kill_switches table and audit trail.
- At most one state transition in progress at a time (mutex). Race between
  halt and fill event: halt wins, fill is recorded but no new orders generated.

---

# 19. Acceptance Criteria (Updated)

The system is complete when criteria 1-10 from original Section 19 pass, PLUS:

11. **Failure recovery works**: monitor.reconcile detects artificially introduced
    split-brain state (delete a QuestDB row, verify detection). Re-triggering
    a Dagster job with the same config produces consistent state across all systems.

12. **Security enforced**: intern agent cannot invoke promote.to_production.
    Researcher cannot invoke risk.flatten_positions. data.query rejects
    DROP TABLE attempts. Rate limits fire on repeated halt calls.

13. **Streaming/batch separation holds**: Research pipeline queries never return
    streaming-tagged canonical rows. Live execution uses streaming data correctly.

14. **Risk override block works**: Experiment with risk_overrides cannot be promoted
    beyond research tier. Qualification of override experiment runs under production
    risk config.

15. **Concurrency limits enforced**: 11th concurrent Dagster run queues. 4th shadow
    replay queues. Priority ordering respected.

16. **Kill switch state machine works**: Halt during active trading cancels open orders,
    settles partial fills, transitions to HALTED. Resume requires approval and
    state verification.

17. **Feature invalidation works**: Canonical data correction triggers watermark
    invalidation and full recompute for affected symbols.

18. **Regime freeze works**: Two experiments with different regime_thresholds.yml
    produce different experiment IDs.

---

# 25. Design Notes for Implementation

These items are not PRD requirements but are flagged for implementation attention:

**25.1 Audit Trail Cryptographic Hardening (v2)**
v1 relies on QuestDB append-only semantics for audit immutability. For v2,
implement hash chaining (each audit entry includes SHA256 of previous entry)
and periodic snapshots to an external write-once sink. This upgrades from
operational immutability to cryptographic tamper-evidence.

**25.2 Resource-Aware Scheduling**
Concurrency limits in §23.1 are logical (count-based). Dagster run tags should
include resource requirements (estimated memory, GPU needs). Dagster's run
coordinator can be configured with resource-based limits to prevent OOM from
concurrent shadow replays or training jobs.

**25.3 Broker Event Idempotency**
Every broker event (fill, cancel, reject) must be processed idempotently,
keyed by broker_order_id + event_type. Duplicate WebSocket events, clock skew,
and partial fill reporting edge cases must be handled at the execution engine level.

**25.4 Risk Override Training Bias**
§24.1 blocks promotion of risk-override experiments and requires retraining.
However, implementers should be aware that a policy initially trained under
relaxed risk may exhibit subtle behavioral artifacts even after retraining
under production risk (due to weight initialization from the relaxed checkpoint).
Best practice: retrain from scratch under production risk, not fine-tune from
relaxed checkpoint.

---

# YATS PRD — Appendix: Standalone Implementation Specifications

This appendix makes the PRD self-contained. An implementer can build YATS
from the PRD + this appendix alone.

---

# A. SignalWeightEnv — Full Specification

## A.1 Environment Interface

SignalWeightEnv implements the old Gym API (not Gymnasium):

```
reset() -> observation (tuple of floats)
step(action) -> (observation, reward, done, info)
```

It is NOT Gymnasium-compatible. It does not implement Env base class,
observation_space, action_space, or the (terminated, truncated) split.

## A.2 Configuration (SignalWeightEnvConfig)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| symbols | list[str] | required | Ordered symbol list |
| observation_columns | list[str] | required | Feature columns (must include 'close') |
| action_clip | tuple[float, float] | (0.0, 1.0) | Min/max for action clipping |
| transaction_cost_bp | float | from cost_config | Transaction cost in basis points |
| slippage_bp | float | 0.0 | Slippage in basis points |
| execution_sim | ExecutionSimConfig or None | None | Execution simulator config |
| regime_feature_names | list[str] or None | None | Regime feature column names |

## A.3 Observation Vector Construction (_build_observation)

Observation is a flat tuple constructed in this order:

1. **Per-symbol features (symbol-major order):**
   Symbols sorted alphabetically. For each symbol, feature values in
   observation_columns order (excluding regime columns).
   Layout: `sym1_feat1, sym1_feat2, ..., sym2_feat1, sym2_feat2, ...`

2. **Regime features (appended if enabled):**
   Regime feature values in regime_feature_names order.
   Layout: `market_vol_20d, market_trend_20d, dispersion_20d, corr_mean_20d`

3. **Previous weights (always appended):**
   One weight per symbol in the same sorted symbol order.
   Layout: `prev_weight_sym1, prev_weight_sym2, ...`

Total observation length = (n_symbols × n_features) + n_regime_features + n_symbols

## A.4 Action Processing

1. **Clip:** Raw action clipped to `action_clip` bounds (default 0.0 to 1.0)
2. **Project:** `project_weights` applies deterministic risk constraints:
   a. Long-only clamp (all weights >= 0)
   b. Per-symbol max weight clamp
   c. Exposure cap (sum of weights <= max, ensuring min cash)
   d. Optional max turnover vs previous weights (scaled L1 delta)
3. **Execute:** Projected weights become target weights for the step

## A.5 Step Execution

1. Advance to next row in feature data
2. Compute portfolio value change from price changes and current weights
3. Apply transaction costs: `cost = |weight_change| × transaction_cost_bp / 10000`
4. If execution_sim enabled, run ExecutionSimulator (see A.7)
5. Compute reward: `log(new_value / old_value)` (after costs and execution sim)
6. Build new observation from next row
7. Check done condition (last row reached)

## A.6 Step Info Dict

Base keys (always present):
| Key | Type | Description |
|-----|------|-------------|
| timestamp | str | Current row timestamp |
| price_close | dict | {symbol: close_price} |
| raw_action | ndarray | Pre-projection action |
| weights | ndarray | Post-projection weights |
| weight_target | ndarray | Target weights before execution sim |
| weight_realized | ndarray | Actual weights after execution sim (= weights if no sim) |
| portfolio_value | float | Portfolio value after step |
| cost_paid | float | Transaction costs paid |
| reward | float | Step reward |

Execution sim keys (present if execution_sim enabled):
| Key | Type | Description |
|-----|------|-------------|
| execution_slippage_bps | float | Realized slippage in basis points |
| missed_fill_ratio | float | Fraction of orders that failed to fill |
| unfilled_notional | float | Notional value of unfilled orders |
| order_type_counts | dict | Count by order type |

Regime keys (present if regime features enabled):
| Key | Type | Description |
|-----|------|-------------|
| regime_features | tuple | Regime feature values |
| regime_state | str | Regime label if available |

Hierarchy key (present if passed into step):
| Key | Type | Description |
|-----|------|-------------|
| mode | str | Current hierarchy mode (risk_on/neutral/defensive) |

## A.7 ExecutionSimulator

When enabled, resolves target weights into realized weights with simulated execution:

For each symbol:
1. Compute weight delta from previous weights
2. Determine order type and price offsets
3. Compute effective OHLC band with range shrink + slippage model
4. Test fill: if price within band, fill at target weight; otherwise missed fill
5. Aggregate slippage (actual fill price vs target) and missed-fill stats
6. Return executed weights and execution info

Configuration (ExecutionSimConfig):
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| enabled | bool | False | Enable execution simulator |
| slippage_model | str | 'flat' | 'flat' or 'volume_scaled' |
| slippage_bp | float | 5.0 | Base slippage in bps |
| fill_probability | float | 0.99 | Probability of fill per order |
| range_shrink | float | 0.5 | OHLC range shrink factor |

---

# B. Reward Shaping — Full Specification

## B.1 Reward Adapter

Training wraps the env with RewardAdapterEnv, which applies a versioned
reward function selected by `policy_params.reward_version` in the experiment spec.

## B.2 reward_v1

Identity function. Returns the base environment reward unchanged.

```
reward_v1(base_reward, info) = base_reward
```

## B.3 reward_v2

Multi-component shaped reward with configurable penalties.

**Configuration:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| turnover_scale | float | 0.10 | Scaling factor for turnover penalty |
| drawdown_scale | float | 0.50 | Scaling factor for drawdown penalty |
| cost_scale | float | 0.00 | Scaling factor for cost penalty (off by default) |
| clip_reward | tuple[float,float] or None | None | Optional (min, max) clipping |

**Computation:**

```
base_reward = env.step() reward (log-return after costs)

turnover = 0.5 * sum(|w_t - w_{t-1}|)       # L1 weight change, halved
turnover_penalty = turnover * turnover_scale   # default: turnover * 0.10

drawdown_increase = max(0, drawdown_t - drawdown_{t-1})  # only penalize increases
drawdown_penalty = drawdown_increase * drawdown_scale      # default: increase * 0.50

cost_penalty = (cost_amount / prev_portfolio_value) * cost_scale  # default: 0 (off)

final_reward = base_reward - turnover_penalty - drawdown_penalty - cost_penalty

if clip_reward is not None:
    final_reward = clamp(final_reward, clip_reward[0], clip_reward[1])
```

**Info dict extension:**
reward_v2 adds `reward_components` to info:
```python
{
    "base": base_reward,
    "turnover_penalty": turnover_penalty,
    "drawdown_penalty": drawdown_penalty,
    "cost_penalty": cost_penalty,
    "final": final_reward
}
```

---

# C. ExperimentSpec — Full Specification

## C.1 Dataclass Definition

ExperimentSpec is a Python dataclass (not Pydantic).

### Required Fields

| Field | Type | Validation |
|-------|------|------------|
| experiment_name | str | Non-empty |
| symbols | tuple[str, ...] | Non-empty, sorted, deduplicated |
| start_date | date | Must be before end_date |
| end_date | date | Must be after start_date |
| interval | str | Must be 'daily' |
| feature_set | str | Non-empty, must be registered |
| policy | str | One of: 'equal_weight', 'sma', 'ppo', 'sac', or sac_* variants |
| policy_params | Mapping[str, Any] | Defaults to {} if not provided |
| cost_config | CostConfig | transaction_cost_bp >= 0 |
| seed | int | Any integer |

### Optional Fields

| Field | Type | Default | Validation |
|-------|------|---------|------------|
| evaluation_split | EvaluationSplitConfig or None | None | If provided: train_ratio + test_ratio = 1.0, both > 0, test_window_months in {1,3,4,6,12} |
| risk_config | RiskConfig | RiskConfig() | See C.3 |
| execution_sim | ExecutionSimConfig or None | None | See A.7 |
| notes | str or None | None | Free text |
| regime_feature_set | str or None | None | If provided, must be registered regime detector |
| regime_labeling | str or None | None | If provided, must be 'v1' or 'v2' |
| hierarchy_enabled | bool | False | |
| controller_config | Mapping[str, Any] or None | None | Required if hierarchy_enabled |
| allocator_by_mode | Mapping[str, Mapping[str, Any]] or None | None | Required if hierarchy_enabled |

### YATS Additional Fields (frozen at creation time)

| Field | Type | Description |
|-------|------|-------------|
| regime_thresholds_hash | str | SHA256 of configs/regime_thresholds.yml |
| regime_detector_version | str | Version of registered detector |
| regime_universe | tuple[str, ...] | Symbols used for regime computation |
| feature_set_yaml_hash | str | SHA256 of feature set YAML definition |
| risk_overrides | Mapping[str, Any] or None | Non-production risk thresholds (blocks promotion) |
| _base_spec_hash | str or None | SHA256 of base spec if composed via inheritance |

## C.2 Sub-Configs

### CostConfig
| Field | Type | Default | Validation |
|-------|------|---------|------------|
| transaction_cost_bp | float | required | >= 0 |
| slippage_bp | float | 0.0 | >= 0 |

### EvaluationSplitConfig
| Field | Type | Default | Validation |
|-------|------|---------|------------|
| train_ratio | float | required | > 0, sum with test_ratio = 1.0 |
| test_ratio | float | required | > 0 |
| test_window_months | int or None | None | If provided: one of {1, 3, 4, 6, 12} |

### RiskConfig
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| max_gross_exposure | float | 1.0 | Max sum of absolute weights |
| min_cash | float | 0.0 | Minimum cash fraction |
| max_symbol_weight | float | 1.0 | Max weight per symbol |
| max_daily_turnover | float | 1.0 | Max daily turnover fraction |
| max_active_positions | int | 999 | Max number of non-zero positions |
| daily_loss_limit | float | -1.0 | Daily PnL floor (negative = % loss) |
| trailing_drawdown_limit | float | -1.0 | Max peak-to-trough drawdown |

## C.3 Hierarchy Validation

When `hierarchy_enabled = True`:
- `controller_config` is required and must contain:
  - `update_frequency`: one of 'weekly', 'monthly', 'every_k_steps'
  - `vol_threshold_high`: float (regime vol threshold for defensive mode)
  - `trend_threshold_high`: float (regime trend threshold for risk_on mode)
  - `dispersion_threshold_high`: float (regime dispersion threshold for risk_on)
  - `min_hold_steps`: int (minimum steps before mode can change)
  - `k_steps`: int (required if update_frequency = 'every_k_steps')
- `allocator_by_mode` is required and must contain keys: 'risk_on', 'neutral', 'defensive'
  Each value is a mapping with allocator configuration.

## C.4 Experiment ID Derivation

```python
canonical_dict = spec.to_canonical_dict()  # all fields, sorted, normalized
canonical_json = json.dumps(canonical_dict, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
experiment_id = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
```

The ID is fully deterministic: same spec fields produce same ID.

---

# D. Qualification — Full Specification

## D.1 Gate Evaluation Order

Qualification runs gates in this exact order:

1. **Regression gates** (candidate vs baseline metrics comparison)
2. **Constraint violations** (safety: NaN/Inf, max drawdown, turnover cap)
3. **Regime hard gates** (high-vol drawdown/exposure regressions)
4. **Regime soft gates** (Sharpe degradation, stability checks)
5. **Artifact presence check** (spec, metrics.json, run_summary.json)
6. **Sweep robustness** (optional, if sweep data available)
7. **Execution evidence** (locate execution metrics or trigger shadow replay)
8. **Execution hard gates** (fill rate, reject rate, slippage, halts)
9. **Execution soft gates** (deltas vs baseline: slippage, fees, turnover)
10. **Regime-execution diagnostics** (optional)

## D.2 Regression Gate Methodology

For each metric:
```
delta = candidate_value - baseline_value
delta_pct = (delta / baseline_value) * 100    # None if baseline ≈ 0

# For higher-is-better metrics (Sharpe, return, win_rate):
degradation = max(0, baseline_value - candidate_value)
violation = degradation > |baseline_value| * (threshold_pct / 100)

# For lower-is-better metrics (drawdown, turnover):
degradation = max(0, candidate_value - baseline_value)
violation = degradation > |baseline_value| * (threshold_pct / 100)
```

## D.3 All Gate Thresholds (Defaults)

### Regression Gates
| Metric | Direction | Threshold | Hard/Soft |
|--------|-----------|-----------|-----------|
| performance.sharpe | higher_is_better | 5% degradation | Hard |
| performance.max_drawdown | lower_is_better | 10% worsening | Soft |
| trading.turnover_1d_mean | lower_is_better | 15% increase | Soft |
| safety.nan_inf_violations | equals_zero | must be 0 | Hard |

### Constraint Gates
| Check | Threshold | Hard/Soft |
|-------|-----------|-----------|
| safety.constraint_violations | must be 0 | Hard |
| performance.max_drawdown | <= max_drawdown config (default 1.0) | Hard |
| trading.turnover_1d_mean | <= max_turnover config (default 1.0) | Hard |

### Regime Qualification Gates
| Gate | Threshold | Hard/Soft |
|------|-----------|-----------|
| High-vol drawdown regression | 10% | Hard |
| High-vol exposure cap | 0.40 | Hard |
| Sharpe degradation (regime-conditional) | 15% | Soft |
| Turnover increase (high-vol) | 20% | Soft |
| Mode transition fraction | 0.10 (max fraction of steps with mode change) | Soft |
| Execution drawdown regression | 2% | Hard |
| Defensive exposure cap (if hierarchy) | checked | Soft |

### Execution Qualification Gates
| Gate | Threshold | Hard/Soft |
|------|-----------|-----------|
| Fill rate | >= 99% | Hard |
| Reject rate | <= 1% | Hard |
| P95 slippage | <= 25 bps | Hard |
| Execution halts | 0 | Hard |
| Avg slippage delta vs baseline | 5 bps | Soft |
| Total fees delta vs baseline | 10% | Soft |
| Turnover drift vs baseline | 10% | Soft |

## D.4 Baseline == Candidate (Phase 1 Mode)

When baseline_id equals candidate_id:
- Delta-based execution checks are skipped (skip_delta_checks=True)
- A soft warning is added: "baseline_is_candidate"
- Regression gates still run but with zero deltas (all pass trivially)
- Promotion limited to research tier only (§24.2)

---

# E. Hierarchical Policy — Full Specification

## E.1 Modes

Three modes, always these three:
| Mode | Description | Typical Behavior |
|------|-------------|-----------------|
| risk_on | Favorable regime detected | Higher allocation, broader positions |
| neutral | Default / unclear regime | Moderate allocation |
| defensive | High-volatility regime detected | Reduced allocation, tighter positions |

## E.2 ModeController Decision Logic

Rule-based on regime features. No learning. Evaluated at update cadence.

```python
def select_mode(regime_features):
    vol = regime_features['market_vol_20d']
    trend = regime_features['market_trend_20d']
    dispersion = regime_features['dispersion_20d']

    if vol >= vol_threshold_high:
        return 'defensive'
    elif trend >= trend_threshold_high and dispersion >= dispersion_threshold_high:
        return 'risk_on'
    else:
        return 'neutral'  # fallback
```

## E.3 Update Cadence and Hold

- `update_frequency`: when the controller re-evaluates mode
  - 'weekly': every 5 trading days
  - 'monthly': every 21 trading days
  - 'every_k_steps': every k steps (k from controller_config)
- `min_hold_steps`: minimum steps after a mode change before next change allowed
- Between updates (or during hold): previous mode continues unchanged

## E.4 Mode Transitions

- Transitions are **immediate** when update is due and hold period has elapsed
- No blending, smoothing, or gradual transition between modes
- Transition logged in evaluation/mode_timeline.json:
  ```json
  [
    {"step": 0, "mode": "neutral", "timestamp": "..."},
    {"step": 42, "mode": "defensive", "timestamp": "..."},
    {"step": 85, "mode": "neutral", "timestamp": "..."}
  ]
  ```

## E.5 Allocator Interface

Each mode has a registered allocator. Interface contract:

```python
def act(obs: np.ndarray, context: Mapping[str, Any]) -> np.ndarray:
    """
    Args:
        obs: observation vector (same as env observation)
        context: additional info (regime_features, mode, etc.)
    Returns:
        Raw weight vector (n_symbols,). Will be passed through
        project_weights for risk constraints.
    """
```

Allocator types: can be PPO checkpoint, SAC checkpoint, SMA policy, equal-weight,
or any custom callable matching this interface. Registered in allocator_by_mode
mapping in the experiment spec.

---

# F. Shadow Engine — Full Specification

## F.1 ReplayMarketDataSource

Builds per-date snapshots from canonical data for shadow replay.

### Snapshot Schema

Each snapshot is a dict:
```python
{
    "as_of": datetime,                      # Date of this snapshot
    "symbols": tuple[str, ...],             # Ordered symbol list
    "panel": {                              # Per-symbol feature data
        "AAPL": {"close": 150.0, "ret_1d": 0.012, ...},
        "MSFT": {"close": 310.0, "ret_1d": -0.003, ...},
        ...
    },
    "regime_features": (0.15, 0.02, 0.08, 0.45),  # Regime feature values
    "regime_feature_names": ("market_vol_20d", "market_trend_20d", ...),
    "observation_columns": ("close", "ret_1d", "rv_21d", ...)
}
```

### Data Loading

1. Query canonical_equity_ohlcv for date range + symbols (batch mode only,
   filtered by reconcile_method='batch')
2. Query features table for same range + symbols + feature_set
3. Align dates (intersection of both queries)
4. Compute regime features if regime_feature_set specified in experiment spec
5. Build ordered snapshots, one per trading date

## F.2 ShadowEngine

### Execution Flow

```
for each snapshot in replay_window:
    1. Build observation from snapshot (same format as env)
    2. Load policy and produce target weights
    3. Route to execution path:
       - execution_mode=none: direct rebalance
       - execution_mode=sim: full order lifecycle
    4. Record step to logs/steps.jsonl
    5. Update state (positions, weights, portfolio value)
    6. Save state to state.json (resume checkpoint)
```

### execution_mode=none (Direct Rebalance)

```
target_weights = policy.act(observation)
projected_weights = project_weights(target_weights, risk_config)
cost = sum(|projected - previous_weights|) * transaction_cost_bp / 10000
portfolio_value = portfolio_value * (1 + weighted_return - cost)
current_weights = projected_weights
```

### execution_mode=sim (Broker Simulation)

```
target_weights = policy.act(observation)
projected_weights = project_weights(target_weights, risk_config)

# OrderCompiler: convert weight deltas to orders
orders = compile_orders(projected_weights, current_weights, portfolio_value)

# ExecutionRiskEngine: check each order against risk limits
approved_orders, rejected_orders = risk_engine.check(orders, portfolio_state)

# SimBrokerAdapter: deterministic fill
for order in approved_orders:
    fill_price = snapshot_price + slippage
    fill(order, fill_price)

# Record execution metrics
record_metrics(fill_rate, reject_rate, slippage, fees, regime_bucket)
```

## F.3 Resume (state.json)

State persisted after each step for crash recovery:

```json
{
    "step_index": 142,
    "positions": {"AAPL": 100, "MSFT": 50},
    "weights": [0.3, 0.2, ...],
    "portfolio_value": 105432.50,
    "cash": 52000.00,
    "peak_value": 106000.00,
    "orders_pending": [],
    "last_timestamp": "2024-06-15",
    "dagster_run_id": "...",
    "allowlist_metadata": {...}
}
```

On resume: load state.json, advance replay_window to step_index, continue.

---

# G. Risk Engine — Full Specification

## G.1 Risk Engine — Full Implementation

YATS implements the complete RISK_POLICY.md. Quanto only implemented
constraints 1-4. YATS adds constraints 5-12 as new implementation work.

### Decision Types

| Decision | Meaning |
|----------|---------|
| pass | Order approved unchanged |
| size_reduce | Order approved at reduced size to satisfy constraint |
| reject | Order fully rejected |
| halt | Trading halted — no further orders until resume |

**size_reduce logic:** When a constraint would reject an order, the risk engine
first attempts to reduce the order size to the maximum that satisfies the
constraint. If the reduced size is >= minimum_order_threshold (configurable,
default 0.01 of NAV), the order proceeds at reduced size. If below threshold,
the order is rejected. The original and reduced sizes are logged in
risk_decisions.

### Evaluation Order

Constraints evaluated in this exact order. Earlier constraints can short-circuit
(HALT stops all further evaluation).

**Group 1: Kill Switches (HALT — immediate trading stop)**

1. `daily_loss_limit`: if daily PnL < limit → HALT all
2. `trailing_drawdown_limit`: if drawdown from peak > limit → HALT all

**Group 2: Global Limits (REJECT ALL or SIZE_REDUCE batch)**

3. `max_gross_exposure`: if projected gross > limit
   - Size_reduce: scale all order sizes proportionally to bring within limit
   - If scaling below threshold → REJECT ALL
4. `max_daily_turnover`: if projected turnover > limit
   - Size_reduce: scale order sizes to fit within remaining turnover budget
   - If scaling below threshold → REJECT ALL
5. `max_net_exposure`: if |projected net| > limit
   - Size_reduce: reduce directional orders to bring net within limit
6. `max_leverage`: if projected leverage > limit
   - Size_reduce: scale down to fit leverage limit

**Group 3: Per-Symbol Limits (REJECT or SIZE_REDUCE individual orders)**

7. `max_symbol_weight`: if projected symbol weight > limit
   - Size_reduce: cap order so resulting weight = max_symbol_weight
8. `max_active_positions`: if position count > limit
   - Reject new position orders (existing position adjustments pass)
9. `concentration_limit`: if top N positions > max_top_n_concentration
   - Size_reduce: cap orders that would increase top-N concentration
10. `max_adv_pct`: if order shares > max_adv_pct × 20-day ADV
    - Size_reduce: cap to max_adv_pct × ADV

**Group 4: Volatility Constraints (SIZE_REDUCE)**

11. `volatility_scaling`: effective_size = base_size × (target_vol / current_vol)
    - Applied to all order sizes before submission
    - Capped by hard exposure limits (Group 2)
12. `vol_regime_brakes`: if current_vol > vol_regime_threshold
    - Reduce max_symbol_weight by vol_brake_position_reduction factor
    - Reduce max_gross_exposure by vol_brake_exposure_reduction factor
    - These reduced limits are used for Group 2-3 checks

**Group 5: Signal Constraints (REJECT or SIZE_REDUCE)**

13. `min_confidence`: if signal confidence < min_confidence
    - Reject order entirely (no size_reduce — low confidence means don't trade)
14. `min_holding_period`: if position held < min_holding_period bars
    - Reject close/reduce orders for that position

**Group 6: Cash Floor (REJECT ALL)**

15. `min_cash`: if projected cash < min_cash threshold → REJECT ALL remaining orders

### Important: Evaluation Interactions

- Vol regime brakes (12) modify the effective limits used by Groups 2-3.
  They are evaluated FIRST within Group 4, then Groups 2-3 use the adjusted limits.
- Volatility scaling (11) modifies order sizes before they enter Groups 2-3.
- The actual evaluation flow is:
  1. Check kill switches (1-2)
  2. Apply vol regime brakes if applicable (12) — adjusts limits
  3. Apply volatility scaling to all order sizes (11)
  4. Check global limits with adjusted limits (3-6)
  5. Check per-symbol limits with adjusted limits (7-10)
  6. Check signal constraints (13-14)
  7. Check cash floor (15)

## G.2 Default Thresholds (configs/risk.yml)

```yaml
# Kill switches
daily_loss_limit: -0.05          # -5% daily loss triggers halt
trailing_drawdown_limit: -0.20   # -20% drawdown triggers halt

# Global limits
max_gross_exposure: 1.0          # 100% of NAV
max_net_exposure: 1.0            # 100% of NAV
max_leverage: 1.0                # No leverage in v1

# Per-symbol limits
max_symbol_weight: 0.20          # 20% max per symbol
max_active_positions: 50
min_cash: 0.02                   # 2% minimum cash

# Turnover
max_daily_turnover: 0.50         # 50% of NAV daily

# Concentration
max_top_n_concentration: 0.60    # Top 5 positions <= 60%
top_n: 5

# Liquidity
max_adv_pct: 0.05               # 5% of 20-day ADV

# Volatility
target_vol: 0.15                 # 15% annualized target
vol_regime_threshold: 0.30       # 30% vol triggers brakes
vol_brake_position_reduction: 0.50  # Halve position size in high-vol
vol_brake_exposure_reduction: 0.50  # Halve gross exposure in high-vol

# Signal constraints
min_confidence: 0.0              # No confidence gating by default
min_holding_period: 1            # 1 bar minimum hold

# Size reduce
minimum_order_threshold: 0.01   # 1% of NAV — below this, reject instead of size_reduce

# Operational
max_broker_errors: 5             # Consecutive broker errors before halt
data_staleness_threshold: 300    # Seconds before data considered stale
```

---

# H. Regression Gates — Full Specification

## H.1 Metric Extraction

Metrics are extracted from evaluation/metrics.json using these paths:

| Metric Path | Direction | Description |
|-------------|-----------|-------------|
| performance.sharpe | higher_is_better | Sharpe ratio |
| performance.calmar | higher_is_better | Calmar ratio |
| performance.sortino | higher_is_better | Sortino ratio |
| performance.total_return | higher_is_better | Total return |
| performance.annualized_return | higher_is_better | Annualized return |
| performance.max_drawdown | lower_is_better | Maximum drawdown (negative) |
| performance.max_drawdown_duration | lower_is_better | Days in max drawdown |
| trading.turnover_1d_mean | lower_is_better | Mean daily turnover |
| trading.turnover_1d_std | lower_is_better | Turnover standard deviation |
| trading.win_rate | higher_is_better | Win rate |
| trading.profit_factor | higher_is_better | Profit factor |
| safety.nan_inf_violations | equals_zero | Must be exactly 0 |

## H.2 Comparison Methodology

For two experiments (candidate vs baseline):

```python
for metric in metrics:
    delta = candidate[metric] - baseline[metric]

    if abs(baseline[metric]) < 1e-9:
        delta_pct = None  # Avoid division by zero
    else:
        delta_pct = (delta / baseline[metric]) * 100

    # Determine if this is a regression
    if metric.direction == 'higher_is_better':
        degradation = max(0, baseline[metric] - candidate[metric])
        improved = delta > 0
    elif metric.direction == 'lower_is_better':
        degradation = max(0, candidate[metric] - baseline[metric])
        improved = delta < 0
    elif metric.direction == 'equals_zero':
        degradation = abs(candidate[metric])
        improved = candidate[metric] == 0

    # Check gate violation
    if threshold is not None:
        violation = degradation > abs(baseline[metric]) * (threshold / 100)
```

---

# I. Regime Detection — Full Specification

## I.1 Regime Feature Computation (v1)

Input: aligned close prices for regime universe symbols (default: SPY, QQQ, IWM).

| Feature | Computation | Window |
|---------|-------------|--------|
| market_vol_20d | Annualized std of log returns of equal-weight portfolio | 20 days |
| market_trend_20d | Cumulative log return of equal-weight portfolio | 20 days |
| dispersion_20d | Std of individual symbol returns on each date | 20 days rolling mean |
| corr_mean_20d | Mean pairwise correlation of symbol returns | 20 days |

## I.2 Regime Labeling

### v1 (Quantile-Based)
Bucket `market_vol_20d` into terciles:
- Bottom tercile → `low_vol`
- Middle tercile → `mid_vol`
- Top tercile → `high_vol`

### v2 (Threshold-Based)
Uses explicit thresholds from `configs/regime_thresholds.yml`:

```yaml
vol_high: 0.20       # Annualized vol above this = high_vol
vol_low: 0.12        # Below this = low_vol
trend_up: 0.02       # 20d return above this = trend_up
trend_down: -0.02    # Below this = trend_down
```

Produces 6 buckets (vol × trend):
- high_vol_trend_up, high_vol_trend_down, high_vol_trend_flat
- low_vol_trend_up, low_vol_trend_down, low_vol_trend_flat

## I.3 Pluggable Registration (YATS)

Regime detectors registered in `configs/regime_detectors/`:

```yaml
# configs/regime_detectors/regime_v1_1.yml
name: regime_v1_1
version: "1.1"
universe: [SPY, QQQ, IWM]    # Fixed primary universe
features:
  - market_vol_20d
  - market_trend_20d
  - dispersion_20d
  - corr_mean_20d
labeling: v2
compute_module: research.features.regime_features_v1
label_module: research.eval.regime_slicing
```

---

# J. Feature Computation — Full Specification

## J.1 Per-Symbol Features (OHLCV-Derived)

All computed from canonical_equity_ohlcv:

| Feature | Formula | Lookback |
|---------|---------|----------|
| ret_1d | log(close_t / close_{t-1}) | 1 day |
| ret_5d | log(close_t / close_{t-5}) | 5 days |
| ret_21d | log(close_t / close_{t-21}) | 21 days |
| rv_21d | std(ret_1d) * sqrt(252) over 21 days | 21 days |
| rv_63d | std(ret_1d) * sqrt(252) over 63 days | 63 days |
| dist_from_20d_high | (close - max(close, 20d)) / max(close, 20d) | 20 days |
| dist_from_20d_low | (close - min(close, 20d)) / min(close, 20d) | 20 days |

## J.2 Cross-Sectional Features

Computed across entire universe per date:

| Feature | Formula | Dependencies |
|---------|---------|--------------|
| mom_3m | Cumulative return over 63 trading days | close prices |
| mom_12m_excl_1m | Cumulative return months 2-12 (skip most recent month) | close prices |
| log_mkt_cap | log(shares_outstanding * close_price) | canonical_financial_metrics + canonical_equity_ohlcv |
| size_rank | Percentile rank of log_mkt_cap within universe on each date | log_mkt_cap |
| value_rank | Percentile rank of 1/pe_ttm within universe on each date | pe_ttm |

Cross-sectional normalization: z-scored within each date after winsorization.

## J.3 Fundamental Features

From canonical_fundamentals and canonical_financial_metrics.
Forward-filled to daily frequency with point-in-time semantics.

| Feature | Source | Notes |
|---------|--------|-------|
| pe_ttm | canonical_financial_metrics.pe_ratio | TTM price/earnings |
| ps_ttm | canonical_financial_metrics.ps_ratio | TTM price/sales |
| pb | canonical_financial_metrics.pb_ratio | Price/book |
| ev_ebitda | canonical_financial_metrics.ev_ebitda | EV/EBITDA |
| roe | canonical_financial_metrics.roe | Return on equity |
| gross_margin | canonical_financial_metrics.gross_margin | Gross margin |
| operating_margin | canonical_financial_metrics.operating_margin | Operating margin |
| fcf_margin | canonical_financial_metrics.fcf_margin | FCF margin |
| debt_equity | canonical_financial_metrics.debt_to_equity | Debt/equity |
| eps_growth_1y | canonical_financial_metrics.eps_growth_yoy | YoY EPS growth |
| revenue_growth_1y | canonical_financial_metrics.revenue_growth_yoy | YoY revenue growth |

## J.4 Labels (for RL training)

| Label | Formula | Description |
|-------|---------|-------------|
| fwd_ret_1d | log(close_{t+1} / close_t) | 1-day forward return |
| fwd_ret_5d | log(close_{t+5} / close_t) | 5-day forward return |
| fwd_ret_21d | log(close_{t+21} / close_t) | 21-day forward return |

Labels are NOT included in observation vectors. They are used for evaluation
and signal analysis only.

## J.5 Global Rules

- All timestamps UTC
- No forward-fill for price data (NaN if missing)
- Fundamentals forward-filled until next report (point-in-time)
- Winsorize at 1st and 99th percentile per symbol before cross-sectional normalization
- Cross-sectional features z-scored within each date
- Features are deterministic: same canonical input produces identical features
- Feature pipeline version hash included in output for lineage
