# YATS — Yet Another Trading System

YATS is a trading research platform whose referee keeps saying **no**. It runs the full loop (multi-vendor ingestion, canonical data plane, deterministic features, walk-forward training and evaluation, shadow replay, paper execution), but the part it takes most seriously is the certification gate: honest next-bar execution, purged walk-forward optimization, and a Deflated Sharpe Ratio test with a **public trial clock** that counts every configuration ever tried against every new result. After 63 logged trials across PPO, supervised, feature-ablation, and risk-overlay sweeps, nothing has crossed the bar yet. The negative results are published in-repo, because a platform that can't say "no signal" rigorously can't be trusted when it eventually says "yes."

## Who this is for

- Quant researchers who want their backtests **distrusted by default**: every new result deflated against every trial ever burned.
- ML people who want a leak-proof evaluation harness: purged anchored folds, lookback-aware purge buffers, next-bar fills.
- Anyone who wants to watch a Deflated Sharpe referee reject 63 straight attempts in public, with receipts.
- **Not** for anyone seeking turnkey profits: the best certified alpha here is *none*, and that is the point.

## Quickstart (no API keys needed)

```bash
git clone <this repo> && cd yats
make demo
```

`make demo` replays the bundled `demo/` dataset through the research harness (feature computation, a small purged walk-forward sweep, and a Deflated Sharpe verdict) with no vendor credentials required. See [Setup with real data](#setup-with-real-data) to plug in live vendors.

The demo install is deliberately lean: it does **not** pull the RL stack (torch, stable-baselines3, gymnasium live in an optional `rl` extra). Demo install is about 800 MB; the full research stack (`uv sync --extra rl`, CPU-pinned torch) is about 1.5 GB.

Run the test suite (no services required; live-DB tests skip automatically, and the RL trainer tests need the `rl` extra):

```bash
PYTHONPATH=.:pipelines uv run --extra rl --with pytest --with pytest-timeout \
  pytest tests -q --timeout=120 -k "not live"
```

1,716 tests collected; 1,684 run without live vendors/DB.

## Architecture

```mermaid
flowchart LR
    subgraph vendors [Vendors]
        A[Alpaca<br/>OHLCV + trading]
        T[ThetaData<br/>options]
        F[financialdatasets.ai<br/>fundamentals, 13F]
    end

    vendors --> I[Ingest<br/>Dagster jobs]
    I --> R[raw_* tables<br/>append-only,<br/>vendor-shaped]
    R --> C[(canonical_*<br/>point-in-time, DEDUP-<br/>idempotent, lineage)]
    C --> FE[Features<br/>deterministic registry,<br/>lookback accounting]
    FE --> E[Experiments<br/>spec-hashed<br/>content-addressed IDs]
    E --> H[WFO / DSR harness<br/>purged folds,<br/>Deflated Sharpe + trial clock]
    H --> Q[Qualification gates<br/>hard + soft]
    Q --> P[Promotion tiers<br/>research → candidate<br/>→ production]
    P --> S[Shadow replay<br/>→ paper trading]

    M[MCP server<br/>72 tools, TypeScript] -.-> C
    M -.-> H
    D[Ops dashboard<br/>read-only FastAPI] -.-> C
```

- **Raw → canonical**: every vendor write is append-only raw; only reconciled canonical tables (with lineage) feed anything downstream.
- **Honest execution**: signals computed on bar *t* fill at bar *t+1* (`fill_timing`: next_close or next_open); no same-bar fills.
- **Purged WFO**: anchored expanding folds with purge = label horizon plus a feature-memory buffer, so no training fold sees leaked future data.
- **Deflated Sharpe with a trial clock**: every config in every sweep increments a cumulative trial count; DSR deflates each new result against the whole history. Current clock: **63 trials** ([docs/research/full_span_verdicts.md](docs/research/full_span_verdicts.md)).
- **MCP-native**: all capabilities are exposed as MCP tools callable by agents or notebooks; a read-only [dashboard](docs/dashboard.md) sits alongside.

### Anatomy of an experiment

Every run is an `ExperimentSpec` ([research/experiments/spec.py](research/experiments/spec.py)), frozen and content-addressed: the SHA256 of its canonical JSON is the `experiment_id`, so changing any field is a new trial on the clock.

```yaml
experiment_name: alpha1_lgbm_21d
symbols: [AMD, NFLX]          # sorted + deduped, so ordering can't fork the hash
interval: daily
feature_set: core_v1          # registry-resolved; its max lookback sizes the purge
policy: ppo                   # equal_weight | sma | ppo | sac[_*] | hierarchical
cost_config: {transaction_cost_bp: 5.0, slippage_bp: 0.0}
seed: 42                      # one seed, deterministic run
wfo_config:
  n_periods: 4
  train_window: 504           # anchored mode: window expands, never drops history
  label_horizon: 1            # bars purged at every train/test boundary
  purge_buffer: null          # null = auto from the feature set's max lookback
execution_lag_days: 1         # decide on bar t, fill on bar t+1; same-bar fills (0)
fill_timing: next_close       # are legacy-only and warn loudly: a close-time signal
                              # filled at that same close is lookahead, not execution
```

## Results (the honest table)

**Best certified alpha: none.** The certification bar is DSR ≥ 0.95 after deflation over all trials ever burned.

| Sweep | Question | Best result | Verdict |
|---|---|---|---|
| [2d: first WFO sweep](docs/research/wfo_sweep_2d_results.md) | Real alpha in the signal set with PPO? | DSR 0.645 | No |
| [3d: insider/institutional A/B](docs/research/wfo_sweep_3d_results.md) | Do insider features help? | Enriched arm uniformly worse; DSR 0.835 | No |
| [4b: regime_v2 3-arm](docs/research/wfo_sweep_4b_results.md) | Do market-implied regime features help? | Worse overall, better final fold; DSR 0.770 | No (extractor is the bottleneck) |
| [ALPHA-1: supervised track](docs/research/wfo_sweep_alpha1_results.md) | Ridge/LightGBM vs PPO? | Sharpe 1.68 on the short span; DSR 0.885 | No |
| [Full-span re-verdicts, honest execution](docs/research/full_span_verdicts.md) | Do results survive 6.5 years + next-bar fills? | lgbm_21d: OOS Sharpe 0.905, DSR 0.943 | No |
| [Vol-targeting overlay on lgbm_21d, trial 63](docs/research/full_span_verdicts.md) | Does the ALPHA-3 risk layer lift DSR? | **OOS Sharpe 1.134, strict DSR 0.921 vs the 0.95 bar** | Closest yet, still no |

Trial clock at last verdict: **63 trials burned**. Nothing is certified; the closest attempt sits at DSR 0.92 against the 0.95 bar. On trial 63 a naive 3-trial benchmark printed DSR 0.988, and was rejected in favor of the strict computation deflated over all 63 trials, which is exactly the self-flattering shortcut the public trial clock exists to prevent (see the addendum in [full_span_verdicts.md](docs/research/full_span_verdicts.md)).

An earlier era of Sharpe ≈ 2.17 results on a 2-year window died under span extension and honest fills; that post-mortem is in the full-span verdicts doc. The docs above are kept as a **lab notebook**: dated, immutable, and linked from every claim.

Raw sweep outputs live in [`docs/research/receipts/`](docs/research/receipts/). Honest scope of the reproduction claim: re-running these sweeps requires vendor API keys and a populated database (see [`docs/ingestion.md`](docs/ingestion.md)); results will drift on vendor restatements, and no input-data snapshot is distributed. The receipts are the record of what was observed, not a bit-reproducible artifact. The `make demo` verdict, by contrast, is fully deterministic.

## Setup with real data

Full vendor setup, the one-command symbol backfill, and the raw → canonical pipeline are documented in [docs/ingestion.md](docs/ingestion.md).

```bash
cp .env.example .env       # fill in vendor keys (Alpaca, ThetaData, financialdatasets.ai)
docker compose up -d       # QuestDB
python scripts/bootstrap_db.py
PYTHONPATH=.:pipelines uv run python -m yats_pipelines.backfill \
    --symbols NFLX,AMD --start 2020-01-01
```

Reference material:

- [docs/REFERENCE.md](docs/REFERENCE.md): MCP tool catalog, QuestDB schemas, config reference
- [docs/ingestion.md](docs/ingestion.md): ingestion architecture and backfill runbook
- [docs/dashboard.md](docs/dashboard.md): read-only operations dashboard
- [docs/your_first_experiment.ipynb](docs/your_first_experiment.ipynb): general-audience walkthrough: run your first experiment
- [docs/yats_for_quanto_users.ipynb](docs/yats_for_quanto_users.ipynb): notebook walkthrough for quant researchers
- [configs/risk.yml](configs/risk.yml): the static risk contract (no model may override it)

## Troubleshooting

- **pytest hangs or deadlocks on macOS**: set `OMP_NUM_THREADS=1` (`make test` already does): torch and lightgbm each bundle their own libomp, and the duplicate OpenMP runtimes deadlock in torch's QR init.
- **`live` tests fail**: they need a running Theta terminal / live DB; run with `-k "not live"` as in the Quickstart.
- **Connection refused on real-data paths**: QuestDB must be up: `docker compose up -d`. `make demo` needs no services at all.
- **MCP tool calls clamped to `intern`**: elevated roles must be operator-allowlisted via `YATS_ALLOWED_ROLES` (comma-separated role names, or `*`); anything else falls back to least privilege.
- **financialdatasets.ai data looks truncated or wrong**: known vendor limits (insider trades cap at 500 rows/ticker, 13F `value_usd` unreliable before 2025-06, ETFs have no insider/fundamentals data) are documented in [docs/ingestion.md](docs/ingestion.md#vendor-caveats).
- **`npm test` (MCP server) misbehaves**: use Node ≥ 22, the version CI pins.

## Roadmap (not yet built)

None of the following exists today; no dates are promised.

- **v1.1 (external LLM policies over MCP)**: let external agents propose allocations through the MCP tool surface, subject to the same risk contract and certification gate as any other policy.
- **Universe expansion**: beyond the dev10 development universe toward the full S&P 500 list already in `configs/universes/`.
- **Live trading**: the execution layer and kill switches exist and are paper-tested; live capital waits on a certified policy, i.e. on the referee finally saying yes.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for dev setup, test commands, and the no-lookahead discipline every PR is held to.

## License

[MIT](LICENSE), copyright YATS contributors.
