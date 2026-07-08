# Stage 2d — First WFO-Validated Sweep: Results (2026-07-08)

**Question**: is there real, multiple-testing-survived alpha in the current
signal set (price/vol + fundamentals + regime + options-implied) with PPO?

**Verdict: NO — no config survives deflation.** Best DSR 0.645 (threshold
0.95). Honest infrastructure milestone: the platform can now answer this
question rigorously, and the answer for this signal set + policy class is
"not yet distinguishable from selection noise."

## Setup
- Runner: `research/scripts/wfo_sweep_2d.py` (summary JSON:
  `.yats_data/wfo_sweeps/sweep_2d_full_v1/sweep_summary.json`)
- Universe: dev10 (AAPL AMZN GOOGL JPM META MSFT NVDA QQQ SPY TSLA), daily,
  2024-07-01..2026-07-02 (502 bars), transaction costs 5bp.
- Features: `sweep_v1` = full matrix minus `mom_12m_excl_1m` (its 252-bar
  memory makes purge_buffer=max-lookback geometrically infeasible on 502 bars;
  the feature is null for its first 252 bars regardless). 28 observation
  columns incl. all 5 options features + 4 regime scalars.
- WFO: anchored/expanding, 4 folds, purge=1 (label horizon) + buffer=64
  (feature memory, ya-gy051 sizing). Train windows 185→323 bars; 46-bar OOS
  blocks; concatenated OOS n=184 per config.
- Grid (N=8, the trial set DSR deflates over): lr {3e-4, 1e-4} ×
  ent_coef {0.0, 0.01} × gamma {0.99, 0.95}; PPO, seed 42, n_steps 256,
  batch 64, total_timesteps 20,000 (full convergence per fold — never reduced).

## Results

| cfg | lr | ent | γ | OOS Sharpe | PSR vs 0 | DSR | fold Sharpes |
|---|---|---|---|---|---|---|---|
| 0 | 3e-4 | 0.00 | 0.99 | 0.709 | 0.723 | 0.619 | +2.29 +0.99 +1.20 −1.38 |
| 1 | 3e-4 | 0.00 | 0.95 | 0.242 | 0.581 | 0.465 | +0.92 +0.79 +2.34 −2.44 |
| 2 | 3e-4 | 0.01 | 0.99 | **0.794** | 0.746 | **0.645** | +2.33 +2.16 +0.84 −1.71 |
| 3 | 3e-4 | 0.01 | 0.95 | 0.272 | 0.590 | 0.475 | +1.56 +0.40 +2.80 −2.56 |
| 4 | 1e-4 | 0.00 | 0.99 | 0.192 | 0.564 | 0.449 | +1.43 +1.95 +0.81 −2.64 |
| 5 | 1e-4 | 0.00 | 0.95 | 0.275 | 0.591 | 0.476 | +1.52 +2.27 +0.47 −2.38 |
| 6 | 1e-4 | 0.01 | 0.99 | 0.321 | 0.606 | 0.492 | +1.84 +1.76 +0.78 −2.31 |
| 7 | 1e-4 | 0.01 | 0.95 | 0.074 | 0.525 | 0.410 | +1.45 +2.25 +0.78 −3.11 |

Sweep-level SR0 (expected max Sharpe under H0 from cross-config variance): 0.346.
Rank decay across folds: 0.536 (> 0.5 ⇒ config rankings unstable — the
hyperparameter "winner" flips fold to fold, consistent with noise).

## Reading
1. **No deflated significance.** All 8 configs land DSR 0.41–0.65 vs the 0.95
   bar. Positive raw OOS Sharpes (0.07–0.79) are consistent with selection
   effects at N=8 trials and n=184 OOS observations.
2. **Universal fold-4 failure** (8/8 configs negative, −1.4 to −3.1, early-2026
   OOS window): every variant learned something that did not survive the most
   recent regime. This is the strongest structural signal in the sweep —
   regime-dependence, not hyperparameter choice, dominates outcomes.
3. **Honest purging matters**: an unpurged smoke run showed OOS Sharpe 2.0 on
   the same pipeline; purge+buffer dropped it to ≈0. Most of the apparent edge
   was boundary leakage.

## What this does NOT say
- It does not say the signal set is worthless — n=184 OOS bars and a 2-year
  span is a small sample; a real 0.5-Sharpe edge would be hard to certify here.
- It does not certify PPO at 20k timesteps as the best extractor of these
  features.

## Next directions (user-prioritized)
1. Insider-trades signal from FinancialDatasets (user's stated next alpha
   source; raw table already scaffolded).
2. Longer equity/fundamentals span (options remain 2yr) to grow OOS n.
3. Regime-conditioned policies (the fold-4 failure is the motivation).
Every future idea gets judged by this same WFO+DSR harness.
