# Stage 4b — regime_v2 3-Arm Sweep: Results (2026-07-08)

**Question**: do market-implied regime features (SPY IV level/zscore, VRP,
term inversion, skew zscore, GEX, 5d transition deltas) improve deflated OOS
performance — and specifically the fold-4 (recent-regime) failure?

| arm | mean OOS Sharpe | fold-4 mean |
|---|---|---|
| A `sweep_v1` (baseline) | **2.17** | +0.24 |
| B `sweep_v1r` (+regime_v2, PRIMARY) | 1.08 (worse 7/8) | **+1.32** |
| C `sweep_v3` (full stack) | 0.95 | −1.25 |

Best DSR 0.770 < 0.95 over the 24-trial union (SR₀ 1.38); rank decay 0.33.
Geometry: matched purge 1+105 all arms, anchored ×4, n≈148 OOS bars.
**Deflation clock: 48 trials.**

## The replicated pattern — and what it actually means
Across two INDEPENDENT feature families (insider/institutional in 3d,
market-implied regime here), the same signature appears:
- adding observation dimensions **degrades total OOS** (−0.9 to −1.1 mean), and
- **improves the final fold** (+1.1 to +1.3) when the features are slow-moving,
- while combining both families (arm C) loses even the fold-4 benefit.

This is no longer a statement about any one signal. It says the **extractor is
the bottleneck**: a 20k-step PPO on 10 symbols cannot exploit additional
inputs; they act as implicit late-window regularizers and noise elsewhere.

## Decisions
1. Research set remains `sweep_v1`.
2. Next per approved sequence: span extension (running), then champion
   re-verdict on the extended span, then **extractor ablations**
   (timesteps/capacity/SAC/reward), then **regime-conditioned policy** —
   feed the slow features (regime_v2, inst) to the ModeController/hierarchy
   layer instead of the PPO observation vector. Both fold-4 results predict
   exactly this architecture.
