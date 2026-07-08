# Stage 3d — Insider/Institutional A/B Sweep: Results (2026-07-08)

**Question**: do the 6 insider/institutional features (insider_net_buy_90d,
insider_buy_intensity_30d, insider_cluster_30d, exec_net_buy_90d,
inst_ownership_pct, inst_top10_share) marginally improve deflated OOS
performance on top of sweep_v1?

**Verdict: NO — the enriched arm is uniformly WORSE.** Every one of the 8
matched configs degrades (ΔSharpe −0.73 to −1.31, mean −0.99). No config in
either arm survives deflation over the 16-trial union (best DSR 0.835 vs
0.95; SR₀ = 1.057). Do not adopt sweep_v2 as the research feature set.

## Setup
Runner `research/scripts/wfo_sweep_3d.py`; summary
`.yats_data/wfo_sweeps/sweep_3d_insider_ab/sweep_summary.json`.
Same 8-config PPO grid as 2d, seed 42, 20k timesteps, 5bp costs. Anchored
4-fold WFO, **matched geometry both arms: purge = 1 + 105** (sweep_v2's
feature memory; baseline deviates from its auto-sized 64 so OOS windows are
identical). n = 148 OOS bars/config.

## Results (mean OOS Sharpe per arm)

| arm | features | mean | range | fold-4 mean |
|---|---|---|---|---|
| A `sweep_v1` | 28 obs cols | **2.17** | 1.77–2.41 | +0.24 |
| B `sweep_v2` | 34 obs cols (+6) | **1.18** | 0.56–1.58 | +1.25 |

Per-config Δ(B−A): −1.23 −0.73 −1.20 −1.31 −0.73 −0.98 −0.98 −0.77 — 8/8 negative.
Rank decay across the union: 0.342 (stable rankings this time).

## Reading
1. **The added features hurt overall performance in every configuration.**
   Most likely mechanisms, in order of plausibility: (a) 6 extra observation
   dimensions of sparse, slow-moving, NaN-heavy input (insider features are
   null on 0–100% of days per symbol; JPM has essentially no insider data)
   act as noise the 20k-timestep PPO fits at the expense of signal;
   (b) the signals genuinely carry no daily-horizon alpha for mega-caps —
   consistent with the literature: insider-trade alpha concentrates in
   small/mid caps, and 13F ownership levels move quarterly.
2. **One genuine positive: fold 4.** Arm B's final-window Sharpe is positive
   in 8/8 configs (mean +1.25 vs +0.24) — the slow institutional features may
   stabilize the recent-regime window even while dragging total performance.
   Worth revisiting if regime-conditioned policies (4a/4b) make selective use
   of slow features possible.
3. **Window-placement caution.** The same baseline grid scored 0.07–0.79 at
   purge=64 (2d) and 1.77–2.41 at purge=105 (different OOS windows). With
   ~46-bar folds, OOS Sharpe is substantially a window lottery — precisely
   why DSR (still not significant, 24 trials burned to date) and not raw
   Sharpe is the decision criterion, and why the span-extension enabler
   (ya-22h1b) matters.

## Decisions
- Research feature set remains `sweep_v1` (+regime_v2 pending 4a/4b).
- sweep_v2 features stay computed and available; revisit insider signals on
  small/mid-cap universes or as inputs to a regime/controller layer rather
  than raw PPO observations.
- Deflation clock: 8 (2d) + 16 (3d) = **24 trials**.
