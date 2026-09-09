# Full-Span Re-Verdicts under Honest Execution (2026-08-21)

First results on 2020-01..2026-07 (~1,630 bars) with execution_lag_days=1
(b77155f). Anchored 4-fold WFO, matched geometry, OOS n ≈ 1,300 bars/config.

| track | best config | OOS Sharpe | DSR | rank decay |
|---|---|---|---|---|
| PPO champion (8-cfg grid, sweep_v1) | lr 3e-4 grid best | 0.756 | 0.898 | 0.32 |
| **Supervised (6-cfg, ALPHA-1)** | **lgbm_21d** | **0.905** | **0.943** | 0.33 |

Neither crosses DSR 0.95; no certified alpha yet. But:

1. **Supervised beats PPO on its first outing** (0.905 vs 0.756, same span,
   same honest fills, ~40x cheaper to run). The decompose-prediction-from-
   allocation thesis (docs/research/wfo_sweep_4b_results.md) is supported.
2. **The 2.17-era numbers are dead.** The 2-yr purge-105 window's Sharpes were
   window lottery + same-bar fill inflation, exactly as suspected. The span
   extension and EXEC-LAG were the right calls, in the right order.
3. lgbm_21d at DSR 0.943 on a real sample with stable rankings is the closest
   this platform has come to certified skill. Next levers, in order of
   promise: risk overlay (ALPHA-3 vol targeting, merged but not yet applied
   to the supervised track; dispersion reduction directly raises DSR), then
   narrow ablations around lgbm_21d (each is a trial; clock now 62).
4. Runner bug to fix: wfo_sweep_alpha1.py compares vs a stale hardcoded
   "PPO champion Sharpe=2.17"; the honest same-span comparison is 0.756.

Deflation clock: 48 + 8 (champion rerun) + 6 (supervised) = **62 trials**.

## Addendum (2026-09-08): vol-targeting overlay on lgbm_21d (trial 63)

Applying the ALPHA-3 risk layer (10% vol target) to the lgbm_21d winner, same
full-span WFO geometry: **OOS Sharpe 1.134** (from 0.905), all four folds
positive (1.76/1.08/0.58/1.13), n=1,292. A naive 3-trial benchmark printed
DSR 0.988; the STRICT computation (expected-max benchmark scaled to all 63
trials over the full-span honest-fill pool) gives **DSR 0.921: not
significant**. (Mixing dead-methodology-era trials into the pool would be
methodologically wrong but is reported for transparency: 0.19.)

Standing verdict: still nothing certified. Closest attempt: 0.92 vs the 0.95
bar. Deflation clock: **63 trials**. Raw result JSON:
`.yats_data/wfo_sweeps/vt_lgbm21_result.json`.
