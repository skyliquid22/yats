# ALPHA-1 — Supervised Cross-Sectional Alpha Track: Results (2026-07-09)

**Question**: Can a supervised cross-sectional model (Ridge / LightGBM predicting
residualized forward returns) match or beat the PPO champion (sweep_v1, mean OOS Sharpe 2.17)
at matched geometry — and does it avoid the feature-addition degradation the extractor bottleneck
analysis (4b) identified as PPO's failure mode?

**Verdict: SUPERVISED IS WEAKER.** Best supervised config (ridge_21d_a10) achieves mean OOS
Sharpe 1.680 vs PPO 2.17 (ΔvsPPO = −0.49). No config survives deflation over the 54-trial
cumulative union (best DSR 0.885, SR₀ = 0.177, threshold 0.95). Rank decay 0.600 = high
regime instability. PPO remains the research champion.

## Setup

Runner `research/scripts/wfo_sweep_alpha1.py`; summary
`.yats_data/wfo_sweeps/sweep_alpha1/sweep_summary.json`.
Module: `research/alpha/` (transforms, targets, models, portfolio).
New deps: `lightgbm>=4.0`, `scikit-learn>=1.4` (added to `pyproject.toml`).

**Universe**: dev10 (AAPL, AMZN, GOOGL, JPM, META, MSFT, NVDA, QQQ, SPY, TSLA).
**Span**: 2024-07-01 – 2026-07-02 (502 trading days).
**Feature set**: sweep_v1, 27 obs cols (ohlcv×7, cross-sectional×4, fundamental×11,
options×5; regime cols excluded — cross-sectionally constant, degenerate after rank-normalization).
**Feature transform**: cross-sectional rank normalization per date (percentile rank → z-score).
**Target**: residualized forward return (fwd_h − β_i × fwd_SPY), rolling 60-day OLS beta.
**Portfolio**: rank-weighted long-only, max_symbol_weight = 0.30.
**WFO**: anchored, 4 folds, train_window=250, label_horizon=21, purge_buffer=63
(gap = 84 bars; 42 OOS bars/fold, 168 total).

*Geometry note*: test windows are 42 bars/fold vs PPO champion's ~36 bars/fold (different purge
gap: 84 for supervised vs 106 for PPO). Test dates are offset; direct Sharpe comparison is
approximate.

## Model Grid (6 configs — ALPHA-1 trial set)

| # | config | model | horizon | reg |
|---|--------|-------|---------|-----|
| 0 | ridge_5d_a1   | Ridge | 5d  | α=1.0  |
| 1 | ridge_5d_a10  | Ridge | 5d  | α=10.0 |
| 2 | ridge_21d_a1  | Ridge | 21d | α=1.0  |
| 3 | ridge_21d_a10 | Ridge | 21d | α=10.0 |
| 4 | lgbm_5d       | LGBM  | 5d  | λ=0.1  |
| 5 | lgbm_21d      | LGBM  | 21d | λ=0.1  |

## Results

| config | OOS Sharpe | fold-1 | fold-2 | fold-3 | fold-4 | DSR | SR₀ |
|--------|-----------|--------|--------|--------|--------|-----|-----|
| ridge_5d_a1   | 1.431 | +3.70 | +4.28 | +0.23 | −1.67 | 0.839 | 0.177 |
| ridge_5d_a10  | 1.398 | +3.70 | +3.87 | +0.17 | −1.58 | 0.832 | 0.177 |
| ridge_21d_a1  | 1.606 | +3.79 | +3.50 | +0.52 | −0.52 | 0.872 | 0.177 |
| **ridge_21d_a10** | **1.680** | +3.73 | +3.42 | +0.50 | **−0.28** | **0.885** | 0.177 |
| lgbm_5d       | 1.266 | +2.97 | +4.65 | +0.00 | −1.25 | 0.808 | 0.177 |
| lgbm_21d      | 1.440 | +3.33 | +4.03 | +0.43 | −1.48 | 0.843 | 0.177 |

Rank decay (across folds): **0.600** — high, rankings unstable.
PPO champion (sweep_v1, 4b sweep, ~36-bar folds): mean OOS Sharpe **2.17**.
Sweep elapsed: 11 seconds (vs hours for PPO WFO sweep).

## Reading

1. **Supervised is weaker than PPO at the same horizon.** Mean OOS Sharpe 1.680 vs 2.17
   (−23%). This is the expected baseline — a linear cross-sectional model and a 200-step PPO
   predict very different things. The supervised model is a direction-neutral predictor that
   ranks stocks by expected residual return; PPO outputs portfolio weights directly with
   temporal dynamics.

2. **21d horizon dominates 5d.** Ridge_21d configs outscore ridge_5d by ~0.25 in OOS Sharpe.
   The 21d label aggregates more signal (slower fundamental/momentum factors operate on longer
   horizons) and overlapping labels help averaging. LightGBM shows the same pattern.

3. **Moderate regularization helps.** Ridge α=10 > α=1 for the 21d target (1.680 vs 1.606).
   With only 10 symbols and 166–292 training samples, stronger shrinkage towards zero is
   appropriate. LightGBM doesn't help here — small dataset, few symbols, noise overwhelms
   nonlinear capacity.

4. **High rank decay (0.60) and fold-4 negativity persist.** The identical pattern from PPO:
   folds 1–2 strong (3–4 Sharpe), fold 3 near-zero, fold 4 negative. The supervised model
   doesn't solve the regime-shift problem; it just expresses it differently. Best config
   (ridge_21d_a10) fold-4 = −0.28 — better than most PPO configs but still negative.

5. **No config survives deflation (54-trial clock).** SR₀ = 0.177 is low (cross-config
   Sharpe variance is moderate) but 6 trials is insufficient to achieve significance at 0.95
   given only 42 OOS bars/fold. Cumulative deflation clock: **48 (prior) + 6 = 54 trials**.

6. **The extractor-bottleneck hypothesis is confirmed from the other direction.** If the PPO
   extractor (20k timesteps) were the bottleneck, adding features hurts (4b result). The
   supervised model exploits the same 27 features directly and also doesn't match PPO. This
   suggests the PPO's strength isn't purely feature utilization — it has temporal policy
   dynamics the cross-sectional model lacks.

## Decisions

1. PPO remains the research champion for portfolio policy. Supervised track is not adopted
   as a replacement.
2. Supervised scores are useful as **input signals**: use alpha predictions from ridge_21d_a10
   as a feature or allocation constraint in a hybrid RL/alpha architecture (ALPHA-3 hook).
3. The consistent fold-4 failure across BOTH supervised and RL models is a data-regime issue,
   not an architecture issue. The 2025-H2/2026 window requires span extension (ya-22h1b,
   already running) before re-verdict.
4. LightGBM doesn't add value over Ridge at 10-symbol scale. If universe expands (ALPHA-4:
   1k symbols), LightGBM should be revisited — nonlinear capacity matters at scale.
5. **Deflation clock: 54 trials.**
