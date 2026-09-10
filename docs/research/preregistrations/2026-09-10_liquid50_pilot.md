# Pre-Registration: liquid50 Universe-Breadth Pilot (2026-09-10)

**Status: REGISTERED — no runs launched.** Registered per the convention in
`docs/research/preregistrations/README.md`. Deflation clock at registration:
**63 trials** (`docs/research/full_span_verdicts.md`).

## Background and hypothesis

The platform's best result to date is lgbm_21d + 10% vol targeting on the
10-name mega-cap universe (dev10), 2020-01..2026-07: OOS Sharpe 1.134, strict
DSR **0.921** vs the 0.95 certification bar at 63 trials. Close, not
certified.

The fundamental law of active management says IR ≈ IC × √breadth: if the
signal class has real (but thin) cross-sectional skill, widening the universe
from 10 to 50 names should raise risk-adjusted performance without any change
to the signal itself. Ten names is a degenerate cross-section — rank
normalization over 10 values is coarse, and one idiosyncratic name dominates a
fold. Fifty names is the cheapest meaningful test of whether the incumbent
signal class scales with breadth.

**H1 (breadth)**: the incumbent supervised signal class ({ridge, lgbm}
cross-sectional rank models on price/fundamental/regime features), moved from
10 to 50 names with no other changes, improves risk-adjusted OOS performance,
with certification (strict DSR > 0.95) as the headline success condition.

**H2 (insider/institutional resurrection)**: insider and institutional
features, which were unrankable on 10 names, contribute incremental OOS
performance at 50 names. *Honest caveat, stated in advance*: a
top-50-by-liquidity universe is still large-cap, and the insider-trading
literature consistently finds insider alpha concentrated in small/mid-caps
where information asymmetry is largest. A null result for H2 here does **not**
kill the insider hypothesis in general — it kills it for large-caps only. The
small/mid-cap version would need its own universe and its own
pre-registration.

## Universe

`configs/universes/liquid50.yml` — top 50 US equities by trailing dollar
volume, ETFs excluded, constituency and survivorship documented in that
config (built in a parallel branch; this prereg binds to the config file as
committed at launch time).

**Survivorship-bias acknowledgment**: liquid50 is selected using
present-day liquidity, so the pilot's span includes names partly *because*
they survived and grew. The pilot accepts this bias, documented here, because
(a) the primary readout is a same-universe A/B against the 10-name incumbent,
which carries the same flavor of bias, and (b) building point-in-time
constituency is a data-engineering project in its own right. **A point-in-time
constituency pipeline is required before any claim is extended to a ~250-name
universe or before any certified result from this pilot is represented as
investable.**

## Arms and exact configs

Feature set: `configs/feature_sets/breadth_v1.yml` (sweep_v1 minus options —
no options data for the 40 new names, and 40-null columns would bias
cross-sectional ranks — plus mom_12m_excl_1m restored, plus
insider/institutional groups per sweep_v2).

- **Arm A — pure breadth test (H1)**: breadth_v1 **without** the insider and
  institutional groups (i.e. its ohlcv, cross_sectional, fundamental, and
  regime groups only; to be committed as an exact group-subset config file
  before launch). This isolates the breadth effect on the incumbent signal
  class: same features that certified-adjacent on 10 names, now on 50.
- **Arm B — resurrection test (H2)**: full breadth_v1, adding the four
  insider and two institutional features. B-vs-A is the marginal-signal
  readout, subject to the large-cap caveat above.

Per arm, 4 base configs at the incumbent regularization (ALPHA-1 winners:
ridge α=10.0, lgbm λ=0.1):

    {ridge, lgbm} × {5d, 21d}

plus **one** vol-target (10%, ALPHA-3 risk layer) overlay applied to that
arm's best base config by OOS Sharpe. The overlay choice rule is committed
here in advance; no other overlays, ablations, or regularization values will
be run under this registration.

## Trial budget

| arm | trials | configs |
|---|---|---|
| A: breadth_v1 minus insider/institutional | 4 | ridge_5d_a10, ridge_21d_a10, lgbm_5d, lgbm_21d |
| A: vol-target 10% overlay on arm-A best base | 1 | chosen by OOS Sharpe among arm A's 4 base configs |
| B: full breadth_v1 | 4 | ridge_5d_a10, ridge_21d_a10, lgbm_5d, lgbm_21d |
| B: vol-target 10% overlay on arm-B best base | 1 | chosen by OOS Sharpe among arm B's 4 base configs |
| **total charged to deflation clock** | **10** | **clock 63 → 73** |

Every DSR computed under this registration uses the post-family count of
**73 trials**, even for configs evaluated before all 10 runs complete.

## Geometry and execution

- **Span**: 2020-01 .. present (~1,640+ trading days at launch).
- **WFO**: anchored, 4 folds, matched to the full-span incumbent geometry.
- **Purge**: purge_buffer = 1 + max feature lookback = **253 bars**
  (max lookback 252 from mom_12m_excl_1m, per the feature registry). This is
  exactly the purge that was infeasible on the old 502-bar span and is the
  reason mom_12m_excl_1m can return.
- **Label horizon**: 21 bars (max prediction horizon), purged at fold edges
  as in the incumbent harness.
- **Execution**: `execution_lag_days = 1`, `fill_timing = next_close`,
  `transaction_cost_bp = 5`. Honest-fill rules identical to the b77155f
  regime; no same-bar fills anywhere.
- **Expected OOS sample**: ~1,200+ bars per config (4 folds; slightly below
  the incumbent's 1,292 because the larger purge consumes more of the span).

## Success and failure criteria (committed in advance)

- **Primary / certification**: any of the 10 configs achieves **strict DSR >
  0.95**, where strict means the expected-maximum-Sharpe benchmark scaled to
  all **73 trials** over the honest-fill result pool (the same computation
  that produced 0.921 for the incumbent — not the naive per-family
  benchmark). First crossing = first certified alpha on the platform.
- **Secondary readout (the direct breadth-effect measurement)**: same-class
  Sharpe comparison, Sharpe(50-name) vs Sharpe(10-name), for each of the four
  base config classes against their full-span dev10 counterparts, plus arm B
  minus arm A per class for the insider/institutional margin. Reported
  regardless of certification outcome.
- **Failure is a reportable result**: if no config clears 0.95 and the
  Sharpe(50) vs Sharpe(10) comparison shows no improvement, the conclusion is
  that breadth does not rescue this signal class at large-cap liquidity — the
  √breadth term is not the binding constraint — and the next lever must be IC
  (signal quality), not more names. That verdict gets written up with the same
  prominence a success would.

## What will be reported either way

A results document `docs/research/liquid50_pilot_results.md` containing: all
10 configs' per-fold and pooled OOS Sharpes, strict DSRs at 73 trials, rank
decay, the Sharpe(50) vs Sharpe(10) table, arm B−A margins, and raw result
JSON receipts under `docs/research/receipts/`. The deflation clock is updated
to 73 in `docs/research/full_span_verdicts.md` (or its successor) when the
first run launches, not when results look good.
