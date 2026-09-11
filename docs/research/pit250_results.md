# PIT-250 Sweep — Results (2026-09-11)

Pre-registration: [`preregistrations/2026-09-11_pit250_sweep.md`](preregistrations/2026-09-11_pit250_sweep.md)
(3 amendments, all dated, none silent). Two runs of the same 5 configs:

- **Run 1** — receipt [`receipts/pit250_sweep.json`](receipts/pit250_sweep.json), trials 74-78, clock 73 → 78. Insider panel later found data-defective (amendment 3).
- **Run 2** — receipt [`receipts/pit250_sweep_run2.json`](receipts/pit250_sweep_run2.json), trials 79-83, clock 78 → **83**. Corrected full-history insider/13F panel, registered before running.

Universe: 520 point-in-time symbols (quarterly top-250 by 90d median dollar
volume, 2020-2026), delisted names participating until delisting via the
curated supplement (amendment 1), funds excluded (amendment 2).

## Primary: NOT CERTIFIED — twice, and the correction cut the best number

| config | run 1 Sharpe | run 2 Sharpe (real insider data) |
|---|---|---|
| lgbm_21d (insider-incl) | 0.447 | **0.283** |
| lgbm_5d (insider-incl) | 0.651 | **0.358** |
| lgbm_21d_no_insider | 0.437 | **0.450** |
| best base + vt10 | 0.689 (5d) | 0.386 (21d_no_ins) |
| best base + rcvt | 0.724 (5d) | 0.326 (21d_no_ins) |

Run 2 best: `lgbm_21d_no_insider` at OOS Sharpe 0.450 net, strict DSR
**0.439** vs the 0.95 bar at 83 trials. Nothing close. Run 1's better-looking
0.724 was produced on a panel whose insider columns were ~97% null — i.e. its
insider-inclusive models were accidental no-insider models with 6 dead
columns.

## The insider hypothesis is now, honestly, dead at scale

With real Form 3/4/5 history (66% row coverage, 487 symbols, ~500k trades),
adding insider features *subtracted* Sharpe at every horizon:
insider-inclusive minus control margin = **−0.167**. The full gradient reads
−0.99 @10 mega-caps, +0.10 @50 large-caps, **−0.17 @250 PIT names**. The
pilot's +0.10 — 3-of-4 positive, mean small — no longer looks like down-cap
strengthening; it looks like noise between two nearly-identical models. The
evaluation harness did what it exists to do: the moment the data became real,
the story fell apart.

## Breadth did not deliver either

250 PIT names vs 50: every insider-inclusive config degraded; the clean
control was flat (0.437 → 0.450). The IR ≈ IC×√breadth lift has now failed
to materialize at two scale-ups in a row. Mid-caps carry the best per-name
contribution (cap-tercile readout: mid ≈ 0.69 contribution Sharpe vs large
≈ 0.15 for lgbm_21d), so the breadth premium exists in cross-section — but
it is not additive enough to survive costs and deflation.

## Coverage report (the amendment-1 commitment)

Delisted names have real alternative data: insider 69% / institutional 83%
row coverage among span-end-inactive symbols (vs 78%/88% active). The
survivorship-bias correction is substantive, not cosmetic. Remaining honest
gaps: ADRs and foreign issuers file no Form 4s (structurally null insider
columns); the curated delisted supplement is non-exhaustive; SIVB/FRC/SBNY
sat just below the liquidity screen ($222-234M median $vol vs $250M cutoff)
and are correctly absent from membership.

## Infrastructure findings this family surfaced (all fixed, committed)

1. FD insider endpoint hard-caps at ~10 rows/request → cursor pagination
   (`212d185`); 13F quarters now span the backfill window.
2. regime_v2 features were silently all-null since introduction (label
   mismatch at the join; `c6862af`) — legacy sweep_v1r/v3 trials listing
   `spy_atm_iv` effectively ran without it.
3. QuestDB reaps idle PG connections during long feature computes →
   reconnecting cursor (`00a04f0`).
4. T-1 data-window defaults now anchor to America/New_York (`10575d3`).

## Fold-1 caveat

The 253-bar purge (mom_12m lookback) leaves fold 1 with a 1-bar train
window on the 1,680-date span; it contributes a forced 0.0 to 4-fold base
Sharpes and is excluded from overlay folds. Registered geometry, reported
as-is; a longer span, not a smaller purge, is the fix.

## Standing verdict

Certified strategies: **0** at 83 trials. The incumbent best attempt remains
the 10-name vol-targeted lgbm_21d (1.134 gross, strict DSR 0.921 @63; the
gross/net caveat from the pilot doc still applies). The breadth arc is
concluded: neither 50 nor 250 names improved on it, and the insider
hypothesis failed its scale test. Next directions worth registering must
change the *signal*, not just the universe.
