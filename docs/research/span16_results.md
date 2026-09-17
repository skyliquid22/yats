# SPAN-16 Family — Results (2026-09-17)

Pre-registration: [`preregistrations/2026-09-16_span16_family.md`](preregistrations/2026-09-16_span16_family.md).
Receipt: [`receipts/span16_sweep.json`](receipts/span16_sweep.json).
5 trials as registered; clock 83 → **88**. Universe: span16 PIT membership,
631 all-time symbols, 620 in-panel, 2,629 trading dates.

## Primary: NOT CERTIFIED — every trial negative

| trial | primary Sharpe | all-folds | strict DSR |
|---|---|---|---|
| lgbm_5d_clean_N | **−0.144** | −0.109 | 0.007 |
| lgbm_5d_clean_R | −0.191 | −0.029 | 0.005 |
| lgbm_21d_clean_N | −0.277 | −0.188 | 0.003 |
| lgbm_21d_clean_R | −0.317 | −0.117 | 0.002 |
| lgbm_5d_clean_N + vt10 | −0.375 | — | 0.001 |

SR0 rose to 0.877 (pool std widened by this family's own negative
sharpes — the registered method, working in the punitive direction).

## Kill criterion FIRED: label engineering is dead

N-minus-R margins: **+0.040 (21d), +0.047 (5d)** — both under the
pre-committed 0.10 bar. PCA factor neutralization helped, mildly and
consistently, and nowhere near enough. Per the registration: the next
family must source new *data*, not new transforms.

## The real finding: the signal is non-stationary, and anchored training poisons it

The era split (receipted daily series) is stark and uniform across all
five trials:

- **Pre-2020 OOS: +1.10 to +1.34.** The excluded early folds agree
  (fold 0, ~2017: +1.26 — excluded by the pre-committed rule *despite*
  being positive; the rule was fixed ex ante and cut against us, which is
  exactly what makes it credible).
- **Post-2020 OOS: −0.21 to −0.50** — across the same 2023-2026 windows
  where pit250 run 2 scored **+0.45** with the identical clean config and
  features.

The controlled difference between those two numbers is the training
window: pit250 trained anchored-from-2020 (recent data only); span16
trained anchored-from-2016 (majority stale regime). ≈0.85 Sharpe of pure
training-window effect. The cross-sectional momentum/fundamental signal
this platform has been testing existed in 2016-2019, decayed hard after
2020, and models fed the old regime actively mispredict the new one.
Longer history bought more statistical power and used it to reject the
strategy class — the harness working as designed.

Corollary: the certification hope built on "same Sharpe, more bars" was
wrong in the interesting way — the Sharpe was not a property of the
strategy, it was a property of the era.

## Deviations and honest gaps

1. Membership starts 2016-04-01, not 2016-01-04 (the builder drops the
   first rebalance on window completeness); one quarter shorter than
   registered, 7 complete folds vs "expected ~8".
2. **Sector diagnostic: unavailable.** The FD account exhausted its quota
   mid-family (~150k cursor-paginated requests across the span16
   re-ingests); every FD endpoint returns 402 as of 2026-09-17. All panel
   data (fundamentals, metrics, insider, 13F) was fully ingested *before*
   exhaustion — trials are unaffected. Facts-based sector labels could
   not be fetched afterward.
3. Delisted coverage is reported in active/inactive form (insider 69% /
   institutional 83% on inactive names carries over); the promised
   era-stratification of that readout was not implemented this cycle.
4. Launch-glue defects (date types, fold arithmetic, param type, overlay
   key) required five relaunches; all fixed and committed before the
   receipted run; no trial numbers changed across relaunches
   (deterministic training).

## Productization delivered with this family

Daily OOS net return series are now **in the receipt** (5 × 1,440 days) —
conditional analyses are henceforth free reads of committed artifacts.
Plus, from this family's infrastructure arc: FD cursor pagination for
fundamentals/metrics, reconnecting ILP sender, QuestDB container restart
policy, PCA neutralization module with causality tests.

## Standing verdict

Certified: **0** at 88 trials. The incumbent 10-name result (0.921 @63)
stands as a historical receipt of a dead era, not a live candidate. Three
dead levers are now on the books (universe scale, label transforms,
alternative-data features) plus one dead signal class on the modern era.
The live, receipt-backed hypothesis for the next registration is
**training-window locality**: rolling recent-window training vs anchored
— same data, same features, one geometry change, directly implied by the
pit250-vs-span16 controlled comparison.
