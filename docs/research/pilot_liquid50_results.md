# Liquid50 Pilot — Results (2026-09-10)

Pre-registration: [`preregistrations/2026-09-10_liquid50_pilot.md`](preregistrations/2026-09-10_liquid50_pilot.md).
Receipt: [`receipts/pilot_liquid50.json`](receipts/pilot_liquid50.json). 10 trials as registered; clock 63 → **73**.

## Primary: NOT CERTIFIED
Best: arm B `lgbm_21d + vt10` — OOS Sharpe **1.157** (net of 5bp), strict DSR **0.881**
vs the 0.95 bar at 73 trials (SR0 rose to 0.520 with the clock). The incumbent
10-name best (1.134 gross, DSR 0.921 at 63) remains the closest attempt.
Comparability caveat: pilot Sharpes are net of 5bp costs; incumbent receipts
were gross (the cost hook did not exist then) — the pilot is held to the
stricter standard.

## Secondary: breadth alone did not deliver
Same-class Sharpe, liquid50 vs dev10: mixed (lgbm_5d +0.23/+0.39, lgbm_21d
−0.24/−0.06, ridge ≈ flat). The IR ≈ IC×√breadth lift did not materialize at
50 large-cap names — consistent with some combination of: IC decaying in the
added (equally crowded) large caps, survivorship bias working against the
thesis's cleanliness, and net-vs-gross accounting. Rank decay A=0.67, B=0.50.

## The genuine finding: the insider/institutional block flipped sign
B−A margins (same configs, only the insider+institutional columns differ):
**+0.12, −0.07, +0.16, +0.19 — 3 of 4 positive, mean ≈ +0.10.** At 10
mega-caps this block *subtracted* ≈1.0 Sharpe (3d A/B); at 50 names it adds.
The pre-registered caveat anticipated this direction: insider alpha
concentrates down-cap. This is the strongest evidence yet for the next step —
point-in-time-constituency expansion toward 250 names including mid-caps,
where both the breadth and insider hypotheses make their real test.

## Standing verdict
73 trials, 0 certified. The referee holds.
