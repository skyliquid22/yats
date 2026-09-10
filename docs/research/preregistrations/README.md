# Pre-Registrations

Every planned trial family gets a dated pre-registration document in this
directory **before any run is launched**. This is the platform's answer to the
garden of forking paths: the deflation clock (see
`docs/research/full_span_verdicts.md`) only means something if trial counts
are committed up front, not reconstructed after seeing results.

## The rule

**Runs without a pre-registration do not get counted as evidence.** They may
be useful for debugging or infrastructure work, but their Sharpes and DSRs
carry no evidential weight toward certification, and they may not be cited in
verdict documents as support for a hypothesis.

## What a pre-registration must state

Each document is named `YYYY-MM-DD_<family_name>.md` (date = registration
date, before the first run) and must contain:

1. **Hypothesis** — what question the trial family answers, stated so that
   both outcomes are informative. If a null result would be shrugged off as
   "we'll tweak and retry", the hypothesis is not ready to register.
2. **Exact configs** — feature set (by config file), universe (by config
   file), model grid with regularization values, WFO geometry (folds,
   anchoring, purge, label horizon), execution assumptions (fill timing,
   execution lag, costs). Enough detail that someone else could launch the
   identical runs.
3. **Trial count charged to the deflation clock** — an explicit budget table:
   how many trials, and the clock's before/after values. Every config
   evaluated against OOS data is a trial. Overlays and ablations count.
4. **Success and failure criteria** — the numeric bar (e.g. strict DSR at the
   post-family trial count) and what secondary readouts will be examined,
   committed in advance.
5. **What will be reported either way** — the results document that will be
   written whether the family succeeds or fails, and where raw result
   artifacts will land. Failed families are reportable results, not
   embarrassments to be buried.

## Amendments

If a registered family must change after registration (data problem, config
bug discovered mid-run), the pre-registration is amended in place with a dated
addendum explaining what changed and why — never silently rewritten. Trials
already run against the old design still count against the clock.
