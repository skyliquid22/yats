# Receipts

Raw sweep outputs backing the verdict docs. Coverage note: the deflation
clock counts 63 trials; these files evidence 55. The original 8-trial
2026-07-08 sweep (2-year span, pre-execution-lag methodology) had its summary
overwritten when the full-span rerun reused its output directory; its per-config
numbers survive in `../wfo_sweep_2d_results.md`. All 63 are counted in every
DSR computation regardless.

Reproduction: these runs require vendor API keys and a populated database
(see `../../ingestion.md`). Re-running will drift on vendor restatements —
no input-data snapshot is distributed. The receipts are the record of what
was observed, not a bit-reproducible artifact.
