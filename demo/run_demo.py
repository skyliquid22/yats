"""Offline walk-forward + Deflated Sharpe demo. One command, no keys, no DB.

Loads the synthetic bundle from demo/data/ (generating it first if missing),
then runs the REAL supervised WFO sweep machinery from
research/scripts/wfo_sweep_alpha1.py — same panel prep, same purged anchored
walk-forward folds, same portfolio construction, same DSR — at a reduced
2-config grid so it finishes in a couple of minutes on a laptop.

Usage:
    PYTHONPATH=.:pipelines uv run python demo/run_demo.py
    (or simply: make demo)
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "pipelines")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

SWEEP_SCRIPT = ROOT / "research" / "scripts" / "wfo_sweep_alpha1.py"
DEFAULT_PANEL = ROOT / "demo" / "data" / "demo_panel.parquet"

# Reduced grid: 2 of the 6 production ALPHA-1 configs (ridge is dependency-light
# and fast; the demo needs >=2 configs for a sweep-level DSR to exist at all).
DEMO_GRID = [
    {"model": "ridge", "horizon": 5,  "reg": 1.0, "label": "ridge_5d_a1"},
    {"model": "ridge", "horizon": 21, "reg": 1.0, "label": "ridge_21d_a1"},
]


def _load_sweep_module():
    """Import research/scripts/wfo_sweep_alpha1.py without duplicating its math."""
    spec = importlib.util.spec_from_file_location("wfo_sweep_alpha1_demo", SWEEP_SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def verdict_hash(summary: dict) -> str:
    """Stable hash of the demo's numerical verdict (rounded to 8 decimals)."""
    payload = {
        "best": summary["best_label"],
        "rank_decay": round(summary["rank_decay"], 8),
        "configs": [
            {
                "label": c["label"],
                "sharpe": round(c["sharpe"], 8),
                "per_fold": [None if s is None else round(s, 8)
                             for s in c["per_fold_oos_sharpe"]],
                "dsr": round(c["dsr"], 8),
                "significant": bool(c["dsr_significant"]),
                "benchmark_sharpe": round(c["benchmark_sharpe"], 8),
            }
            for c in summary["configs"]
        ],
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def run_demo(panel_path: Path = DEFAULT_PANEL, grid: list[dict] | None = None) -> dict:
    """Run the reduced WFO sweep on the bundle; return the summary dict."""
    grid = grid or DEMO_GRID
    prev = os.environ.get("DEMO_PANEL_PATH")
    os.environ["DEMO_PANEL_PATH"] = str(panel_path)
    try:
        mod = _load_sweep_module()
        panel = mod.load_panel()
    finally:
        if prev is None:
            os.environ.pop("DEMO_PANEL_PATH", None)
        else:
            os.environ["DEMO_PANEL_PATH"] = prev

    # Evaluate the portfolio over the bundle's own universe.
    mod.SYMBOLS = sorted(panel["symbol"].unique())
    dates = sorted(panel["date"].unique())
    data = list(range(len(dates)))

    per_config: list[dict] = []
    for cfg in grid:
        t0 = time.time()
        train_fn = mod.make_train_fn(panel, cfg, dates)
        eval_fn = mod.make_eval_fn(panel, dates)
        result = mod.run_wfo(data, mod.WFO_CFG, train_fn=train_fn, eval_fn=eval_fn)
        cfg_summary = mod.summarize_config_result(cfg, result)
        cfg_summary["elapsed_s"] = round(time.time() - t0, 2)
        per_config.append(cfg_summary)

    # Sweep-level Deflated Sharpe (same call the production sweep uses).
    dsr_results = mod.compute_sweep_dsr(per_config)
    for c, d in zip(per_config, dsr_results):
        c["dsr"] = d["dsr"]
        c["dsr_significant"] = d["is_significant"]
        c["benchmark_sharpe"] = d["benchmark_sharpe"]
        c["dsr_z"] = d.get("z_score", 0.0)
    rank_decay = mod.compute_sweep_wfo_rank_decay(
        [c["per_fold_oos_sharpe"] for c in per_config]
    )

    best = max(per_config, key=lambda c: c["dsr"])
    summary = {
        "panel_path": str(panel_path),
        "n_dates": len(dates),
        "symbols": mod.SYMBOLS,
        "wfo": {
            "mode": "anchored",
            "n_periods": mod.WFO_CFG.n_periods,
            "train_window": mod.WFO_CFG.train_window,
            "label_horizon": mod.WFO_CFG.label_horizon,
            "purge_buffer": mod.WFO_CFG.purge_buffer,
        },
        "configs": per_config,
        "rank_decay": float(rank_decay),
        "best_label": best["label"],
    }
    summary["verdict_hash"] = verdict_hash(summary)
    return summary


def print_report(summary: dict) -> None:
    best = next(c for c in summary["configs"] if c["label"] == summary["best_label"])
    wfo = summary["wfo"]
    line = "=" * 72

    print()
    print(line)
    print("DEMO SWEEP RESULTS — synthetic data, real machinery")
    print(line)
    print(f"Universe: {len(summary['symbols'])} synthetic symbols, "
          f"{summary['n_dates']} trading days")
    print(f"WFO: {wfo['n_periods']} anchored folds, train_window={wfo['train_window']}, "
          f"purge gap={wfo['label_horizon']}+{wfo['purge_buffer']} bars")
    print()
    for c in summary["configs"]:
        folds = ", ".join(
            "n/a" if s is None else f"{s:+.2f}" for s in c["per_fold_oos_sharpe"]
        )
        print(f"  {c['label']:<14} OOS Sharpe {c['sharpe']:+.3f}   "
              f"per-fold [{folds}]   ({c['elapsed_s']:.1f}s)")
        print(f"  {'':<14} DSR {c['dsr']:.3f} vs luck-benchmark SR0 "
              f"{c['benchmark_sharpe']:.3f}  "
              f"-> {'SIGNIFICANT' if c['dsr_significant'] else 'not significant'}")
    print()
    print(f"  WFO rank-decay across folds: {summary['rank_decay']:.3f} "
          "(0 = stable config ranking, 1 = fully inverted)")
    print()
    n = len(summary["configs"])
    verdict = ("PROMOTE — the edge survives deflation"
               if best["dsr_significant"]
               else "DO NOT PROMOTE — the observed Sharpe is consistent with "
                    "selection luck")
    print(f"VERDICT: best config = {best['label']} "
          f"(OOS Sharpe {best['sharpe']:+.3f}, DSR {best['dsr']:.3f}) -> {verdict}")
    print(f"verdict_hash: {summary['verdict_hash']}")
    print(line)
    print()
    print("What the Deflated Sharpe Ratio just did: we backtested "
          f"{n} configurations on the same history and are tempted to report the\n"
          "best one — but the best of several backtests is inflated by selection "
          "bias; even pure-noise strategies produce a lucky winner. DSR\n"
          "(Bailey & de Prado 2014) corrects for this: from the cross-config "
          "Sharpe variance it estimates the Sharpe the best of "
          f"{n} noise trials\n"
          f"would show (benchmark SR0 = {best['benchmark_sharpe']:.3f}), then — "
          "accounting for the return series' skewness, fat tails, and sample\n"
          f"length — computes the probability that {best['label']}'s "
          f"out-of-sample Sharpe of {best['sharpe']:+.3f} genuinely exceeds that "
          f"luck benchmark.\nDSR = {best['dsr']:.3f} means a "
          f"{100 * best['dsr']:.1f}% probability the edge is real rather than "
          "selection luck; only DSR > 0.95 clears the promotion\n"
          "bar. The out-of-sample Sharpes come from purged anchored walk-forward "
          "folds (an 84-bar gap between train and test blocks removes\n"
          "label and feature-memory leakage), so the whole verdict is "
          "look-ahead-clean end to end.")
    print()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--panel", type=Path, default=DEFAULT_PANEL)
    ap.add_argument("--json-out", type=Path, default=None,
                    help="optionally write the full summary JSON here")
    args = ap.parse_args()

    if not args.panel.exists():
        print(f"[DEMO] bundle {args.panel} missing — generating it first")
        from demo.generate_data import write_bundle
        write_bundle(args.panel)

    t0 = time.time()
    summary = run_demo(args.panel)
    summary["elapsed_s"] = round(time.time() - t0, 1)
    print_report(summary)
    print(f"[DEMO] total runtime {summary['elapsed_s']:.1f}s "
          "(no network, no database, no vendor keys)")
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(summary, indent=2, default=str) + "\n")
        print(f"[DEMO] summary written to {args.json_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
