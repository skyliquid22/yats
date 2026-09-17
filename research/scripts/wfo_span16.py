"""SPAN-16 family: 2016-2026 span + PCA-neutralized labels (5 trials, clock 83 -> 88).

Pre-registration: docs/research/preregistrations/2026-09-16_span16_family.md
Receipt:          docs/research/receipts/span16_sweep.json

Trials (exactly 5, no additions):
  1. lgbm_21d_clean_R   incumbent SPY-residualized labels
  2. lgbm_5d_clean_R
  3. lgbm_21d_clean_N   top-5 trailing-PCA factor-neutralized labels
  4. lgbm_5d_clean_N
  5. best base by primary OOS Sharpe + vt10 overlay (committed rule)

Registered geometry: anchored folds, test window 289 bars, train anchor 250,
label purge = horizon, purge_buffer 253, lag 1, next_close, 5bp.
PRE-COMMITTED fold rule (differs from pit250, declared in the prereg): folds
whose train window is < 300 bars are EXCLUDED from the primary OOS Sharpe
and reported separately; both aggregations appear in the receipt.

The receipt stores each trial's daily OOS net return series (dates+values),
per the family's productization commitment.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

RIG = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RIG), str(RIG / "pipelines")]

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

# Span override MUST precede the pilot import chain: the pilot pins its
# fetch window from these env vars at import time.
os.environ.setdefault("YATS_SWEEP_START", "2016-01-04")

import importlib.util  # noqa: E402


def _load(name: str, rel: str):
    spec = importlib.util.spec_from_file_location(name, RIG / rel)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


pit = _load("wfo_pit250", "research/scripts/wfo_pit250.py")
pilot = pit.pilot
alpha1 = pit.alpha1
pilot.START = "2016-01-04"  # belt-and-braces with the env override

from research.alpha.neutralize import pca_neutralize_column  # noqa: E402
from research.eval.metrics import compute_sharpe  # noqa: E402
from research.eval.wfo import compute_sweep_wfo_rank_decay, run_wfo  # noqa: E402
from research.experiments.spec import WFOConfig  # noqa: E402
from research.universe import membership as membership_mod  # noqa: E402

# ---------------------------------------------------------------------------
# Registered family constants
# ---------------------------------------------------------------------------

FEATURE_SET = "breadth_v1"          # clean cols are a subset of stored rows
MEMBERSHIP_PARQUET = RIG / "configs" / "universes" / "span16_membership.parquet"
START = "2016-01-04"
END = os.environ.get("YATS_SWEEP_END") or None

TEST_WINDOW = 289                   # matched to prior families
TRAIN_ANCHOR = 250
LABEL_PURGE_BUFFER = 253
MIN_TRAIN_BARS = 300                # pre-committed fold-exclusion rule
PCA_N_FACTORS = 5
PCA_WINDOW = 252

BASE_GRID = [
    {"model": "lgbm", "horizon": 21, "reg": 0.1, "label": "lgbm_21d_clean_R", "labels": "R"},
    {"model": "lgbm", "horizon": 5, "reg": 0.1, "label": "lgbm_5d_clean_R", "labels": "R"},
    {"model": "lgbm", "horizon": 21, "reg": 0.1, "label": "lgbm_21d_clean_N", "labels": "N"},
    {"model": "lgbm", "horizon": 5, "reg": 0.1, "label": "lgbm_5d_clean_N", "labels": "N"},
]
TOTAL_TRIALS = len(BASE_GRID) + 1   # + vt10 on best base

EXECUTION_LAG = pit.EXECUTION_LAG
FILL_TIMING = pit.FILL_TIMING
COST_BP = pit.COST_BP

OUT_DIR = RIG / ".yats_data" / "wfo_sweeps" / "span16_sweep"
RECEIPT_PATH = RIG / "docs" / "research" / "receipts" / "span16_sweep.json"
PREREG_PATH = "docs/research/preregistrations/2026-09-16_span16_family.md"
RUN2_RECEIPT = RIG / "docs" / "research" / "receipts" / "pit250_sweep_run2.json"
SECTOR_CACHE = RIG / ".yats_data" / "company_sectors.json"

N_TRIALS_DEFLATION = 88             # 83 post-pit250 + this family's 5
CERTIFICATION_BAR = 0.95
KILL_CRITERION_MARGIN = 0.10        # N must beat R twin by this, else dead


def mark(msg: str) -> None:
    print(f"[SPAN16] {datetime.now().strftime('%H:%M:%S')} {msg}", flush=True)


# ---------------------------------------------------------------------------
# Membership (explicit table — pit250's artifact stays untouched)
# ---------------------------------------------------------------------------

def load_span16_membership():
    table = membership_mod.load_membership(MEMBERSHIP_PARQUET)

    def is_member(symbol: str, as_of) -> bool:
        return membership_mod.is_member(symbol, as_of, membership=table)

    symbols = membership_mod.all_symbols(table)
    rebalance_dates = sorted(pd.to_datetime(table["rebalance_date"].unique()))
    return table, symbols, is_member, rebalance_dates


def clean_feature_cols() -> list[str]:
    """breadth_v1 minus insider minus institutional groups (27 cols)."""
    groups = pilot.load_feature_groups()
    return [
        c for g, cols in groups.items() if g not in ("insider", "institutional")
        for c in cols
    ]


# ---------------------------------------------------------------------------
# Fold rule (pre-committed): primary Sharpe excludes folds with short train
# ---------------------------------------------------------------------------

def summarize_with_fold_rule(cfg: dict, result) -> dict:
    """Config summary where the PRIMARY sharpe excludes degenerate folds.

    A fold is excluded when its train window is < MIN_TRAIN_BARS bars.
    ``sharpe_all_folds`` preserves the pit250-style aggregation for
    comparability; both are receipted.
    """
    summary = alpha1.summarize_config_result(dict(cfg), result)
    summary["sharpe_all_folds"] = summary["sharpe"]

    included_returns: list[float] = []
    excluded: list[dict] = []
    for fold in result.folds:
        train_bars = fold.train_end - fold.train_start
        if train_bars >= MIN_TRAIN_BARS and fold.oos_returns:
            included_returns.extend(fold.oos_returns)
        else:
            excluded.append({
                "fold_index": fold.fold_index,
                "train_bars": int(train_bars),
                "oos_sharpe": fold.oos_sharpe,
                "n_obs": len(fold.oos_returns),
            })
    summary["sharpe"] = (
        compute_sharpe(pd.Series(included_returns)) if len(included_returns) >= 2 else None
    )
    if len(included_returns) >= 4:
        from scipy import stats as sp_stats
        summary["skewness"] = float(sp_stats.skew(included_returns))
        summary["kurtosis"] = float(sp_stats.kurtosis(included_returns)) + 3.0
    summary["n_obs_primary"] = len(included_returns)
    summary["excluded_folds"] = excluded
    summary["fold_rule"] = f"train_bars >= {MIN_TRAIN_BARS}"
    return summary


def included_capture_indices(result) -> list[int]:
    """Map capture-list positions to folds passing the fold rule.

    Captures append once per fold whose model trained (eval with model=None
    appends nothing), in fold order.
    """
    keep: list[int] = []
    pos = 0
    for fold in result.folds:
        trained = bool(fold.oos_returns)
        if trained:
            if fold.train_end - fold.train_start >= MIN_TRAIN_BARS:
                keep.append(pos)
            pos += 1
    return keep


# ---------------------------------------------------------------------------
# Daily series extraction (receipted per the productization commitment)
# ---------------------------------------------------------------------------

def daily_net_series(captures: list[dict], capture_indices: list[int]) -> pd.Series:
    from research.eval.metrics import compute_turnover

    pieces = []
    for i in capture_indices:
        w, r = captures[i]["weights"], captures[i]["returns"]
        gross = (w * r).sum(axis=1)
        net = gross - compute_turnover(w) * (COST_BP / 1e4)
        pieces.append(net)
    if not pieces:
        return pd.Series(dtype=float)
    out = pd.concat(pieces)
    out.index = pd.to_datetime(out.index)  # panel dates are datetime.date
    out = out.sort_index()
    return out[~out.index.duplicated(keep="first")]


def series_to_receipt(s: pd.Series) -> dict:
    return {
        "dates": [str(pd.Timestamp(d).date()) for d in s.index],
        "net_returns": [float(v) for v in s.to_numpy()],
    }


# ---------------------------------------------------------------------------
# Strict deflation @88 — pool loaded from the pit250 run-2 receipt
# ---------------------------------------------------------------------------

def registered_pool() -> list[float]:
    """35 pre-family sharpes: run-2 receipt's 30-pool + its 5 per-config."""
    r2 = json.loads(RUN2_RECEIPT.read_text())
    pool = [float(x) for x in r2["strict_deflation"]["pool_sharpes"]]
    if len(pool) != 35:
        raise ValueError(f"run-2 receipt pool has {len(pool)} entries, expected 35")
    return pool


def strict_deflation(per_config: list[dict]) -> dict:
    from compute.stats.deflated_sharpe import (
        _expected_max_sr_benchmark,
        probabilistic_sharpe_ratio,
    )

    pool = registered_pool() + [
        float(c["sharpe"]) for c in per_config if c["sharpe"] is not None
    ]
    emax = _expected_max_sr_benchmark(N_TRIALS_DEFLATION)
    sr0 = float(np.std(pool)) * emax  # population std (ddof=0), as incumbent
    per = []
    for c in per_config:
        sh = c["sharpe"]
        if sh is None:
            per.append({"config": c["label"], "sharpe": None,
                        "strict_dsr": None, "certified": False})
            continue
        psr = probabilistic_sharpe_ratio(
            observed_sharpe=float(sh),
            benchmark_sharpe=sr0,
            n_observations=int(c.get("n_obs_primary") or c["n_obs"]),
            returns_skewness=float(c.get("skewness", 0.0)),
            returns_kurtosis=float(c.get("kurtosis", 3.0)),
        )
        per.append({
            "config": c["label"],
            "sharpe": float(sh),
            "strict_dsr": psr["dsr"],
            "certified": bool(psr["dsr"] > CERTIFICATION_BAR),
        })
    return {
        "method": (
            "expected-max benchmark scaled to all 88 trials; std over the "
            "full honest-fill pool (35 registered + this family's 5)"
        ),
        "n_trials_emax": N_TRIALS_DEFLATION,
        "expected_max_sr": float(emax),
        "pool_sharpes": pool,
        "sr0": sr0,
        "certification_bar": CERTIFICATION_BAR,
        "per_config": per,
        "any_certified": any(p["certified"] for p in per),
    }


# ---------------------------------------------------------------------------
# Secondary readouts (no clock charge)
# ---------------------------------------------------------------------------

def era_split_readout(series_by_label: dict[str, pd.Series]) -> dict:
    out = {}
    cut = pd.Timestamp("2020-01-01")
    for label, s in series_by_label.items():
        pre, post = s[s.index < cut], s[s.index >= cut]
        out[label] = {
            "pre_2020": {"n": len(pre), "sharpe": compute_sharpe(pre) if len(pre) > 40 else None},
            "post_2020": {"n": len(post), "sharpe": compute_sharpe(post) if len(post) > 40 else None},
        }
    return out


def regime_conditional_readout(
    series_by_label: dict[str, pd.Series], panel: pd.DataFrame
) -> dict:
    """Conditional Sharpe by regime tercile; thresholds from PRE-TEST data.

    For each trial's daily series, day t is bucketed using the t-1 lagged
    regime value against tercile thresholds computed over all panel dates
    strictly before the trial's first OOS date (train-window discipline —
    no full-sample thresholds in receipted output).
    """
    regime_cols = ["market_vol_20d", "market_trend_20d", "dispersion_20d", "corr_mean_20d"]
    have = [c for c in regime_cols if c in panel.columns]
    if not have:
        return {}
    daily_regime = (
        panel.groupby("date")[have].first().sort_index().shift(1)  # t-1 lag
    )
    daily_regime.index = pd.to_datetime(daily_regime.index)
    out: dict = {}
    for label, s in series_by_label.items():
        if s.empty:
            continue
        first_oos = s.index.min()
        pre = daily_regime[daily_regime.index < first_oos]
        joined = pd.DataFrame({"ret": s}).join(daily_regime, how="left")
        res = {}
        for c in have:
            if pre[c].notna().sum() < 100:
                continue
            lo, hi = pre[c].quantile([1 / 3, 2 / 3])
            sub = joined.dropna(subset=[c])
            buckets = {
                "low": sub[sub[c] <= lo]["ret"],
                "mid": sub[(sub[c] > lo) & (sub[c] <= hi)]["ret"],
                "high": sub[sub[c] > hi]["ret"],
            }
            res[c] = {
                b: {"n": len(v), "sharpe": compute_sharpe(v) if len(v) > 40 else None}
                for b, v in buckets.items()
            }
        out[label] = res
    return out


def load_sector_map(symbols: list[str]) -> dict[str, str]:
    """Static sector labels from the FD facts API, disk-cached. Diagnostic
    only (registered caveat: not point-in-time; missing for old delistings)."""
    cache: dict[str, str | None] = {}
    if SECTOR_CACHE.exists():
        cache = json.loads(SECTOR_CACHE.read_text())
    missing = [s for s in symbols if s not in cache]
    if missing:
        from yats_pipelines.resources.financialdatasets import FinancialDatasetsResource
        fd = FinancialDatasetsResource()
        for sym in missing:
            try:
                facts = fd._get("/company/facts", {"ticker": sym}).get("company_facts") or {}
                cache[sym] = facts.get("sector")
            except Exception:
                cache[sym] = None
        SECTOR_CACHE.parent.mkdir(parents=True, exist_ok=True)
        SECTOR_CACHE.write_text(json.dumps(cache, indent=0))
    return {s: v for s, v in cache.items() if v}


def sector_diagnostic(
    captures: list[dict], capture_indices: list[int], symbols: list[str]
) -> dict:
    """Coverage + best-trial mean absolute weight share per sector."""
    sectors = load_sector_map(symbols)
    shares: dict[str, float] = {}
    total = 0.0
    for i in capture_indices:
        w = captures[i]["weights"].abs()
        for sym in w.columns:
            sec = sectors.get(sym, "UNKNOWN")
            v = float(w[sym].sum())
            shares[sec] = shares.get(sec, 0.0) + v
            total += v
    return {
        "sector_coverage_symbols": round(len(sectors) / max(len(symbols), 1), 3),
        "mean_abs_weight_share": {
            k: round(v / total, 4) for k, v in sorted(shares.items(), key=lambda kv: -kv[1])
        } if total else {},
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    t0 = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    table, symbols, is_member, rebalance_dates = load_span16_membership()
    cols = clean_feature_cols()
    mark(
        f"UNIVERSE span16 all-time symbols={len(symbols)} "
        f"({MEMBERSHIP_PARQUET.name}); clean cols={len(cols)}"
    )
    mark(
        f"PREREGISTERED FAMILY: {TOTAL_TRIALS} trials, deflation clock "
        f"83 -> {N_TRIALS_DEFLATION} ({PREREG_PATH})"
    )

    features_df, closes_df = pilot._fetch_frames_questdb(symbols, cols)
    mark(f"DATA features rows={len(features_df)} px rows={len(closes_df)} span=[{START}, {END or 'latest'}]")

    panel_r = pit.build_pit_panel(features_df, closes_df, cols, is_member)
    dates = sorted(panel_r["date"].unique())
    n_dates = len(dates)
    mark(
        f"PIT PANEL rows={len(panel_r)} dates={n_dates} "
        f"symbols={panel_r['symbol'].nunique()}"
    )

    # geometry: anchor + (purge_buffer + label_horizon) gap + n tests
    gap = LABEL_PURGE_BUFFER + 21
    n_periods = max(2, (n_dates - TRAIN_ANCHOR - gap) // TEST_WINDOW)
    wfo_cfg = WFOConfig(
        train_window=TRAIN_ANCHOR,
        test_window=TEST_WINDOW,
        label_horizon=21,
        purge_buffer=LABEL_PURGE_BUFFER,
        n_periods=n_periods,
    )
    mark(
        f"WFO geometry: anchored {n_periods} folds, test_window={TEST_WINDOW}, "
        f"train_anchor={TRAIN_ANCHOR}, purge_buffer={LABEL_PURGE_BUFFER}, "
        f"lag={EXECUTION_LAG}, fill={FILL_TIMING}, cost={COST_BP}bp, "
        f"fold rule: exclude train<{MIN_TRAIN_BARS} from primary"
    )

    # N-variant panel: same features, PCA-neutralized resid columns
    mark("Building N-variant labels (top-5 trailing-PCA neutralization)...")
    panel_n = panel_r.copy()
    for h in (5, 21):
        neutral = pca_neutralize_column(
            panel_r, f"fwd_{h}d", closes_df.rename(columns=str),
            rebalance_dates, n_factors=PCA_N_FACTORS, window=PCA_WINDOW,
        )
        panel_n[f"fwd_{h}d_resid"] = neutral
        mark(
            f"  fwd_{h}d: neutralized non-null={int(neutral.notna().sum())} "
            f"of raw non-null={int(panel_r[f'fwd_{h}d'].notna().sum())}"
        )
    panels = {"R": panel_r, "N": panel_n}

    per_config: list[dict] = []
    captures: dict[str, list[dict]] = {}
    results: dict[str, object] = {}
    series_by_label: dict[str, pd.Series] = {}
    data = list(range(n_dates))

    for i, cfg in enumerate(BASE_GRID):
        cfg_t0 = time.time()
        mark(
            f"CONFIG {i + 1}/{len(BASE_GRID)}: {cfg['label']} "
            f"({cfg['model']} h={cfg['horizon']}d labels={cfg['labels']})"
        )
        panel = panels[cfg["labels"]]
        capture: list[dict] = []
        train_fn = alpha1.make_train_fn(panel, cfg, dates, feature_cols=cols)
        eval_fn = alpha1.make_eval_fn(
            panel, dates, symbols=symbols,
            fill_timing=FILL_TIMING, execution_lag=EXECUTION_LAG,
            cost_bp=COST_BP, fold_capture=capture,
        )
        result = run_wfo(data, wfo_cfg, train_fn=train_fn, eval_fn=eval_fn)
        summary = summarize_with_fold_rule(cfg, result)
        summary["elapsed_s"] = time.time() - cfg_t0
        per_config.append(summary)
        captures[cfg["label"]] = capture
        results[cfg["label"]] = result
        keep = included_capture_indices(result)
        series_by_label[cfg["label"]] = daily_net_series(capture, keep)
        mark(
            f"{cfg['label']} done: primary sharpe="
            f"{summary['sharpe'] if summary['sharpe'] is None else round(summary['sharpe'], 3)} "
            f"(all-folds {round(summary['sharpe_all_folds'], 3)}) "
            f"folds={['%.2f' % (s or 0) for s in summary['per_fold_oos_sharpe']]} "
            f"excluded={[e['fold_index'] for e in summary['excluded_folds']]} "
            f"({summary['elapsed_s']:.1f}s)"
        )

    # Trial 5 (committed rule): vt10 on best base by PRIMARY OOS Sharpe
    best = max(per_config, key=lambda c: c["sharpe"] or -np.inf)
    mark(f"BEST BASE by primary Sharpe: {best['label']} ({best['sharpe']:.3f}) — applying vt10")
    result = results[best["label"]]
    keep = included_capture_indices(result)
    kept_captures = [captures[best["label"]][i] for i in keep]
    best.setdefault("variant", best.get("labels", "clean"))  # pit overlay helper expects it
    overlay = pit.apply_vt10_overlay(best, kept_captures)
    overlay["label"] = f"{best['label']}_vt10"
    overlay["n_obs_primary"] = overlay["n_obs"]
    overlay["fold_rule"] = f"train_bars >= {MIN_TRAIN_BARS} (inherited from base)"
    per_config.append(overlay)
    # overlay daily series: recompute scaled net from kept captures
    from research.portfolio.risk_layer import PortfolioRiskConfig, apply_risk_layer_batch
    from research.eval.metrics import compute_turnover
    vt_pieces = []
    for cap in kept_captures:
        w, r = cap["weights"], cap["returns"]
        scaled = apply_risk_layer_batch(w, r, None, PortfolioRiskConfig(vol_target=pilot.VOL_TARGET))
        vt_pieces.append((scaled * r).sum(axis=1) - compute_turnover(scaled) * (COST_BP / 1e4))
    if vt_pieces:
        vt_series = pd.concat(vt_pieces)
        vt_series.index = pd.to_datetime(vt_series.index)
        series_by_label[overlay["label"]] = vt_series.sort_index()
    else:
        series_by_label[overlay["label"]] = pd.Series(dtype=float)
    mark(f"{overlay['label']} done: sharpe={overlay['sharpe']:.3f}")

    deflation = strict_deflation(per_config)
    for c, d in zip(per_config, deflation["per_config"]):
        c["strict_dsr"] = d["strict_dsr"]
        c["certified"] = d["certified"]

    # Kill criterion (pre-committed)
    by_label = {c["label"]: c for c in per_config}
    n_margins = {
        h: (by_label[f"lgbm_{h}d_clean_N"]["sharpe"] or -np.inf)
        - (by_label[f"lgbm_{h}d_clean_R"]["sharpe"] or -np.inf)
        for h in (21, 5)
    }
    label_engineering_dead = all(m < KILL_CRITERION_MARGIN for m in n_margins.values())

    era = era_split_readout(series_by_label)
    regime_cond = regime_conditional_readout(series_by_label, panel_r)
    best_final = max(per_config, key=lambda c: c["sharpe"] or -np.inf)
    base_of_best = best_final.get("base_config", best_final["label"])
    sector_diag = sector_diagnostic(
        captures[base_of_best], included_capture_indices(results[base_of_best]), symbols
    )
    coverage = pit.coverage_readout(
        features_df[pit.eligibility_mask(features_df, is_member)],
        pilot.load_feature_groups(), is_member, dates[-1],
    )
    rank_decay = compute_sweep_wfo_rank_decay(
        [c["per_fold_oos_sharpe"] for c in per_config if "overlay" not in c]
    )

    receipt = {
        "sweep": "span16_sweep",
        "preregistration": PREREG_PATH,
        "universe": "span16",
        "membership_parquet": "configs/universes/span16_membership.parquet",
        "n_symbols_all_time": len(symbols),
        "symbols": symbols,
        "feature_set": FEATURE_SET,
        "clean_cols": cols,
        "span": {"start": START, "end": END or str(dates[-1])[:10], "n_dates": n_dates},
        "wfo": {
            "mode": "anchored", "n_periods": n_periods, "test_window": TEST_WINDOW,
            "train_anchor": TRAIN_ANCHOR, "purge_buffer": LABEL_PURGE_BUFFER,
            "fold_rule": f"primary excludes folds with train_bars < {MIN_TRAIN_BARS}",
        },
        "execution": {
            "lag_days": EXECUTION_LAG, "fill_timing": FILL_TIMING, "cost_bp": COST_BP,
        },
        "labels": {
            "R": "forward returns residualized vs rolling SPY beta (incumbent)",
            "N": (
                f"forward returns minus top-{PCA_N_FACTORS} trailing-PCA factor "
                f"reconstruction; {PCA_WINDOW}-bar window ending t-1, re-estimated "
                "each membership rebalance"
            ),
        },
        "trials_charged": TOTAL_TRIALS,
        "deflation_clock": N_TRIALS_DEFLATION,
        "configs": per_config,
        "strict_deflation": deflation,
        "kill_criterion": {
            "rule": f"N beats R twin by >= {KILL_CRITERION_MARGIN} at both horizons",
            "margins": {str(k): (None if not np.isfinite(v) else round(float(v), 4)) for k, v in n_margins.items()},
            "label_engineering_dead": bool(label_engineering_dead),
        },
        "daily_series": {label: series_to_receipt(s) for label, s in series_by_label.items()},
        "era_split": era,
        "regime_conditional": regime_cond,
        "sector_diagnostic": sector_diag,
        "delisted_coverage": coverage,
        "rank_decay": rank_decay,
        "elapsed_hours": (time.time() - t0) / 3600,
    }
    RECEIPT_PATH.parent.mkdir(parents=True, exist_ok=True)
    RECEIPT_PATH.write_text(json.dumps(receipt, indent=2, default=str))
    mark(f"RECEIPT written to {RECEIPT_PATH}")
    (OUT_DIR / "span16_summary.json").write_text(json.dumps(receipt, indent=2, default=str))

    mark(
        f"STRICT DEFLATION: SR0={deflation['sr0']:.4f} "
        f"(= std(pool n={len(deflation['pool_sharpes'])}) x "
        f"E[max]@{N_TRIALS_DEFLATION}={deflation['expected_max_sr']:.3f}) bar={CERTIFICATION_BAR}"
    )
    for p in sorted(deflation["per_config"], key=lambda x: -(x["sharpe"] or 9e9) if x["sharpe"] else 0):
        mark(
            f"  {p['config']}: sharpe={p['sharpe'] if p['sharpe'] is None else round(p['sharpe'], 3)} "
            f"strict_dsr={p['strict_dsr'] if p['strict_dsr'] is None else round(p['strict_dsr'], 3)} "
            f"certified={p['certified']}"
        )
    verdict = "CERTIFIED" if deflation["any_certified"] else "NOT CERTIFIED"
    best_p = max(deflation["per_config"], key=lambda x: x["strict_dsr"] or -1)
    mark(
        f"VERDICT: {verdict} — best {best_p['config']} "
        f"strict_dsr={round(best_p['strict_dsr'], 3) if best_p['strict_dsr'] else None} "
        f"vs bar {CERTIFICATION_BAR} at {N_TRIALS_DEFLATION} trials"
    )
    mark(f"KILL CRITERION: label_engineering_dead={label_engineering_dead} margins={n_margins}")
    mark(
        f"SWEEP COMPLETE ({(time.time() - t0) / 60:.1f}m) "
        f"trials={len(per_config)}/{TOTAL_TRIALS} clock={N_TRIALS_DEFLATION}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
