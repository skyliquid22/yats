"""liquid50 universe-breadth pilot — PRE-REGISTERED sweep runner.

Implements docs/research/preregistrations/2026-09-10_liquid50_pilot.md
EXACTLY. Every run of this script spends pre-registered trials: the full
family is 10 trials and moves the deflation clock 63 -> 73. Do not add
configs, ablations, or regularization values here — the prereg forbids it.

Design (all committed in advance in the prereg):
    Universe:     configs/universes/liquid50.yml (50 names, ETFs excluded).
    Feature set:  breadth_v1 (features table, feature_set='breadth_v1').
    Arm A (H1):   breadth_v1 WITHOUT the insider + institutional groups —
                  ohlcv, cross_sectional, fundamental, regime only. Pure
                  breadth test of the incumbent signal class.
                  (Note: the prereg's Arm A includes the regime group even
                  though ALPHA-1 excluded regime columns from model inputs
                  as cross-sectionally constant; the prereg definition wins.
                  Constant-per-date columns rank-normalize to 0 and are
                  inert, so this is a definitional not numerical choice.)
    Arm B (H2):   full breadth_v1 (adds 4 insider + 2 institutional cols).
                  Same stored features — the arms differ ONLY in the input
                  column list handed to the model.
    Grid/arm:     ridge_5d_a10, ridge_21d_a10 (ridge alpha=10.0),
                  lgbm_5d, lgbm_21d (lgbm lambda=0.1) — ALPHA-1 incumbents —
                  plus ONE 10% vol-target overlay on the arm's best base
                  config by OOS Sharpe. 2 x (4 + 1) = 10 trials.
    Geometry:     anchored 4-fold WFO, train_window=250 (matched to the
                  full-span incumbent), label_horizon=21,
                  purge_buffer=253 (= 1 + max lookback 252, mom_12m_excl_1m).
    Execution:    execution_lag_days=1, fill_timing='next_close',
                  transaction_cost_bp=5 (charged on daily turnover).
    Span:         2020-01 .. YATS_SWEEP_END (default: latest available).
    Deflation:    STRICT — SR0 = std(honest-fill pool INCLUDING these 10
                  trials) x expected-max benchmark at 73 trials; per-config
                  DSR = PSR(sharpe; SR0). Certification bar: DSR > 0.95.

Model math is imported from research/scripts/wfo_sweep_alpha1.py (training,
scoring, rank-weighted portfolio, execution lag) — zero duplicated math.

Short-history symbols (2020-21 IPOs: HOOD, SNOW, PLTR, ...): the panel is
long-format (one row per date x symbol), so pre-listing dates simply have no
row. Downstream this means: rank normalization ranks whichever symbols exist
on each date (NaNs excluded per column); training drops any row with a NaN
feature or target (train_alpha_model.dropna); prediction scores NaN-feature
rows 0.0 (neutral); portfolio construction reindexes over the full symbol
list and fills missing weights/returns with 0. No forward- or back-filling
anywhere — absent history is skipped, never fabricated.

SPY is NOT a member of liquid50 (ETFs are excluded) but is required as the
market proxy for target residualization. Its prices are fetched as a
sidecar, its rows ride along the panel only through target computation
(feature columns all-NaN, so it cannot contaminate cross-sectional ranks),
and it is dropped before training/portfolio construction.

Run (operator only — spends the 10 pre-registered trials):
    OMP_NUM_THREADS=1 PYTHONPATH=.:pipelines uv run python research/scripts/wfo_pilot_liquid50.py

Receipt: docs/research/receipts/pilot_liquid50.json
All progress lines carry the [PILOT50] prefix.
"""
from __future__ import annotations

import importlib.util
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import yaml

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger("wfo_pilot_liquid50")

# Repo root, resolved relative to this file so fresh checkouts work anywhere.
RIG = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RIG))
sys.path.insert(0, str(RIG / "pipelines"))

from compute.stats.deflated_sharpe import (  # noqa: E402
    _expected_max_sr_benchmark,
    probabilistic_sharpe_ratio,
)
from research.alpha.targets import compute_forward_returns, residualize_vs_spy  # noqa: E402
from research.alpha.transforms import rank_normalize_cross_sectional  # noqa: E402
from research.eval.metrics import compute_sharpe, compute_turnover  # noqa: E402
from research.eval.wfo import compute_sweep_wfo_rank_decay, run_wfo  # noqa: E402
from research.experiments.spec import PortfolioRiskConfig, WFOConfig  # noqa: E402
from research.portfolio.risk_layer import apply_risk_layer_batch  # noqa: E402


def _load_alpha1():
    """Import research/scripts/wfo_sweep_alpha1.py without duplicating its math."""
    path = RIG / "research" / "scripts" / "wfo_sweep_alpha1.py"
    spec = importlib.util.spec_from_file_location("wfo_sweep_alpha1_pilot", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


alpha1 = _load_alpha1()

# ---------------------------------------------------------------------------
# Pre-registered constants (2026-09-10_liquid50_pilot.md)
# ---------------------------------------------------------------------------

UNIVERSE_YML = RIG / "configs" / "universes" / "liquid50.yml"
FEATURE_SET_YML = RIG / "configs" / "feature_sets" / "breadth_v1.yml"
FEATURE_SET = "breadth_v1"
SPY = "SPY"  # market proxy for residualization only — NOT tradable here

# Arm definitions: same stored features, column-subset difference only.
ARMS = ("A", "B")
ARM_A_EXCLUDED_GROUPS = ("insider", "institutional")

# Per-arm base grid at the incumbent regularization (ALPHA-1 winners:
# ridge alpha=10.0, lgbm lambda=0.1). No other values may be run.
BASE_GRID = [
    {"model": "ridge", "horizon": 5, "reg": 10.0, "label": "ridge_5d_a10"},
    {"model": "ridge", "horizon": 21, "reg": 10.0, "label": "ridge_21d_a10"},
    {"model": "lgbm", "horizon": 5, "reg": 0.1, "label": "lgbm_5d"},
    {"model": "lgbm", "horizon": 21, "reg": 0.1, "label": "lgbm_21d"},
]
N_OVERLAYS_PER_ARM = 1  # exactly one vol-target overlay per arm, on its best base
TOTAL_TRIALS = len(ARMS) * (len(BASE_GRID) + N_OVERLAYS_PER_ARM)  # == 10
VOL_TARGET = 0.10  # ALPHA-3 risk layer, 10% annualized

# WFO geometry (prereg "Geometry and execution"): anchored 4-fold, matched to
# the full-span incumbent (train_window=250 per docs/research/receipts/
# sweep_alpha1.json), label_horizon=21 (max prediction horizon),
# purge_buffer=253 = 1 + max feature lookback 252 (mom_12m_excl_1m).
TRAIN_WINDOW = 250
LABEL_HORIZON = 21
PURGE_BUFFER = 253
N_PERIODS = 4
WFO_CFG = WFOConfig(
    train_window=TRAIN_WINDOW,
    label_horizon=LABEL_HORIZON,
    purge_buffer=PURGE_BUFFER,
    n_periods=N_PERIODS,
)

# Execution — pinned by the prereg (b77155f honest-fill regime); deliberately
# NOT env-overridable so a run cannot silently deviate from the registration.
EXECUTION_LAG = 1
FILL_TIMING = "next_close"
COST_BP = 5.0

# Span: 2020-01 .. present. END=None means "latest available" (no upper bound).
START = os.environ.get("YATS_SWEEP_START", "2020-01-01")
END = os.environ.get("YATS_SWEEP_END")  # None -> latest

OUT_DIR = RIG / ".yats_data" / "wfo_sweeps" / "pilot_liquid50"
RECEIPT_PATH = RIG / "docs" / "research" / "receipts" / "pilot_liquid50.json"
PREREG_PATH = "docs/research/preregistrations/2026-09-10_liquid50_pilot.md"

# ---------------------------------------------------------------------------
# Strict deflation inputs (prereg "Success and failure criteria")
# ---------------------------------------------------------------------------

# Full-span honest-fill result pool at registration time — the same 15 values
# used for the incumbent's strict DSR 0.921 (docs/research/receipts/
# vt_lgbm21_result.json strict_deflation.pool_sharpes): 8 PPO-champion rerun
# configs + 6 ALPHA-1 supervised configs + the vol-target overlay (trial 63).
HONEST_FILL_POOL_PRE_PILOT = [
    0.29, 0.402, 0.41, 0.417, 0.508, 0.527, 0.602, 0.756,  # PPO champion rerun (8)
    0.367, 0.585, 0.624, 0.635, 0.73, 0.905,               # ALPHA-1 supervised (6)
    1.1339,                                                # vt overlay, trial 63
]

# Every DSR under this registration deflates at the POST-family clock.
N_TRIALS_DEFLATION = 73  # 63 at registration + the 10 trials of this family
CERTIFICATION_BAR = 0.95

# Secondary readout: same-class full-span dev10 incumbents (hardcoded from
# docs/research/receipts/sweep_alpha1.json — comparison display only, these
# numbers are not recomputed here).
INCUMBENT_DEV10_SHARPES = {
    "ridge_5d_a10": 0.6244563012567517,
    "ridge_21d_a10": 0.7304903814928329,
    "lgbm_5d": 0.3666183161146466,
    "lgbm_21d": 0.9049149200370546,
}


def mark(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[PILOT50] {ts} {msg}", flush=True)


# ---------------------------------------------------------------------------
# Config parsing: universe + arm column subsets (derived from YAML groups)
# ---------------------------------------------------------------------------

def load_universe(path: Path = UNIVERSE_YML) -> list[str]:
    """Load the liquid50 ticker list from its YAML config."""
    data = yaml.safe_load(Path(path).read_text())
    tickers = list(data["tickers"])
    return tickers


def load_feature_groups(path: Path = FEATURE_SET_YML) -> dict[str, list[str]]:
    """Load feature-set groups from YAML: {group_name: [column, ...]}.

    Scalar metadata keys (name, description) are skipped; group order is
    preserved as written in the file.
    """
    data = yaml.safe_load(Path(path).read_text())
    return {
        key: list(cols)
        for key, cols in data.items()
        if isinstance(cols, list)
    }


def arm_feature_cols(groups: dict[str, list[str]], arm: str) -> list[str]:
    """Derive the model-input column list for an arm from the YAML groups.

    Arm A: all groups EXCEPT insider + institutional (pure breadth test).
    Arm B: all groups (full breadth_v1 — the resurrection test).
    """
    if arm == "A":
        keep = [g for g in groups if g not in ARM_A_EXCLUDED_GROUPS]
    elif arm == "B":
        keep = list(groups)
    else:
        raise ValueError(f"Unknown arm {arm!r}; expected 'A' or 'B'")
    return [col for g in keep for col in groups[g]]


def build_arm_grids() -> dict[str, list[dict]]:
    """Per-arm base grids: the 4 incumbent configs, tagged with their arm."""
    return {arm: [dict(cfg, arm=arm) for cfg in BASE_GRID] for arm in ARMS}


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _fetch_frames_questdb(
    symbols: list[str], feature_cols: list[str]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch breadth_v1 features (universe) + OHLCV (universe + SPY sidecar).

    Returns (features_df, closes_df); closes_df includes SPY, features_df
    does not. END=None fetches through the latest available bar.
    """
    import psycopg2

    from yats_pipelines.resources.questdb import QuestDBResource

    qdb = QuestDBResource()
    conn = psycopg2.connect(
        host=qdb.pg_host, port=qdb.pg_port, user=qdb.pg_user,
        password=qdb.pg_password, database=qdb.pg_database,
    )
    conn.autocommit = True
    cur = conn.cursor()

    end_clause = " AND timestamp <= %s" if END else ""

    feat_params: tuple = (tuple(symbols), FEATURE_SET, START)
    if END:
        feat_params = feat_params + (END,)
    cur.execute(
        "SELECT timestamp, symbol, " + ", ".join(feature_cols) +
        " FROM features WHERE symbol IN %s AND feature_set = %s"
        " AND timestamp >= %s" + end_clause + " ORDER BY timestamp, symbol",
        feat_params,
    )
    features_df = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    features_df.rename(columns={"timestamp": "date"}, inplace=True)
    features_df["date"] = pd.to_datetime(features_df["date"]).dt.date

    px_symbols = list(symbols) + ([] if SPY in symbols else [SPY])
    px_params: tuple = (tuple(px_symbols), START)
    if END:
        px_params = px_params + (END,)
    cur.execute(
        "SELECT timestamp, symbol, open, close FROM canonical_equity_ohlcv"
        " WHERE symbol IN %s AND timestamp >= %s" + end_clause +
        " ORDER BY timestamp, symbol",
        px_params,
    )
    closes_df = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    closes_df.rename(columns={"timestamp": "date"}, inplace=True)
    closes_df["date"] = pd.to_datetime(closes_df["date"]).dt.date
    cur.close()
    conn.close()

    return features_df, closes_df


def build_panel(
    features_df: pd.DataFrame,
    closes_df: pd.DataFrame,
    feature_cols: list[str],
) -> pd.DataFrame:
    """Assemble the (date, symbol) panel: features + prices + targets.

    Pure function of the two input frames (DB-free, unit-testable):

    1. Inner-join features with prices — a symbol contributes a row only on
       dates where BOTH exist (short-history names simply start later; no
       fill of any kind).
    2. Append SPY price rows as a residualization sidecar (feature columns
       all-NaN — excluded from cross-sectional ranks by construction).
    3. Rank-normalize feature columns cross-sectionally per date over
       whatever symbols exist that date (NaN-skip per column).
    4. Compute forward returns (5d/21d), the 1-day realized return used for
       portfolio P&L, and SPY-residualized targets.
    5. Drop the SPY sidecar rows — SPY is not tradable in this pilot.

    Same transform order and functions as the ALPHA-1 loader.
    """
    panel = features_df.merge(closes_df, on=["date", "symbol"], how="inner")
    panel = panel[panel["symbol"] != SPY]

    spy_px = closes_df[closes_df["symbol"] == SPY].copy()
    if spy_px.empty:
        raise ValueError("SPY prices missing — required for target residualization")
    for col in feature_cols:
        spy_px[col] = np.nan

    full = (
        pd.concat([panel, spy_px], ignore_index=True)
        .sort_values(["date", "symbol"])
        .reset_index(drop=True)
    )

    # Rank-normalize features cross-sectionally per date (SPY rows are all-NaN
    # in every feature column, so they cannot perturb any rank).
    full = rank_normalize_cross_sectional(full, feature_cols, date_col="date")

    # Forward returns + 1-day realized return (per-symbol, no cross-fill)
    for h in (5, 21):
        full[f"fwd_{h}d"] = compute_forward_returns(
            full, h, close_col="close", date_col="date", symbol_col="symbol",
        )
    full["ret_1d_realized"] = compute_forward_returns(
        full, 1, close_col="close", date_col="date", symbol_col="symbol",
    )

    # Residualize forward returns vs SPY rolling beta (SPY rows in-panel here)
    for h in (5, 21):
        spy_mask = full["symbol"] == SPY
        spy_fwd = full.loc[spy_mask].set_index("date")[f"fwd_{h}d"]
        spy_fwd = spy_fwd[~spy_fwd.index.duplicated(keep="first")]
        full[f"fwd_{h}d_resid"] = residualize_vs_spy(
            full, f"fwd_{h}d", spy_fwd,
            date_col="date", symbol_col="symbol", close_col="close",
        )

    # Drop the sidecar: SPY is not part of the tradable cross-section.
    out = full[full["symbol"] != SPY].sort_values(["date", "symbol"]).reset_index(drop=True)
    return out


def load_panel(symbols: list[str], feature_cols: list[str]) -> pd.DataFrame:
    """Fetch from QuestDB and assemble the pilot panel."""
    features_df, closes_df = _fetch_frames_questdb(symbols, feature_cols)
    mark(
        f"DATA features rows={len(features_df)} px rows={len(closes_df)} "
        f"span=[{START}, {END or 'latest'}]"
    )
    panel = build_panel(features_df, closes_df, feature_cols)
    mark(
        f"PANEL rows={len(panel)} dates={panel['date'].nunique()} "
        f"symbols={panel['symbol'].nunique()} (SPY sidecar dropped)"
    )
    return panel


# ---------------------------------------------------------------------------
# Vol-target overlay (ALPHA-3 risk layer) on an arm's best base config
# ---------------------------------------------------------------------------

def apply_vol_target_overlay(
    arm: str,
    base_summary: dict,
    fold_captures: list[dict],
    cost_bp: float = COST_BP,
    vol_target: float = VOL_TARGET,
) -> dict:
    """Apply the 10% vol-target overlay to a base config's captured WFO folds.

    Per fold (strictly causal, no cross-fold state): scale the fill-date
    weights via research.portfolio.risk_layer.apply_risk_layer_batch
    (trailing-vol window excludes the current bar), recompute portfolio
    returns, and re-charge the 5bp cost on the SCALED weights' turnover.
    Returns a per-config summary dict shaped like the base configs'.
    """
    if not fold_captures:
        raise ValueError("No captured folds — cannot apply vol-target overlay")

    risk_cfg = PortfolioRiskConfig(vol_target=vol_target)
    concatenated: list[float] = []
    per_fold_sharpe: list[float | None] = []
    for fold in fold_captures:
        weights, returns = fold["weights"], fold["returns"]
        scaled = apply_risk_layer_batch(weights, returns, None, risk_cfg)
        gross = (scaled * returns).sum(axis=1)
        net = gross - compute_turnover(scaled) * (cost_bp / 1e4)
        per_fold_sharpe.append(compute_sharpe(net) if len(net) >= 2 else None)
        concatenated.extend(float(r) for r in net.tolist())

    valid = [s for s in per_fold_sharpe if s is not None]
    result = SimpleNamespace(
        concatenated_oos_returns=concatenated,
        per_fold_oos_sharpe=per_fold_sharpe,
        median_oos_sharpe=float(np.median(valid)) if valid else None,
    )
    cfg = {
        "arm": arm,
        "model": base_summary["model"],
        "horizon": base_summary["horizon"],
        "reg": base_summary["reg"],
        "label": f"{base_summary['label']}_vt10",
        "overlay": "vol_target_10pct",
        "base_config": base_summary["label"],
    }
    return alpha1.summarize_config_result(cfg, result)


# ---------------------------------------------------------------------------
# Strict deflation (prereg: expected-max @ 73 over the honest-fill pool)
# ---------------------------------------------------------------------------

def strict_deflation(per_config: list[dict]) -> dict:
    """STRICT DSR per the prereg: pool includes these 10 trials; E[max] @ 73.

    SR0 = std(honest-fill pool INCLUDING the pilot trials, population std)
          x expected-max of N_TRIALS_DEFLATION=73 standard normals
    (the same computation that produced the incumbent's 0.921 — NOT the
    naive per-family benchmark). Per-config DSR = PSR(sharpe; SR0).
    """
    pool = list(HONEST_FILL_POOL_PRE_PILOT) + [float(c["sharpe"]) for c in per_config]
    emax = _expected_max_sr_benchmark(N_TRIALS_DEFLATION)
    sr0 = float(np.std(pool)) * emax  # population std (ddof=0), as incumbent

    per_config_out = []
    for c in per_config:
        psr = probabilistic_sharpe_ratio(
            observed_sharpe=float(c["sharpe"]),
            benchmark_sharpe=sr0,
            n_observations=int(c["n_obs"]),
            returns_skewness=float(c["skewness"]),
            returns_kurtosis=float(c["kurtosis"]),
        )
        per_config_out.append({
            "arm": c["arm"],
            "config": c["label"],
            "sharpe": float(c["sharpe"]),
            "strict_dsr": psr["dsr"],
            "certified": bool(psr["dsr"] > CERTIFICATION_BAR),
        })

    return {
        "method": (
            f"expected-max benchmark scaled to all {N_TRIALS_DEFLATION} trials; "
            "std over the full-span honest-fill pool including this family's 10 trials"
        ),
        "n_trials_emax": N_TRIALS_DEFLATION,
        "expected_max_sr": float(emax),
        "pool_sharpes": [float(s) for s in pool],
        "sr0": sr0,
        "certification_bar": CERTIFICATION_BAR,
        "per_config": per_config_out,
        "any_certified": any(c["certified"] for c in per_config_out),
    }


# ---------------------------------------------------------------------------
# Secondary readout: Sharpe(liquid50) vs the 10-name incumbents
# ---------------------------------------------------------------------------

def secondary_readout(per_config: list[dict]) -> list[dict]:
    """Same-class Sharpe comparison vs full-span dev10 incumbents + B-A margin."""
    by_arm_label = {
        (c["arm"], c["label"]): float(c["sharpe"])
        for c in per_config
        if "overlay" not in c
    }
    rows = []
    for cfg in BASE_GRID:
        label = cfg["label"]
        sharpe_a = by_arm_label.get(("A", label))
        sharpe_b = by_arm_label.get(("B", label))
        dev10 = INCUMBENT_DEV10_SHARPES[label]
        rows.append({
            "config": label,
            "sharpe_liquid50_arm_a": sharpe_a,
            "sharpe_liquid50_arm_b": sharpe_b,
            "sharpe_dev10_incumbent": dev10,
            "delta_a_vs_dev10": None if sharpe_a is None else sharpe_a - dev10,
            "delta_b_vs_dev10": None if sharpe_b is None else sharpe_b - dev10,
            "margin_b_minus_a": (
                None if sharpe_a is None or sharpe_b is None else sharpe_b - sharpe_a
            ),
        })
    return rows


# ---------------------------------------------------------------------------
# Receipt
# ---------------------------------------------------------------------------

def build_receipt(
    per_config: list[dict],
    deflation: dict,
    secondary: list[dict],
    *,
    symbols: list[str],
    arm_cols: dict[str, list[str]],
    rank_decay_by_arm: dict[str, float | None],
    span_end: str | None,
    elapsed_hours: float | None = None,
) -> dict:
    """Assemble the receipt dict written to RECEIPT_PATH."""
    return {
        "sweep": "liquid50_breadth_pilot",
        "preregistration": PREREG_PATH,
        "universe": "liquid50",
        "n_symbols": len(symbols),
        "symbols": list(symbols),
        "feature_set": FEATURE_SET,
        "arm_feature_cols": {arm: list(cols) for arm, cols in arm_cols.items()},
        "span": [START, span_end or "latest"],
        "wfo": {
            "mode": "anchored",
            "n_periods": N_PERIODS,
            "train_window": TRAIN_WINDOW,
            "label_horizon": LABEL_HORIZON,
            "purge_buffer": PURGE_BUFFER,
        },
        "execution": {
            "execution_lag_days": EXECUTION_LAG,
            "fill_timing": FILL_TIMING,
            "transaction_cost_bp": COST_BP,
        },
        "trials_charged": TOTAL_TRIALS,
        "deflation_clock": N_TRIALS_DEFLATION,
        "configs": per_config,
        "strict_deflation": deflation,
        "secondary_readout": secondary,
        "rank_decay_by_arm": rank_decay_by_arm,
        "elapsed_hours": elapsed_hours,
        "incumbent_dev10_source": "docs/research/receipts/sweep_alpha1.json",
    }


# ---------------------------------------------------------------------------
# Sweep driver
# ---------------------------------------------------------------------------

def run_arm(
    panel: pd.DataFrame,
    dates: list,
    arm: str,
    grid: list[dict],
    feature_cols: list[str],
    symbols: list[str],
) -> list[dict]:
    """Run one arm: 4 base configs + the vol-target overlay on its best base."""
    data = list(range(len(dates)))
    per_config: list[dict] = []
    captures: dict[str, list[dict]] = {}

    for i, cfg in enumerate(grid):
        cfg_t0 = time.time()
        mark(
            f"ARM {arm} CONFIG {i + 1}/{len(grid)}: {cfg['label']} "
            f"({cfg['model']} h={cfg['horizon']}d reg={cfg['reg']}, "
            f"{len(feature_cols)} input cols)"
        )
        capture: list[dict] = []
        train_fn = alpha1.make_train_fn(panel, cfg, dates, feature_cols=feature_cols)
        eval_fn = alpha1.make_eval_fn(
            panel, dates,
            symbols=symbols,
            fill_timing=FILL_TIMING,
            execution_lag=EXECUTION_LAG,
            cost_bp=COST_BP,
            fold_capture=capture,
        )
        result = run_wfo(data, WFO_CFG, train_fn=train_fn, eval_fn=eval_fn)
        summary = alpha1.summarize_config_result(cfg, result)
        summary["elapsed_s"] = time.time() - cfg_t0
        per_config.append(summary)
        captures[cfg["label"]] = capture
        mark(
            f"ARM {arm} {cfg['label']} done: OOS sharpe={summary['sharpe']:.3f} "
            f"folds={['%.2f' % (s or 0) for s in summary['per_fold_oos_sharpe']]} "
            f"n={summary['n_obs']} ({summary['elapsed_s']:.1f}s)"
        )

    # Overlay choice rule (committed in the prereg): the arm's best BASE
    # config by OOS Sharpe gets the single 10% vol-target overlay.
    best = max(per_config, key=lambda c: c["sharpe"])
    mark(f"ARM {arm} best base by OOS Sharpe: {best['label']} ({best['sharpe']:.3f}) — applying vt10 overlay")
    overlay = apply_vol_target_overlay(arm, best, captures[best["label"]])
    per_config.append(overlay)
    mark(
        f"ARM {arm} {overlay['label']} done: OOS sharpe={overlay['sharpe']:.3f} "
        f"folds={['%.2f' % (s or 0) for s in overlay['per_fold_oos_sharpe']]}"
    )
    return per_config


def print_verdict(deflation: dict, secondary: list[dict]) -> None:
    """Print the strict-deflation verdict + the secondary breadth readout."""
    mark(
        f"STRICT DEFLATION: SR0={deflation['sr0']:.4f} "
        f"(= std(pool n={len(deflation['pool_sharpes'])}) x "
        f"E[max]@{deflation['n_trials_emax']}={deflation['expected_max_sr']:.3f}) "
        f"bar={deflation['certification_bar']}"
    )
    for c in sorted(deflation["per_config"], key=lambda x: -x["strict_dsr"]):
        mark(
            f"  arm={c['arm']} {c['config']}: sharpe={c['sharpe']:.3f} "
            f"strict_dsr={c['strict_dsr']:.3f} certified={c['certified']}"
        )
    best = max(deflation["per_config"], key=lambda x: x["strict_dsr"])
    if deflation["any_certified"]:
        mark(
            f"VERDICT: CERTIFIED — {best['config']} (arm {best['arm']}) clears "
            f"strict DSR {CERTIFICATION_BAR} at {N_TRIALS_DEFLATION} trials "
            f"(dsr={best['strict_dsr']:.3f})"
        )
    else:
        mark(
            f"VERDICT: NOT CERTIFIED — best {best['config']} (arm {best['arm']}) "
            f"strict_dsr={best['strict_dsr']:.3f} vs bar {CERTIFICATION_BAR} "
            f"at {N_TRIALS_DEFLATION} trials"
        )

    mark("SECONDARY (breadth): same-class Sharpe, liquid50 vs dev10 incumbents "
         "(dev10 from docs/research/receipts/sweep_alpha1.json):")
    for row in secondary:
        fmt = lambda v: "n/a" if v is None else f"{v:+.3f}"  # noqa: E731
        a = row["sharpe_liquid50_arm_a"]
        b = row["sharpe_liquid50_arm_b"]
        mark(
            f"  {row['config']}: A={'n/a' if a is None else f'{a:.3f}'} "
            f"B={'n/a' if b is None else f'{b:.3f}'} "
            f"dev10={row['sharpe_dev10_incumbent']:.3f} "
            f"dA={fmt(row['delta_a_vs_dev10'])} dB={fmt(row['delta_b_vs_dev10'])} "
            f"B-A={fmt(row['margin_b_minus_a'])}"
        )


def main() -> int:
    t0 = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    symbols = load_universe()
    groups = load_feature_groups()
    arm_cols = {arm: arm_feature_cols(groups, arm) for arm in ARMS}
    mark(
        f"UNIVERSE liquid50 n={len(symbols)}; feature_set={FEATURE_SET}; "
        f"arm A cols={len(arm_cols['A'])} (minus {list(ARM_A_EXCLUDED_GROUPS)}), "
        f"arm B cols={len(arm_cols['B'])}"
    )
    mark(
        f"PREREGISTERED FAMILY: {TOTAL_TRIALS} trials, deflation clock "
        f"63 -> {N_TRIALS_DEFLATION} ({PREREG_PATH})"
    )

    # Arm B's columns are the full breadth_v1 set — fetch/rank once, both
    # arms consume column subsets of the same stored features.
    panel = load_panel(symbols, arm_cols["B"])
    dates = sorted(panel["date"].unique())
    mark(
        f"WFO geometry: {len(dates)} dates, anchored {N_PERIODS} folds, "
        f"train_window={TRAIN_WINDOW}, label_horizon={LABEL_HORIZON}, "
        f"purge_buffer={PURGE_BUFFER}, lag={EXECUTION_LAG}, "
        f"fill={FILL_TIMING}, cost={COST_BP}bp"
    )

    grids = build_arm_grids()
    per_config: list[dict] = []
    rank_decay_by_arm: dict[str, float | None] = {}
    for arm in ARMS:
        arm_results = run_arm(panel, dates, arm, grids[arm], arm_cols[arm], symbols)
        base_results = [c for c in arm_results if "overlay" not in c]
        rank_decay_by_arm[arm] = compute_sweep_wfo_rank_decay(
            [c["per_fold_oos_sharpe"] for c in base_results]
        )
        per_config.extend(arm_results)

    if len(per_config) != TOTAL_TRIALS:
        mark(
            f"WARNING trial count {len(per_config)} != preregistered {TOTAL_TRIALS} "
            "— receipt records actual configs run; the clock still charges 10"
        )

    deflation = strict_deflation(per_config)
    for c, d in zip(per_config, deflation["per_config"]):
        c["strict_dsr"] = d["strict_dsr"]
        c["certified"] = d["certified"]

    secondary = secondary_readout(per_config)
    receipt = build_receipt(
        per_config, deflation, secondary,
        symbols=symbols,
        arm_cols=arm_cols,
        rank_decay_by_arm=rank_decay_by_arm,
        span_end=END,
        elapsed_hours=(time.time() - t0) / 3600,
    )

    RECEIPT_PATH.parent.mkdir(parents=True, exist_ok=True)
    RECEIPT_PATH.write_text(json.dumps(receipt, indent=2, default=str))
    mark(f"RECEIPT written to {RECEIPT_PATH}")
    raw_out = OUT_DIR / "pilot_summary.json"
    raw_out.write_text(json.dumps(receipt, indent=2, default=str))
    mark(f"RAW SUMMARY written to {raw_out}")

    print_verdict(deflation, secondary)
    mark(
        f"PILOT COMPLETE ({(time.time() - t0) / 60:.1f}m) "
        f"trials={len(per_config)}/{TOTAL_TRIALS} clock={N_TRIALS_DEFLATION} "
        f"rank_decay A={rank_decay_by_arm.get('A')} B={rank_decay_by_arm.get('B')}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
