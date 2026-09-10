"""ALPHA-1: supervised cross-sectional alpha sweep.

Trains Ridge + LightGBM models predicting residualized forward returns
(5d and 21d) through the existing WFO harness, at matched geometry to
the PPO champion sweep (4b), for an honest supervised-vs-PPO comparison.

Trial grid (6 configs — documented for DSR deflation accounting):
    {ridge, lgbm} × {5d, 21d} × {α=1.0, α=10.0 for ridge; λ=0.1 for lgbm}
    Fixed: feature_set=sweep_v1, dev10 universe, seed=42.

Methodology:
    - Features: sweep_v1 set (27 obs cols, no regime), rank-normalized
      cross-sectionally per date (percentile rank → z-score).
    - Target: residualized forward return (fwd_h - beta_i × fwd_SPY),
      rolling 60-day OLS beta estimation.
    - WFO: anchored, 4 folds, label_horizon=21, purge_buffer=63.
      Total gap = 84 bars (clean boundary for 21d labels + feature memory).
    - Portfolio: rank-weighted long-only, max_symbol_weight=0.30.
    - DSR: sweep-level DSR from cross-config Sharpe variance.
      Cumulative deflation clock = 48 (prior) + 6 (this sweep) = 54 trials.

All progress lines carry the [ALPHA1] prefix for the monitor.
"""
from __future__ import annotations

import json
import os
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from scipy import stats as sp_stats

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger("wfo_sweep_alpha1")

# Repo root, resolved relative to this file so fresh checkouts work anywhere.
RIG = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RIG))
sys.path.insert(0, str(RIG / "pipelines"))

from compute.stats.deflated_sharpe import compute_sweep_dsr, probabilistic_sharpe_ratio
from research.alpha.models import AlphaModelResult, predict_alpha_scores, train_alpha_model
from research.alpha.portfolio import portfolio_returns_from_weights, rank_weighted_portfolio
from research.alpha.targets import compute_forward_returns, residualize_vs_spy
from research.alpha.transforms import rank_normalize_cross_sectional
from research.eval.metrics import compute_sharpe, compute_turnover
from research.eval.wfo import compute_sweep_wfo_rank_decay, run_wfo
from research.experiments.spec import WFOConfig
from yats_pipelines.resources.questdb import QuestDBResource

SYMBOLS = ["AAPL", "AMZN", "GOOGL", "JPM", "META", "MSFT", "NVDA", "QQQ", "SPY", "TSLA"]
START = os.environ.get("YATS_SWEEP_START", "2024-07-01")
END = os.environ.get("YATS_SWEEP_END", "2026-07-02")
FEATURE_SET = "sweep_v1"
OUT_DIR = RIG / ".yats_data" / "wfo_sweeps" / "sweep_alpha1"

# Features to use for cross-sectional prediction (exclude regime cols — constant
# across symbols on a given date, uninformative after rank normalization).
FEATURE_COLS = [
    # ohlcv
    "ret_1d", "ret_5d", "ret_21d", "rv_21d", "rv_63d",
    "dist_20d_high", "dist_20d_low",
    # cross-sectional
    "mom_3m", "log_mkt_cap", "size_rank", "value_rank",
    # fundamental
    "pe_ttm", "ps_ttm", "pb", "ev_ebitda", "roe",
    "gross_margin", "operating_margin", "fcf_margin",
    "debt_equity", "eps_growth_1y", "revenue_growth_1y",
    # options
    "atm_iv", "skew_25d", "iv_term_slope", "put_call_oi_ratio", "net_gamma_exposure",
]

# Model grid: 6 configs across {ridge,lgbm} × {5d,21d} × regularization
GRID = [
    {"model": "ridge", "horizon": 5,  "reg": 1.0,  "label": "ridge_5d_a1"},
    {"model": "ridge", "horizon": 5,  "reg": 10.0, "label": "ridge_5d_a10"},
    {"model": "ridge", "horizon": 21, "reg": 1.0,  "label": "ridge_21d_a1"},
    {"model": "ridge", "horizon": 21, "reg": 10.0, "label": "ridge_21d_a10"},
    {"model": "lgbm",  "horizon": 5,  "reg": 0.1,  "label": "lgbm_5d"},
    {"model": "lgbm",  "horizon": 21, "reg": 0.1,  "label": "lgbm_21d"},
]

# WFO geometry matched to PPO champion (4b), adjusted for supervised label horizon.
# label_horizon=21 (max prediction horizon) ensures clean boundary at fold edges.
# purge_buffer=63 = sweep_v1 max feature lookback (rv_63d, mom_3m).
WFO_CFG = WFOConfig(train_window=250, label_horizon=21, purge_buffer=63, n_periods=4)

# PPO champion OOS Sharpe (sweep_v1, 4b) for supervised-vs-RL comparison.
# Mean across 8 matched-geometry configs; used as reference only, not for DSR.
PPO_CHAMPION_SHARPE = 2.17

# Execution lag: 1=fill next bar (honest for EOD features), 0=old same-bar fill
EXECUTION_LAG = 1

# Unified execution timing (V11-1) — decision EOD(t), fill at:
#   'next_close' (default): close(t+1); weight earns close(t+1)->close(t+2)
#   'next_open': open(t+1); weight earns open(t+1)->close(t+1) (intraday)
FILL_TIMING = os.environ.get("YATS_FILL_TIMING", "next_close")
if FILL_TIMING not in ("next_close", "next_open"):
    raise ValueError(f"YATS_FILL_TIMING must be 'next_close' or 'next_open', got '{FILL_TIMING}'")
if FILL_TIMING == "next_open" and EXECUTION_LAG == 0:
    raise ValueError("fill_timing='next_open' requires EXECUTION_LAG=1 (no same-day open fills)")


def mark(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[ALPHA1] {ts} {msg}", flush=True)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _fetch_panel_questdb() -> pd.DataFrame:
    """Fetch sweep_v1 features + open/close prices from QuestDB (default path)."""
    qdb = QuestDBResource()
    conn = psycopg2.connect(
        host=qdb.pg_host, port=qdb.pg_port, user=qdb.pg_user,
        password=qdb.pg_password, database=qdb.pg_database,
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(
        "SELECT timestamp, symbol, " + ", ".join(FEATURE_COLS) +
        " FROM features WHERE symbol IN %s AND feature_set = %s"
        " AND timestamp >= %s AND timestamp <= %s ORDER BY timestamp, symbol",
        (tuple(SYMBOLS), FEATURE_SET, START, END),
    )
    feat_cols_db = [d[0] for d in cur.description]
    features_df = pd.DataFrame(cur.fetchall(), columns=feat_cols_db)
    features_df.rename(columns={"timestamp": "date"}, inplace=True)
    features_df["date"] = pd.to_datetime(features_df["date"]).dt.date

    cur.execute(
        "SELECT timestamp, symbol, open, close FROM canonical_equity_ohlcv"
        " WHERE symbol IN %s AND timestamp >= %s AND timestamp <= %s"
        " ORDER BY timestamp, symbol",
        (tuple(SYMBOLS), START, END),
    )
    closes_df = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    closes_df.rename(columns={"timestamp": "date"}, inplace=True)
    closes_df["date"] = pd.to_datetime(closes_df["date"]).dt.date
    cur.close()
    conn.close()

    panel = features_df.merge(closes_df, on=["date", "symbol"], how="inner")
    return panel.sort_values(["date", "symbol"]).reset_index(drop=True)


def _load_panel_from_file(path: Path) -> pd.DataFrame:
    """DEMO_PANEL_PATH hook: load a pre-built raw panel bundle (parquet or CSV).

    Used by demo/run_demo.py to run the sweep machinery without a live QuestDB.
    The bundle must carry columns: date, symbol, open, close + FEATURE_COLS.
    All downstream transforms (rank-normalization, forward returns,
    residualization) are identical to the QuestDB path.
    """
    if path.suffix in (".parquet", ".pq"):
        panel = pd.read_parquet(path)
    else:
        panel = pd.read_csv(path)
    required = ["date", "symbol", "open", "close", *FEATURE_COLS]
    missing = [c for c in required if c not in panel.columns]
    if missing:
        raise ValueError(f"DEMO_PANEL_PATH bundle {path} missing columns: {missing}")
    panel = panel.copy()
    panel["date"] = pd.to_datetime(panel["date"]).dt.date
    return panel.sort_values(["date", "symbol"]).reset_index(drop=True)


def load_panel() -> pd.DataFrame:
    """Build the (date, symbol) panel: features + prices + targets.

    Default source is QuestDB. If the DEMO_PANEL_PATH environment variable is
    set, the raw panel is loaded from that file instead (offline demo mode);
    everything downstream is unchanged.

    Returns a DataFrame with columns: date, symbol, open, close, <feature_cols>,
    fwd_5d, fwd_5d_resid, fwd_21d, fwd_21d_resid, ret_1d_realized, ret_oc_1d.
    Index: integer (reset), sorted by (date, symbol).
    """
    demo_path = os.environ.get("DEMO_PANEL_PATH")
    if demo_path:
        panel = _load_panel_from_file(Path(demo_path))
        mark(f"DATA source=DEMO_PANEL_PATH ({demo_path}) — QuestDB not used")
    else:
        panel = _fetch_panel_questdb()

    mark(f"DATA rows={len(panel)} dates={panel['date'].nunique()} symbols={panel['symbol'].nunique()}")

    # Rank-normalize features cross-sectionally per date
    panel = rank_normalize_cross_sectional(panel, FEATURE_COLS, date_col="date")
    mark("FEATURES rank-normalized cross-sectionally")

    # Compute raw forward returns
    for h in (5, 21):
        panel[f"fwd_{h}d"] = compute_forward_returns(
            panel, h, close_col="close", date_col="date", symbol_col="symbol",
        )

    # Compute 1-day realized return for portfolio P&L evaluation
    panel["ret_1d_realized"] = compute_forward_returns(
        panel, 1, close_col="close", date_col="date", symbol_col="symbol",
    )

    # Same-date intraday return open(t)->close(t) for fill_timing='next_open':
    # a weight filled at open(t) marked to close(t) earns close(t)/open(t) - 1.
    open_prices = pd.to_numeric(panel["open"], errors="coerce")
    close_prices = pd.to_numeric(panel["close"], errors="coerce")
    panel["ret_oc_1d"] = close_prices / open_prices.where(open_prices > 0) - 1.0

    # Residualize forward returns vs SPY rolling beta
    for h in (5, 21):
        spy_mask = panel["symbol"] == "SPY"
        spy_fwd = panel.loc[spy_mask].set_index("date")[f"fwd_{h}d"]
        spy_fwd = spy_fwd[~spy_fwd.index.duplicated(keep="first")]
        panel[f"fwd_{h}d_resid"] = residualize_vs_spy(
            panel, f"fwd_{h}d", spy_fwd,
            date_col="date", symbol_col="symbol", close_col="close",
        )

    mark("TARGETS forward returns and residuals computed")
    return panel


# ---------------------------------------------------------------------------
# WFO train/eval functions (constructed per-config)
# ---------------------------------------------------------------------------

def make_train_fn(panel: pd.DataFrame, cfg: dict, dates: list, feature_cols: list[str] | None = None):
    """Factory: returns train_fn(date_indices) -> AlphaModelResult.

    feature_cols=None keeps the historical behavior (module-level FEATURE_COLS,
    the sweep_v1 columns). Callers running other feature sets (e.g. the
    liquid50 breadth pilot) pass their own column list — the training math is
    identical either way.
    """
    horizon = cfg["horizon"]
    target_col = f"fwd_{horizon}d_resid"
    cols = list(feature_cols) if feature_cols is not None else FEATURE_COLS

    def train_fn(date_indices: list[int]) -> AlphaModelResult | None:
        fold_dates = set(dates[i] for i in date_indices if i < len(dates))
        train_panel = panel[panel["date"].isin(fold_dates)].copy()
        try:
            return train_alpha_model(
                train_panel, cols, target_col,
                model_type=cfg["model"],
                horizon=horizon,
                reg=cfg["reg"],
            )
        except ValueError as e:
            logger.warning("train_fn failed: %s", e)
            return None

    return train_fn


def make_eval_fn(
    panel: pd.DataFrame,
    dates: list,
    *,
    symbols: list[str] | None = None,
    fill_timing: str | None = None,
    execution_lag: int | None = None,
    cost_bp: float = 0.0,
    fold_capture: list | None = None,
):
    """Factory: returns eval_fn(date_indices, model) -> (returns, sharpe).

    Defaults (all None / 0.0) reproduce the historical ALPHA-1 behavior
    exactly: module-level SYMBOLS / FILL_TIMING / EXECUTION_LAG, gross
    returns, no capture. Optional extensions used by the liquid50 pilot:

    - symbols / fill_timing / execution_lag: per-call overrides of the
      module-level defaults (the math is unchanged).
    - cost_bp: linear transaction cost in basis points, charged per unit of
      daily turnover (sum |Δw| across symbols at each fill date, per
      research.eval.metrics.compute_turnover — the fold's first fill date is
      charged full establishment turnover from an all-zero book).
      cost_bp=0.0 is bit-identical to the historical gross path.
    - fold_capture: a list; when provided, each eval call (one per WFO fold,
      in fold order) appends {"weights": DataFrame, "returns": DataFrame},
      both indexed by fill date with one column per symbol. Used to apply
      post-hoc portfolio overlays (e.g. vol targeting) without re-running
      training.
    """
    syms = list(symbols) if symbols is not None else SYMBOLS
    timing = fill_timing if fill_timing is not None else FILL_TIMING
    lag = execution_lag if execution_lag is not None else EXECUTION_LAG
    if timing not in ("next_close", "next_open"):
        raise ValueError(f"fill_timing must be 'next_close' or 'next_open', got '{timing}'")
    if timing == "next_open" and lag == 0:
        raise ValueError("fill_timing='next_open' requires execution_lag=1 (no same-day open fills)")

    def eval_fn(
        date_indices: list[int], model_result: AlphaModelResult | None
    ) -> tuple[list[float], float | None]:
        if model_result is None:
            return [], None

        fold_dates = sorted(dates[i] for i in date_indices if i < len(dates))
        test_panel = panel[panel["date"].isin(set(fold_dates))].copy()

        # Predict alpha scores per (date, symbol)
        scores_series = predict_alpha_scores(test_panel, model_result)
        test_panel = test_panel.copy()
        test_panel["alpha_score"] = scores_series.values

        # Build rank-weighted portfolio per date.
        # Unified execution timing (V11-1): the weight decided from obs(t)
        # sits at fill-date t+1 (shift below). The return it earns at t+1 is
        #   next_close: ret_1d_realized[t+1] = close(t+1)->close(t+2)
        #   next_open:  ret_oc_1d[t+1] = open(t+1)->close(t+1) (intraday)
        return_col = "ret_oc_1d" if timing == "next_open" else "ret_1d_realized"
        weights_by_date: dict = {}
        returns_by_date: dict = {}
        for dt, grp in test_panel.groupby("date", sort=True):
            grp = grp.set_index("symbol")
            scores = grp["alpha_score"]
            weights_by_date[dt] = rank_weighted_portfolio(scores, max_symbol_weight=0.30)
            returns_by_date[dt] = grp[return_col]

        # Apply execution lag: decision from close(t-1) fills at close(t)
        # (next_close, earning ret_1d_realized[t]) or at open(t) (next_open,
        # earning ret_oc_1d[t])
        if lag > 0:
            sorted_dts = sorted(weights_by_date.keys())
            weights_by_date = {
                sorted_dts[i + 1]: weights_by_date[sorted_dts[i]]
                for i in range(len(sorted_dts) - 1)
            }

        port_returns = portfolio_returns_from_weights(weights_by_date, returns_by_date, syms)

        # Optional extensions (no-ops on the historical path): capture per-fold
        # weights/returns matrices and/or charge linear transaction costs.
        # fill_dates is sorted identically to portfolio_returns_from_weights'
        # internal iteration, so row i of the matrices is port_returns[i].
        if cost_bp > 0.0 or fold_capture is not None:
            fill_dates = sorted(set(weights_by_date) & set(returns_by_date))
            if fill_dates:
                weights_df = pd.DataFrame(
                    [weights_by_date[dt].reindex(syms).fillna(0.0) for dt in fill_dates],
                    index=fill_dates,
                )
                rets_df = pd.DataFrame(
                    [
                        pd.to_numeric(returns_by_date[dt].reindex(syms), errors="coerce").fillna(0.0)
                        for dt in fill_dates
                    ],
                    index=fill_dates,
                )
                if fold_capture is not None:
                    fold_capture.append({"weights": weights_df, "returns": rets_df})
                if cost_bp > 0.0:
                    turnover = compute_turnover(weights_df)
                    port_returns = [
                        float(r) - float(t) * cost_bp / 1e4
                        for r, t in zip(port_returns, turnover.tolist())
                    ]

        if len(port_returns) < 2:
            return port_returns, None
        oos_sharpe = compute_sharpe(pd.Series(port_returns))
        return port_returns, oos_sharpe

    return eval_fn


# ---------------------------------------------------------------------------
# Per-config summary (shared by main() and demo/run_demo.py)
# ---------------------------------------------------------------------------

def summarize_config_result(cfg: dict, result) -> dict:
    """Compute OOS Sharpe, moments, and PSR-vs-zero for one WFO config result."""
    oos = np.asarray(result.concatenated_oos_returns, dtype=float)
    oos = oos[~np.isnan(oos)]
    sharpe = compute_sharpe(pd.Series(oos)) if len(oos) > 2 else 0.0
    skew = float(sp_stats.skew(oos)) if len(oos) >= 3 else 0.0
    kurt = float(sp_stats.kurtosis(oos)) + 3.0 if len(oos) >= 4 else 3.0
    psr = probabilistic_sharpe_ratio(
        observed_sharpe=sharpe, benchmark_sharpe=0.0,
        n_observations=len(oos), returns_skewness=skew, returns_kurtosis=kurt,
    )
    return {
        **cfg,
        "sharpe": sharpe,
        "skewness": skew,
        "kurtosis": kurt,
        "n_obs": len(oos),
        "per_fold_oos_sharpe": result.per_fold_oos_sharpe,
        "median_oos_sharpe": result.median_oos_sharpe,
        "psr_vs_zero": psr["dsr"],
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    t0 = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    panel = load_panel()
    dates = sorted(panel["date"].unique())
    data = list(range(len(dates)))
    mark(f"WFO data: {len(dates)} dates, train_window={WFO_CFG.train_window}, "
         f"label_horizon={WFO_CFG.label_horizon}, purge_buffer={WFO_CFG.purge_buffer}, "
         f"n_periods={WFO_CFG.n_periods}")

    per_config: list[dict] = []

    for i, cfg in enumerate(GRID):
        cfg_t0 = time.time()
        mark(f"CONFIG {i}/{len(GRID)}: {cfg['label']} ({cfg['model']} h={cfg['horizon']}d α={cfg['reg']})")

        train_fn = make_train_fn(panel, cfg, dates)
        eval_fn = make_eval_fn(panel, dates)

        try:
            result = run_wfo(data, WFO_CFG, train_fn=train_fn, eval_fn=eval_fn)
        except Exception as exc:
            mark(f"CONFIG {i} FAILED: {exc}")
            per_config.append({**cfg, "failed": str(exc)})
            continue

        cfg_summary = summarize_config_result(cfg, result)
        cfg_summary["elapsed_s"] = time.time() - cfg_t0
        per_config.append(cfg_summary)
        mark(
            f"CONFIG {i} done: OOS sharpe={cfg_summary['sharpe']:.3f} "
            f"folds={['%.2f' % (s or 0) for s in result.per_fold_oos_sharpe]} "
            f"psr0={cfg_summary['psr_vs_zero']:.3f} ({(time.time() - cfg_t0):.1f}s)"
        )

    # DSR over this sweep (6 configs)
    ok = [c for c in per_config if "failed" not in c]
    rank_decay = None
    if len(ok) >= 2:
        dsr_results = compute_sweep_dsr(ok)
        for c, dsr in zip(ok, dsr_results):
            c["dsr"] = dsr["dsr"]
            c["dsr_significant"] = dsr["is_significant"]
            c["benchmark_sharpe"] = dsr["benchmark_sharpe"]
        rank_decay = compute_sweep_wfo_rank_decay([c["per_fold_oos_sharpe"] for c in ok])

    summary = {
        "sweep": "alpha1_supervised_wfo",
        "grid_size": len(GRID),
        "symbols": SYMBOLS,
        "span": [START, END],
        "feature_set": FEATURE_SET,
        "feature_cols": FEATURE_COLS,
        "wfo": {
            "mode": "anchored",
            "n_periods": WFO_CFG.n_periods,
            "train_window": WFO_CFG.train_window,
            "label_horizon": WFO_CFG.label_horizon,
            "purge_buffer": WFO_CFG.purge_buffer,
        },
        "ppo_champion_sharpe": PPO_CHAMPION_SHARPE,
        "configs": per_config,
        "rank_decay": rank_decay,
        "elapsed_hours": (time.time() - t0) / 3600,
    }
    out = OUT_DIR / "sweep_summary.json"
    out.write_text(json.dumps(summary, indent=2, default=str))
    mark(f"SUMMARY written to {out}")

    best = max(ok, key=lambda c: c.get("dsr", 0), default=None)
    if best:
        delta_vs_ppo = (best["sharpe"] - PPO_CHAMPION_SHARPE) if PPO_CHAMPION_SHARPE else None
        mark(
            f"VERDICT best-config={best['label']} sharpe={best['sharpe']:.3f} "
            f"dsr={best.get('dsr', 0):.3f} significant={best.get('dsr_significant')} "
            f"ΔvsPPO={delta_vs_ppo:+.2f} rank_decay={rank_decay:.3f}" if rank_decay else
            f"VERDICT best-config={best['label']} sharpe={best['sharpe']:.3f}"
        )
    n_sig = sum(1 for c in ok if c.get("dsr_significant"))
    mark(
        f"SWEEP COMPLETE ({(time.time() - t0) / 60:.1f}m) "
        f"configs_ok={len(ok)}/{len(GRID)} dsr_significant={n_sig} "
        f"vs PPO champion Sharpe={PPO_CHAMPION_SHARPE:.2f}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
