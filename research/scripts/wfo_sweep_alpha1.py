"""ALPHA-1: supervised cross-sectional alpha sweep (ya-z2ki9).

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

RIG = Path("/Users/ahmed/gt/yats/mayor/rig")
sys.path.insert(0, str(RIG))
sys.path.insert(0, str(RIG / "pipelines"))

from compute.stats.deflated_sharpe import compute_sweep_dsr, probabilistic_sharpe_ratio
from research.alpha.models import AlphaModelResult, predict_alpha_scores, train_alpha_model
from research.alpha.portfolio import portfolio_returns_from_weights, rank_weighted_portfolio
from research.alpha.targets import compute_forward_returns, residualize_vs_spy
from research.alpha.transforms import rank_normalize_cross_sectional
from research.eval.metrics import compute_sharpe
from research.eval.wfo import compute_sweep_wfo_rank_decay, run_wfo
from research.experiments.spec import WFOConfig
from yats_pipelines.resources.questdb import QuestDBResource

SYMBOLS = ["AAPL", "AMZN", "GOOGL", "JPM", "META", "MSFT", "NVDA", "QQQ", "SPY", "TSLA"]
START, END = "2024-07-01", "2026-07-02"
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


def mark(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[ALPHA1] {ts} {msg}", flush=True)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_panel() -> pd.DataFrame:
    """Fetch sweep_v1 features + close prices from QuestDB; build panel DataFrame.

    Returns a DataFrame with columns: date, symbol, close, <feature_cols>,
    fwd_5d, fwd_5d_resid, fwd_21d, fwd_21d_resid, ret_1d_realized.
    Index: integer (reset), sorted by (date, symbol).
    """
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
        "SELECT timestamp, symbol, close FROM canonical_equity_ohlcv"
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
    panel = panel.sort_values(["date", "symbol"]).reset_index(drop=True)

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

def make_train_fn(panel: pd.DataFrame, cfg: dict, dates: list):
    """Factory: returns train_fn(date_indices) -> AlphaModelResult."""
    horizon = cfg["horizon"]
    target_col = f"fwd_{horizon}d_resid"

    def train_fn(date_indices: list[int]) -> AlphaModelResult | None:
        fold_dates = set(dates[i] for i in date_indices if i < len(dates))
        train_panel = panel[panel["date"].isin(fold_dates)].copy()
        try:
            return train_alpha_model(
                train_panel, FEATURE_COLS, target_col,
                model_type=cfg["model"],
                horizon=horizon,
                reg=cfg["reg"],
            )
        except ValueError as e:
            logger.warning("train_fn failed: %s", e)
            return None

    return train_fn


def make_eval_fn(panel: pd.DataFrame, dates: list):
    """Factory: returns eval_fn(date_indices, model) -> (returns, sharpe)."""

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

        # Build rank-weighted portfolio per date
        weights_by_date: dict = {}
        returns_by_date: dict = {}
        for dt, grp in test_panel.groupby("date", sort=True):
            grp = grp.set_index("symbol")
            scores = grp["alpha_score"]
            weights_by_date[dt] = rank_weighted_portfolio(scores, max_symbol_weight=0.30)
            returns_by_date[dt] = grp["ret_1d_realized"]

        port_returns = portfolio_returns_from_weights(weights_by_date, returns_by_date, SYMBOLS)
        if len(port_returns) < 2:
            return port_returns, None
        oos_sharpe = compute_sharpe(pd.Series(port_returns))
        return port_returns, oos_sharpe

    return eval_fn


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

        oos = np.asarray(result.concatenated_oos_returns, dtype=float)
        oos = oos[~np.isnan(oos)]
        sharpe = compute_sharpe(pd.Series(oos)) if len(oos) > 2 else 0.0
        skew = float(sp_stats.skew(oos)) if len(oos) >= 3 else 0.0
        kurt = float(sp_stats.kurtosis(oos)) + 3.0 if len(oos) >= 4 else 3.0
        psr = probabilistic_sharpe_ratio(
            observed_sharpe=sharpe, benchmark_sharpe=0.0,
            n_observations=len(oos), returns_skewness=skew, returns_kurtosis=kurt,
        )
        per_config.append({
            **cfg,
            "sharpe": sharpe,
            "skewness": skew,
            "kurtosis": kurt,
            "n_obs": len(oos),
            "per_fold_oos_sharpe": result.per_fold_oos_sharpe,
            "median_oos_sharpe": result.median_oos_sharpe,
            "psr_vs_zero": psr["dsr"],
            "elapsed_s": time.time() - cfg_t0,
        })
        mark(
            f"CONFIG {i} done: OOS sharpe={sharpe:.3f} "
            f"folds={['%.2f' % (s or 0) for s in result.per_fold_oos_sharpe]} "
            f"psr0={psr['dsr']:.3f} ({(time.time() - cfg_t0):.1f}s)"
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
