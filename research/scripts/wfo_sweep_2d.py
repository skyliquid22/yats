"""Stage 2d driver: anchored 4-period WFO sweep on the complete feature matrix.

Trial grid (the set DSR deflates over — documented, N=8):
    learning_rate in {3e-4, 1e-4} x ent_coef in {0.0, 0.01} x gamma in {0.99, 0.95}
    fixed: policy=ppo, feature_set=full_v1, dev10 universe, seed=42,
           n_steps=256, batch_size=64, total_timesteps=20000, reward v1.

Methodology (agreed, do not re-litigate): anchored/expanding WFO (4 folds),
purge_buffer auto-sized from feature-set max lookback (forward-only geometry,
no after-embargo), FULL-convergence folds (timesteps never reduced), per-config
PSR on concatenated OOS returns, sweep-level DSR from cross-config Sharpe
variance (compute_sweep_dsr), rank-decay across folds.

All progress lines carry the [2D] prefix for the monitor.
"""
import json
import sys
import time
import logging
from datetime import date, datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger("wfo_sweep_2d")

import numpy as np
import pandas as pd
import psycopg2
from scipy import stats as sp_stats

RIG = Path("/Users/ahmed/gt/yats/mayor/rig")
sys.path.insert(0, str(RIG))
sys.path.insert(0, str(RIG / "pipelines"))

import research.features.ohlcv_features  # noqa: F401 — registers lookbacks
import research.features.cross_sectional_features  # noqa: F401
import research.features.fundamental_features  # noqa: F401
import research.features.regime_features_v1  # noqa: F401
import research.features.options_features_v1  # noqa: F401
from research.experiments.spec import CostConfig, ExperimentSpec, WFOConfig
from research.eval.wfo import run_wfo, compute_sweep_wfo_rank_decay
from research.eval.metrics import compute_sharpe
from research.features.columns import load_feature_columns
from research.training.ppo_trainer import train_ppo, rollout_ppo
from compute.stats.deflated_sharpe import compute_sweep_dsr, probabilistic_sharpe_ratio
from yats_pipelines.jobs.experiment_run import (
    _build_returns_df,
    _dataframe_to_env_rows,
    _merge_closes_into_features,
)
from yats_pipelines.resources.questdb import QuestDBResource

SYMBOLS = ["AAPL", "AMZN", "GOOGL", "JPM", "META", "MSFT", "NVDA", "QQQ", "SPY", "TSLA"]
FEATURE_SET = "sweep_v1"
START, END = "2024-07-01", "2026-07-02"
OUT_DIR = RIG / ".yats_data" / "wfo_sweeps" / "sweep_2d_full_v1"

GRID = []
for lr in (3e-4, 1e-4):
    for ent in (0.0, 0.01):
        for gamma in (0.99, 0.95):
            GRID.append({
                "learning_rate": lr, "ent_coef": ent, "gamma": gamma,
                "n_steps": 256, "batch_size": 64,
                "total_timesteps": 20000, "reward_version": "v1",
            })


def mark(msg: str) -> None:
    print(f"[2D] {datetime.now(timezone.utc).strftime('%H:%M:%S')} {msg}", flush=True)


def fetch_env_rows():
    """Fetch full_v1 env rows exactly as fetch_features does (with feature_set filter)."""
    obs_cols, regime_cols = load_feature_columns(FEATURE_SET, RIG / "configs")
    qdb = QuestDBResource()
    conn = psycopg2.connect(host=qdb.pg_host, port=qdb.pg_port, user=qdb.pg_user,
                            password=qdb.pg_password, database=qdb.pg_database)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM features WHERE symbol IN %s AND feature_set = %s "
        "AND timestamp >= %s AND timestamp <= %s ORDER BY timestamp, symbol",
        (tuple(SYMBOLS), FEATURE_SET, START, END),
    )
    cols = [d[0] for d in cur.description]
    feats = pd.DataFrame(cur.fetchall(), columns=cols)
    cur.close()
    cur = conn.cursor()
    cur.execute(
        "SELECT timestamp, symbol, close FROM canonical_equity_ohlcv "
        "WHERE symbol IN %s AND timestamp >= %s AND timestamp <= %s "
        "ORDER BY timestamp, symbol",
        (tuple(SYMBOLS), START, END),
    )
    closes = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    cur.close()
    conn.close()
    df, n_dropped = _merge_closes_into_features(feats, closes)
    rows = _dataframe_to_env_rows(df, SYMBOLS, obs_cols, regime_cols)
    mark(f"DATA rows={len(rows)} obs_cols={len(obs_cols)} regime={len(regime_cols)} dropped={n_dropped}")
    return rows, obs_cols, regime_cols


def make_spec(cfg_idx: int, params: dict) -> ExperimentSpec:
    return ExperimentSpec(
        experiment_name=f"wfo2d_cfg{cfg_idx}",
        symbols=tuple(SYMBOLS),
        start_date=date(2024, 7, 1),
        end_date=date(2026, 7, 2),
        interval="daily",
        feature_set=FEATURE_SET,
        policy="ppo",
        policy_params=params,
        cost_config=CostConfig(transaction_cost_bp=5.0, slippage_bp=0.0),
        seed=42,
        wfo_config=WFOConfig(train_window=250),
        notes="Stage 2d anchored 4-period WFO sweep on complete feature matrix",
    )


def main() -> int:
    t0 = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    data, obs_cols, regime_cols = fetch_env_rows()

    wfo_cfg = WFOConfig(train_window=250)  # anchored 1yr initial window, 4 folds ~63 OOS bars each
    mark(f"SWEEP start: {len(GRID)} configs x {wfo_cfg.n_periods} anchored folds, "
         f"timesteps={GRID[0]['total_timesteps']}")

    per_config = []
    for i, params in enumerate(GRID):
        cfg_t0 = time.time()
        spec = make_spec(i, params)
        ckpt_dir = OUT_DIR / f"cfg{i}"
        ckpt_dir.mkdir(exist_ok=True)

        def train_fn(train_data, _spec=spec, _dir=ckpt_dir):
            result = train_ppo(_spec, train_data, _dir,
                               observation_columns=obs_cols,
                               regime_feature_names=regime_cols or None)
            return result.checkpoint_path

        def eval_fn(test_data, checkpoint_path, _spec=spec):
            weights_list, _, _ = rollout_ppo(
                _spec, test_data, Path(checkpoint_path),
                observation_columns=obs_cols,
                regime_feature_names=regime_cols or None)
            dates = [row.get("timestamp", f"t{k}") for k, row in enumerate(test_data[1:])]
            weights_df = pd.DataFrame(weights_list, index=dates[:len(weights_list)],
                                      columns=sorted(SYMBOLS))
            returns_df = _build_returns_df(test_data, sorted(SYMBOLS))
            common = weights_df.index.intersection(returns_df.index)
            port = (weights_df.loc[common] * returns_df.loc[common]).sum(axis=1).dropna()
            sharpe = compute_sharpe(port) if len(port) > 2 else None
            return list(port.values), sharpe

        try:
            result = run_wfo(data, wfo_cfg, train_fn=train_fn, eval_fn=eval_fn,
                             feature_set=FEATURE_SET)
        except Exception as exc:
            mark(f"CONFIG {i} FAILED: {exc}")
            per_config.append({"config_index": i, "params": params, "failed": str(exc)})
            continue

        oos = np.asarray(result.concatenated_oos_returns, dtype=float)
        oos = oos[~np.isnan(oos)]
        sharpe = compute_sharpe(pd.Series(oos)) if len(oos) > 2 else 0.0
        skew = float(sp_stats.skew(oos)) if len(oos) >= 3 else 0.0
        kurt = float(sp_stats.kurtosis(oos)) + 3.0 if len(oos) >= 4 else 3.0  # standard
        psr = probabilistic_sharpe_ratio(
            observed_sharpe=sharpe, benchmark_sharpe=0.0,
            n_observations=len(oos), returns_skewness=skew, returns_kurtosis=kurt)
        per_config.append({
            "config_index": i, "params": params,
            "sharpe": sharpe, "skewness": skew, "kurtosis": kurt, "n_obs": len(oos),
            "per_fold_oos_sharpe": result.per_fold_oos_sharpe,
            "median_oos_sharpe": result.median_oos_sharpe,
            "psr_vs_zero": psr["dsr"],
        })
        mark(f"CONFIG {i} done: OOS sharpe={sharpe:.3f} folds={['%.2f' % (s or 0) for s in result.per_fold_oos_sharpe]} "
             f"psr0={psr['dsr']:.3f} ({(time.time() - cfg_t0) / 60:.1f}m)")

    ok = [c for c in per_config if "failed" not in c]
    if len(ok) >= 2:
        dsr_results = compute_sweep_dsr(ok)
        for cfg, dsr in zip(ok, dsr_results):
            cfg["dsr"] = dsr["dsr"]
            cfg["dsr_significant"] = dsr["is_significant"]
            cfg["benchmark_sharpe"] = dsr["benchmark_sharpe"]
        rank_decay = compute_sweep_wfo_rank_decay(
            [c["per_fold_oos_sharpe"] for c in ok])
    else:
        dsr_results, rank_decay = [], None

    summary = {
        "sweep": "2d_full_v1_anchored_wfo",
        "grid_size": len(GRID),
        "feature_set": FEATURE_SET,
        "symbols": SYMBOLS,
        "span": [START, END],
        "wfo": {"mode": "anchored", "n_periods": 4},
        "configs": per_config,
        "rank_decay": rank_decay,
        "elapsed_hours": (time.time() - t0) / 3600,
    }
    out = OUT_DIR / "sweep_summary.json"
    out.write_text(json.dumps(summary, indent=2, default=str))
    mark(f"SUMMARY written to {out}")

    best = max(ok, key=lambda c: c.get("dsr", 0), default=None)
    if best:
        mark(f"VERDICT best-config dsr={best.get('dsr', 0):.3f} "
             f"significant={best.get('dsr_significant')} sharpe={best['sharpe']:.3f} "
             f"SR0={best.get('benchmark_sharpe', 0):.3f} rank_decay={rank_decay}")
    n_sig = sum(1 for c in ok if c.get("dsr_significant"))
    mark(f"SWEEP COMPLETE OK ({(time.time() - t0) / 3600:.1f}h) "
         f"configs_ok={len(ok)}/{len(GRID)} dsr_significant={n_sig}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
