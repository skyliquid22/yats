"""Dagster experiment_run job — full experiment lifecycle.

Steps: load spec → fetch features from QuestDB → build env → train (if RL policy)
→ evaluate → write artifacts → update experiment_index in QuestDB.

PRD §15.3 (lines 1287-1291).
"""

import hashlib
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd
from dagster import Config, OpExecutionContext, In, Nothing, Out, job, op

logger = logging.getLogger(__name__)

_DATA_ROOT = Path(".yats_data")


class ExperimentRunConfig(Config):
    """Run config for experiment_run job."""

    experiment_id: str
    data_root: str = ".yats_data"
    allow_empty_data: bool = False


# ---------------------------------------------------------------------------
# Ops
# ---------------------------------------------------------------------------


@op(out=Out(dict))
def load_experiment_spec(context: OpExecutionContext, config: ExperimentRunConfig) -> dict:
    """Load experiment spec from registry."""
    from research.experiments.registry import get

    data_root = Path(config.data_root)
    exp = get(config.experiment_id, data_root=data_root)
    spec_data = exp["spec"]
    context.log.info(
        "Loaded spec for %s (policy=%s, features=%s)",
        config.experiment_id, spec_data.get("policy"), spec_data.get("feature_set"),
    )
    return spec_data


@op(ins={"spec_data": In(dict)}, out=Out(dict))
def fetch_features(context: OpExecutionContext, config: ExperimentRunConfig, spec_data: dict) -> dict:
    """Fetch feature data from QuestDB for the experiment's symbols and date range.

    Returns a dict with 'data' (list of row dicts), 'returns' (per-symbol returns),
    'observation_columns', 'regime_feature_names', and 'data_hash'.
    """
    import psycopg2

    from research.features.columns import load_feature_columns
    from yats_pipelines.resources.questdb import QuestDBResource

    symbols = sorted(spec_data.get("symbols", []))
    start_date = spec_data.get("start_date", "")
    end_date = spec_data.get("end_date", "")
    feature_set_name = spec_data.get("feature_set", "core_v1")

    # Load column lists via shared helper — same contract as ReplayMarketDataSource
    configs_dir = Path(__file__).resolve().parents[3] / "configs"
    observation_columns, regime_cols = load_feature_columns(feature_set_name, configs_dir)

    # Query features from QuestDB
    qdb = QuestDBResource()
    try:
        conn = psycopg2.connect(
            host=qdb.pg_host, port=qdb.pg_port,
            user=qdb.pg_user, password=qdb.pg_password,
            database=qdb.pg_database,
        )
        conn.autocommit = True

        # Build parameterized query — no f-string interpolation of user values
        q_parts = ["symbol IN %s"]
        q_params: list = [tuple(symbols)]
        if start_date:
            q_parts.append("timestamp >= %s")
            q_params.append(start_date)
        if end_date:
            q_parts.append("timestamp <= %s")
            q_params.append(end_date)
        where_clause = " AND ".join(q_parts)

        # feature_set MUST be filtered (features query only — the closes query
        # below reuses where_clause against canonical_equity_ohlcv, which has
        # no feature_set column): multiple sets coexist in the features table
        # (core_v1/options_v1/full_v1), and without the filter each
        # (symbol, timestamp) yields one row per set — two of them partially
        # null — corrupting the env rows.
        cur = conn.cursor()
        cur.execute(
            f"SELECT * FROM features WHERE {where_clause} AND feature_set = %s "
            "ORDER BY timestamp, symbol",
            [*q_params, feature_set_name],
        )
        col_names = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        cur.close()

        features_df = pd.DataFrame(rows, columns=col_names)

        # Deduplicate on (symbol, timestamp, feature_set, feature_set_version) to guard
        # against cross-run duplicate rows in the features table; keep last written.
        if not features_df.empty:
            dedup_cols = [c for c in ["symbol", "timestamp", "feature_set", "feature_set_version"]
                          if c in features_df.columns]
            if dedup_cols:
                features_df = features_df.drop_duplicates(subset=dedup_cols, keep="last")

        # Join canonical close prices — features table has no close column
        closes_cur = conn.cursor()
        closes_cur.execute(
            "SELECT timestamp, symbol, close FROM canonical_equity_ohlcv "
            f"WHERE {where_clause} ORDER BY timestamp, symbol",
            q_params,
        )
        closes_col_names = [desc[0] for desc in closes_cur.description]
        closes_rows = closes_cur.fetchall()
        closes_cur.close()
        conn.close()

        closes_df = pd.DataFrame(closes_rows, columns=closes_col_names)
        df, n_dropped = _merge_closes_into_features(features_df, closes_df)
        if n_dropped > 0:
            context.log.info("Dropped %d feature rows missing canonical close", n_dropped)

        # Convert to list-of-dicts format expected by SignalWeightEnv
        data_rows = _dataframe_to_env_rows(df, symbols, observation_columns, regime_cols)
        data_hash = hashlib.sha256(
            json.dumps(data_rows, default=str, sort_keys=True).encode()
        ).hexdigest()[:16]

    except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
        context.log.warning("QuestDB not available (%s) — using empty data", exc)
        data_rows = []
        data_hash = "no_data"

    context.log.info("Fetched %d rows for %d symbols", len(data_rows), len(symbols))

    if not data_rows and not config.allow_empty_data:
        raise RuntimeError(
            f"fetch_features returned 0 rows for experiment '{config.experiment_id}'. "
            "Check that QuestDB is populated and the date range / symbols are correct. "
            "Set allow_empty_data=true in run config to proceed with empty data."
        )

    return {
        "data": data_rows,
        "observation_columns": observation_columns,
        "regime_feature_names": regime_cols,
        "data_hash": data_hash,
    }


@op(ins={"spec_data": In(dict), "features": In(dict)}, out=Out(dict))
def train_policy(context: OpExecutionContext, config: ExperimentRunConfig, spec_data: dict, features: dict) -> dict:
    """Train the RL policy (if applicable). Returns training result info."""
    policy = spec_data.get("policy", "")
    data = features["data"]

    if policy not in ("ppo", "sac") and not policy.startswith("sac_"):
        context.log.info("Policy '%s' is not RL — skipping training", policy)
        return {"trained": False, "checkpoint_path": None}

    if not data:
        context.log.warning("No training data available — skipping training")
        return {"trained": False, "checkpoint_path": None}

    data_root = Path(config.data_root)
    output_dir = data_root / "experiments" / config.experiment_id / "runs"

    # Reconstruct ExperimentSpec for the trainer
    spec = _reconstruct_spec(spec_data)
    obs_cols = features["observation_columns"]
    regime_cols = features["regime_feature_names"]

    # Honor evaluation_split: train only on the training partition.
    train_data, _, _ = _apply_evaluation_split(data, spec)
    if len(train_data) < 2:
        context.log.warning("Training partition too small (%d rows) — skipping training", len(train_data))
        return {"trained": False, "checkpoint_path": None}
    context.log.info(
        "Evaluation split: training on %d/%d rows", len(train_data), len(data)
    )

    if policy == "ppo":
        from research.training.ppo_trainer import train_ppo

        result = train_ppo(
            spec, train_data, output_dir,
            observation_columns=obs_cols,
            regime_feature_names=regime_cols or None,
        )
        context.log.info("PPO training complete: %d timesteps", result.total_timesteps)
        return {"trained": True, "checkpoint_path": str(result.checkpoint_path)}
    else:
        from research.training.sac_trainer import train_sac

        result = train_sac(
            spec, train_data, output_dir,
            observation_columns=obs_cols,
            regime_feature_names=regime_cols or None,
        )
        context.log.info("SAC training complete: %d timesteps", result.total_timesteps)
        return {"trained": True, "checkpoint_path": str(result.checkpoint_path)}


@op(ins={"spec_data": In(dict), "features": In(dict), "train_result": In(dict)}, out=Out(dict))
def evaluate_experiment(
    context: OpExecutionContext,
    config: ExperimentRunConfig,
    spec_data: dict,
    features: dict,
    train_result: dict,
) -> dict:
    """Evaluate the experiment and write metrics.json."""
    from research.eval.evaluate import evaluate_to_json

    data = features["data"]
    if not data:
        context.log.warning("No data for evaluation — returning empty metrics")
        return {"metrics": {}}

    spec = _reconstruct_spec(spec_data)
    data_root = Path(config.data_root)
    eval_dir = data_root / "experiments" / config.experiment_id / "evaluation"
    eval_dir.mkdir(parents=True, exist_ok=True)

    # Honor evaluation_split: evaluate ONLY on the held-out test partition.
    _, eval_data, fn_split_meta = _apply_evaluation_split(data, spec)
    split_meta: dict[str, Any] = {}
    if spec.evaluation_split is not None:
        split_meta = {
            "evaluation_split": {
                "train_rows": fn_split_meta.get("train_rows", 0),
                "test_rows": len(eval_data),
                "train_start": data[0].get("timestamp"),
                "train_end": fn_split_meta.get("train_boundary_date"),
                "test_start": fn_split_meta.get("test_boundary_date"),
                "test_end": eval_data[-1].get("timestamp") if eval_data else None,
                "purged_label_bars": fn_split_meta.get("purged_label_bars", 0),
                "purged_buffer_bars": fn_split_meta.get("purged_buffer_bars", 0),
            }
        }
        context.log.info(
            "Evaluation split: evaluating on %d/%d rows "
            "(purged_label=%d purged_buffer=%d test window: %s to %s)",
            len(eval_data), len(data),
            fn_split_meta.get("purged_label_bars", 0),
            fn_split_meta.get("purged_buffer_bars", 0),
            split_meta["evaluation_split"]["test_start"],
            split_meta["evaluation_split"]["test_end"],
        )

    # Generate weights based on policy type
    policy = spec_data.get("policy", "")
    symbols = sorted(spec_data.get("symbols", []))
    n_symbols = len(symbols)

    if train_result.get("trained") and train_result.get("checkpoint_path"):
        # Roll out trained RL policy on the evaluation partition
        checkpoint = Path(train_result["checkpoint_path"])
        obs_cols = features["observation_columns"]
        regime_cols = features["regime_feature_names"]

        if policy == "ppo":
            from research.training.ppo_trainer import rollout_ppo
            weights_list, _, _ = rollout_ppo(
                spec, eval_data, checkpoint,
                observation_columns=obs_cols,
                regime_feature_names=regime_cols or None,
            )
        else:
            from research.training.sac_trainer import rollout_sac
            weights_list, _, _ = rollout_sac(
                spec, eval_data, checkpoint,
                observation_columns=obs_cols,
                regime_feature_names=regime_cols or None,
            )

        # Build DataFrames
        dates = [row.get("timestamp", f"t{i}") for i, row in enumerate(eval_data[1:])]  # Skip first row (reset)
        weights_df = pd.DataFrame(weights_list, index=dates[:len(weights_list)], columns=symbols)
    else:
        # Non-RL policy: run real rollout (raises for unsupported types)
        weights_list = _rollout_non_rl_policy(policy, eval_data, symbols, spec_data)
        dates = [row.get("timestamp", f"t{i}") for i, row in enumerate(eval_data[1:])]
        weights_df = pd.DataFrame(weights_list, index=dates[:len(weights_list)], columns=symbols)

    # Build returns DataFrame from eval partition
    returns_df = _build_returns_df(eval_data, symbols)

    # Align DataFrames
    common_idx = weights_df.index.intersection(returns_df.index)
    if len(common_idx) == 0:
        context.log.warning("No common dates between weights and returns")
        return {"metrics": {}}
    weights_df = weights_df.loc[common_idx]
    returns_df = returns_df.loc[common_idx]

    # Build regime features DataFrame if available
    regime_df = None
    regime_cols = features.get("regime_feature_names", [])
    if regime_cols and spec.regime_labeling:
        regime_df = _build_regime_df(eval_data, regime_cols)
        if regime_df is not None:
            regime_df = regime_df.loc[regime_df.index.intersection(common_idx)]

    # Run evaluation (pass purge/embargo metadata for embedding in metrics.json)
    metrics = evaluate_to_json(
        spec, weights_df, returns_df,
        output_path=eval_dir / "metrics.json",
        regime_features=regime_df,
        data_hash=features.get("data_hash"),
        split_metadata=split_meta if split_meta else None,
    )

    # Write run artifacts per PRD §8.2
    _write_run_artifacts(
        data_root=data_root,
        experiment_id=config.experiment_id,
        spec_data=spec_data,
        weights_df=weights_df,
        metrics=metrics,
        features=features,
        dagster_run_id=context.run_id if hasattr(context, "run_id") else None,
    )

    context.log.info("Evaluation complete: sharpe=%.4f", metrics.get("performance", {}).get("sharpe", 0.0))
    return {"metrics": metrics}


@op(ins={"spec_data": In(dict), "eval_result": In(dict)})
def update_experiment_index(
    context: OpExecutionContext,
    config: ExperimentRunConfig,
    spec_data: dict,
    eval_result: dict,
) -> None:
    """Update experiment_index row in QuestDB with evaluation metrics."""
    from research.experiments.registry import write_index_row

    spec = _reconstruct_spec(spec_data)
    metrics_raw = eval_result.get("metrics", {})

    # Extract flat metrics for index
    perf = metrics_raw.get("performance", {})
    trading = metrics_raw.get("trading", {})
    flat_metrics = {
        "sharpe": perf.get("sharpe"),
        "calmar": perf.get("calmar"),
        "max_drawdown": perf.get("max_drawdown"),
        "total_return": perf.get("total_return"),
        "annualized_return": perf.get("annualized_return"),
        "win_rate": trading.get("win_rate"),
        "turnover_1d_mean": trading.get("turnover_1d_mean"),
    }
    flat_metrics = {k: v for k, v in flat_metrics.items() if v is not None}

    try:
        write_index_row(
            spec,
            metrics=flat_metrics if flat_metrics else None,
            dagster_run_id=context.run_id if hasattr(context, "run_id") else None,
        )
        context.log.info("Updated experiment_index for %s", config.experiment_id)
    except Exception as e:
        context.log.warning("Failed to update experiment_index: %s", e)


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------


@job(tags={"yats/concurrency_pool": "experiment", "dagster/priority": "40"})
def experiment_run():
    """Run a single experiment: load → fetch → train → evaluate → index."""
    spec_data = load_experiment_spec()
    features = fetch_features(spec_data)
    train_result = train_policy(spec_data, features)
    eval_result = evaluate_experiment(spec_data, features, train_result)
    update_experiment_index(spec_data, eval_result)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _apply_evaluation_split(
    data: list[dict[str, Any]],
    spec: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    """Split data into (train_data, test_data, split_meta) per spec.evaluation_split.

    Returns (full_data, full_data, {}) when evaluation_split is None.

    When a split is configured, applies a forward-only purge with two before-side
    components (AFML Ch. 7):

    1. Compute the raw split boundary (split_idx = first bar of the test set).
    2. LABEL PURGE: drop the last label_horizon bars from train. These bars'
       reward windows reach forward into the test period — deterministic leakage.
    3. PURGE BUFFER: drop an additional purge_buffer bars from train. These bars'
       feature windows overlap with early test bars due to serial correlation in
       the feature computation — distributional leakage.

    Both components are applied exclusively on the before-side (train_end):
        train_end = split_idx - label_horizon - purge_buffer_bars

    Classic after-embargo is intentionally omitted: the split is forward-only
    (test is the terminal edge), so there is no post-test training to embargo —
    see the WFO harness for why splits are always forward-only.

    When split.purge_buffer is None, purge_buffer_bars is auto-computed as the
    maximum feature lookback for the spec's feature set (queried from the feature
    registry). An explicit integer overrides the default.

    Returns split_meta with purged_label_bars, purged_buffer_bars, and boundary dates.
    """
    split = spec.evaluation_split
    if split is None:
        return data, data, {}

    n = len(data)
    if split.test_window_months is not None:
        # Approximate 21 trading days per month; clamp so each partition has ≥2 bars.
        test_size = min(split.test_window_months * 21, n - 2)
        split_idx = max(2, n - test_size)
    else:
        split_idx = max(2, int(n * split.train_ratio))

    label_horizon = split.label_horizon

    if split.purge_buffer is None:
        from research.features.feature_registry import registry
        purge_buffer_bars = registry.max_lookback(spec.feature_set)
    else:
        purge_buffer_bars = split.purge_buffer

    # Both purge components are on the before-side only.
    # test_start stays at split_idx — no after-embargo.
    train_end = split_idx - label_horizon - purge_buffer_bars
    test_start = split_idx

    # Clamp so each partition has at least 1 bar; fall back to clean split if degenerate
    train_end = max(1, train_end)
    test_start = min(n - 1, test_start)
    if train_end >= test_start:
        train_end = split_idx
        test_start = split_idx

    train_data = data[:train_end]
    test_data = data[test_start:]

    split_meta = {
        "purged_label_bars": label_horizon,
        "purged_buffer_bars": purge_buffer_bars,
        "train_rows": train_end,
        "test_rows": n - test_start,
        "train_boundary_date": data[train_end - 1].get("timestamp") if train_end > 0 else None,
        "test_boundary_date": data[test_start].get("timestamp") if test_start < n else None,
    }

    return train_data, test_data, split_meta


def _rollout_non_rl_policy(
    policy_name: str,
    data: list[dict[str, Any]],
    symbols: list[str],
    spec_data: dict,
) -> list[np.ndarray]:
    """Run a non-RL policy rollout over data rows, returning a list of weight vectors.

    Supported policies: 'equal_weight', 'sma'.
    Raises ValueError for any other policy type — callers must not silently substitute.
    """
    n_symbols = len(symbols)
    if policy_name == "equal_weight":
        from research.policies.equal_weight_policy import EqualWeightPolicy
        policy = EqualWeightPolicy(n_symbols)
    elif policy_name == "sma":
        from research.policies.sma_weight_policy import SMAWeightPolicy
        params = spec_data.get("policy_params", {})
        policy = SMAWeightPolicy(
            n_symbols,
            short_window=int(params.get("short_window", 5)),
            long_window=int(params.get("long_window", 20)),
        )
    else:
        raise ValueError(
            f"Unsupported policy type '{policy_name}'. "
            "Supported non-RL policies: 'equal_weight', 'sma'. "
            "RL policies (ppo, sac) require a trained checkpoint."
        )

    if hasattr(policy, "reset"):
        policy.reset()

    # Iterate data[:-1]: weight at bar t uses close[t] and earns r(t→t+1).
    # Iterating data[1:] would use close[t+1] to earn r(t→t+1) — one-bar lookahead.
    weights_list: list[np.ndarray] = []
    for row in data[:-1]:
        close = row.get("close", {})
        if isinstance(close, dict):
            prices = np.array([close.get(s, 0.0) for s in symbols], dtype=np.float64)
        else:
            prices = np.full(n_symbols, float(close), dtype=np.float64)
        w = policy.act(prices, {"close_prices": prices})
        weights_list.append(w)

    return weights_list


def _write_run_artifacts(
    data_root: Path,
    experiment_id: str,
    spec_data: dict,
    weights_df: "pd.DataFrame",
    metrics: dict,
    features: dict,
    dagster_run_id: Optional[str],
) -> None:
    """Write runs/rollout.json and logs/run_summary.json per PRD §8.2."""
    runs_dir = data_root / "experiments" / experiment_id / "runs"
    logs_dir = data_root / "experiments" / experiment_id / "logs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    rollout_path = runs_dir / "rollout.json"
    summary_path = logs_dir / "run_summary.json"

    rollout = {
        "metadata": {
            "experiment_id": experiment_id,
            "policy": spec_data.get("policy"),
            "symbols": sorted(spec_data.get("symbols", [])),
            "data_hash": features.get("data_hash"),
        },
        "inputs_used": {
            "data_hash": features.get("data_hash"),
            "observation_columns": features.get("observation_columns", []),
            "feature_set": spec_data.get("feature_set"),
        },
        "series": {
            "dates": list(weights_df.index),
            "weights": {col: list(weights_df[col]) for col in weights_df.columns},
        },
        "performance": metrics.get("performance", {}),
        "trading": metrics.get("trading", {}),
        "safety": metrics.get("safety", {}),
    }

    eval_dir = data_root / "experiments" / experiment_id / "evaluation"
    run_summary = {
        "experiment_id": experiment_id,
        "dagster_run_id": dagster_run_id,
        "artifacts": {
            "rollout_json": str(rollout_path),
            "metrics_json": str(eval_dir / "metrics.json"),
        },
        "policy": spec_data.get("policy"),
        "symbols": sorted(spec_data.get("symbols", [])),
    }

    rollout_path.write_text(json.dumps(rollout, default=str, indent=2))
    summary_path.write_text(json.dumps(run_summary, default=str, indent=2))


def _reconstruct_spec(spec_data: dict) -> Any:
    """Reconstruct an ExperimentSpec from a spec dict."""
    from research.experiments.spec import (
        CostConfig,
        EvaluationSplitConfig,
        ExperimentSpec,
        ExecutionSimConfig,
        RiskConfig,
    )

    # Handle sub-configs
    cost_raw = spec_data.get("cost_config", {})
    if isinstance(cost_raw, dict):
        cost_config = CostConfig(**cost_raw)
    else:
        cost_config = cost_raw

    risk_raw = spec_data.get("risk_config")
    if isinstance(risk_raw, dict):
        risk_config = RiskConfig(**risk_raw)
    elif risk_raw is None:
        risk_config = RiskConfig()
    else:
        risk_config = risk_raw

    eval_split_raw = spec_data.get("evaluation_split")
    eval_split = None
    if isinstance(eval_split_raw, dict):
        eval_split = EvaluationSplitConfig(**eval_split_raw)

    exec_sim_raw = spec_data.get("execution_sim")
    exec_sim = None
    if isinstance(exec_sim_raw, dict):
        exec_sim = ExecutionSimConfig(**exec_sim_raw)

    # Convert date strings
    start_date = spec_data["start_date"]
    end_date = spec_data["end_date"]
    if isinstance(start_date, str):
        start_date = date.fromisoformat(start_date)
    if isinstance(end_date, str):
        end_date = date.fromisoformat(end_date)

    symbols = spec_data.get("symbols", [])
    if isinstance(symbols, list):
        symbols = tuple(symbols)

    return ExperimentSpec(
        experiment_name=spec_data["experiment_name"],
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        interval=spec_data.get("interval", "daily"),
        feature_set=spec_data["feature_set"],
        policy=spec_data["policy"],
        policy_params=spec_data.get("policy_params", {}),
        cost_config=cost_config,
        seed=spec_data.get("seed", 42),
        evaluation_split=eval_split,
        risk_config=risk_config,
        execution_sim=exec_sim,
        notes=spec_data.get("notes"),
        regime_feature_set=spec_data.get("regime_feature_set"),
        regime_labeling=spec_data.get("regime_labeling"),
        hierarchy_enabled=spec_data.get("hierarchy_enabled", False),
        controller_config=spec_data.get("controller_config"),
        allocator_by_mode=spec_data.get("allocator_by_mode"),
        regime_thresholds_hash=spec_data.get("regime_thresholds_hash", ""),
        regime_detector_version=spec_data.get("regime_detector_version", ""),
        regime_universe=tuple(spec_data.get("regime_universe", ())),
        feature_set_yaml_hash=spec_data.get("feature_set_yaml_hash", ""),
        risk_overrides=spec_data.get("risk_overrides"),
        _base_spec_hash=spec_data.get("_base_spec_hash"),
    )


def _dataframe_to_env_rows(
    df: pd.DataFrame,
    symbols: list[str],
    observation_columns: list[str],
    regime_cols: list[str],
) -> list[dict[str, Any]]:
    """Convert a QuestDB features DataFrame to list-of-dicts for SignalWeightEnv."""
    if df.empty:
        return []

    # Group by timestamp — skip rows with NaN feature values rather than zero-filling,
    # since zero is not a neutral value for bounded features like dist_20d_high.
    rows: list[dict[str, Any]] = []
    for ts, group in df.groupby("timestamp"):
        row: dict[str, Any] = {"timestamp": str(ts)}

        for col in observation_columns:
            if col in group.columns:
                per_sym = {}
                for _, r in group.iterrows():
                    sym = r.get("symbol", "")
                    if sym in symbols:
                        val = r[col]
                        if pd.notna(val):
                            per_sym[sym] = float(val)
                row[col] = per_sym

        # Regime features are scalar (market-wide); skip NaN rather than coercing to 0.0
        for col in regime_cols:
            if col in group.columns:
                val = group[col].iloc[0]
                if pd.notna(val):
                    row[col] = float(val)

        rows.append(row)

    return rows


def _merge_closes_into_features(
    features_df: pd.DataFrame,
    closes_df: pd.DataFrame,
) -> tuple[pd.DataFrame, int]:
    """Inner-join canonical close prices onto features rows by (timestamp, symbol).

    Rows in features_df without a matching close price are dropped (logged by caller).
    Returns (merged_df, n_dropped).
    """
    if features_df.empty:
        return features_df, 0

    before = len(features_df)
    merged = features_df.merge(
        closes_df[["timestamp", "symbol", "close"]],
        on=["timestamp", "symbol"],
        how="inner",
    )
    return merged, before - len(merged)


def _build_returns_df(data: list[dict[str, Any]], symbols: list[str]) -> pd.DataFrame:
    """Build returns DataFrame from env data rows (close-to-close returns).

    Missing close prices are stored as NaN (not 0.0) to avoid fabricating -100% returns.
    """
    dates = []
    close_vals: dict[str, list[float]] = {s: [] for s in symbols}

    for row in data:
        dates.append(row.get("timestamp", ""))
        close = row.get("close", {})
        for s in symbols:
            if isinstance(close, dict):
                raw = close.get(s)
                close_vals[s].append(float(raw) if raw is not None else float("nan"))
            else:
                close_vals[s].append(float(close) if close is not None else float("nan"))

    close_df = pd.DataFrame(close_vals, index=dates)
    returns_df = close_df.pct_change().iloc[1:]  # Drop first NaN row
    return returns_df


def _build_regime_df(
    data: list[dict[str, Any]], regime_cols: list[str],
) -> Optional[pd.DataFrame]:
    """Build regime features DataFrame from data rows."""
    if not regime_cols:
        return None

    dates = []
    regime_vals: dict[str, list[float]] = {c: [] for c in regime_cols}

    for row in data:
        dates.append(row.get("timestamp", ""))
        for c in regime_cols:
            val = row.get(c, 0.0)
            regime_vals[c].append(float(val) if not isinstance(val, dict) else 0.0)

    return pd.DataFrame(regime_vals, index=dates)
