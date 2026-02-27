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
    import yaml

    from yats_pipelines.resources.questdb import QuestDBResource

    symbols = sorted(spec_data.get("symbols", []))
    start_date = spec_data.get("start_date", "")
    end_date = spec_data.get("end_date", "")
    feature_set_name = spec_data.get("feature_set", "core_v1")

    # Load feature set config
    configs_dir = Path(__file__).resolve().parents[3] / "configs"
    fs_path = configs_dir / "feature_sets" / f"{feature_set_name}.yml"
    if fs_path.exists():
        with open(fs_path) as f:
            fs_config = yaml.safe_load(f)
    else:
        fs_config = {"ohlcv": [], "cross_sectional": [], "fundamental": [], "regime": []}

    # Build column lists
    ohlcv_cols = fs_config.get("ohlcv", [])
    cs_cols = fs_config.get("cross_sectional", [])
    fund_cols = fs_config.get("fundamental", [])
    regime_cols = fs_config.get("regime", [])
    all_feature_cols = ohlcv_cols + cs_cols + fund_cols
    observation_columns = ["close"] + [c for c in all_feature_cols if c != "close"]

    # Query features from QuestDB
    qdb = QuestDBResource()
    try:
        conn = psycopg2.connect(
            host=qdb.pg_host, port=qdb.pg_port,
            user=qdb.pg_user, password=qdb.pg_password,
            database=qdb.pg_database,
        )
        conn.autocommit = True

        # Query OHLCV + feature data
        symbol_filter = ", ".join(f"'{s}'" for s in symbols)
        date_conditions = []
        if start_date:
            date_conditions.append(f"timestamp >= '{start_date}'")
        if end_date:
            date_conditions.append(f"timestamp <= '{end_date}'")
        where_clause = f"symbol IN ({symbol_filter})"
        if date_conditions:
            where_clause += " AND " + " AND ".join(date_conditions)

        cur = conn.cursor()
        cur.execute(
            f"SELECT * FROM features WHERE {where_clause} ORDER BY timestamp, symbol"
        )
        col_names = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        cur.close()
        conn.close()

        # Convert to list-of-dicts format expected by SignalWeightEnv
        df = pd.DataFrame(rows, columns=col_names)
        data_rows = _dataframe_to_env_rows(df, symbols, observation_columns, regime_cols)
        data_hash = hashlib.sha256(
            json.dumps(data_rows, default=str, sort_keys=True).encode()
        ).hexdigest()[:16]

    except Exception:
        context.log.warning("QuestDB not available, using empty data")
        data_rows = []
        data_hash = "no_data"

    context.log.info("Fetched %d rows for %d symbols", len(data_rows), len(symbols))

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

    if policy == "ppo":
        from research.training.ppo_trainer import train_ppo

        result = train_ppo(
            spec, data, output_dir,
            observation_columns=obs_cols,
            regime_feature_names=regime_cols or None,
        )
        context.log.info("PPO training complete: %d timesteps", result.total_timesteps)
        return {"trained": True, "checkpoint_path": str(result.checkpoint_path)}
    else:
        from research.training.sac_trainer import train_sac

        result = train_sac(
            spec, data, output_dir,
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

    # Generate weights based on policy type
    policy = spec_data.get("policy", "")
    symbols = sorted(spec_data.get("symbols", []))
    n_symbols = len(symbols)

    if train_result.get("trained") and train_result.get("checkpoint_path"):
        # Roll out trained RL policy
        checkpoint = Path(train_result["checkpoint_path"])
        obs_cols = features["observation_columns"]
        regime_cols = features["regime_feature_names"]

        if policy == "ppo":
            from research.training.ppo_trainer import rollout_ppo
            weights_list, _, _ = rollout_ppo(
                spec, data, checkpoint,
                observation_columns=obs_cols,
                regime_feature_names=regime_cols or None,
            )
        else:
            from research.training.sac_trainer import rollout_sac
            weights_list, _, _ = rollout_sac(
                spec, data, checkpoint,
                observation_columns=obs_cols,
                regime_feature_names=regime_cols or None,
            )

        # Build DataFrames
        dates = [row.get("timestamp", f"t{i}") for i, row in enumerate(data[1:])]  # Skip first row (reset)
        weights_df = pd.DataFrame(weights_list, index=dates[:len(weights_list)], columns=symbols)
    else:
        # Non-RL policy: equal weight
        dates = [row.get("timestamp", f"t{i}") for i, row in enumerate(data[1:])]
        n_rows = len(dates)
        equal_w = np.full((n_rows, n_symbols), 1.0 / n_symbols)
        weights_df = pd.DataFrame(equal_w, index=dates, columns=symbols)

    # Build returns DataFrame from data
    returns_df = _build_returns_df(data, symbols)

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
        regime_df = _build_regime_df(data, regime_cols)
        if regime_df is not None:
            regime_df = regime_df.loc[regime_df.index.intersection(common_idx)]

    # Run evaluation
    metrics = evaluate_to_json(
        spec, weights_df, returns_df,
        output_path=eval_dir / "metrics.json",
        regime_features=regime_df,
        data_hash=features.get("data_hash"),
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
        "win_rate": perf.get("win_rate"),
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


@job
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

    # Group by timestamp
    rows: list[dict[str, Any]] = []
    for ts, group in df.groupby("timestamp"):
        row: dict[str, Any] = {"timestamp": str(ts)}

        for col in observation_columns:
            if col in group.columns:
                per_sym = {}
                for _, r in group.iterrows():
                    sym = r.get("symbol", "")
                    if sym in symbols:
                        per_sym[sym] = float(r[col]) if pd.notna(r[col]) else 0.0
                row[col] = per_sym

        # Regime features are scalar (market-wide)
        for col in regime_cols:
            if col in group.columns:
                row[col] = float(group[col].iloc[0]) if pd.notna(group[col].iloc[0]) else 0.0

        rows.append(row)

    return rows


def _build_returns_df(data: list[dict[str, Any]], symbols: list[str]) -> pd.DataFrame:
    """Build returns DataFrame from env data rows (close-to-close returns)."""
    dates = []
    close_vals: dict[str, list[float]] = {s: [] for s in symbols}

    for row in data:
        dates.append(row.get("timestamp", ""))
        close = row.get("close", {})
        for s in symbols:
            if isinstance(close, dict):
                close_vals[s].append(float(close.get(s, 0.0)))
            else:
                close_vals[s].append(float(close))

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
