"""Experiment registry — filesystem layout + QuestDB experiment_index.

Creates and manages experiment directories under .yats_data/experiments/<ID>/
and writes/updates the experiment_index table in QuestDB.

PRD §8.2 (lines 625-667).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from research.experiments.spec import ExperimentSpec

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DATA_ROOT = Path(".yats_data")

_SUBDIRS = ("spec", "runs", "evaluation", "logs", "promotion")


# ---------------------------------------------------------------------------
# Filesystem registry
# ---------------------------------------------------------------------------


def _experiments_root(data_root: Path | None = None) -> Path:
    """Return the experiments root directory."""
    root = data_root or _DATA_ROOT
    return root / "experiments"


def create(
    spec: ExperimentSpec,
    *,
    data_root: Path | None = None,
    questdb_resource: Any | None = None,
) -> str:
    """Create an experiment: directory structure + QuestDB index row.

    1. Compute experiment_id from spec
    2. Create directory tree under .yats_data/experiments/<ID>/
    3. Write spec/experiment_spec.json
    4. Write QuestDB experiment_index row

    Returns the experiment_id.
    """
    experiment_id = spec.experiment_id
    exp_dir = _experiments_root(data_root) / experiment_id

    if exp_dir.exists():
        logger.info("Experiment %s already exists, skipping creation", experiment_id)
        return experiment_id

    # Create subdirectories
    for subdir in _SUBDIRS:
        (exp_dir / subdir).mkdir(parents=True, exist_ok=True)

    # Write canonical spec
    spec_path = exp_dir / "spec" / "experiment_spec.json"
    canonical = spec.to_canonical_dict()
    canonical["experiment_id"] = experiment_id
    spec_path.write_text(
        json.dumps(canonical, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    # Write QuestDB index row
    if questdb_resource is not None:
        write_index_row(spec, questdb_resource=questdb_resource)

    logger.info("Created experiment %s at %s", experiment_id, exp_dir)
    return experiment_id


def get(
    experiment_id: str,
    *,
    data_root: Path | None = None,
) -> dict[str, Any]:
    """Retrieve an experiment's spec and artifact paths.

    Returns a dict with:
      - spec: the parsed experiment_spec.json
      - artifacts: dict mapping subdir name → Path
      - path: the experiment root directory
    """
    exp_dir = _experiments_root(data_root) / experiment_id
    if not exp_dir.exists():
        raise FileNotFoundError(f"Experiment not found: {experiment_id}")

    spec_path = exp_dir / "spec" / "experiment_spec.json"
    spec_data = json.loads(spec_path.read_text(encoding="utf-8"))

    artifacts = {}
    for subdir in _SUBDIRS:
        subdir_path = exp_dir / subdir
        if subdir_path.exists():
            artifacts[subdir] = subdir_path

    return {
        "spec": spec_data,
        "artifacts": artifacts,
        "path": exp_dir,
    }


def exists(
    experiment_id: str,
    *,
    data_root: Path | None = None,
) -> bool:
    """Check if an experiment directory exists."""
    exp_dir = _experiments_root(data_root) / experiment_id
    return exp_dir.is_dir()


def get_artifacts_path(
    experiment_id: str,
    *,
    data_root: Path | None = None,
) -> Path:
    """Return the root path for an experiment's artifacts."""
    exp_dir = _experiments_root(data_root) / experiment_id
    if not exp_dir.exists():
        raise FileNotFoundError(f"Experiment not found: {experiment_id}")
    return exp_dir


def list_experiments(
    *,
    data_root: Path | None = None,
    universe: str | None = None,
    feature_set: str | None = None,
    policy_type: str | None = None,
    questdb_resource: Any | None = None,
) -> list[dict[str, Any]]:
    """List experiments, optionally filtered.

    If questdb_resource is provided, queries experiment_index for filtered
    results. Otherwise, scans the filesystem.
    """
    if questdb_resource is not None:
        return _list_from_questdb(
            questdb_resource,
            universe=universe,
            feature_set=feature_set,
            policy_type=policy_type,
        )

    return _list_from_filesystem(
        data_root=data_root,
        universe=universe,
        feature_set=feature_set,
        policy_type=policy_type,
    )


def _list_from_filesystem(
    *,
    data_root: Path | None = None,
    universe: str | None = None,
    feature_set: str | None = None,
    policy_type: str | None = None,
) -> list[dict[str, Any]]:
    """Scan filesystem for experiments, applying optional filters."""
    root = _experiments_root(data_root)
    if not root.exists():
        return []

    results = []
    for exp_dir in sorted(root.iterdir()):
        if not exp_dir.is_dir():
            continue
        spec_path = exp_dir / "spec" / "experiment_spec.json"
        if not spec_path.exists():
            continue

        spec_data = json.loads(spec_path.read_text(encoding="utf-8"))

        # Apply filters
        if universe is not None:
            spec_universe = _derive_universe(spec_data)
            if spec_universe != universe:
                continue
        if feature_set is not None and spec_data.get("feature_set") != feature_set:
            continue
        if policy_type is not None and spec_data.get("policy") != policy_type:
            continue

        results.append({
            "experiment_id": spec_data.get("experiment_id", exp_dir.name),
            "experiment_name": spec_data.get("experiment_name"),
            "feature_set": spec_data.get("feature_set"),
            "policy": spec_data.get("policy"),
            "universe": _derive_universe(spec_data),
            "path": exp_dir,
        })

    return results


# ---------------------------------------------------------------------------
# QuestDB experiment index
# ---------------------------------------------------------------------------


@dataclass
class QuestDBWriter:
    """Thin wrapper for writing to experiment_index via ILP."""

    ilp_host: str = "localhost"
    ilp_port: int = 9009
    pg_host: str = "localhost"
    pg_port: int = 8812
    pg_user: str = "admin"
    pg_password: str = "quest"
    pg_database: str = "qdb"

    @classmethod
    def from_resource(cls, resource: Any) -> QuestDBWriter:
        """Create from a QuestDBResource."""
        return cls(
            ilp_host=resource.ilp_host,
            ilp_port=resource.ilp_port,
            pg_host=resource.pg_host,
            pg_port=resource.pg_port,
            pg_user=resource.pg_user,
            pg_password=resource.pg_password,
            pg_database=resource.pg_database,
        )


def write_index_row(
    spec: ExperimentSpec,
    *,
    questdb_resource: Any | None = None,
    metrics: dict[str, Any] | None = None,
    dagster_run_id: str | None = None,
    qualification_status: str | None = None,
    promotion_tier: str | None = None,
    nan_inf_violations: int | None = None,
    canonical_hash: str | None = None,
    stale: bool | None = None,
    data_root: Path | None = None,
) -> None:
    """Write or update an experiment_index row in QuestDB via ILP.

    Called on creation (spec fields only) and after evaluation (with metrics).
    canonical_hash records the canonical data hash used during evaluation.
    stale indicates the experiment's canonical data has since changed.
    """
    from questdb.ingress import Protocol, Sender, TimestampNanos

    if questdb_resource is None:
        from pipelines.yats_pipelines.resources.questdb import QuestDBResource
        questdb_resource = QuestDBResource()

    writer = QuestDBWriter.from_resource(questdb_resource)
    experiment_id = spec.experiment_id
    now = datetime.now(timezone.utc)

    # Build symbol columns (SYMBOL type in QuestDB)
    symbols = {
        "experiment_id": experiment_id,
        "feature_set": spec.feature_set,
        "policy_type": spec.policy,
    }
    if spec.regime_feature_set is not None:
        symbols["regime_feature_set"] = spec.regime_feature_set
    if spec.regime_labeling is not None:
        symbols["regime_labeling"] = spec.regime_labeling
    if qualification_status is not None:
        symbols["qualification_status"] = qualification_status
    if promotion_tier is not None:
        symbols["promotion_tier"] = promotion_tier

    # Derive "reward_version" from policy_params if available
    reward_version = spec.policy_params.get("reward_version")
    if reward_version is not None:
        symbols["reward_version"] = str(reward_version)

    # Build regular columns
    columns: dict[str, Any] = {
        "universe": _derive_universe_from_spec(spec),
    }

    # Spec/metrics path pointers
    exp_root = _experiments_root(data_root) / experiment_id
    columns["spec_path"] = str(exp_root / "spec" / "experiment_spec.json")

    if metrics is not None:
        columns["metrics_path"] = str(exp_root / "evaluation" / "metrics.json")
        for key in (
            "sharpe", "calmar", "max_drawdown", "total_return",
            "annualized_return", "win_rate", "turnover_1d_mean",
        ):
            val = metrics.get(key)
            if val is not None:
                columns[key] = float(val)

    if nan_inf_violations is not None:
        columns["nan_inf_violations"] = nan_inf_violations

    if canonical_hash is not None:
        columns["canonical_hash"] = canonical_hash

    if stale is not None:
        columns["stale"] = stale

    if dagster_run_id is not None:
        columns["dagster_run_id"] = dagster_run_id

    with Sender(Protocol.Tcp, writer.ilp_host, writer.ilp_port) as sender:
        sender.row(
            "experiment_index",
            symbols=symbols,
            columns=columns,
            at=TimestampNanos(int(now.timestamp() * 1_000_000_000)),
        )
        sender.flush()

    logger.info("Wrote experiment_index row for %s", experiment_id)


def _list_from_questdb(
    questdb_resource: Any,
    *,
    universe: str | None = None,
    feature_set: str | None = None,
    policy_type: str | None = None,
) -> list[dict[str, Any]]:
    """Query experiment_index from QuestDB via PG wire."""
    import psycopg2

    writer = QuestDBWriter.from_resource(questdb_resource)

    conditions = []
    params: list[Any] = []
    if universe is not None:
        conditions.append("universe = %s")
        params.append(universe)
    if feature_set is not None:
        conditions.append("feature_set = %s")
        params.append(feature_set)
    if policy_type is not None:
        conditions.append("policy_type = %s")
        params.append(policy_type)

    query = "SELECT * FROM experiment_index"
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY timestamp DESC"

    conn = psycopg2.connect(
        host=writer.pg_host,
        port=writer.pg_port,
        user=writer.pg_user,
        password=writer.pg_password,
        database=writer.pg_database,
    )
    conn.autocommit = True

    try:
        cur = conn.cursor()
        cur.execute(query, params or None)
        col_names = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        cur.close()
        return [dict(zip(col_names, row)) for row in rows]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _derive_universe(spec_data: dict[str, Any]) -> str:
    """Derive a universe string from spec symbols list."""
    symbols = spec_data.get("symbols", [])
    if isinstance(symbols, (list, tuple)) and len(symbols) > 10:
        return f"custom_{len(symbols)}"
    if isinstance(symbols, (list, tuple)):
        return ",".join(sorted(symbols))
    return str(symbols)


def _derive_universe_from_spec(spec: ExperimentSpec) -> str:
    """Derive universe string from an ExperimentSpec."""
    if len(spec.symbols) > 10:
        return f"custom_{len(spec.symbols)}"
    return ",".join(sorted(spec.symbols))
