"""ShadowEngine — replay market data through a policy for shadow execution.

Implements PRD Appendix F.2 with execution_mode=none (direct rebalance).
For each snapshot in the replay window, builds an observation, runs the
policy, projects weights through risk constraints, applies transaction
costs, and tracks portfolio value.

Artifacts are written under .yats_data/shadow/<experiment_id>/<run_id>/.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import numpy as np

from research.experiments.spec import ExperimentSpec
from research.risk.project_weights import project_weights
from research.shadow.data_source import Snapshot
from research.shadow.logging import StepLogger, build_step_entry
from research.shadow.portfolio import ShadowPortfolio
from research.shadow.questdb_writer import ExecutionTableWriter

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Policy protocol
# ---------------------------------------------------------------------------

class PolicyProtocol(Protocol):
    """Minimal interface for policies used by ShadowEngine."""

    def act(
        self, obs: np.ndarray, context: dict[str, Any] | None = None,
    ) -> np.ndarray: ...


# ---------------------------------------------------------------------------
# ShadowEngine
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ShadowRunConfig:
    """Configuration for a shadow run."""

    experiment_id: str
    run_id: str
    output_dir: Path
    initial_value: float = 1_000_000.0


class ShadowEngine:
    """Replay snapshots through a policy with direct rebalance.

    PRD Appendix F.2, execution_mode=none.

    For each snapshot:
    1. Build observation (same format as SignalWeightEnv)
    2. Policy produces target weights
    3. project_weights with risk_config
    4. cost = sum(|projected - previous|) * transaction_cost_bp / 10000
    5. portfolio_value *= (1 + weighted_return - cost)
    6. Log step to steps.jsonl
    7. Save state to state.json
    """

    def __init__(
        self,
        spec: ExperimentSpec,
        policy: PolicyProtocol,
        snapshots: list[Snapshot],
        config: ShadowRunConfig,
        *,
        questdb_writer: ExecutionTableWriter | None = None,
    ) -> None:
        self._spec = spec
        self._policy = policy
        self._snapshots = snapshots
        self._config = config
        self._questdb_writer = questdb_writer

        self._symbols = spec.symbols
        self._n_symbols = len(self._symbols)

        # Artifact paths
        self._logs_dir = config.output_dir / "logs"
        self._logs_dir.mkdir(parents=True, exist_ok=True)
        self._step_logger = StepLogger(self._logs_dir / "steps.jsonl")
        self._state_path = config.output_dir / "state.json"
        self._summary_path = config.output_dir / "summary.json"

        # Portfolio
        self._portfolio = ShadowPortfolio(
            symbols=self._symbols,
            portfolio_value=config.initial_value,
        )

        # Replay state
        self._step_index = 0
        self._dagster_run_id: str | None = None

        # Track daily returns for Sharpe computation
        self._daily_returns: list[float] = []

    @property
    def portfolio(self) -> ShadowPortfolio:
        return self._portfolio

    @property
    def step_index(self) -> int:
        return self._step_index

    def resume(self) -> None:
        """Resume from state.json if it exists."""
        if not self._state_path.exists():
            logger.info("No state.json found — starting fresh")
            return

        state = json.loads(self._state_path.read_text(encoding="utf-8"))
        self._step_index = state["step_index"]
        self._dagster_run_id = state.get("dagster_run_id")
        self._portfolio = ShadowPortfolio.from_state_dict(
            self._symbols, state,
        )
        logger.info(
            "Resumed from step %d (portfolio_value=%.2f)",
            self._step_index, self._portfolio.portfolio_value,
        )

    def run(self, dagster_run_id: str | None = None) -> dict[str, Any]:
        """Execute the full shadow replay.

        Returns a summary dict.
        """
        self._dagster_run_id = dagster_run_id
        start_step = self._step_index
        total_snapshots = len(self._snapshots)

        if start_step >= total_snapshots:
            logger.warning("All snapshots already processed (step %d/%d)", start_step, total_snapshots)
            return self._build_summary()

        logger.info(
            "Starting shadow run: %d snapshots (from step %d)",
            total_snapshots, start_step,
        )

        # We need at least 2 snapshots to compute returns
        for i in range(max(start_step, 1), total_snapshots):
            self._step_index = i
            prev_snap = self._snapshots[i - 1]
            curr_snap = self._snapshots[i]

            self._execute_step(i, prev_snap, curr_snap)
            self._save_state()

        summary = self._build_summary()
        self._save_summary(summary)

        # Write execution_metrics summary to QuestDB
        if self._questdb_writer is not None:
            self._questdb_writer.write_metrics(
                sharpe=summary.get("sharpe", 0.0),
                max_drawdown=summary.get("max_drawdown", 0.0),
                total_return=summary.get("total_return", 0.0),
            )

        logger.info(
            "Shadow run complete: %d steps, final_value=%.2f",
            total_snapshots - max(start_step, 1), self._portfolio.portfolio_value,
        )
        return summary

    def _execute_step(
        self,
        step_index: int,
        prev_snap: Snapshot,
        curr_snap: Snapshot,
    ) -> None:
        """Execute a single shadow step (direct rebalance)."""
        # 1. Build observation from current snapshot
        obs = self._build_observation(curr_snap)

        # 2. Policy produces target weights
        context = self._build_policy_context(curr_snap)
        raw_weights = self._policy.act(obs, context)

        # 3. Project weights through risk constraints
        projected = project_weights(
            raw_weights,
            self._spec.risk_config,
            self._portfolio.weights,
        )

        # 4. Compute per-symbol returns (close-to-close)
        returns = self._compute_returns(prev_snap, curr_snap)
        weighted_return = float(np.dot(self._portfolio.weights, returns))

        # 5. Transaction cost
        turnover = float(np.abs(projected - self._portfolio.weights).sum())
        cost = turnover * self._spec.cost_config.transaction_cost_bp / 10_000

        # 6. Update portfolio
        prev_weights = self._portfolio.weights.tolist()
        self._portfolio.apply_step(projected, weighted_return, cost)
        self._daily_returns.append(weighted_return - cost)

        # 7. Log step to JSONL
        entry = build_step_entry(
            step_index=step_index,
            timestamp=curr_snap.as_of.isoformat(),
            weights=projected.tolist(),
            previous_weights=prev_weights,
            symbols=self._symbols,
            returns_per_symbol=returns.tolist(),
            weighted_return=weighted_return,
            cost=cost,
            portfolio_value=self._portfolio.portfolio_value,
            peak_value=self._portfolio.peak_value,
            drawdown=self._portfolio.drawdown,
        )
        self._step_logger.log_step(entry)

        # 8. Write to QuestDB execution_log (per-symbol rows)
        if self._questdb_writer is not None:
            # In direct rebalance mode: fill at close, no slippage, no rejects
            close_prices = [
                curr_snap.panel.get(sym, {}).get("close", 0.0)
                for sym in self._symbols
            ]
            per_symbol_fee = cost / self._n_symbols if self._n_symbols > 0 else 0.0
            self._questdb_writer.write_step(
                timestamp=curr_snap.as_of,
                step=step_index,
                symbols=self._symbols,
                target_weights=raw_weights.tolist(),
                realized_weights=projected.tolist(),
                fill_prices=close_prices,
                slippage_bps=[0.0] * self._n_symbols,
                fees_per_symbol=[per_symbol_fee] * self._n_symbols,
                rejected=[False] * self._n_symbols,
                reject_reasons=[""] * self._n_symbols,
                portfolio_value=self._portfolio.portfolio_value,
                cash=self._portfolio.cash,
                regime_bucket=self._get_regime_bucket(curr_snap),
            )

    def _build_observation(self, snap: Snapshot) -> np.ndarray:
        """Build flat observation vector from snapshot (same format as SignalWeightEnv).

        Layout: [per-symbol features (symbol-major)] + [regime features] + [prev weights]
        """
        parts: list[float] = []

        # Per-symbol observation features (symbol-major, sorted symbols)
        for sym in self._symbols:
            sym_data = snap.panel.get(sym, {})
            for col in snap.observation_columns:
                parts.append(float(sym_data.get(col, 0.0)))

        # Regime features
        for val in snap.regime_features:
            parts.append(float(val))

        # Previous weights
        for w in self._portfolio.weights:
            parts.append(float(w))

        return np.array(parts, dtype=np.float64)

    def _build_policy_context(self, snap: Snapshot) -> dict[str, Any]:
        """Build context dict for policy.act()."""
        context: dict[str, Any] = {}

        # Close prices for policies that need them (e.g. SMA)
        close_prices = []
        for sym in self._symbols:
            close_prices.append(snap.panel.get(sym, {}).get("close", 0.0))
        context["close_prices"] = close_prices

        # Regime features for hierarchical policies
        if snap.regime_feature_names:
            regime_dict = {}
            for name, val in zip(snap.regime_feature_names, snap.regime_features):
                regime_dict[name] = val
            context["regime_features"] = regime_dict

        return context

    def _get_regime_bucket(self, snap: Snapshot) -> str:
        """Extract regime bucket label from snapshot regime features."""
        if snap.regime_feature_names and snap.regime_features:
            for name, val in zip(snap.regime_feature_names, snap.regime_features):
                if "regime" in name.lower() or "bucket" in name.lower():
                    return str(val)
        return "unknown"

    def _compute_returns(self, prev: Snapshot, curr: Snapshot) -> np.ndarray:
        """Compute per-symbol close-to-close returns."""
        returns = np.zeros(self._n_symbols)
        for i, sym in enumerate(self._symbols):
            prev_close = prev.panel.get(sym, {}).get("close", 0.0)
            curr_close = curr.panel.get(sym, {}).get("close", 0.0)
            if prev_close > 0:
                returns[i] = (curr_close - prev_close) / prev_close
        return returns

    def _save_state(self) -> None:
        """Persist resume state to state.json."""
        state = self._portfolio.to_state_dict()
        state["step_index"] = self._step_index
        state["last_timestamp"] = self._snapshots[self._step_index].as_of.isoformat()
        state["orders_pending"] = []
        state["dagster_run_id"] = self._dagster_run_id
        self._state_path.write_text(
            json.dumps(state, indent=2) + "\n", encoding="utf-8",
        )

    def _compute_sharpe(self, annualization: float = 252.0) -> float:
        """Compute annualized Sharpe ratio from daily returns."""
        if len(self._daily_returns) < 2:
            return 0.0
        arr = np.array(self._daily_returns)
        mean_ret = float(arr.mean())
        std_ret = float(arr.std(ddof=1))
        if std_ret == 0:
            return 0.0
        return mean_ret / std_ret * np.sqrt(annualization)

    def _build_summary(self) -> dict[str, Any]:
        """Build run summary dict."""
        total_steps = len(self._snapshots)
        return {
            "experiment_id": self._config.experiment_id,
            "run_id": self._config.run_id,
            "execution_mode": "none",
            "total_snapshots": total_steps,
            "steps_executed": max(0, self._step_index - 0),
            "initial_value": self._config.initial_value,
            "final_value": self._portfolio.portfolio_value,
            "total_return": (
                (self._portfolio.portfolio_value / self._config.initial_value) - 1.0
                if self._config.initial_value > 0 else 0.0
            ),
            "sharpe": self._compute_sharpe(),
            "peak_value": self._portfolio.peak_value,
            "max_drawdown": self._portfolio.drawdown,
            "final_weights": self._portfolio.weights.tolist(),
            "dagster_run_id": self._dagster_run_id,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

    def _save_summary(self, summary: dict[str, Any]) -> None:
        """Write summary.json."""
        self._summary_path.write_text(
            json.dumps(summary, indent=2) + "\n", encoding="utf-8",
        )


# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------


def load_policy(spec: ExperimentSpec) -> PolicyProtocol:
    """Load a policy instance from an ExperimentSpec.

    Supports: equal_weight, sma, ppo, sac (and sac_* variants).
    For RL policies (ppo, sac), loads from the experiment's checkpoint.
    """
    n_symbols = len(spec.symbols)
    policy_name = spec.policy

    if policy_name == "equal_weight":
        from research.policies.equal_weight_policy import EqualWeightPolicy
        return EqualWeightPolicy(n_symbols)

    if policy_name == "sma":
        from research.policies.sma_weight_policy import SMAWeightPolicy
        params = dict(spec.policy_params)
        return SMAWeightPolicy(
            n_symbols,
            short_window=params.get("short_window", 5),
            long_window=params.get("long_window", 20),
        )

    if policy_name in ("ppo", "sac") or policy_name.startswith("sac_"):
        raise NotImplementedError(
            f"RL policy '{policy_name}' loading for shadow replay "
            "requires a trained checkpoint — not yet supported in shadow engine."
        )

    raise ValueError(f"Unknown policy: {policy_name}")


def create_shadow_run(
    spec: ExperimentSpec,
    snapshots: list[Snapshot],
    *,
    data_root: Path | None = None,
    run_id: str | None = None,
    initial_value: float = 1_000_000.0,
) -> ShadowEngine:
    """Create a ShadowEngine with standard artifact layout.

    Artifacts under: <data_root>/shadow/<experiment_id>/<run_id>/
    """
    if data_root is None:
        data_root = Path(".yats_data")
    if run_id is None:
        run_id = uuid.uuid4().hex[:12]

    output_dir = data_root / "shadow" / spec.experiment_id / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    config = ShadowRunConfig(
        experiment_id=spec.experiment_id,
        run_id=run_id,
        output_dir=output_dir,
        initial_value=initial_value,
    )

    policy = load_policy(spec)

    return ShadowEngine(
        spec=spec,
        policy=policy,
        snapshots=snapshots,
        config=config,
    )
