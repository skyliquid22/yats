"""SignalWeightEnv — RL environment for portfolio weight allocation.

Old Gym API (NOT Gymnasium): reset() -> obs, step(action) -> (obs, reward, done, info).
Receives pre-built feature rows from the runner — no direct QuestDB access.

PRD §7.1 and Appendix A.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from research.envs.execution_simulator import ExecutionSimulator
from research.experiments.spec import ExecutionSimConfig, RiskConfig
from research.risk.project_weights import project_weights


@dataclass
class SignalWeightEnvConfig:
    """Configuration for SignalWeightEnv."""

    symbols: list[str]
    observation_columns: list[str]
    action_clip: tuple[float, float] = (0.0, 1.0)
    transaction_cost_bp: float = 0.0
    slippage_bp: float = 0.0
    execution_sim: ExecutionSimConfig | None = None
    regime_feature_names: list[str] | None = None
    risk_config: RiskConfig = field(default_factory=RiskConfig)

    def __post_init__(self):
        if "close" not in self.observation_columns:
            raise ValueError("observation_columns must include 'close'")
        if not self.symbols:
            raise ValueError("symbols must be non-empty")


class SignalWeightEnv:
    """RL environment for signal-weighted portfolio allocation.

    Implements old Gym API:
        reset() -> observation (tuple)
        step(action) -> (observation, reward, done, info)

    The env receives pre-built feature data (list of dicts, one per timestep).
    Each dict maps column names to per-symbol values or scalar regime features.
    """

    def __init__(self, config: SignalWeightEnvConfig):
        self.config = config
        self._symbols = sorted(config.symbols)
        self._n_symbols = len(self._symbols)

        # Separate observation columns from regime columns
        regime_set = set(config.regime_feature_names or [])
        self._feature_cols = [
            c for c in config.observation_columns if c not in regime_set
        ]
        self._regime_cols = list(config.regime_feature_names or [])
        self._n_features = len(self._feature_cols)
        self._n_regime = len(self._regime_cols)

        # Observation length: (n_symbols * n_features) + n_regime + n_symbols
        self._obs_len = (
            self._n_symbols * self._n_features
            + self._n_regime
            + self._n_symbols
        )

        # Execution simulator
        self._exec_sim: ExecutionSimulator | None = None
        if config.execution_sim is not None and config.execution_sim.enabled:
            self._exec_sim = ExecutionSimulator(config.execution_sim)

        # State
        self._data: list[dict[str, Any]] = []
        self._step_idx: int = 0
        self._weights: np.ndarray = np.zeros(self._n_symbols)
        self._portfolio_value: float = 1.0
        self._done: bool = True

    def reset(self, data: list[dict[str, Any]] | None = None) -> tuple:
        """Reset the environment.

        Args:
            data: Pre-built feature rows. Each row is a dict mapping column
                names to either per-symbol dicts ({symbol: value}) or scalar
                values (for regime features). Must be provided on first reset
                or to load new data.

        Returns:
            Initial observation as a tuple of floats.
        """
        if data is not None:
            self._data = data
        if not self._data:
            raise ValueError("No data provided. Pass data to reset().")

        self._step_idx = 0
        self._weights = np.zeros(self._n_symbols)
        self._portfolio_value = 1.0
        self._done = False

        return self._build_observation()

    def step(self, action: np.ndarray | list | tuple, **kwargs: Any) -> tuple:
        """Execute one step.

        Args:
            action: Target portfolio weights (n_symbols,).
            **kwargs: Optional 'mode' for hierarchy info.

        Returns:
            (observation, reward, done, info) tuple.
        """
        if self._done:
            raise RuntimeError("Episode is done. Call reset() first.")

        action = np.asarray(action, dtype=np.float64)

        # 1. Clip action
        lo, hi = self.config.action_clip
        raw_action = action.copy()
        action = np.clip(action, lo, hi)

        # 2. Project weights through risk constraints
        target_weights = project_weights(
            action, self.config.risk_config, self._weights
        )

        # 3. Advance to next row
        self._step_idx += 1
        done = self._step_idx >= len(self._data) - 1
        self._done = done

        row = self._data[min(self._step_idx, len(self._data) - 1)]

        # 4. Compute portfolio value change from price changes
        prev_row = self._data[self._step_idx - 1]
        returns = self._compute_returns(prev_row, row)
        port_return = np.dot(self._weights, returns)
        new_value = self._portfolio_value * (1.0 + port_return)

        # 5. Transaction costs
        weight_change = np.abs(target_weights - self._weights)
        cost = weight_change.sum() * self.config.transaction_cost_bp / 10000.0
        new_value *= (1.0 - cost)

        # 6. Execution simulator
        realized_weights = target_weights.copy()
        exec_info: dict[str, Any] = {}

        if self._exec_sim is not None:
            row_ohlc = self._extract_ohlc(row)
            result = self._exec_sim.simulate(
                self._weights, target_weights, row_ohlc, new_value
            )
            realized_weights = result.realized_weights
            exec_info = {
                "execution_slippage_bps": result.execution_slippage_bps,
                "missed_fill_ratio": result.missed_fill_ratio,
                "unfilled_notional": result.unfilled_notional,
                "order_type_counts": result.order_type_counts,
            }
            # Apply execution slippage to value
            slippage_cost = result.execution_slippage_bps / 10000.0
            traded_notional = np.abs(target_weights - self._weights).sum()
            new_value *= (1.0 - slippage_cost * traded_notional)

        # 7. Reward: log return
        if new_value > 0 and self._portfolio_value > 0:
            reward = math.log(new_value / self._portfolio_value)
        else:
            reward = -10.0  # catastrophic loss sentinel

        # 8. Update state
        prev_value = self._portfolio_value
        self._portfolio_value = new_value
        self._weights = realized_weights.copy()

        # 9. Build observation
        if not self._done:
            obs = self._build_observation()
        else:
            obs = self._build_observation()  # terminal obs from last row

        # 10. Build info dict
        info = self._build_info(
            row=row,
            raw_action=raw_action,
            target_weights=target_weights,
            realized_weights=realized_weights,
            cost=cost * prev_value,
            reward=reward,
            exec_info=exec_info,
            **kwargs,
        )

        return obs, reward, self._done, info

    def _build_observation(self) -> tuple:
        """Construct flat observation vector per PRD A.3."""
        row = self._data[self._step_idx]
        parts: list[float] = []

        # 1. Per-symbol features (symbol-major order, symbols sorted)
        for sym in self._symbols:
            for col in self._feature_cols:
                val = row[col]
                if isinstance(val, dict):
                    parts.append(float(val.get(sym, 0.0)))
                else:
                    parts.append(float(val))

        # 2. Regime features (if enabled)
        for col in self._regime_cols:
            val = row.get(col, 0.0)
            if isinstance(val, dict):
                # Regime features are typically scalar, but handle dict case
                parts.append(float(list(val.values())[0]) if val else 0.0)
            else:
                parts.append(float(val))

        # 3. Previous weights (always)
        for w in self._weights:
            parts.append(float(w))

        return tuple(parts)

    def _compute_returns(
        self, prev_row: dict[str, Any], curr_row: dict[str, Any]
    ) -> np.ndarray:
        """Compute per-symbol returns from close prices."""
        returns = np.zeros(self._n_symbols)
        for i, sym in enumerate(self._symbols):
            prev_close = self._get_close(prev_row, sym)
            curr_close = self._get_close(curr_row, sym)
            if prev_close > 0:
                returns[i] = (curr_close - prev_close) / prev_close
        return returns

    def _get_close(self, row: dict[str, Any], symbol: str) -> float:
        """Extract close price for a symbol from a row."""
        close = row.get("close", {})
        if isinstance(close, dict):
            return float(close.get(symbol, 0.0))
        return float(close)

    def _extract_ohlc(self, row: dict[str, Any]) -> dict[str, dict[str, float]]:
        """Extract per-symbol OHLC data for execution simulator."""
        ohlc: dict[str, dict[str, float]] = {}
        for sym in self._symbols:
            ohlc[sym] = {
                "open": float(row.get("open", {}).get(sym, 0.0) if isinstance(row.get("open"), dict) else row.get("open", 0.0)),
                "high": float(row.get("high", {}).get(sym, 0.0) if isinstance(row.get("high"), dict) else row.get("high", 0.0)),
                "low": float(row.get("low", {}).get(sym, 0.0) if isinstance(row.get("low"), dict) else row.get("low", 0.0)),
                "close": float(row.get("close", {}).get(sym, 0.0) if isinstance(row.get("close"), dict) else row.get("close", 0.0)),
                "volume": float(row.get("volume", {}).get(sym, 0.0) if isinstance(row.get("volume"), dict) else row.get("volume", 0.0)),
            }
        return ohlc

    def _build_info(
        self,
        row: dict[str, Any],
        raw_action: np.ndarray,
        target_weights: np.ndarray,
        realized_weights: np.ndarray,
        cost: float,
        reward: float,
        exec_info: dict[str, Any],
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Build step info dict per PRD A.6."""
        # Base keys (always present)
        info: dict[str, Any] = {
            "timestamp": row.get("timestamp", ""),
            "price_close": {
                sym: self._get_close(row, sym) for sym in self._symbols
            },
            "raw_action": raw_action,
            "weights": target_weights,
            "weight_target": target_weights,
            "weight_realized": realized_weights,
            "portfolio_value": self._portfolio_value,
            "cost_paid": cost,
            "reward": reward,
        }

        # Execution sim keys
        if exec_info:
            info.update(exec_info)

        # Regime keys
        if self._regime_cols:
            regime_vals = tuple(
                float(row.get(c, 0.0)) if not isinstance(row.get(c), dict)
                else 0.0
                for c in self._regime_cols
            )
            info["regime_features"] = regime_vals
            info["regime_state"] = row.get("regime_state", "")

        # Hierarchy mode
        if "mode" in kwargs:
            info["mode"] = kwargs["mode"]

        return info

    @property
    def observation_length(self) -> int:
        """Total length of the observation vector."""
        return self._obs_len
