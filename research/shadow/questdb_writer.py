"""QuestDB ILP writer for unified execution tables.

Writes per-step rows to execution_log and per-run summary rows to
execution_metrics via QuestDB's ILP (InfluxDB Line Protocol) ingress.

PRD §9.3 (lines 723-748) / P3.4.
"""

from __future__ import annotations

import logging
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from questdb.ingress import Protocol, Sender, TimestampNanos

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class QuestDBWriterConfig:
    """Connection config for QuestDB ILP writer."""

    ilp_host: str = "localhost"
    ilp_port: int = 9009


@dataclass
class _StepAccumulator:
    """Accumulates per-step stats for final metrics computation."""

    total_fees: float = 0.0
    total_turnover: float = 0.0
    slippage_bps_values: list[float] = field(default_factory=list)
    fills: int = 0
    rejects: int = 0
    total_symbols: int = 0


class ExecutionTableWriter:
    """Write shadow execution data to QuestDB unified tables via ILP.

    After each step, call :meth:`write_step` to emit per-symbol rows to
    ``execution_log``.  At run completion, call :meth:`write_metrics` to
    emit a summary row to ``execution_metrics``.
    """

    def __init__(
        self,
        config: QuestDBWriterConfig,
        experiment_id: str,
        run_id: str,
        mode: str = "shadow",
        dagster_run_id: str | None = None,
    ) -> None:
        self._config = config
        self._experiment_id = experiment_id
        self._run_id = run_id
        self._mode = mode
        self._dagster_run_id = dagster_run_id or ""
        self._acc = _StepAccumulator()

    def write_step(
        self,
        *,
        timestamp: datetime,
        step: int,
        symbols: tuple[str, ...],
        target_weights: list[float],
        realized_weights: list[float],
        fill_prices: list[float],
        slippage_bps: list[float],
        fees_per_symbol: list[float],
        rejected: list[bool],
        reject_reasons: list[str],
        portfolio_value: float,
        cash: float,
        regime_bucket: str,
    ) -> None:
        """Write per-symbol execution_log rows for one step."""
        ts_nanos = TimestampNanos(int(timestamp.timestamp() * 1_000_000_000))

        with Sender(Protocol.Tcp, self._config.ilp_host, self._config.ilp_port) as sender:
            for i, sym in enumerate(symbols):
                sender.row(
                    "execution_log",
                    symbols={
                        "experiment_id": self._experiment_id,
                        "run_id": self._run_id,
                        "mode": self._mode,
                        "symbol": sym,
                        "regime_bucket": regime_bucket or "unknown",
                    },
                    columns={
                        "step": step,
                        "target_weight": target_weights[i],
                        "realized_weight": realized_weights[i],
                        "fill_price": fill_prices[i],
                        "slippage_bps": slippage_bps[i],
                        "fees": fees_per_symbol[i],
                        "rejected": rejected[i],
                        "reject_reason": reject_reasons[i],
                        "portfolio_value": portfolio_value,
                        "cash": cash,
                        "dagster_run_id": self._dagster_run_id,
                    },
                    at=ts_nanos,
                )
            sender.flush()

        # Accumulate stats for metrics
        for i in range(len(symbols)):
            self._acc.total_symbols += 1
            if rejected[i]:
                self._acc.rejects += 1
            else:
                self._acc.fills += 1
            self._acc.slippage_bps_values.append(slippage_bps[i])
            self._acc.total_fees += fees_per_symbol[i]
        self._acc.total_turnover += sum(
            abs(target_weights[i] - realized_weights[i]) for i in range(len(symbols))
        )

    def write_metrics(
        self,
        *,
        timestamp: datetime | None = None,
        sharpe: float = 0.0,
        max_drawdown: float = 0.0,
        total_return: float = 0.0,
        execution_halts: int = 0,
    ) -> None:
        """Write a summary row to execution_metrics."""
        ts = timestamp or datetime.now(timezone.utc)
        ts_nanos = TimestampNanos(int(ts.timestamp() * 1_000_000_000))

        total = self._acc.fills + self._acc.rejects
        fill_rate = self._acc.fills / total if total > 0 else 1.0
        reject_rate = self._acc.rejects / total if total > 0 else 0.0

        slips = self._acc.slippage_bps_values
        avg_slippage = statistics.mean(slips) if slips else 0.0
        p95_slippage = (
            sorted(slips)[int(len(slips) * 0.95)] if len(slips) > 1 else avg_slippage
        )

        with Sender(Protocol.Tcp, self._config.ilp_host, self._config.ilp_port) as sender:
            sender.row(
                "execution_metrics",
                symbols={
                    "experiment_id": self._experiment_id,
                    "run_id": self._run_id,
                    "mode": self._mode,
                },
                columns={
                    "fill_rate": fill_rate,
                    "reject_rate": reject_rate,
                    "avg_slippage_bps": avg_slippage,
                    "p95_slippage_bps": p95_slippage,
                    "total_fees": self._acc.total_fees,
                    "total_turnover": self._acc.total_turnover,
                    "execution_halts": execution_halts,
                    "sharpe": sharpe,
                    "max_drawdown": max_drawdown,
                    "total_return": total_return,
                    "dagster_run_id": self._dagster_run_id,
                },
                at=ts_nanos,
            )
            sender.flush()

        logger.info(
            "Wrote execution_metrics: experiment=%s run=%s fill_rate=%.2f sharpe=%.4f",
            self._experiment_id, self._run_id, fill_rate, sharpe,
        )
