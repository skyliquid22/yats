"""Heartbeat writer — trading_heartbeat table for livelock detection.

Implements PRD §13.5 Heartbeat Monitoring:
- Writes a heartbeat row to QuestDB every loop iteration
- Tracked fields: loop_iteration, orders_pending, last_bar_received, last_fill_received
- Dagster Sensor monitors this table for anomalies
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from questdb.ingress import Protocol, Sender, TimestampNanos

logger = logging.getLogger(__name__)


@dataclass
class HeartbeatWriter:
    """Writes heartbeat rows to QuestDB trading_heartbeat table.

    Called once per execution loop iteration. The Dagster health sensor
    monitors this table for anomalies (no heartbeat, stale bars, livelock).
    """

    ilp_host: str = "localhost"
    ilp_port: int = 9009
    experiment_id: str = ""
    mode: str = "paper"

    def write(
        self,
        *,
        loop_iteration: int,
        orders_pending: int = 0,
        last_bar_received: datetime | None = None,
        last_fill_received: datetime | None = None,
    ) -> None:
        """Write a heartbeat row to QuestDB."""
        now = datetime.now(timezone.utc)
        ts = TimestampNanos(int(now.timestamp() * 1_000_000_000))

        columns: dict[str, object] = {
            "loop_iteration": loop_iteration,
            "orders_pending": orders_pending,
        }

        if last_bar_received is not None:
            columns["last_bar_received"] = TimestampNanos(
                int(last_bar_received.timestamp() * 1_000_000_000)
            )
        if last_fill_received is not None:
            columns["last_fill_received"] = TimestampNanos(
                int(last_fill_received.timestamp() * 1_000_000_000)
            )

        with Sender(Protocol.Tcp, self.ilp_host, self.ilp_port) as sender:
            sender.row(
                "trading_heartbeat",
                symbols={
                    "experiment_id": self.experiment_id,
                    "mode": self.mode,
                },
                columns=columns,
                at=ts,
            )
            sender.flush()

        logger.debug(
            "Heartbeat: experiment=%s iteration=%d pending=%d",
            self.experiment_id, loop_iteration, orders_pending,
        )
