"""Risk decision logging — writes constraint evaluations to QuestDB.

Every risk decision from project_weights_full() is logged to the
risk_decisions table via QuestDB ILP for audit and analysis.

PRD Appendix G: "Every risk decision logged to risk_decisions QuestDB table."
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from research.risk.project_weights import Decision, RiskDecision, RiskResult

logger = logging.getLogger(__name__)


class RiskDecisionsWriter:
    """Writes risk decisions to the risk_decisions QuestDB table via ILP.

    Table schema (from create_tables.py):
        timestamp TIMESTAMP,
        rule_id SYMBOL,
        experiment_id SYMBOL,
        mode SYMBOL,
        input_metrics STRING,
        decision SYMBOL,
        action_taken STRING,
        original_size DOUBLE,
        reduced_size DOUBLE,
        dagster_run_id STRING
    """

    def __init__(
        self,
        experiment_id: str,
        mode: str,
        *,
        ilp_host: str = "localhost",
        ilp_port: int = 9009,
        dagster_run_id: str | None = None,
    ) -> None:
        self._experiment_id = experiment_id
        self._mode = mode
        self._ilp_host = ilp_host
        self._ilp_port = ilp_port
        self._dagster_run_id = dagster_run_id or ""

    def log_result(self, result: RiskResult) -> None:
        """Log all decisions from a RiskResult to QuestDB.

        Only logs non-PASS decisions to avoid flooding the table.
        """
        non_pass = [d for d in result.decisions if d.decision != Decision.PASS]
        if not non_pass:
            return

        try:
            from questdb.ingress import Protocol, Sender, TimestampNanos

            now = datetime.now(timezone.utc)
            ts = TimestampNanos(int(now.timestamp() * 1_000_000_000))

            with Sender(Protocol.Tcp, self._ilp_host, self._ilp_port) as sender:
                for d in non_pass:
                    original_size = 0.0
                    reduced_size = 0.0
                    if d.original_weights is not None:
                        original_size = float(d.original_weights.sum())
                    if d.reduced_weights is not None:
                        reduced_size = float(d.reduced_weights.sum())

                    sender.row(
                        "risk_decisions",
                        symbols={
                            "rule_id": d.rule_id,
                            "experiment_id": self._experiment_id,
                            "mode": self._mode,
                            "decision": d.decision.value,
                        },
                        columns={
                            "input_metrics": json.dumps(d.details),
                            "action_taken": d.decision.value,
                            "original_size": original_size,
                            "reduced_size": reduced_size,
                            "dagster_run_id": self._dagster_run_id,
                        },
                        at=ts,
                    )
                sender.flush()

        except Exception as exc:
            logger.error("Failed to write risk_decisions: %s", exc)

    def log_decisions(self, decisions: list[RiskDecision]) -> None:
        """Log a list of individual risk decisions."""
        result = RiskResult(
            weights=__import__("numpy").zeros(0),
            decisions=decisions,
        )
        self.log_result(result)
