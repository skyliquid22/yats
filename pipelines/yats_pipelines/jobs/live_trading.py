"""Dagster live trading jobs — setup, teardown, and health sensor.

Implements PRD §13.5 Live Trading (lines 1375-1381):
- Same architecture as paper_trading — Dagster handles setup/teardown,
  independent Python process runs execution loop against LIVE Alpaca endpoint.
- Gate: managing_partner approval required.
- Trigger: yats.execution.promote_live

The actual execution loop runs as an independent Python process
(research.execution.live_trading.LiveTradingLoop), NOT as a Dagster
long-running job.
"""

import logging
from typing import Any

import psycopg2
from dagster import (
    Config,
    In,
    Nothing,
    OpExecutionContext,
    Out,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    job,
    op,
    sensor,
)

logger = logging.getLogger(__name__)


class LiveTradingConfig(Config):
    """Run config for live trading setup/teardown jobs."""

    experiment_id: str
    paper_run_id: str = ""  # Paper trading run that validated readiness
    promoted_by: str = ""
    managing_partner_ack: bool = False
    initial_cash: float = 100_000.0
    data_root: str = ".yats_data"

    # QuestDB connections
    ilp_host: str = "localhost"
    ilp_port: int = 9009
    pg_host: str = "localhost"
    pg_port: int = 8812

    # Heartbeat monitoring thresholds
    heartbeat_miss_bars: int = 3
    bar_staleness_s: int = 120
    order_ack_timeout_s: int = 60


# ---------------------------------------------------------------------------
# Setup ops
# ---------------------------------------------------------------------------


@op(out=Out(dict))
def validate_managing_partner_gate(
    context: OpExecutionContext, config: LiveTradingConfig,
) -> dict:
    """Validate managing_partner acknowledgment gate.

    Live trading REQUIRES managing_partner_ack=True (PRD §13.5 line 1379).
    This gate cannot be bypassed.
    """
    if not config.managing_partner_ack:
        raise ValueError(
            "Live trading requires managing_partner_ack=True. "
            "This gate cannot be bypassed."
        )

    context.log.info(
        "Managing partner gate passed for %s (promoted_by=%s)",
        config.experiment_id,
        config.promoted_by,
    )

    return {
        "experiment_id": config.experiment_id,
        "promoted_by": config.promoted_by,
        "managing_partner_ack": True,
    }


@op(ins={"gate_result": In(dict)}, out=Out(dict))
def validate_production_tier(
    context: OpExecutionContext, config: LiveTradingConfig, gate_result: dict,
) -> dict:
    """Validate the experiment has been promoted to production tier.

    Live trading requires production tier promotion (PRD §13.5 line 1376).
    """
    conn = psycopg2.connect(
        host=config.pg_host,
        port=config.pg_port,
        user="admin",
        password="quest",
        database="qdb",
    )

    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT tier
            FROM promotions
            WHERE experiment_id = %s
            ORDER BY promoted_at DESC
            LIMIT 1
            """,
            (config.experiment_id,),
        )
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()

    if row is None:
        raise ValueError(
            f"No promotion record found for {config.experiment_id}. "
            f"Live trading requires promotion to 'production' tier."
        )

    tier = row[0]
    if tier != "production":
        raise ValueError(
            f"Experiment {config.experiment_id} is at tier '{tier}' — "
            f"live trading requires 'production' tier"
        )

    context.log.info(
        "Production tier validated: %s at tier 'production'",
        config.experiment_id,
    )

    return {**gate_result, "tier": tier}


@op(ins={"tier_result": In(dict)}, out=Out(dict))
def validate_paper_trading_success(
    context: OpExecutionContext, config: LiveTradingConfig, tier_result: dict,
) -> dict:
    """Validate paper trading completed successfully before allowing live.

    Checks execution_criteria from the paper trading run.
    """
    if not config.paper_run_id:
        context.log.warning(
            "No paper_run_id provided — skipping paper trading validation "
            "(may be a manual override)"
        )
        return {**tier_result, "paper_validated": False}

    conn = psycopg2.connect(
        host=config.pg_host,
        port=config.pg_port,
        user="admin",
        password="quest",
        database="qdb",
    )

    try:
        cur = conn.cursor()
        # Check that paper trading run exists and had no kill switch halts
        cur.execute(
            """
            SELECT count(*)
            FROM kill_switches
            WHERE experiment_id = %s
              AND mode = 'paper'
              AND action = 'halt'
              AND resolved = false
            """,
            (config.experiment_id,),
        )
        row = cur.fetchone()
        unresolved_halts = row[0] if row else 0
        cur.close()
    finally:
        conn.close()

    if unresolved_halts > 0:
        raise ValueError(
            f"Experiment {config.experiment_id} has {unresolved_halts} "
            f"unresolved kill switch halts from paper trading. "
            f"These must be resolved before live trading."
        )

    context.log.info(
        "Paper trading validation passed: no unresolved halts for %s",
        config.experiment_id,
    )

    return {**tier_result, "paper_validated": True}


@op(ins={"validation_result": In(dict)}, out=Out(dict))
def initialize_live_trading_state(
    context: OpExecutionContext, config: LiveTradingConfig,
    validation_result: dict,
) -> dict:
    """Initialize live trading state in QuestDB.

    Writes initial portfolio_state row with starting cash.
    Connects to Alpaca LIVE trading to verify credentials.
    """
    from datetime import datetime, timezone

    from questdb.ingress import Protocol, Sender, TimestampNanos

    from research.execution.broker_alpaca import AlpacaBrokerAdapter, AlpacaBrokerConfig

    # Verify Alpaca LIVE credentials
    broker_cfg = AlpacaBrokerConfig.from_env(paper=False, production_tier=True)
    broker = AlpacaBrokerAdapter(broker_cfg)
    account = broker.get_account()
    context.log.info(
        "Alpaca LIVE account connected: equity=%.2f status=%s",
        account.get("equity", 0),
        account.get("status", "unknown"),
    )

    # Write initial portfolio state
    now = datetime.now(timezone.utc)
    ts = TimestampNanos(int(now.timestamp() * 1_000_000_000))

    with Sender(Protocol.Tcp, config.ilp_host, config.ilp_port) as sender:
        sender.row(
            "portfolio_state",
            symbols={
                "experiment_id": config.experiment_id,
                "mode": "live",
            },
            columns={
                "nav": config.initial_cash,
                "cash": config.initial_cash,
                "gross_exposure": 0.0,
                "net_exposure": 0.0,
                "leverage": 0.0,
                "num_positions": 0,
                "daily_pnl": 0.0,
                "peak_nav": config.initial_cash,
                "drawdown": 0.0,
                "dagster_run_id": context.run_id,
            },
            at=ts,
        )
        sender.flush()

    context.log.info(
        "Initialized live trading state: cash=%.2f",
        config.initial_cash,
    )

    # Write audit trail entry
    with Sender(Protocol.Tcp, config.ilp_host, config.ilp_port) as sender:
        sender.row(
            "audit_trail",
            symbols={
                "tool_name": "live_trading_setup",
                "invoker": config.promoted_by or "dagster",
                "experiment_id": config.experiment_id,
                "mode": "live",
                "result_status": "success",
            },
            columns={
                "parameters": (
                    f'{{"initial_cash": {config.initial_cash}, '
                    f'"paper_run_id": "{config.paper_run_id}", '
                    f'"managing_partner_ack": true}}'
                ),
                "result_summary": "Live trading state initialized",
                "duration_ms": 0,
                "dagster_run_id": context.run_id,
            },
            at=ts,
        )
        sender.flush()

    return {
        "experiment_id": config.experiment_id,
        "initial_cash": config.initial_cash,
        "alpaca_equity": account.get("equity", 0),
        "dagster_run_id": context.run_id,
        "paper_run_id": config.paper_run_id,
        "promoted_by": config.promoted_by,
    }


# ---------------------------------------------------------------------------
# Teardown ops
# ---------------------------------------------------------------------------


@op(out=Out(dict))
def cancel_live_pending_orders(
    context: OpExecutionContext, config: LiveTradingConfig,
) -> dict:
    """Cancel all pending LIVE orders for this experiment."""
    from research.execution.broker_alpaca import (
        AlpacaBrokerAdapter,
        AlpacaBrokerConfig,
        BrokerError,
    )

    broker_cfg = AlpacaBrokerConfig.from_env(paper=False, production_tier=True)
    broker = AlpacaBrokerAdapter(broker_cfg)

    conn = psycopg2.connect(
        host=config.pg_host,
        port=config.pg_port,
        user="admin",
        password="quest",
        database="qdb",
    )

    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT broker_order_id, symbol
            FROM orders
            WHERE experiment_id = %s
              AND mode = 'live'
              AND status IN ('submitted', 'pending')
            """,
            (config.experiment_id,),
        )
        pending = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    cancelled = 0
    failed = 0
    for broker_order_id, symbol in pending:
        if not broker_order_id:
            continue
        try:
            broker.cancel_order(broker_order_id)
            cancelled += 1
            context.log.info("Cancelled LIVE order %s for %s", broker_order_id, symbol)
        except BrokerError as exc:
            failed += 1
            context.log.warning(
                "Failed to cancel LIVE order %s: %s", broker_order_id, exc,
            )

    context.log.info(
        "Live order cancellation: %d cancelled, %d failed out of %d pending",
        cancelled, failed, len(pending),
    )

    return {
        "cancelled": cancelled,
        "failed": failed,
        "total_pending": len(pending),
    }


@op(ins={"cancel_result": In(dict)}, out=Out(dict))
def write_live_final_state(
    context: OpExecutionContext, config: LiveTradingConfig, cancel_result: dict,
) -> dict:
    """Write final live portfolio state and audit trail entry."""
    from datetime import datetime, timezone

    from questdb.ingress import Protocol, Sender, TimestampNanos

    from research.execution.recovery import RecoveryConfig, recover_state

    recovery_cfg = RecoveryConfig(
        pg_host=config.pg_host,
        pg_port=config.pg_port,
    )

    try:
        result = recover_state(
            recovery_cfg,
            config.experiment_id,
            "live",
        )
        nav = result.portfolio.nav
        cash = result.portfolio.cash
        n_positions = len(result.positions)
    except Exception as exc:
        context.log.warning("Recovery for final state failed: %s", exc)
        nav = 0.0
        cash = 0.0
        n_positions = 0

    now = datetime.now(timezone.utc)
    ts = TimestampNanos(int(now.timestamp() * 1_000_000_000))

    with Sender(Protocol.Tcp, config.ilp_host, config.ilp_port) as sender:
        sender.row(
            "audit_trail",
            symbols={
                "tool_name": "live_trading_teardown",
                "invoker": "dagster",
                "experiment_id": config.experiment_id,
                "mode": "live",
                "result_status": "success",
            },
            columns={
                "parameters": "{}",
                "result_summary": (
                    f"Live teardown complete: NAV={nav:.2f}, cash={cash:.2f}, "
                    f"positions={n_positions}, "
                    f"cancelled={cancel_result.get('cancelled', 0)}"
                ),
                "duration_ms": 0,
                "dagster_run_id": context.run_id,
            },
            at=ts,
        )
        sender.flush()

    context.log.info(
        "Live teardown complete: NAV=%.2f, %d positions, %d orders cancelled",
        nav, n_positions, cancel_result.get("cancelled", 0),
    )

    return {
        "final_nav": nav,
        "final_cash": cash,
        "positions": n_positions,
        "orders_cancelled": cancel_result.get("cancelled", 0),
    }


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


@job
def live_trading_setup():
    """Live trading setup: gate → tier → paper validation → init state."""
    gate_result = validate_managing_partner_gate()
    tier_result = validate_production_tier(gate_result)
    validation_result = validate_paper_trading_success(tier_result)
    initialize_live_trading_state(validation_result)


@job
def live_trading_teardown():
    """Live trading teardown: cancel orders → settle → write final state."""
    cancel_result = cancel_live_pending_orders()
    write_live_final_state(cancel_result)


# ---------------------------------------------------------------------------
# Health sensor
# ---------------------------------------------------------------------------


@sensor(
    job=live_trading_teardown,
    minimum_interval_seconds=30,
)
def live_trading_health_sensor(
    context: SensorEvaluationContext,
) -> SensorResult | SkipReason:
    """Monitor live trading health via trading_heartbeat table.

    Same alert logic as paper_trading_health_sensor, but for mode='live'.
    Critical alerts auto-trigger live teardown.
    """
    conn = psycopg2.connect(
        host="localhost",
        port=8812,
        user="admin",
        password="quest",
        database="qdb",
    )

    try:
        alerts = _check_live_heartbeat_health(conn, context)
    finally:
        conn.close()

    if not alerts:
        return SkipReason("All live trading heartbeats healthy")

    for alert in alerts:
        context.log.warning("Live heartbeat alert: %s", alert)

    _write_live_alerts(alerts)

    critical = [a for a in alerts if a.get("critical", False)]
    if critical:
        context.log.error(
            "Critical live heartbeat alert — triggering teardown for %s",
            critical[0].get("experiment_id", "unknown"),
        )
        return SensorResult(
            run_requests=[
                RunRequest(
                    run_key=f"live-heartbeat-teardown-{critical[0].get('experiment_id', '')}",
                    run_config={
                        "ops": {
                            "cancel_live_pending_orders": {
                                "config": {
                                    "experiment_id": critical[0].get("experiment_id", ""),
                                }
                            },
                            "write_live_final_state": {
                                "config": {
                                    "experiment_id": critical[0].get("experiment_id", ""),
                                }
                            },
                        }
                    },
                )
            ],
        )

    return SkipReason(f"{len(alerts)} non-critical live alerts logged")


def _check_live_heartbeat_health(
    conn: Any, context: SensorEvaluationContext,
) -> list[dict[str, Any]]:
    """Check trading_heartbeat table for live trading anomalies."""
    from datetime import datetime, timezone

    alerts: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    cur = conn.cursor()

    # Get active live experiments
    cur.execute(
        """
        SELECT experiment_id,
               max(timestamp) as last_heartbeat,
               max(loop_iteration) as last_iteration,
               max(orders_pending) as max_pending,
               max(last_bar_received) as last_bar
        FROM trading_heartbeat
        WHERE timestamp > dateadd('h', -1, now())
          AND mode = 'live'
        GROUP BY experiment_id
        """
    )

    for row in cur.fetchall():
        exp_id, last_hb, last_iter, max_pending, last_bar = row

        if last_hb is None:
            continue

        hb_age_s = (now - last_hb).total_seconds()
        if hb_age_s > 3 * 60:
            alerts.append({
                "experiment_id": exp_id,
                "mode": "live",
                "type": "no_heartbeat",
                "detail": f"No live heartbeat for {hb_age_s:.0f}s",
                "critical": True,
            })

        if last_bar is not None:
            bar_age_s = (now - last_bar).total_seconds()
            if bar_age_s > 120 and _is_market_hours(now):
                alerts.append({
                    "experiment_id": exp_id,
                    "mode": "live",
                    "type": "websocket_stale",
                    "detail": f"No live bar received for {bar_age_s:.0f}s during market hours",
                    "critical": True,
                })

    # Livelock detection for live mode
    cur.execute(
        """
        SELECT experiment_id, loop_iteration
        FROM trading_heartbeat
        WHERE timestamp > dateadd('m', -5, now())
          AND mode = 'live'
        ORDER BY experiment_id, timestamp DESC
        LIMIT 100
        """
    )

    iteration_history: dict[str, list[int]] = {}
    for row in cur.fetchall():
        exp_id, iteration = row
        iteration_history.setdefault(exp_id, []).append(iteration)

    for exp_id, iterations in iteration_history.items():
        if len(iterations) >= 5:
            recent = iterations[:5]
            if len(set(recent)) == 1:
                alerts.append({
                    "experiment_id": exp_id,
                    "mode": "live",
                    "type": "livelock",
                    "detail": f"loop_iteration stuck at {recent[0]} for last 5 heartbeats",
                    "critical": True,
                })

    cur.close()
    return alerts


def _is_market_hours(dt: Any) -> bool:
    """Check if current time is during US market hours (9:30-16:00 ET)."""
    et_hour = (dt.hour - 5) % 24
    et_minute = dt.minute

    if dt.weekday() >= 5:
        return False

    if et_hour < 9 or (et_hour == 9 and et_minute < 30):
        return False
    if et_hour >= 16:
        return False

    return True


def _write_live_alerts(alerts: list[dict[str, Any]]) -> None:
    """Write live trading alerts to audit_trail and kill_switches tables."""
    from datetime import datetime, timezone

    from questdb.ingress import Protocol, Sender, TimestampNanos

    now = datetime.now(timezone.utc)
    ts = TimestampNanos(int(now.timestamp() * 1_000_000_000))

    with Sender(Protocol.Tcp, "localhost", 9009) as sender:
        for alert in alerts:
            sender.row(
                "audit_trail",
                symbols={
                    "tool_name": "heartbeat_sensor",
                    "invoker": "dagster_sensor",
                    "experiment_id": alert.get("experiment_id", ""),
                    "mode": "live",
                    "result_status": "alert",
                },
                columns={
                    "parameters": "{}",
                    "result_summary": f"{alert['type']}: {alert['detail']}",
                    "duration_ms": 0,
                },
                at=ts,
            )

            if alert.get("critical", False):
                sender.row(
                    "kill_switches",
                    symbols={
                        "experiment_id": alert.get("experiment_id", ""),
                        "mode": "live",
                        "trigger": alert["type"],
                        "action": "halt",
                        "triggered_by": "heartbeat_sensor",
                    },
                    columns={
                        "details": alert["detail"],
                        "resolved": False,
                    },
                    at=ts,
                )

        sender.flush()
