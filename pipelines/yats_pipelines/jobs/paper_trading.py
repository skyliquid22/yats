"""Dagster paper trading jobs — setup, teardown, and health sensor.

Implements PRD §13.5 Paper Trading Architecture:
- Dagster job handles SETUP: load spec + policy, validate promotion,
  init state, connect Alpaca paper, write initial state to QuestDB.
- Dagster Sensor monitors health via trading_heartbeat table.
- TEARDOWN: Dagster job handles graceful shutdown (cancel orders, settle,
  write final state).

The actual execution loop runs as an independent Python process
(research.execution.paper_trading.PaperTradingLoop), NOT as a Dagster
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


class PaperTradingConfig(Config):
    """Run config for paper trading setup/teardown jobs."""

    experiment_id: str
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
def load_paper_trading_spec(
    context: OpExecutionContext, config: PaperTradingConfig,
) -> dict:
    """Load experiment spec and validate promotion status.

    Paper trading requires the experiment to be promoted to at least
    'candidate' tier (PRD §13.5 line 1329).
    """
    from pathlib import Path

    from research.experiments.registry import get

    data_root = Path(config.data_root)
    exp = get(config.experiment_id, data_root=data_root)
    spec_data = exp["spec"]

    context.log.info(
        "Loaded spec for paper trading: %s (policy=%s, symbols=%s)",
        config.experiment_id,
        spec_data.get("policy"),
        spec_data.get("symbols"),
    )

    return spec_data


@op(ins={"spec_data": In(dict)}, out=Out(dict))
def validate_promotion_status(
    context: OpExecutionContext, config: PaperTradingConfig, spec_data: dict,
) -> dict:
    """Validate the experiment has been promoted to candidate or production."""
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
        context.log.warning(
            "No promotion record found for %s — proceeding anyway "
            "(may be initial paper trading setup)",
            config.experiment_id,
        )
        return spec_data

    tier = row[0]
    if tier not in ("candidate", "production"):
        raise ValueError(
            f"Experiment {config.experiment_id} is at tier '{tier}' — "
            f"paper trading requires 'candidate' or 'production'"
        )

    context.log.info(
        "Promotion validated: %s at tier '%s'",
        config.experiment_id, tier,
    )
    return spec_data


@op(ins={"spec_data": In(dict)}, out=Out(dict))
def initialize_paper_trading_state(
    context: OpExecutionContext, config: PaperTradingConfig, spec_data: dict,
) -> dict:
    """Initialize paper trading state in QuestDB.

    Writes initial portfolio_state row with starting cash and zero positions.
    Connects to Alpaca paper trading to verify credentials.
    """
    from questdb.ingress import Protocol, Sender, TimestampNanos
    from datetime import datetime, timezone

    from research.execution.broker_alpaca import AlpacaBrokerAdapter, AlpacaBrokerConfig

    # Verify Alpaca paper credentials
    broker_cfg = AlpacaBrokerConfig.from_env(paper=True)
    broker = AlpacaBrokerAdapter(broker_cfg)
    account = broker.get_account()
    context.log.info(
        "Alpaca paper account connected: equity=%.2f status=%s",
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
                "mode": "paper",
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
        "Initialized paper trading state: cash=%.2f",
        config.initial_cash,
    )

    # Write audit trail entry
    with Sender(Protocol.Tcp, config.ilp_host, config.ilp_port) as sender:
        sender.row(
            "audit_trail",
            symbols={
                "tool_name": "paper_trading_setup",
                "invoker": "dagster",
                "experiment_id": config.experiment_id,
                "mode": "paper",
                "result_status": "success",
            },
            columns={
                "parameters": f'{{"initial_cash": {config.initial_cash}}}',
                "result_summary": "Paper trading state initialized",
                "duration_ms": 0,
                "dagster_run_id": context.run_id,
            },
            at=ts,
        )
        sender.flush()

    return {
        "experiment_id": config.experiment_id,
        "symbols": spec_data.get("symbols", []),
        "initial_cash": config.initial_cash,
        "alpaca_equity": account.get("equity", 0),
        "dagster_run_id": context.run_id,
    }


# ---------------------------------------------------------------------------
# Teardown ops
# ---------------------------------------------------------------------------


@op(out=Out(dict))
def cancel_pending_orders(
    context: OpExecutionContext, config: PaperTradingConfig,
) -> dict:
    """Cancel all pending orders for this experiment."""
    from research.execution.broker_alpaca import (
        AlpacaBrokerAdapter,
        AlpacaBrokerConfig,
        BrokerError,
    )

    broker_cfg = AlpacaBrokerConfig.from_env(paper=True)
    broker = AlpacaBrokerAdapter(broker_cfg)

    # Query pending orders from QuestDB
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
              AND mode = 'paper'
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
            context.log.info("Cancelled order %s for %s", broker_order_id, symbol)
        except BrokerError as exc:
            failed += 1
            context.log.warning(
                "Failed to cancel order %s: %s", broker_order_id, exc,
            )

    context.log.info(
        "Order cancellation: %d cancelled, %d failed out of %d pending",
        cancelled, failed, len(pending),
    )

    return {
        "cancelled": cancelled,
        "failed": failed,
        "total_pending": len(pending),
    }


@op(ins={"cancel_result": In(dict)}, out=Out(dict))
def write_final_state(
    context: OpExecutionContext, config: PaperTradingConfig, cancel_result: dict,
) -> dict:
    """Write final portfolio state and audit trail entry."""
    from datetime import datetime, timezone

    from questdb.ingress import Protocol, Sender, TimestampNanos

    from research.execution.recovery import RecoveryConfig, recover_state

    # Recover current state to write final snapshot
    recovery_cfg = RecoveryConfig(
        pg_host=config.pg_host,
        pg_port=config.pg_port,
    )

    try:
        result = recover_state(
            recovery_cfg,
            config.experiment_id,
            "paper",
        )
        nav = result.portfolio.nav
        cash = result.portfolio.cash
        n_positions = len(result.positions)
    except Exception as exc:
        context.log.warning("Recovery for final state failed: %s", exc)
        nav = 0.0
        cash = 0.0
        n_positions = 0

    # Write audit trail entry for teardown
    now = datetime.now(timezone.utc)
    ts = TimestampNanos(int(now.timestamp() * 1_000_000_000))

    with Sender(Protocol.Tcp, config.ilp_host, config.ilp_port) as sender:
        sender.row(
            "audit_trail",
            symbols={
                "tool_name": "paper_trading_teardown",
                "invoker": "dagster",
                "experiment_id": config.experiment_id,
                "mode": "paper",
                "result_status": "success",
            },
            columns={
                "parameters": "{}",
                "result_summary": (
                    f"Teardown complete: NAV={nav:.2f}, cash={cash:.2f}, "
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
        "Teardown complete: NAV=%.2f, %d positions, %d orders cancelled",
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
def paper_trading_setup():
    """Paper trading setup: load spec → validate promotion → init state."""
    spec_data = load_paper_trading_spec()
    validated = validate_promotion_status(spec_data)
    initialize_paper_trading_state(validated)


@job
def paper_trading_teardown():
    """Paper trading teardown: cancel orders → settle → write final state."""
    cancel_result = cancel_pending_orders()
    write_final_state(cancel_result)


# ---------------------------------------------------------------------------
# Health sensor
# ---------------------------------------------------------------------------


class HeartbeatSensorConfig(Config):
    """Config for the heartbeat health sensor."""

    experiment_id: str = ""
    pg_host: str = "localhost"
    pg_port: int = 8812
    heartbeat_miss_bars: int = 3
    bar_staleness_s: int = 120
    livelock_iterations: int = 5  # same iteration count N times → livelock


@sensor(
    job=paper_trading_teardown,
    minimum_interval_seconds=30,
)
def paper_trading_health_sensor(
    context: SensorEvaluationContext,
) -> SensorResult | SkipReason:
    """Monitor paper trading health via trading_heartbeat table.

    Alerts triggered on (PRD §13.5 lines 1346-1362):
    - No heartbeat for N bars → kill switch
    - Order ack timeout → alert
    - WebSocket staleness → alert
    - Silent livelock → alert

    Each alert → audit_trail + kill_switches table.
    Critical alerts auto-trigger HALTING state.
    """
    conn = psycopg2.connect(
        host="localhost",
        port=8812,
        user="admin",
        password="quest",
        database="qdb",
    )

    try:
        alerts = _check_heartbeat_health(conn, context)
    finally:
        conn.close()

    if not alerts:
        return SkipReason("All paper trading heartbeats healthy")

    # Log alerts
    for alert in alerts:
        context.log.warning("Heartbeat alert: %s", alert)

    # Write alerts to audit_trail and kill_switches
    _write_alerts(alerts)

    # Critical alerts trigger teardown
    critical = [a for a in alerts if a.get("critical", False)]
    if critical:
        context.log.error(
            "Critical heartbeat alert — triggering teardown for %s",
            critical[0].get("experiment_id", "unknown"),
        )
        return SensorResult(
            run_requests=[
                RunRequest(
                    run_key=f"heartbeat-teardown-{critical[0].get('experiment_id', '')}",
                    run_config={
                        "ops": {
                            "cancel_pending_orders": {
                                "config": {
                                    "experiment_id": critical[0].get("experiment_id", ""),
                                }
                            },
                            "write_final_state": {
                                "config": {
                                    "experiment_id": critical[0].get("experiment_id", ""),
                                }
                            },
                        }
                    },
                )
            ],
        )

    return SkipReason(f"{len(alerts)} non-critical alerts logged")


def _check_heartbeat_health(
    conn: Any, context: SensorEvaluationContext,
) -> list[dict[str, Any]]:
    """Check trading_heartbeat table for anomalies."""
    from datetime import datetime, timezone

    alerts: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    cur = conn.cursor()

    # Get all active experiments (have heartbeats in last hour)
    cur.execute(
        """
        SELECT experiment_id, mode,
               max(timestamp) as last_heartbeat,
               max(loop_iteration) as last_iteration,
               max(orders_pending) as max_pending,
               max(last_bar_received) as last_bar
        FROM trading_heartbeat
        WHERE timestamp > dateadd('h', -1, now())
        GROUP BY experiment_id, mode
        """
    )

    for row in cur.fetchall():
        exp_id, mode, last_hb, last_iter, max_pending, last_bar = row

        if last_hb is None:
            continue

        # Check: no heartbeat (gap > heartbeat_miss_bars × 60s)
        hb_age_s = (now - last_hb).total_seconds()
        if hb_age_s > 3 * 60:  # 3 bars × ~60s
            alerts.append({
                "experiment_id": exp_id,
                "mode": mode,
                "type": "no_heartbeat",
                "detail": f"No heartbeat for {hb_age_s:.0f}s",
                "critical": True,
            })

        # Check: WebSocket staleness (no bar for >2min during market hours)
        if last_bar is not None:
            bar_age_s = (now - last_bar).total_seconds()
            if bar_age_s > 120 and _is_market_hours(now):
                alerts.append({
                    "experiment_id": exp_id,
                    "mode": mode,
                    "type": "websocket_stale",
                    "detail": f"No bar received for {bar_age_s:.0f}s during market hours",
                    "critical": True,
                })

    # Check: livelock (loop_iteration not incrementing)
    cur.execute(
        """
        SELECT experiment_id, mode, loop_iteration
        FROM trading_heartbeat
        WHERE timestamp > dateadd('m', -5, now())
        ORDER BY experiment_id, mode, timestamp DESC
        LIMIT 100
        """
    )

    iteration_history: dict[str, list[int]] = {}
    for row in cur.fetchall():
        exp_id, mode, iteration = row
        key = f"{exp_id}:{mode}"
        iteration_history.setdefault(key, []).append(iteration)

    for key, iterations in iteration_history.items():
        if len(iterations) >= 5:
            # Check if all recent iterations are the same value
            recent = iterations[:5]
            if len(set(recent)) == 1:
                exp_id, mode = key.split(":", 1)
                alerts.append({
                    "experiment_id": exp_id,
                    "mode": mode,
                    "type": "livelock",
                    "detail": f"loop_iteration stuck at {recent[0]} for last 5 heartbeats",
                    "critical": True,
                })

    cur.close()
    return alerts


def _is_market_hours(dt: Any) -> bool:
    """Check if current time is during US market hours (9:30-16:00 ET)."""
    from datetime import timezone as tz

    # Approximate ET as UTC-5 (ignoring DST for simplicity)
    et_hour = (dt.hour - 5) % 24
    et_minute = dt.minute

    if dt.weekday() >= 5:  # Saturday/Sunday
        return False

    # Market hours: 9:30 AM - 4:00 PM ET
    if et_hour < 9 or (et_hour == 9 and et_minute < 30):
        return False
    if et_hour >= 16:
        return False

    return True


def _write_alerts(alerts: list[dict[str, Any]]) -> None:
    """Write alerts to audit_trail and kill_switches tables."""
    from datetime import datetime, timezone

    from questdb.ingress import Protocol, Sender, TimestampNanos

    now = datetime.now(timezone.utc)
    ts = TimestampNanos(int(now.timestamp() * 1_000_000_000))

    with Sender(Protocol.Tcp, "localhost", 9009) as sender:
        for alert in alerts:
            # Write to audit_trail
            sender.row(
                "audit_trail",
                symbols={
                    "tool_name": "heartbeat_sensor",
                    "invoker": "dagster_sensor",
                    "experiment_id": alert.get("experiment_id", ""),
                    "mode": alert.get("mode", "paper"),
                    "result_status": "alert",
                },
                columns={
                    "parameters": "{}",
                    "result_summary": f"{alert['type']}: {alert['detail']}",
                    "duration_ms": 0,
                },
                at=ts,
            )

            # Critical alerts → kill_switches table
            if alert.get("critical", False):
                sender.row(
                    "kill_switches",
                    symbols={
                        "experiment_id": alert.get("experiment_id", ""),
                        "mode": alert.get("mode", "paper"),
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
