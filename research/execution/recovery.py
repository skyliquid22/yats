"""Crash recovery — position validation, NAV reconstruction, order reconciliation.

Implements PRD §13.4 State Reconstruction on Crash:
- Current positions: derived from aggregated fills, validated against snapshot
- NAV: reconstructed from most recent portfolio_state + subsequent fills
- Pending orders: reconciled with Alpaca API (source of truth)

Recovery is authoritative: aggregated fills override snapshot on mismatch.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import psycopg2

from research.execution.broker_alpaca import (
    AlpacaBrokerAdapter,
    BrokerError,
    OrderResult,
    OrderSide,
    OrderStatus,
)
from research.execution.state import Position, PortfolioSnapshot

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RecoveryConfig:
    """QuestDB PG wire config for recovery reads."""

    pg_host: str = "localhost"
    pg_port: int = 8812
    pg_user: str = "admin"
    pg_password: str = "quest"
    pg_database: str = "qdb"


@dataclass
class RecoveryResult:
    """Result of crash recovery."""

    positions: dict[str, Position]
    portfolio: PortfolioSnapshot
    pending_orders: list[OrderResult]
    mismatches: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def has_mismatches(self) -> bool:
        return len(self.mismatches) > 0

    @property
    def is_clean(self) -> bool:
        return not self.has_mismatches and not self.warnings


# ---------------------------------------------------------------------------
# Position reconstruction from fills
# ---------------------------------------------------------------------------


def _reconstruct_positions_from_fills(
    conn: Any,
    experiment_id: str,
    mode: str,
) -> dict[str, Position]:
    """Reconstruct positions by aggregating all filled orders.

    This is the authoritative source — PRD §13.4 line 1009-1018.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT symbol, side, fill_quantity, fill_price, fees
        FROM orders
        WHERE experiment_id = %s
          AND mode = %s
          AND status = 'filled'
          AND fill_quantity > 0
        ORDER BY timestamp ASC
        """,
        (experiment_id, mode),
    )

    positions: dict[str, Position] = {}
    for row in cur.fetchall():
        sym, side_str, fill_qty, fill_price, fees = row
        if sym not in positions:
            positions[sym] = Position(symbol=sym)

        pos = positions[sym]
        side = OrderSide.BUY if side_str == "buy" else OrderSide.SELL

        if side == OrderSide.BUY:
            new_cost = fill_qty * fill_price
            pos.cost_basis += new_cost
            pos.quantity += fill_qty
            if pos.quantity > 0:
                pos.avg_entry_price = pos.cost_basis / pos.quantity
        else:
            if pos.quantity > 0 and pos.avg_entry_price > 0:
                sold_qty = min(fill_qty, pos.quantity)
                pnl = sold_qty * (fill_price - pos.avg_entry_price)
                pos.realized_pnl += pnl
                pos.cost_basis -= sold_qty * pos.avg_entry_price
            pos.quantity -= fill_qty

        pos.realized_pnl -= (fees or 0.0)

    cur.close()

    # Remove zero-quantity positions
    return {sym: pos for sym, pos in positions.items() if abs(pos.quantity) > 1e-10}


def _read_position_snapshots(
    conn: Any,
    experiment_id: str,
    mode: str,
) -> dict[str, Position]:
    """Read latest position snapshots from QuestDB."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT symbol, quantity, avg_entry_price, realized_pnl
        FROM positions
        WHERE experiment_id = %s AND mode = %s
        LATEST ON timestamp PARTITION BY symbol
        """,
        (experiment_id, mode),
    )

    positions: dict[str, Position] = {}
    for row in cur.fetchall():
        sym, qty, avg_price, realized = row
        if abs(qty) > 1e-10:
            positions[sym] = Position(
                symbol=sym,
                quantity=qty,
                avg_entry_price=avg_price,
                realized_pnl=realized,
                cost_basis=qty * avg_price,
            )

    cur.close()
    return positions


def validate_positions(
    conn: Any,
    experiment_id: str,
    mode: str,
) -> tuple[dict[str, Position], list[str]]:
    """Validate position snapshots against aggregated fills.

    Returns authoritative positions and a list of mismatch descriptions.
    Aggregation is authoritative on mismatch (PRD §13.4 line 1018).
    """
    aggregated = _reconstruct_positions_from_fills(conn, experiment_id, mode)
    snapshots = _read_position_snapshots(conn, experiment_id, mode)
    mismatches: list[str] = []

    all_symbols = set(aggregated.keys()) | set(snapshots.keys())
    for sym in sorted(all_symbols):
        agg = aggregated.get(sym)
        snap = snapshots.get(sym)

        if agg and not snap:
            mismatches.append(
                f"{sym}: in fills (qty={agg.quantity:.4f}) but missing from snapshot"
            )
        elif snap and not agg:
            mismatches.append(
                f"{sym}: in snapshot (qty={snap.quantity:.4f}) but missing from fills"
            )
        elif agg and snap:
            if abs(agg.quantity - snap.quantity) > 1e-6:
                mismatches.append(
                    f"{sym}: quantity mismatch — fills={agg.quantity:.6f} snapshot={snap.quantity:.6f}"
                )
            if abs(agg.avg_entry_price - snap.avg_entry_price) > 0.01:
                mismatches.append(
                    f"{sym}: avg_entry_price mismatch — fills={agg.avg_entry_price:.4f} "
                    f"snapshot={snap.avg_entry_price:.4f}"
                )

    if mismatches:
        logger.warning(
            "Position mismatches found for experiment=%s mode=%s: %d mismatches. "
            "Using aggregated fills as authoritative.",
            experiment_id, mode, len(mismatches),
        )
    else:
        logger.info(
            "Position validation passed: %d positions consistent",
            len(aggregated),
        )

    # Aggregation is always authoritative
    return aggregated, mismatches


# ---------------------------------------------------------------------------
# NAV reconstruction
# ---------------------------------------------------------------------------


def reconstruct_nav(
    conn: Any,
    experiment_id: str,
    mode: str,
    positions: dict[str, Position],
) -> PortfolioSnapshot:
    """Reconstruct NAV from most recent portfolio_state + subsequent fills.

    PRD §13.4 lines 1020-1021.
    """
    cur = conn.cursor()

    # Get most recent portfolio_state row
    cur.execute(
        """
        SELECT timestamp, nav, cash, gross_exposure, net_exposure,
               leverage, num_positions, daily_pnl, peak_nav, drawdown
        FROM portfolio_state
        WHERE experiment_id = %s AND mode = %s
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (experiment_id, mode),
    )

    row = cur.fetchone()
    if row is None:
        logger.info(
            "No portfolio_state found for experiment=%s mode=%s — "
            "constructing from positions only",
            experiment_id, mode,
        )
        cur.close()
        # Compute from positions alone (fresh start scenario)
        return _compute_snapshot_from_positions(positions)

    snap_ts, nav, cash, gross_exp, net_exp, leverage, n_pos, daily_pnl, peak_nav, dd = row

    # Get fills after the snapshot timestamp
    cur.execute(
        """
        SELECT symbol, side, fill_quantity, fill_price, fees
        FROM orders
        WHERE experiment_id = %s
          AND mode = %s
          AND status = 'filled'
          AND timestamp > %s
          AND fill_quantity > 0
        ORDER BY timestamp ASC
        """,
        (experiment_id, mode, snap_ts),
    )

    subsequent_fills = cur.fetchall()
    cur.close()

    if subsequent_fills:
        logger.info(
            "Found %d fills after last portfolio snapshot — adjusting NAV",
            len(subsequent_fills),
        )
        # Adjust cash for subsequent fills
        for sym, side_str, fill_qty, fill_price, fees in subsequent_fills:
            trade_value = fill_qty * fill_price
            if side_str == "buy":
                cash -= trade_value
            else:
                cash += trade_value
            cash -= (fees or 0.0)

    # Recompute gross/net exposure from authoritative positions
    gross_exposure = 0.0
    net_exposure = 0.0
    for pos in positions.values():
        notional = pos.quantity * pos.avg_entry_price
        gross_exposure += abs(notional)
        net_exposure += notional

    nav = gross_exposure + cash
    if nav > peak_nav:
        peak_nav = nav

    drawdown = 0.0
    if peak_nav > 0:
        drawdown = (nav - peak_nav) / peak_nav

    leverage = gross_exposure / nav if nav > 0 else 0.0

    return PortfolioSnapshot(
        nav=nav,
        cash=cash,
        gross_exposure=gross_exposure,
        net_exposure=net_exposure,
        leverage=leverage,
        num_positions=len(positions),
        daily_pnl=daily_pnl,
        peak_nav=peak_nav,
        drawdown=drawdown,
    )


def _compute_snapshot_from_positions(positions: dict[str, Position]) -> PortfolioSnapshot:
    """Compute a minimal snapshot from positions alone (no prior state)."""
    gross_exposure = 0.0
    net_exposure = 0.0
    for pos in positions.values():
        notional = pos.quantity * pos.avg_entry_price
        gross_exposure += abs(notional)
        net_exposure += notional

    return PortfolioSnapshot(
        nav=gross_exposure,
        cash=0.0,
        gross_exposure=gross_exposure,
        net_exposure=net_exposure,
        leverage=1.0 if gross_exposure > 0 else 0.0,
        num_positions=len(positions),
        peak_nav=gross_exposure,
    )


# ---------------------------------------------------------------------------
# Pending order reconciliation
# ---------------------------------------------------------------------------


def reconcile_pending_orders(
    conn: Any,
    experiment_id: str,
    mode: str,
    broker: AlpacaBrokerAdapter,
) -> tuple[list[OrderResult], list[str]]:
    """Reconcile pending orders with Alpaca API.

    PRD §13.4 lines 1023-1024: Alpaca API is source of truth for broker-side state.
    Pending orders in our orders table are checked against Alpaca's actual state.

    Returns list of truly pending orders and any warning messages.
    """
    warnings: list[str] = []

    # Get orders we think are pending
    cur = conn.cursor()
    cur.execute(
        """
        SELECT order_id, broker_order_id, symbol, side, quantity
        FROM orders
        WHERE experiment_id = %s
          AND mode = %s
          AND status IN ('submitted', 'pending')
        ORDER BY timestamp DESC
        """,
        (experiment_id, mode),
    )

    local_pending = cur.fetchall()
    cur.close()

    if not local_pending:
        logger.info("No pending orders to reconcile")
        return [], warnings

    logger.info("Reconciling %d locally pending orders with Alpaca", len(local_pending))

    still_pending: list[OrderResult] = []
    for order_id, broker_order_id, symbol, side_str, qty in local_pending:
        bid = broker_order_id or order_id
        try:
            result = broker.get_order_status(bid)

            if result.status == OrderStatus.FILLED:
                warnings.append(
                    f"{symbol}: order {bid} was filled while offline "
                    f"(qty={result.filled_qty:.4f} @ {result.filled_avg_price:.4f})"
                )
            elif result.status in (OrderStatus.CANCELLED, OrderStatus.EXPIRED):
                warnings.append(
                    f"{symbol}: order {bid} was {result.status.value} while offline"
                )
            elif result.status in (OrderStatus.REJECTED,):
                warnings.append(
                    f"{symbol}: order {bid} was rejected: {result.reject_reason}"
                )
            elif result.status in (OrderStatus.SUBMITTED, OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED):
                still_pending.append(result)
                logger.info(
                    "Order %s for %s still pending (status=%s)",
                    bid, symbol, result.status.value,
                )
        except BrokerError as exc:
            warnings.append(
                f"{symbol}: failed to check order {bid}: {exc}"
            )

    if warnings:
        for w in warnings:
            logger.warning("Order reconciliation: %s", w)

    return still_pending, warnings


# ---------------------------------------------------------------------------
# Full recovery orchestration
# ---------------------------------------------------------------------------


def recover_state(
    config: RecoveryConfig,
    experiment_id: str,
    mode: str,
    broker: AlpacaBrokerAdapter | None = None,
) -> RecoveryResult:
    """Full crash recovery: validate positions, reconstruct NAV, reconcile orders.

    This is the main entry point for crash recovery on restart.

    Args:
        config: QuestDB PG wire connection config.
        experiment_id: Experiment identifier.
        mode: Execution mode (shadow/paper/live).
        broker: Alpaca broker adapter for pending order reconciliation.
                None skips order reconciliation (e.g., shadow mode).

    Returns:
        RecoveryResult with authoritative state and any issues found.
    """
    logger.info(
        "Starting crash recovery for experiment=%s mode=%s",
        experiment_id, mode,
    )

    conn = psycopg2.connect(
        host=config.pg_host,
        port=config.pg_port,
        user=config.pg_user,
        password=config.pg_password,
        database=config.pg_database,
    )

    try:
        # Step 1: Validate positions (aggregated fills vs snapshot)
        positions, pos_mismatches = validate_positions(conn, experiment_id, mode)

        # Step 2: Reconstruct NAV
        portfolio = reconstruct_nav(conn, experiment_id, mode, positions)

        # Step 3: Reconcile pending orders (if broker available)
        pending_orders: list[OrderResult] = []
        order_warnings: list[str] = []
        if broker is not None:
            pending_orders, order_warnings = reconcile_pending_orders(
                conn, experiment_id, mode, broker,
            )

        result = RecoveryResult(
            positions=positions,
            portfolio=portfolio,
            pending_orders=pending_orders,
            mismatches=pos_mismatches,
            warnings=order_warnings,
        )

        if result.is_clean:
            logger.info(
                "Crash recovery complete — clean state: %d positions, NAV=%.2f",
                len(positions), portfolio.nav,
            )
        else:
            logger.warning(
                "Crash recovery complete with issues: %d mismatches, %d warnings, "
                "%d positions, NAV=%.2f",
                len(pos_mismatches), len(order_warnings),
                len(positions), portfolio.nav,
            )

        return result

    finally:
        conn.close()
