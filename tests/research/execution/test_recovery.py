"""Tests for research.execution.recovery — crash recovery and state reconciliation."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from research.execution.broker_alpaca import (
    BrokerError,
    OrderResult,
    OrderSide,
    OrderStatus,
)
from research.execution.recovery import (
    RecoveryConfig,
    RecoveryResult,
    _compute_snapshot_from_positions,
    _read_position_snapshots,
    _reconstruct_positions_from_fills,
    reconcile_pending_orders,
    reconstruct_nav,
    recover_state,
    validate_positions,
)
from research.execution.state import Position, PortfolioSnapshot


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_cursor(rows, fetchone_row=None):
    """Create a mock cursor returning given rows."""
    cur = MagicMock()
    cur.fetchall.return_value = rows
    if fetchone_row is not None:
        cur.fetchone.return_value = fetchone_row
    else:
        cur.fetchone.return_value = None
    return cur


def _mock_conn_with_cursors(*cursors):
    """Create a mock connection returning cursors in sequence."""
    conn = MagicMock()
    conn.cursor.side_effect = list(cursors)
    return conn


# ---------------------------------------------------------------------------
# Position reconstruction
# ---------------------------------------------------------------------------


class TestReconstructPositionsFromFills:
    def test_single_buy(self):
        cur = _mock_cursor([
            ("AAPL", "buy", 10.0, 150.0, 0.5),
        ])
        conn = MagicMock()
        conn.cursor.return_value = cur

        positions = _reconstruct_positions_from_fills(conn, "exp-1", "paper")

        assert "AAPL" in positions
        assert positions["AAPL"].quantity == 10.0
        assert positions["AAPL"].avg_entry_price == 150.0

    def test_buy_and_partial_sell(self):
        cur = _mock_cursor([
            ("AAPL", "buy", 10.0, 150.0, 0.0),
            ("AAPL", "sell", 3.0, 160.0, 0.0),
        ])
        conn = MagicMock()
        conn.cursor.return_value = cur

        positions = _reconstruct_positions_from_fills(conn, "exp-1", "paper")

        assert positions["AAPL"].quantity == pytest.approx(7.0)
        assert positions["AAPL"].realized_pnl == pytest.approx(30.0)  # 3 * (160-150)

    def test_full_close_removed(self):
        cur = _mock_cursor([
            ("AAPL", "buy", 10.0, 150.0, 0.0),
            ("AAPL", "sell", 10.0, 160.0, 0.0),
        ])
        conn = MagicMock()
        conn.cursor.return_value = cur

        positions = _reconstruct_positions_from_fills(conn, "exp-1", "paper")
        assert "AAPL" not in positions  # fully closed

    def test_multiple_symbols(self):
        cur = _mock_cursor([
            ("AAPL", "buy", 10.0, 150.0, 0.0),
            ("MSFT", "buy", 5.0, 300.0, 0.0),
        ])
        conn = MagicMock()
        conn.cursor.return_value = cur

        positions = _reconstruct_positions_from_fills(conn, "exp-1", "paper")
        assert len(positions) == 2

    def test_fees_reduce_realized_pnl(self):
        cur = _mock_cursor([
            ("AAPL", "buy", 10.0, 150.0, 1.0),
        ])
        conn = MagicMock()
        conn.cursor.return_value = cur

        positions = _reconstruct_positions_from_fills(conn, "exp-1", "paper")
        assert positions["AAPL"].realized_pnl == pytest.approx(-1.0)


# ---------------------------------------------------------------------------
# Position validation
# ---------------------------------------------------------------------------


class TestValidatePositions:
    def test_matching_positions(self):
        # Fills cursor
        fills_cur = _mock_cursor([
            ("AAPL", "buy", 10.0, 150.0, 0.0),
        ])
        # Snapshots cursor
        snap_cur = _mock_cursor([
            ("AAPL", 10.0, 150.0, 0.0),
        ])

        conn = _mock_conn_with_cursors(fills_cur, snap_cur)
        positions, mismatches = validate_positions(conn, "exp-1", "paper")

        assert len(mismatches) == 0
        assert positions["AAPL"].quantity == 10.0

    def test_quantity_mismatch(self):
        fills_cur = _mock_cursor([
            ("AAPL", "buy", 10.0, 150.0, 0.0),
        ])
        snap_cur = _mock_cursor([
            ("AAPL", 8.0, 150.0, 0.0),  # Wrong quantity
        ])

        conn = _mock_conn_with_cursors(fills_cur, snap_cur)
        positions, mismatches = validate_positions(conn, "exp-1", "paper")

        assert len(mismatches) == 1
        assert "quantity mismatch" in mismatches[0]
        # Aggregated fills are authoritative
        assert positions["AAPL"].quantity == 10.0

    def test_missing_from_snapshot(self):
        fills_cur = _mock_cursor([
            ("AAPL", "buy", 10.0, 150.0, 0.0),
        ])
        snap_cur = _mock_cursor([])  # No snapshots

        conn = _mock_conn_with_cursors(fills_cur, snap_cur)
        positions, mismatches = validate_positions(conn, "exp-1", "paper")

        assert len(mismatches) == 1
        assert "missing from snapshot" in mismatches[0]


# ---------------------------------------------------------------------------
# NAV reconstruction
# ---------------------------------------------------------------------------


class TestReconstructNav:
    def test_from_existing_snapshot(self):
        # reconstruct_nav uses a single cursor for both queries
        cur = MagicMock()
        cur.fetchone.return_value = (
            datetime(2024, 1, 15, tzinfo=timezone.utc),  # timestamp
            100000.0,  # nav
            50000.0,   # cash
            50000.0,   # gross_exposure
            50000.0,   # net_exposure
            1.0,       # leverage
            2,         # num_positions
            500.0,     # daily_pnl
            105000.0,  # peak_nav
            -0.05,     # drawdown
        )
        cur.fetchall.return_value = []  # no subsequent fills

        conn = MagicMock()
        conn.cursor.return_value = cur

        positions = {"AAPL": Position("AAPL", 10.0, 150.0)}
        snapshot = reconstruct_nav(conn, "exp-1", "paper", positions)

        assert snapshot.cash == 50000.0
        assert snapshot.num_positions == 1  # recomputed from current positions

    def test_no_prior_snapshot(self):
        state_cur = MagicMock()
        state_cur.fetchone.return_value = None

        conn = MagicMock()
        conn.cursor.return_value = state_cur

        positions = {"AAPL": Position("AAPL", 10.0, 150.0)}
        snapshot = reconstruct_nav(conn, "exp-1", "paper", positions)

        assert snapshot.nav == pytest.approx(1500.0)  # 10 * 150
        assert snapshot.cash == 0.0

    def test_with_subsequent_fills(self):
        # reconstruct_nav uses a single cursor for both queries
        cur = MagicMock()
        cur.fetchone.return_value = (
            datetime(2024, 1, 15, tzinfo=timezone.utc),
            100000.0, 50000.0, 50000.0, 50000.0,
            1.0, 1, 0.0, 100000.0, 0.0,
        )
        # A buy fill after snapshot
        cur.fetchall.return_value = [
            ("AAPL", "buy", 10.0, 150.0, 0.5),
        ]

        conn = MagicMock()
        conn.cursor.return_value = cur

        positions = {"AAPL": Position("AAPL", 10.0, 150.0)}
        snapshot = reconstruct_nav(conn, "exp-1", "paper", positions)

        # Cash reduced by fill value + fees
        assert snapshot.cash == pytest.approx(50000.0 - 1500.0 - 0.5)


# ---------------------------------------------------------------------------
# Pending order reconciliation
# ---------------------------------------------------------------------------


class TestReconcilePendingOrders:
    def test_no_pending_orders(self):
        cur = _mock_cursor([])
        conn = MagicMock()
        conn.cursor.return_value = cur

        broker = MagicMock()
        pending, warnings = reconcile_pending_orders(conn, "exp-1", "paper", broker)

        assert len(pending) == 0
        assert len(warnings) == 0

    def test_order_filled_while_offline(self):
        cur = _mock_cursor([
            ("order-1", "broker-1", "AAPL", "buy", 10.0),
        ])
        conn = MagicMock()
        conn.cursor.return_value = cur

        broker = MagicMock()
        broker.get_order_status.return_value = OrderResult(
            order_id="broker-1",
            client_order_id="order-1",
            symbol="AAPL",
            side=OrderSide.BUY,
            status=OrderStatus.FILLED,
            submitted_at=datetime(2024, 1, 15, tzinfo=timezone.utc),
            filled_qty=10.0,
            filled_avg_price=150.0,
        )

        pending, warnings = reconcile_pending_orders(conn, "exp-1", "paper", broker)

        assert len(pending) == 0
        assert len(warnings) == 1
        assert "filled while offline" in warnings[0]

    def test_order_still_pending(self):
        cur = _mock_cursor([
            ("order-1", "broker-1", "AAPL", "buy", 10.0),
        ])
        conn = MagicMock()
        conn.cursor.return_value = cur

        pending_result = OrderResult(
            order_id="broker-1",
            client_order_id="order-1",
            symbol="AAPL",
            side=OrderSide.BUY,
            status=OrderStatus.SUBMITTED,
            submitted_at=datetime(2024, 1, 15, tzinfo=timezone.utc),
        )
        broker = MagicMock()
        broker.get_order_status.return_value = pending_result

        pending, warnings = reconcile_pending_orders(conn, "exp-1", "paper", broker)

        assert len(pending) == 1
        assert pending[0].order_id == "broker-1"
        assert len(warnings) == 0

    def test_broker_error_warning(self):
        cur = _mock_cursor([
            ("order-1", "broker-1", "AAPL", "buy", 10.0),
        ])
        conn = MagicMock()
        conn.cursor.return_value = cur

        broker = MagicMock()
        broker.get_order_status.side_effect = BrokerError("connection timeout")

        pending, warnings = reconcile_pending_orders(conn, "exp-1", "paper", broker)

        assert len(pending) == 0
        assert len(warnings) == 1
        assert "failed to check" in warnings[0]


# ---------------------------------------------------------------------------
# Full recovery
# ---------------------------------------------------------------------------


class TestRecoverState:
    @patch("research.execution.recovery.psycopg2")
    def test_clean_recovery(self, mock_psycopg2):
        # Set up mock connection with 2 cursors (validate_positions needs 2)
        fills_cur = _mock_cursor([
            ("AAPL", "buy", 10.0, 150.0, 0.0),
        ])
        snap_cur = _mock_cursor([
            ("AAPL", 10.0, 150.0, 0.0),
        ])
        # NAV reconstruction needs 2 cursors
        state_cur = MagicMock()
        state_cur.fetchone.return_value = None
        fills_after_cur = _mock_cursor([])

        conn = MagicMock()
        conn.cursor.side_effect = [fills_cur, snap_cur, state_cur]
        mock_psycopg2.connect.return_value = conn

        cfg = RecoveryConfig()
        result = recover_state(cfg, "exp-1", "paper", broker=None)

        assert result.is_clean
        assert len(result.positions) == 1
        assert "AAPL" in result.positions

    @patch("research.execution.recovery.psycopg2")
    def test_recovery_with_mismatches(self, mock_psycopg2):
        fills_cur = _mock_cursor([
            ("AAPL", "buy", 10.0, 150.0, 0.0),
        ])
        snap_cur = _mock_cursor([
            ("AAPL", 8.0, 150.0, 0.0),  # Mismatch
        ])
        state_cur = MagicMock()
        state_cur.fetchone.return_value = None

        conn = MagicMock()
        conn.cursor.side_effect = [fills_cur, snap_cur, state_cur]
        mock_psycopg2.connect.return_value = conn

        cfg = RecoveryConfig()
        result = recover_state(cfg, "exp-1", "paper", broker=None)

        assert result.has_mismatches
        assert len(result.mismatches) == 1
        # Authoritative positions should be from fills
        assert result.positions["AAPL"].quantity == 10.0


# ---------------------------------------------------------------------------
# Helper tests
# ---------------------------------------------------------------------------


class TestComputeSnapshotFromPositions:
    def test_empty_positions(self):
        snap = _compute_snapshot_from_positions({})
        assert snap.nav == 0.0
        assert snap.leverage == 0.0

    def test_single_position(self):
        positions = {"AAPL": Position("AAPL", 10.0, 150.0)}
        snap = _compute_snapshot_from_positions(positions)

        assert snap.nav == pytest.approx(1500.0)
        assert snap.gross_exposure == pytest.approx(1500.0)
        assert snap.leverage == pytest.approx(1.0)


class TestRecoveryResult:
    def test_clean_result(self):
        result = RecoveryResult(
            positions={},
            portfolio=PortfolioSnapshot(),
            pending_orders=[],
        )
        assert result.is_clean
        assert not result.has_mismatches

    def test_result_with_mismatches(self):
        result = RecoveryResult(
            positions={},
            portfolio=PortfolioSnapshot(),
            pending_orders=[],
            mismatches=["quantity mismatch"],
        )
        assert result.has_mismatches
        assert not result.is_clean
