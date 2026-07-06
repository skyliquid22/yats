"""Tests for research.execution.state — position, order, and portfolio state writers."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from research.execution.broker_alpaca import (
    Fill,
    OrderResult,
    OrderSide,
    OrderStatus,
)
from research.execution.state import (
    OrderWriter,
    PortfolioStateWriter,
    Position,
    PositionWriter,
    PortfolioSnapshot,
    StateWriterConfig,
)


# ---------------------------------------------------------------------------
# Position tests (in-memory state)
# ---------------------------------------------------------------------------


class TestPosition:
    def test_buy_fill(self):
        pos = Position(symbol="AAPL")
        fill = Fill(
            symbol="AAPL",
            side=OrderSide.BUY,
            filled_qty=10.0,
            filled_avg_price=150.0,
            filled_at=datetime(2024, 1, 15, tzinfo=timezone.utc),
        )
        pos.apply_fill(fill)

        assert pos.quantity == 10.0
        assert pos.avg_entry_price == 150.0
        assert pos.cost_basis == 1500.0

    def test_buy_then_sell_realizes_pnl(self):
        pos = Position(symbol="AAPL")

        # Buy 10 @ 150
        buy = Fill(
            symbol="AAPL",
            side=OrderSide.BUY,
            filled_qty=10.0,
            filled_avg_price=150.0,
            filled_at=datetime(2024, 1, 15, tzinfo=timezone.utc),
        )
        pos.apply_fill(buy)

        # Sell 5 @ 160 — profit of 5 * 10 = 50
        sell = Fill(
            symbol="AAPL",
            side=OrderSide.SELL,
            filled_qty=5.0,
            filled_avg_price=160.0,
            filled_at=datetime(2024, 1, 16, tzinfo=timezone.utc),
        )
        pos.apply_fill(sell)

        assert pos.quantity == 5.0
        assert pos.avg_entry_price == 150.0
        assert pos.realized_pnl == pytest.approx(50.0)
        assert pos.cost_basis == pytest.approx(750.0)

    def test_commission_reduces_pnl(self):
        pos = Position(symbol="AAPL")

        buy = Fill(
            symbol="AAPL",
            side=OrderSide.BUY,
            filled_qty=10.0,
            filled_avg_price=150.0,
            filled_at=datetime(2024, 1, 15, tzinfo=timezone.utc),
            commission=1.50,
        )
        pos.apply_fill(buy)
        assert pos.realized_pnl == pytest.approx(-1.50)

    def test_multiple_buys_avg_price(self):
        pos = Position(symbol="AAPL")

        # Buy 10 @ 150
        pos.apply_fill(Fill("AAPL", OrderSide.BUY, 10.0, 150.0, datetime.now(timezone.utc)))
        # Buy 10 @ 160
        pos.apply_fill(Fill("AAPL", OrderSide.BUY, 10.0, 160.0, datetime.now(timezone.utc)))

        assert pos.quantity == 20.0
        assert pos.avg_entry_price == pytest.approx(155.0)

    def test_notional_and_unrealized_pnl(self):
        pos = Position(symbol="AAPL", quantity=10.0, avg_entry_price=150.0)

        assert pos.notional(160.0) == 1600.0
        assert pos.unrealized_pnl(160.0) == pytest.approx(100.0)  # 10 * 10
        assert pos.unrealized_pnl(140.0) == pytest.approx(-100.0)

    def test_zero_position_unrealized(self):
        pos = Position(symbol="AAPL", quantity=0.0)
        assert pos.unrealized_pnl(150.0) == 0.0

    # --- Short position tests ---

    def test_short_open(self):
        pos = Position(symbol="TSLA")
        fill = Fill("TSLA", OrderSide.SELL, 10.0, 200.0, datetime.now(timezone.utc))
        pos.apply_fill(fill)

        assert pos.quantity == -10.0
        assert pos.avg_entry_price == pytest.approx(200.0)
        assert pos.cost_basis == pytest.approx(-2000.0)

    def test_short_add_to_short(self):
        pos = Position(symbol="TSLA")
        # Open short: 10 @ 200
        pos.apply_fill(Fill("TSLA", OrderSide.SELL, 10.0, 200.0, datetime.now(timezone.utc)))
        # Add to short: 10 more @ 220
        pos.apply_fill(Fill("TSLA", OrderSide.SELL, 10.0, 220.0, datetime.now(timezone.utc)))

        assert pos.quantity == -20.0
        assert pos.avg_entry_price == pytest.approx(210.0)  # (2000+2200)/20

    def test_short_cover_profit(self):
        pos = Position(symbol="TSLA")
        pos.apply_fill(Fill("TSLA", OrderSide.SELL, 10.0, 200.0, datetime.now(timezone.utc)))
        # Cover at lower price = profit
        pos.apply_fill(Fill("TSLA", OrderSide.BUY, 10.0, 180.0, datetime.now(timezone.utc)))

        assert pos.quantity == pytest.approx(0.0, abs=1e-9)
        assert pos.realized_pnl == pytest.approx(200.0)  # 10 * (200 - 180)

    def test_short_cover_loss(self):
        pos = Position(symbol="TSLA")
        pos.apply_fill(Fill("TSLA", OrderSide.SELL, 10.0, 200.0, datetime.now(timezone.utc)))
        # Cover at higher price = loss
        pos.apply_fill(Fill("TSLA", OrderSide.BUY, 10.0, 220.0, datetime.now(timezone.utc)))

        assert pos.realized_pnl == pytest.approx(-200.0)  # 10 * (200 - 220)

    def test_short_partial_cover(self):
        pos = Position(symbol="TSLA")
        pos.apply_fill(Fill("TSLA", OrderSide.SELL, 10.0, 200.0, datetime.now(timezone.utc)))
        # Cover 5 @ 180
        pos.apply_fill(Fill("TSLA", OrderSide.BUY, 5.0, 180.0, datetime.now(timezone.utc)))

        assert pos.quantity == -5.0
        assert pos.avg_entry_price == pytest.approx(200.0)  # unchanged
        assert pos.realized_pnl == pytest.approx(100.0)  # 5 * (200 - 180)

    def test_long_to_short_flip(self):
        pos = Position(symbol="AAPL")
        # Buy 10 @ 150
        pos.apply_fill(Fill("AAPL", OrderSide.BUY, 10.0, 150.0, datetime.now(timezone.utc)))
        # Sell 20 @ 160 — realize PnL on 10 long, open 10 short @ 160
        pos.apply_fill(Fill("AAPL", OrderSide.SELL, 20.0, 160.0, datetime.now(timezone.utc)))

        assert pos.quantity == -10.0
        assert pos.avg_entry_price == pytest.approx(160.0)
        assert pos.realized_pnl == pytest.approx(100.0)  # 10 * (160 - 150)

    def test_short_to_long_flip(self):
        pos = Position(symbol="AAPL")
        # Short 10 @ 200
        pos.apply_fill(Fill("AAPL", OrderSide.SELL, 10.0, 200.0, datetime.now(timezone.utc)))
        # Buy 20 @ 180 — cover 10 short (profit 200), open 10 long @ 180
        pos.apply_fill(Fill("AAPL", OrderSide.BUY, 20.0, 180.0, datetime.now(timezone.utc)))

        assert pos.quantity == 10.0
        assert pos.avg_entry_price == pytest.approx(180.0)
        assert pos.realized_pnl == pytest.approx(200.0)  # 10 * (200 - 180)

    def test_short_unrealized_pnl(self):
        pos = Position(symbol="TSLA", quantity=-10.0, avg_entry_price=200.0)

        # Price drops → profit for short
        assert pos.unrealized_pnl(180.0) == pytest.approx(200.0)  # -10*(180-200)
        # Price rises → loss for short
        assert pos.unrealized_pnl(220.0) == pytest.approx(-200.0)  # -10*(220-200)


# ---------------------------------------------------------------------------
# PositionWriter tests
# ---------------------------------------------------------------------------


class TestPositionWriter:
    def test_apply_fill_creates_position(self):
        cfg = StateWriterConfig()
        writer = PositionWriter(cfg, "exp-1", "paper")

        fill = Fill("AAPL", OrderSide.BUY, 10.0, 150.0, datetime.now(timezone.utc))

        with patch("research.execution.state.Sender"):
            writer.apply_fill(fill)

        assert "AAPL" in writer.positions
        assert writer.positions["AAPL"].quantity == 10.0

    def test_apply_fill_removes_closed_position(self):
        cfg = StateWriterConfig()
        writer = PositionWriter(cfg, "exp-1", "paper")

        with patch("research.execution.state.Sender"):
            # Buy 10
            writer.apply_fill(
                Fill("AAPL", OrderSide.BUY, 10.0, 150.0, datetime.now(timezone.utc))
            )
            assert "AAPL" in writer.positions

            # Sell 10 — position closes
            writer.apply_fill(
                Fill("AAPL", OrderSide.SELL, 10.0, 160.0, datetime.now(timezone.utc))
            )
            assert "AAPL" not in writer.positions

    def test_restore_positions(self):
        cfg = StateWriterConfig()
        writer = PositionWriter(cfg, "exp-1", "paper")

        restored = {
            "AAPL": Position("AAPL", 10.0, 150.0),
            "MSFT": Position("MSFT", 5.0, 300.0),
        }
        writer.restore_positions(restored)
        assert len(writer.positions) == 2
        assert writer.positions["AAPL"].quantity == 10.0


# ---------------------------------------------------------------------------
# OrderWriter tests
# ---------------------------------------------------------------------------


class TestOrderWriter:
    def test_write_order(self):
        cfg = StateWriterConfig()
        writer = OrderWriter(cfg, "exp-1", "paper", dagster_run_id="run-1")

        result = OrderResult(
            order_id="order-1",
            client_order_id="client-1",
            symbol="AAPL",
            side=OrderSide.BUY,
            status=OrderStatus.SUBMITTED,
            submitted_at=datetime(2024, 1, 15, tzinfo=timezone.utc),
        )

        with patch("research.execution.state.Sender") as mock_sender_cls:
            mock_sender = MagicMock()
            mock_sender_cls.return_value.__enter__ = MagicMock(return_value=mock_sender)
            mock_sender_cls.return_value.__exit__ = MagicMock(return_value=False)

            writer.write_order(result, quantity=10.0)

            mock_sender.row.assert_called_once()
            call_args = mock_sender.row.call_args
            assert call_args[0][0] == "orders"
            assert call_args[1]["symbols"]["symbol"] == "AAPL"
            assert call_args[1]["symbols"]["side"] == "buy"
            assert call_args[1]["symbols"]["status"] == "submitted"

    def test_write_fill(self):
        cfg = StateWriterConfig()
        writer = OrderWriter(cfg, "exp-1", "paper")

        result = OrderResult(
            order_id="order-1",
            client_order_id="client-1",
            symbol="AAPL",
            side=OrderSide.BUY,
            status=OrderStatus.FILLED,
            submitted_at=datetime(2024, 1, 15, tzinfo=timezone.utc),
            filled_qty=10.0,
            filled_avg_price=150.0,
        )
        fill = Fill("AAPL", OrderSide.BUY, 10.0, 150.0, datetime.now(timezone.utc), commission=0.5)

        with patch("research.execution.state.Sender") as mock_sender_cls:
            mock_sender = MagicMock()
            mock_sender_cls.return_value.__enter__ = MagicMock(return_value=mock_sender)
            mock_sender_cls.return_value.__exit__ = MagicMock(return_value=False)

            writer.write_fill(result, fill, slippage_bps=2.5)

            call_args = mock_sender.row.call_args
            assert call_args[1]["columns"]["fill_price"] == 150.0
            assert call_args[1]["columns"]["fees"] == 0.5
            assert call_args[1]["columns"]["slippage_bps"] == 2.5


# ---------------------------------------------------------------------------
# PortfolioStateWriter tests
# ---------------------------------------------------------------------------


class TestPortfolioStateWriter:
    def test_write_snapshot(self):
        cfg = StateWriterConfig()
        writer = PortfolioStateWriter(cfg, "exp-1", "paper")

        positions = {
            "AAPL": Position("AAPL", 10.0, 150.0),
            "MSFT": Position("MSFT", 5.0, 300.0),
        }
        prices = {"AAPL": 155.0, "MSFT": 310.0}

        with patch("research.execution.state.Sender") as mock_sender_cls:
            mock_sender = MagicMock()
            mock_sender_cls.return_value.__enter__ = MagicMock(return_value=mock_sender)
            mock_sender_cls.return_value.__exit__ = MagicMock(return_value=False)

            snap = writer.write_snapshot(
                positions=positions,
                cash=50_000.0,
                current_prices=prices,
            )

        # 10*155 + 5*310 = 1550 + 1550 = 3100 gross
        assert snap.gross_exposure == pytest.approx(3100.0)
        assert snap.nav == pytest.approx(53100.0)  # gross + cash
        assert snap.num_positions == 2
        assert snap.peak_nav == pytest.approx(53100.0)
        assert snap.drawdown == pytest.approx(0.0)

    def test_drawdown_tracking(self):
        cfg = StateWriterConfig()
        writer = PortfolioStateWriter(cfg, "exp-1", "paper")

        positions = {"AAPL": Position("AAPL", 10.0, 100.0)}

        with patch("research.execution.state.Sender"):
            # First snapshot: NAV = 1000 + 50000 = 51000
            snap1 = writer.write_snapshot(
                positions=positions,
                cash=50_000.0,
                current_prices={"AAPL": 100.0},
            )
            assert snap1.peak_nav == pytest.approx(51000.0)

            # Second snapshot: price drops, NAV = 900 + 50000 = 50900
            snap2 = writer.write_snapshot(
                positions=positions,
                cash=50_000.0,
                current_prices={"AAPL": 90.0},
            )
            assert snap2.peak_nav == pytest.approx(51000.0)  # peak unchanged
            assert snap2.drawdown < 0  # negative drawdown

    def test_restore_peak_nav(self):
        cfg = StateWriterConfig()
        writer = PortfolioStateWriter(cfg, "exp-1", "paper")
        writer.restore_peak_nav(100_000.0)
        assert writer._peak_nav == 100_000.0

    def test_nav_with_short_position(self):
        """Short position: cash increases (receives proceeds), net_exposure decreases."""
        cfg = StateWriterConfig()
        writer = PortfolioStateWriter(cfg, "exp-1", "paper")

        # $100k account, long $50k AAPL, short $30k TSLA
        # After short TSLA: cash = $100k + $30k = $130k
        positions = {
            "AAPL": Position("AAPL", 333.0, 150.0),   # ~$50k long
            "TSLA": Position("TSLA", -100.0, 300.0),  # $30k short
        }
        # Approximate prices
        prices = {"AAPL": 150.0, "TSLA": 300.0}
        cash = 130_000.0  # initial 100k + 30k short proceeds

        with patch("research.execution.state.Sender"):
            snap = writer.write_snapshot(positions=positions, cash=cash, current_prices=prices)

        # net_exposure = 333*150 + (-100)*300 = 49950 - 30000 = 19950
        expected_net = 333 * 150.0 + (-100) * 300.0
        assert snap.net_exposure == pytest.approx(expected_net)
        assert snap.nav == pytest.approx(expected_net + cash)
        # gross_exposure = abs(long) + abs(short) = 49950 + 30000 = 79950
        assert snap.gross_exposure == pytest.approx(333 * 150.0 + 100 * 300.0)
        # NAV must be less than gross + cash (which would double-count shorts)
        assert snap.nav < snap.gross_exposure + cash
