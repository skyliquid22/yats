"""Tests for research.execution.paper_trading — paper trading execution loop."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from research.execution.broker_alpaca import (
    Fill,
    OrderRequest,
    OrderResult,
    OrderSide,
    OrderStatus,
)
from research.execution.paper_trading import PaperTradingConfig, PaperTradingLoop
from research.execution.signal import Signal
from research.execution.state import Position


@pytest.fixture
def config():
    return PaperTradingConfig(
        experiment_id="test-exp-001",
        symbols=["AAPL", "MSFT"],
        initial_cash=100_000.0,
    )


class TestPaperTradingConfig:
    def test_defaults(self):
        cfg = PaperTradingConfig(
            experiment_id="exp-001",
            symbols=["AAPL"],
        )
        assert cfg.initial_cash == 100_000.0
        assert cfg.alpaca_paper is True
        assert cfg.loop_interval_s == 1.0

    def test_custom_config(self):
        cfg = PaperTradingConfig(
            experiment_id="exp-002",
            symbols=["AAPL", "MSFT", "GOOG"],
            initial_cash=500_000.0,
            ilp_host="questdb.local",
        )
        assert cfg.initial_cash == 500_000.0
        assert len(cfg.symbols) == 3
        assert cfg.ilp_host == "questdb.local"


class TestPaperTradingLoop:
    @patch("research.execution.paper_trading.load_production_risk_config")
    @patch("research.execution.paper_trading.AlpacaBrokerAdapter")
    @patch("research.execution.paper_trading.AlpacaBrokerConfig.from_env")
    def test_init(self, mock_from_env, mock_broker_cls, mock_risk, config):
        """PaperTradingLoop initializes with correct state."""
        mock_risk.return_value = MagicMock()
        mock_from_env.return_value = MagicMock()
        mock_broker_cls.return_value = MagicMock()

        loop = PaperTradingLoop(config)

        assert loop.cash == 100_000.0
        assert loop.loop_iteration == 0
        assert loop.is_running is False
        assert loop.positions == {}

    @patch("research.execution.paper_trading.load_production_risk_config")
    @patch("research.execution.paper_trading.AlpacaBrokerAdapter")
    @patch("research.execution.paper_trading.AlpacaBrokerConfig.from_env")
    def test_add_signals(self, mock_from_env, mock_broker_cls, mock_risk, config):
        """Signals can be added to the pending queue."""
        mock_risk.return_value = MagicMock()
        mock_from_env.return_value = MagicMock()
        mock_broker_cls.return_value = MagicMock()

        loop = PaperTradingLoop(config)

        signals = [
            Signal(
                timestamp=datetime(2024, 6, 15, tzinfo=timezone.utc),
                symbol="AAPL",
                target_position_pct=0.1,
                model_version="v1",
            ),
        ]
        loop.add_signals(signals)
        assert len(loop._pending_signals) == 1

    @patch("research.execution.paper_trading.load_production_risk_config")
    @patch("research.execution.paper_trading.AlpacaBrokerAdapter")
    @patch("research.execution.paper_trading.AlpacaBrokerConfig.from_env")
    def test_on_bar_updates_prices(self, mock_from_env, mock_broker_cls, mock_risk, config):
        """Bar callback updates latest prices."""
        mock_risk.return_value = MagicMock()
        mock_from_env.return_value = MagicMock()
        mock_broker_cls.return_value = MagicMock()

        loop = PaperTradingLoop(config)
        loop._on_bar({
            "symbol": "AAPL",
            "close": 185.50,
            "open": 184.0,
            "high": 186.0,
            "low": 183.0,
            "volume": 1000000,
        })

        prices = loop._get_current_prices()
        assert prices["AAPL"] == 185.50
        assert loop._last_bar_received is not None

    @patch("research.execution.paper_trading.load_production_risk_config")
    @patch("research.execution.paper_trading.AlpacaBrokerAdapter")
    @patch("research.execution.paper_trading.AlpacaBrokerConfig.from_env")
    def test_handle_fill_updates_state(self, mock_from_env, mock_broker_cls, mock_risk, config):
        """Fill handling updates positions and cash."""
        mock_risk.return_value = MagicMock()
        mock_from_env.return_value = MagicMock()
        mock_broker = MagicMock()
        mock_broker_cls.return_value = mock_broker

        loop = PaperTradingLoop(config)

        # Mock the state writers
        loop._position_writer = MagicMock()
        loop._position_writer.positions = {}
        loop._order_writer = MagicMock()

        fill_result = OrderResult(
            order_id="order-1",
            client_order_id="client-1",
            symbol="AAPL",
            side=OrderSide.BUY,
            status=OrderStatus.FILLED,
            submitted_at=datetime(2024, 6, 15, tzinfo=timezone.utc),
            filled_qty=10.0,
            filled_avg_price=185.0,
            filled_at=datetime(2024, 6, 15, 0, 1, tzinfo=timezone.utc),
        )

        loop._handle_fill(fill_result)

        # Cash should decrease by trade value
        assert loop.cash == 100_000.0 - (10.0 * 185.0)
        loop._position_writer.apply_fill.assert_called_once()
        loop._order_writer.write_fill.assert_called_once()
        assert loop._last_fill_received is not None

    @patch("research.execution.paper_trading.load_production_risk_config")
    @patch("research.execution.paper_trading.AlpacaBrokerAdapter")
    @patch("research.execution.paper_trading.AlpacaBrokerConfig.from_env")
    def test_handle_sell_fill_increases_cash(self, mock_from_env, mock_broker_cls, mock_risk, config):
        """Sell fill increases cash."""
        mock_risk.return_value = MagicMock()
        mock_from_env.return_value = MagicMock()
        mock_broker_cls.return_value = MagicMock()

        loop = PaperTradingLoop(config)
        loop._position_writer = MagicMock()
        loop._position_writer.positions = {}
        loop._order_writer = MagicMock()

        fill_result = OrderResult(
            order_id="order-2",
            client_order_id="client-2",
            symbol="AAPL",
            side=OrderSide.SELL,
            status=OrderStatus.FILLED,
            submitted_at=datetime(2024, 6, 15, tzinfo=timezone.utc),
            filled_qty=5.0,
            filled_avg_price=190.0,
            filled_at=datetime(2024, 6, 15, 0, 1, tzinfo=timezone.utc),
        )

        loop._handle_fill(fill_result)

        # Cash should increase by trade value
        assert loop.cash == 100_000.0 + (5.0 * 190.0)

    @patch("research.execution.paper_trading.load_production_risk_config")
    @patch("research.execution.paper_trading.AlpacaBrokerAdapter")
    @patch("research.execution.paper_trading.AlpacaBrokerConfig.from_env")
    def test_compute_nav(self, mock_from_env, mock_broker_cls, mock_risk, config):
        """NAV = position exposure + cash."""
        mock_risk.return_value = MagicMock()
        mock_from_env.return_value = MagicMock()
        mock_broker_cls.return_value = MagicMock()

        loop = PaperTradingLoop(config)
        loop._cash = 50_000.0

        positions = {
            "AAPL": Position(symbol="AAPL", quantity=10.0, avg_entry_price=150.0),
            "MSFT": Position(symbol="MSFT", quantity=20.0, avg_entry_price=300.0),
        }
        prices = {"AAPL": 160.0, "MSFT": 310.0}

        nav = loop._compute_nav(positions, prices)
        # 10*160 + 20*310 + 50000 = 1600 + 6200 + 50000 = 57800
        assert nav == 57_800.0

    @patch("research.execution.paper_trading.load_production_risk_config")
    @patch("research.execution.paper_trading.AlpacaBrokerAdapter")
    @patch("research.execution.paper_trading.AlpacaBrokerConfig.from_env")
    def test_stop_sets_running_false(self, mock_from_env, mock_broker_cls, mock_risk, config):
        """Stop method sets running flag to False."""
        mock_risk.return_value = MagicMock()
        mock_from_env.return_value = MagicMock()
        mock_broker_cls.return_value = MagicMock()

        loop = PaperTradingLoop(config)
        loop._running = True
        loop.stop()

        assert loop.is_running is False

    @patch("research.execution.paper_trading.load_production_risk_config")
    @patch("research.execution.paper_trading.AlpacaBrokerAdapter")
    @patch("research.execution.paper_trading.AlpacaBrokerConfig.from_env")
    def test_poll_pending_orders_handles_fill(self, mock_from_env, mock_broker_cls, mock_risk, config):
        """Polling pending orders processes fills correctly."""
        mock_risk.return_value = MagicMock()
        mock_from_env.return_value = MagicMock()
        mock_broker = MagicMock()
        mock_broker_cls.return_value = mock_broker

        loop = PaperTradingLoop(config)
        loop._position_writer = MagicMock()
        loop._position_writer.positions = {}
        loop._order_writer = MagicMock()

        # Add a pending order
        pending_result = OrderResult(
            order_id="order-1",
            client_order_id="client-1",
            symbol="AAPL",
            side=OrderSide.BUY,
            status=OrderStatus.SUBMITTED,
            submitted_at=datetime(2024, 6, 15, tzinfo=timezone.utc),
        )
        loop._pending_orders["order-1"] = pending_result

        # Mock broker returning filled status
        filled_result = OrderResult(
            order_id="order-1",
            client_order_id="client-1",
            symbol="AAPL",
            side=OrderSide.BUY,
            status=OrderStatus.FILLED,
            submitted_at=datetime(2024, 6, 15, tzinfo=timezone.utc),
            filled_qty=10.0,
            filled_avg_price=185.0,
            filled_at=datetime(2024, 6, 15, 0, 1, tzinfo=timezone.utc),
        )
        mock_broker.get_order_status.return_value = filled_result

        loop._poll_pending_orders()

        # Order should be removed from pending
        assert "order-1" not in loop._pending_orders
        # Fill should have been processed
        loop._position_writer.apply_fill.assert_called_once()
