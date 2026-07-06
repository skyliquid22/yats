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


# ---------------------------------------------------------------------------
# Acceptance: daily_pnl + slippage + kill switch integration
# ---------------------------------------------------------------------------


class TestDailyPnlAndKillSwitch:
    """Acceptance: simulated losing day trips DAILY_LOSS → HALTED."""

    @patch("research.execution.paper_trading.load_production_risk_config")
    @patch("research.execution.paper_trading.AlpacaBrokerAdapter")
    @patch("research.execution.paper_trading.AlpacaBrokerConfig.from_env")
    @patch("research.execution.paper_trading.RiskDecisionsWriter")
    @patch("research.execution.paper_trading.HeartbeatWriter")
    @patch("research.execution.kill_switch.Sender")
    def test_daily_loss_trips_kill_switch(
        self,
        mock_ks_sender,
        mock_heartbeat_cls,
        mock_risk_writer_cls,
        mock_from_env,
        mock_broker_cls,
        mock_risk,
        config,
    ):
        """A simulated losing day (-6% vs -5% limit) halts trading TRADING→HALTED."""
        mock_risk.return_value = MagicMock(
            daily_loss_limit=-0.05,
            trailing_drawdown_limit=-0.20,
            max_broker_errors=5,
            data_staleness_threshold=300.0,
        )
        mock_broker_cls.return_value = MagicMock()

        loop = PaperTradingLoop(config)

        # Simulate session start NAV = 100_000; cash has dropped 6%
        loop._session_start_nav = 100_000.0
        loop._cash = 94_000.0  # -6% loss

        # Mock portfolio writer — returns snapshot reflecting computed daily_pnl
        mock_portfolio_writer = MagicMock()
        loop._portfolio_writer = mock_portfolio_writer

        def fake_write_snapshot(*, positions, cash, current_prices, daily_pnl=0.0):
            from research.execution.state import PortfolioSnapshot
            return PortfolioSnapshot(
                nav=cash,
                cash=cash,
                daily_pnl=daily_pnl,
                drawdown=-0.01,
            )

        mock_portfolio_writer.write_snapshot.side_effect = fake_write_snapshot

        # No positions, no bars — nav = cash only
        loop._position_writer = MagicMock()
        loop._position_writer.positions = {}

        # State starts TRADING
        assert loop.trading_state.value == "trading"

        # Evaluate triggers — daily_pnl = (94000 - 100000) / 100000 = -0.06 < -0.05 limit
        loop._evaluate_kill_triggers()

        # DAILY_LOSS should have fired → HALTED
        assert loop.trading_state.value == "halted"

    @patch("research.execution.paper_trading.load_production_risk_config")
    @patch("research.execution.paper_trading.AlpacaBrokerAdapter")
    @patch("research.execution.paper_trading.AlpacaBrokerConfig.from_env")
    @patch("research.execution.paper_trading.RiskDecisionsWriter")
    @patch("research.execution.paper_trading.HeartbeatWriter")
    @patch("research.execution.kill_switch.Sender")
    def test_fill_carries_nonzero_slippage_bps(
        self,
        mock_ks_sender,
        mock_heartbeat_cls,
        mock_risk_writer_cls,
        mock_from_env,
        mock_broker_cls,
        mock_risk,
        config,
    ):
        """Fills where fill price != decision price produce nonzero slippage_bps."""
        mock_risk.return_value = MagicMock()
        mock_broker_cls.return_value = MagicMock()

        loop = PaperTradingLoop(config)
        loop._position_writer = MagicMock()
        loop._position_writer.positions = {}
        loop._order_writer = MagicMock()

        # Record decision price for order at submit time: 100.0
        loop._order_decision_prices["order-slippage"] = 100.0

        # Fill arrives at 100.05 — 5 bps slippage
        fill_result = OrderResult(
            order_id="order-slippage",
            client_order_id="client-slippage",
            symbol="AAPL",
            side=OrderSide.BUY,
            status=OrderStatus.FILLED,
            submitted_at=datetime(2024, 6, 15, tzinfo=timezone.utc),
            filled_qty=10.0,
            filled_avg_price=100.05,
            filled_at=datetime(2024, 6, 15, 0, 1, tzinfo=timezone.utc),
        )

        loop._handle_fill(fill_result)

        call_kwargs = loop._order_writer.write_fill.call_args
        slippage_bps = call_kwargs.kwargs["slippage_bps"]
        assert slippage_bps > 0
        # (100.05 - 100.0) / 100.0 * 10000 = 5.0 bps
        assert abs(slippage_bps - 5.0) < 0.01

    @patch("research.execution.paper_trading.load_production_risk_config")
    @patch("research.execution.paper_trading.AlpacaBrokerAdapter")
    @patch("research.execution.paper_trading.AlpacaBrokerConfig.from_env")
    @patch("research.execution.paper_trading.RiskDecisionsWriter")
    @patch("research.execution.paper_trading.HeartbeatWriter")
    @patch("research.execution.kill_switch.Sender")
    def test_risk_engine_failure_halts_trading(
        self,
        mock_ks_sender,
        mock_heartbeat_cls,
        mock_risk_writer_cls,
        mock_from_env,
        mock_broker_cls,
        mock_risk,
        config,
    ):
        """Risk engine exception triggers RISK_ENGINE_FAILURE halt."""
        mock_risk.return_value = MagicMock()
        mock_broker_cls.return_value = MagicMock()

        loop = PaperTradingLoop(config)

        # Inject a signal to process
        sig = Signal(
            timestamp=datetime(2024, 6, 15, tzinfo=timezone.utc),
            symbol="AAPL",
            target_position_pct=0.1,
            model_version="v1",
        )
        loop._pending_signals = [sig]

        # Wire up state
        loop._position_writer = MagicMock()
        loop._position_writer.positions = {}
        loop._cash = 100_000.0
        loop._latest_bars = {"AAPL": {"close": 185.0}}

        with patch("research.execution.paper_trading.project_weights_full", side_effect=RuntimeError("engine down")):
            loop._process_signals()

        assert loop.trading_state.value == "halted"

    @patch("research.execution.paper_trading.load_production_risk_config")
    @patch("research.execution.paper_trading.AlpacaBrokerAdapter")
    @patch("research.execution.paper_trading.AlpacaBrokerConfig.from_env")
    @patch("research.execution.paper_trading.RiskDecisionsWriter")
    @patch("research.execution.paper_trading.HeartbeatWriter")
    def test_compute_daily_pnl(
        self, mock_heartbeat, mock_risk_writer, mock_from_env, mock_broker_cls, mock_risk, config,
    ):
        """Daily P&L is computed as fractional NAV change from session start."""
        mock_risk.return_value = MagicMock()
        mock_broker_cls.return_value = MagicMock()

        loop = PaperTradingLoop(config)
        loop._session_start_nav = 100_000.0

        assert loop._compute_daily_pnl(100_000.0) == 0.0
        assert abs(loop._compute_daily_pnl(105_000.0) - 0.05) < 1e-10
        assert abs(loop._compute_daily_pnl(97_000.0) - (-0.03)) < 1e-10


# ---------------------------------------------------------------------------
# Fix A: Idempotent fill application (no double-apply on writer failure)
# ---------------------------------------------------------------------------


class TestIdempotentFillApplication:
    """Fill is applied exactly once even if _handle_fill raises a non-BrokerError."""

    @patch("research.execution.paper_trading.load_production_risk_config")
    @patch("research.execution.paper_trading.AlpacaBrokerAdapter")
    @patch("research.execution.paper_trading.AlpacaBrokerConfig.from_env")
    def test_fill_not_reapplied_after_writer_failure(
        self, mock_from_env, mock_broker_cls, mock_risk, config
    ):
        """If _handle_fill raises, order is removed from pending and fill not retried."""
        mock_risk.return_value = MagicMock()
        mock_broker = MagicMock()
        mock_broker_cls.return_value = mock_broker

        loop = PaperTradingLoop(config)
        loop._position_writer = MagicMock()
        loop._position_writer.positions = {}
        loop._order_writer = MagicMock()

        filled_result = OrderResult(
            order_id="order-dup",
            client_order_id="client-dup",
            symbol="AAPL",
            side=OrderSide.BUY,
            status=OrderStatus.FILLED,
            submitted_at=datetime(2024, 6, 15, tzinfo=timezone.utc),
            filled_qty=10.0,
            filled_avg_price=185.0,
            filled_at=datetime(2024, 6, 15, 0, 1, tzinfo=timezone.utc),
        )
        mock_broker.get_order_status.return_value = filled_result

        loop._pending_orders["order-dup"] = filled_result

        # Simulate QuestDB write failure inside _handle_fill
        call_count = 0

        def failing_handle_fill(result):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("QuestDB sender failure")

        loop._handle_fill = failing_handle_fill

        # First poll — _handle_fill raises, but order is removed from pending
        loop._poll_pending_orders()
        assert "order-dup" not in loop._pending_orders
        assert "order-dup" in loop._applied_fills
        assert call_count == 1

        # Re-add the order to pending (simulates a race/retry scenario)
        loop._pending_orders["order-dup"] = filled_result
        loop._poll_pending_orders()

        # _handle_fill must NOT be called again (idempotency check)
        assert call_count == 1
        assert "order-dup" not in loop._pending_orders

    @patch("research.execution.paper_trading.load_production_risk_config")
    @patch("research.execution.paper_trading.AlpacaBrokerAdapter")
    @patch("research.execution.paper_trading.AlpacaBrokerConfig.from_env")
    def test_fill_applied_once_on_success(
        self, mock_from_env, mock_broker_cls, mock_risk, config
    ):
        """Successful fill is applied once and order removed from pending."""
        mock_risk.return_value = MagicMock()
        mock_broker = MagicMock()
        mock_broker_cls.return_value = mock_broker

        loop = PaperTradingLoop(config)
        loop._position_writer = MagicMock()
        loop._position_writer.positions = {}
        loop._order_writer = MagicMock()

        filled_result = OrderResult(
            order_id="order-ok",
            client_order_id="client-ok",
            symbol="AAPL",
            side=OrderSide.BUY,
            status=OrderStatus.FILLED,
            submitted_at=datetime(2024, 6, 15, tzinfo=timezone.utc),
            filled_qty=5.0,
            filled_avg_price=100.0,
            filled_at=datetime(2024, 6, 15, 0, 1, tzinfo=timezone.utc),
        )
        mock_broker.get_order_status.return_value = filled_result
        loop._pending_orders["order-ok"] = filled_result

        loop._poll_pending_orders()

        assert "order-ok" not in loop._pending_orders
        assert "order-ok" in loop._applied_fills
        loop._position_writer.apply_fill.assert_called_once()


# ---------------------------------------------------------------------------
# Fix B: Offline fills applied during recovery
# ---------------------------------------------------------------------------


class TestOfflineFillRecovery:
    """Fills that occurred while offline are applied during crash recovery."""

    @patch("research.execution.paper_trading.load_production_risk_config")
    @patch("research.execution.paper_trading.AlpacaBrokerAdapter")
    @patch("research.execution.paper_trading.AlpacaBrokerConfig.from_env")
    @patch("research.execution.paper_trading.recover_state")
    def test_offline_fills_applied_during_recover_state(
        self, mock_recover, mock_from_env, mock_broker_cls, mock_risk, config
    ):
        """_recover_state applies filled_offline orders to positions and cash."""
        from research.execution.recovery import RecoveryResult
        from research.execution.state import PortfolioSnapshot

        mock_risk.return_value = MagicMock()
        mock_broker_cls.return_value = MagicMock()

        loop = PaperTradingLoop(config)
        loop._position_writer = MagicMock()
        loop._position_writer.positions = {}
        loop._order_writer = MagicMock()

        offline_fill = OrderResult(
            order_id="offline-order-1",
            client_order_id="client-offline-1",
            symbol="AAPL",
            side=OrderSide.BUY,
            status=OrderStatus.FILLED,
            submitted_at=datetime(2024, 6, 15, tzinfo=timezone.utc),
            filled_qty=10.0,
            filled_avg_price=150.0,
            filled_at=datetime(2024, 6, 15, 0, 5, tzinfo=timezone.utc),
        )

        mock_recover.return_value = RecoveryResult(
            positions={},
            portfolio=PortfolioSnapshot(nav=0.0, cash=0.0),
            pending_orders=[],
            filled_offline=[offline_fill],
        )

        loop._recover_state()

        # The offline fill should have been applied — cash decremented
        assert loop.cash == 100_000.0 - (10.0 * 150.0)
        # Position writer should have been called
        loop._position_writer.apply_fill.assert_called_once()
        # Order ID marked as applied so it won't double-apply
        assert "offline-order-1" in loop._applied_fills

    @patch("research.execution.paper_trading.load_production_risk_config")
    @patch("research.execution.paper_trading.AlpacaBrokerAdapter")
    @patch("research.execution.paper_trading.AlpacaBrokerConfig.from_env")
    @patch("research.execution.paper_trading.recover_state")
    def test_offline_fill_writer_error_does_not_abort_recovery(
        self, mock_recover, mock_from_env, mock_broker_cls, mock_risk, config
    ):
        """A write failure for one offline fill doesn't crash the whole recovery."""
        from research.execution.recovery import RecoveryResult
        from research.execution.state import PortfolioSnapshot

        mock_risk.return_value = MagicMock()
        mock_broker_cls.return_value = MagicMock()

        loop = PaperTradingLoop(config)
        loop._position_writer = MagicMock()
        loop._position_writer.apply_fill.side_effect = RuntimeError("QuestDB down")
        loop._position_writer.positions = {}
        loop._order_writer = MagicMock()

        offline_fill = OrderResult(
            order_id="offline-fail",
            client_order_id="client-offline-fail",
            symbol="MSFT",
            side=OrderSide.BUY,
            status=OrderStatus.FILLED,
            submitted_at=datetime(2024, 6, 15, tzinfo=timezone.utc),
            filled_qty=5.0,
            filled_avg_price=300.0,
        )

        mock_recover.return_value = RecoveryResult(
            positions={},
            portfolio=PortfolioSnapshot(nav=0.0, cash=0.0),
            pending_orders=[],
            filled_offline=[offline_fill],
        )

        # Should not raise — recovery continues despite writer failure
        loop._recover_state()


# ---------------------------------------------------------------------------
# Fix C: Partial fills on terminal orders and PARTIALLY_FILLED handling
# ---------------------------------------------------------------------------


class TestPartialFillHandling:
    """Partial fills on terminal/partial orders are correctly applied."""

    @patch("research.execution.paper_trading.load_production_risk_config")
    @patch("research.execution.paper_trading.AlpacaBrokerAdapter")
    @patch("research.execution.paper_trading.AlpacaBrokerConfig.from_env")
    def test_cancelled_order_with_partial_fill_applies_fill(
        self, mock_from_env, mock_broker_cls, mock_risk, config
    ):
        """A CANCELLED order with filled_qty > 0 applies the partial fill."""
        mock_risk.return_value = MagicMock()
        mock_broker = MagicMock()
        mock_broker_cls.return_value = mock_broker

        loop = PaperTradingLoop(config)
        loop._position_writer = MagicMock()
        loop._position_writer.positions = {}
        loop._order_writer = MagicMock()

        cancelled_partial = OrderResult(
            order_id="order-partial",
            client_order_id="client-partial",
            symbol="AAPL",
            side=OrderSide.BUY,
            status=OrderStatus.CANCELLED,
            submitted_at=datetime(2024, 6, 15, tzinfo=timezone.utc),
            filled_qty=3.0,  # partial fill before cancellation
            filled_avg_price=180.0,
            filled_at=datetime(2024, 6, 15, 0, 1, tzinfo=timezone.utc),
        )
        mock_broker.get_order_status.return_value = cancelled_partial
        loop._pending_orders["order-partial"] = cancelled_partial

        loop._poll_pending_orders()

        # Order removed from pending
        assert "order-partial" not in loop._pending_orders
        # Partial fill was applied to positions
        loop._position_writer.apply_fill.assert_called_once()
        # Cash decreased by partial fill value
        assert loop.cash == pytest.approx(100_000.0 - 3.0 * 180.0)
        # The fill was written with FILLED status (for position reconstruction)
        write_fill_call = loop._order_writer.write_fill.call_args
        applied_result = write_fill_call.args[0]
        assert applied_result.status == OrderStatus.FILLED

    @patch("research.execution.paper_trading.load_production_risk_config")
    @patch("research.execution.paper_trading.AlpacaBrokerAdapter")
    @patch("research.execution.paper_trading.AlpacaBrokerConfig.from_env")
    def test_cancelled_order_with_no_fill_writes_terminal_row(
        self, mock_from_env, mock_broker_cls, mock_risk, config
    ):
        """A CANCELLED order with filled_qty == 0 writes terminal status, no fill."""
        mock_risk.return_value = MagicMock()
        mock_broker = MagicMock()
        mock_broker_cls.return_value = mock_broker

        loop = PaperTradingLoop(config)
        loop._position_writer = MagicMock()
        loop._position_writer.positions = {}
        loop._order_writer = MagicMock()

        cancelled_clean = OrderResult(
            order_id="order-cancelled-clean",
            client_order_id="client-cancelled-clean",
            symbol="AAPL",
            side=OrderSide.BUY,
            status=OrderStatus.CANCELLED,
            submitted_at=datetime(2024, 6, 15, tzinfo=timezone.utc),
            filled_qty=0.0,
        )
        mock_broker.get_order_status.return_value = cancelled_clean
        loop._pending_orders["order-cancelled-clean"] = cancelled_clean

        loop._poll_pending_orders()

        assert "order-cancelled-clean" not in loop._pending_orders
        loop._position_writer.apply_fill.assert_not_called()
        loop._order_writer.write_order.assert_called_once()
        assert loop.cash == 100_000.0  # unchanged

    @patch("research.execution.paper_trading.load_production_risk_config")
    @patch("research.execution.paper_trading.AlpacaBrokerAdapter")
    @patch("research.execution.paper_trading.AlpacaBrokerConfig.from_env")
    def test_partially_filled_writes_ledger_stays_in_pending(
        self, mock_from_env, mock_broker_cls, mock_risk, config
    ):
        """PARTIALLY_FILLED orders write a ledger row but remain in _pending_orders."""
        mock_risk.return_value = MagicMock()
        mock_broker = MagicMock()
        mock_broker_cls.return_value = mock_broker

        loop = PaperTradingLoop(config)
        loop._position_writer = MagicMock()
        loop._position_writer.positions = {}
        loop._order_writer = MagicMock()

        partial_result = OrderResult(
            order_id="order-partial-live",
            client_order_id="client-partial-live",
            symbol="MSFT",
            side=OrderSide.BUY,
            status=OrderStatus.PARTIALLY_FILLED,
            submitted_at=datetime(2024, 6, 15, tzinfo=timezone.utc),
            filled_qty=2.0,
            filled_avg_price=300.0,
        )
        mock_broker.get_order_status.return_value = partial_result
        loop._pending_orders["order-partial-live"] = partial_result

        loop._poll_pending_orders()

        # Order must still be in pending (not yet complete)
        assert "order-partial-live" in loop._pending_orders
        # No position update yet
        loop._position_writer.apply_fill.assert_not_called()
        # Ledger updated
        loop._order_writer.write_order.assert_called_once()
