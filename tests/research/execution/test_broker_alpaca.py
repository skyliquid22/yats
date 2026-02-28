"""Tests for research.execution.broker_alpaca — Alpaca broker adapter."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from research.execution.broker_alpaca import (
    AlpacaBrokerAdapter,
    AlpacaBrokerConfig,
    AlpacaStreamAdapter,
    BrokerError,
    Fill,
    OrderRequest,
    OrderResult,
    OrderSide,
    OrderStatus,
    SimBrokerAdapter,
    SimBrokerConfig,
    SimFill,
    _normalize_alpaca_status,
)


# ---------------------------------------------------------------------------
# Config tests
# ---------------------------------------------------------------------------


class TestAlpacaBrokerConfig:
    def test_paper_config(self):
        cfg = AlpacaBrokerConfig(
            api_key="key", api_secret="secret", paper=True
        )
        assert cfg.base_url == "https://paper-api.alpaca.markets"
        assert cfg.paper is True

    def test_live_requires_production_tier(self):
        with pytest.raises(ValueError, match="production_tier must be True"):
            AlpacaBrokerConfig(
                api_key="key",
                api_secret="secret",
                paper=False,
                production_tier=False,
            )

    def test_live_with_production_tier(self):
        cfg = AlpacaBrokerConfig(
            api_key="key",
            api_secret="secret",
            paper=False,
            production_tier=True,
        )
        assert cfg.base_url == "https://api.alpaca.markets"

    def test_from_env(self):
        with patch.dict("os.environ", {
            "APCA_API_KEY_ID": "test_key",
            "APCA_API_SECRET_KEY": "test_secret",
        }):
            cfg = AlpacaBrokerConfig.from_env(paper=True)
            assert cfg.api_key == "test_key"
            assert cfg.api_secret == "test_secret"
            assert cfg.paper is True

    def test_stream_url_paper(self):
        cfg = AlpacaBrokerConfig(api_key="k", api_secret="s", paper=True)
        assert "sandbox" in cfg.stream_url

    def test_stream_url_live(self):
        cfg = AlpacaBrokerConfig(
            api_key="k", api_secret="s", paper=False, production_tier=True
        )
        assert "sandbox" not in cfg.stream_url


# ---------------------------------------------------------------------------
# Status normalization
# ---------------------------------------------------------------------------


class TestStatusNormalization:
    @pytest.mark.parametrize(
        "alpaca_status,expected",
        [
            ("new", OrderStatus.SUBMITTED),
            ("filled", OrderStatus.FILLED),
            ("canceled", OrderStatus.CANCELLED),
            ("rejected", OrderStatus.REJECTED),
            ("expired", OrderStatus.EXPIRED),
            ("partially_filled", OrderStatus.PARTIALLY_FILLED),
            ("pending_new", OrderStatus.PENDING),
            ("unknown_status", OrderStatus.REJECTED),
        ],
    )
    def test_normalize(self, alpaca_status, expected):
        assert _normalize_alpaca_status(alpaca_status) == expected


# ---------------------------------------------------------------------------
# AlpacaBrokerAdapter tests (mocked)
# ---------------------------------------------------------------------------


class TestAlpacaBrokerAdapter:
    def _make_adapter(self) -> AlpacaBrokerAdapter:
        cfg = AlpacaBrokerConfig(api_key="test", api_secret="test", paper=True)
        with patch(
            "research.execution.broker_alpaca.AlpacaBrokerAdapter._create_client"
        ) as mock_create:
            mock_create.return_value = MagicMock()
            adapter = AlpacaBrokerAdapter(cfg)
        return adapter

    def test_missing_credentials(self):
        cfg = AlpacaBrokerConfig(api_key="", api_secret="", paper=True)
        with pytest.raises(BrokerError, match="Missing Alpaca credentials"):
            AlpacaBrokerAdapter(cfg)

    def test_submit_order_success(self):
        adapter = self._make_adapter()

        mock_order = MagicMock()
        mock_order.id = uuid.uuid4()
        mock_order.client_order_id = "test-123"
        mock_order.symbol = "AAPL"
        mock_order.side = MagicMock(value="buy")
        mock_order.status = MagicMock(value="filled")
        mock_order.submitted_at = datetime(2024, 1, 15, tzinfo=timezone.utc)
        mock_order.filled_qty = 10.0
        mock_order.filled_avg_price = 150.0
        mock_order.filled_at = datetime(2024, 1, 15, 0, 0, 1, tzinfo=timezone.utc)

        adapter._client.submit_order.return_value = mock_order

        # Mock the alpaca imports inside submit_order
        mock_alpaca_enums = MagicMock()
        mock_alpaca_requests = MagicMock()

        req = OrderRequest(
            symbol="AAPL", side=OrderSide.BUY, notional=1500.0
        )
        with patch.dict("sys.modules", {
            "alpaca": MagicMock(),
            "alpaca.trading": MagicMock(),
            "alpaca.trading.enums": mock_alpaca_enums,
            "alpaca.trading.requests": mock_alpaca_requests,
        }):
            result = adapter.submit_order(req)

        assert result.symbol == "AAPL"
        assert result.side == OrderSide.BUY
        assert result.status == OrderStatus.FILLED
        assert result.filled_qty == 10.0
        assert result.filled_avg_price == 150.0

    def test_submit_order_insufficient_funds(self):
        adapter = self._make_adapter()
        adapter._client.submit_order.side_effect = Exception(
            "insufficient buying power"
        )

        mock_alpaca_enums = MagicMock()
        mock_alpaca_requests = MagicMock()

        req = OrderRequest(
            symbol="AAPL", side=OrderSide.BUY, notional=1_000_000.0
        )
        with patch.dict("sys.modules", {
            "alpaca": MagicMock(),
            "alpaca.trading": MagicMock(),
            "alpaca.trading.enums": mock_alpaca_enums,
            "alpaca.trading.requests": mock_alpaca_requests,
        }):
            with pytest.raises(BrokerError, match="Insufficient funds"):
                adapter.submit_order(req)

    def test_get_order_status(self):
        adapter = self._make_adapter()

        mock_order = MagicMock()
        mock_order.id = uuid.uuid4()
        mock_order.client_order_id = "test-456"
        mock_order.symbol = "MSFT"
        mock_order.side = MagicMock(value="sell")
        mock_order.status = MagicMock(value="filled")
        mock_order.submitted_at = datetime(2024, 1, 15, tzinfo=timezone.utc)
        mock_order.filled_qty = 5.0
        mock_order.filled_avg_price = 375.0
        mock_order.filled_at = datetime(2024, 1, 15, 0, 0, 2, tzinfo=timezone.utc)

        adapter._client.get_order_by_id.return_value = mock_order

        result = adapter.get_order_status("some-order-id")
        assert result.symbol == "MSFT"
        assert result.side == OrderSide.SELL
        assert result.status == OrderStatus.FILLED

    def test_get_account(self):
        adapter = self._make_adapter()

        mock_acct = MagicMock()
        mock_acct.equity = "100000.00"
        mock_acct.buying_power = "200000.00"
        mock_acct.cash = "50000.00"
        mock_acct.portfolio_value = "100000.00"
        mock_acct.currency = "USD"
        mock_acct.status = MagicMock(value="ACTIVE")

        adapter._client.get_account.return_value = mock_acct

        acct = adapter.get_account()
        assert acct["equity"] == 100000.0
        assert acct["currency"] == "USD"


# ---------------------------------------------------------------------------
# SimBrokerAdapter tests
# ---------------------------------------------------------------------------


class TestSimBrokerConfig:
    def test_defaults(self):
        cfg = SimBrokerConfig()
        assert cfg.slippage_model == "flat"
        assert cfg.slippage_bp == 5.0
        assert cfg.transaction_cost_bp == 5.0

    def test_invalid_model(self):
        with pytest.raises(ValueError, match="slippage_model"):
            SimBrokerConfig(slippage_model="invalid")

    def test_negative_slippage(self):
        with pytest.raises(ValueError, match="slippage_bp"):
            SimBrokerConfig(slippage_bp=-1.0)

    def test_negative_cost(self):
        with pytest.raises(ValueError, match="transaction_cost_bp"):
            SimBrokerConfig(transaction_cost_bp=-1.0)


class TestSimBrokerAdapter:
    def test_flat_slippage_buy(self):
        cfg = SimBrokerConfig(slippage_bp=10.0, transaction_cost_bp=5.0)
        adapter = SimBrokerAdapter(cfg)

        fill = adapter.fill_order(
            symbol="AAPL",
            side=OrderSide.BUY,
            notional=10000.0,
            snapshot_price=150.0,
        )

        assert fill.symbol == "AAPL"
        assert fill.side == OrderSide.BUY
        assert fill.fill_price > 150.0  # buy pays more
        expected_price = 150.0 * (1.0 + 10.0 / 10_000)
        assert fill.fill_price == pytest.approx(expected_price)
        assert fill.slippage_bps == pytest.approx(10.0)
        assert fill.transaction_cost == pytest.approx(10000.0 * 5.0 / 10_000)

    def test_flat_slippage_sell(self):
        cfg = SimBrokerConfig(slippage_bp=10.0, transaction_cost_bp=5.0)
        adapter = SimBrokerAdapter(cfg)

        fill = adapter.fill_order(
            symbol="AAPL",
            side=OrderSide.SELL,
            notional=10000.0,
            snapshot_price=150.0,
        )

        assert fill.fill_price < 150.0  # sell receives less
        expected_price = 150.0 * (1.0 - 10.0 / 10_000)
        assert fill.fill_price == pytest.approx(expected_price)

    def test_volume_scaled_slippage(self):
        cfg = SimBrokerConfig(
            slippage_model="volume_scaled",
            slippage_bp=10.0,
        )
        adapter = SimBrokerAdapter(cfg)

        # High volume = less slippage
        fill_high = adapter.fill_order(
            symbol="AAPL",
            side=OrderSide.BUY,
            notional=10000.0,
            snapshot_price=150.0,
            volume=2_000_000,
        )

        # Low volume = more slippage
        fill_low = adapter.fill_order(
            symbol="AAPL",
            side=OrderSide.BUY,
            notional=10000.0,
            snapshot_price=150.0,
            volume=100_000,
        )

        assert fill_low.slippage_bps > fill_high.slippage_bps

    def test_zero_volume_max_slippage(self):
        cfg = SimBrokerConfig(
            slippage_model="volume_scaled",
            slippage_bp=10.0,
        )
        adapter = SimBrokerAdapter(cfg)

        fill = adapter.fill_order(
            symbol="AAPL",
            side=OrderSide.BUY,
            notional=10000.0,
            snapshot_price=150.0,
            volume=0.0,
        )

        # Zero volume should give max slippage (5x cap)
        assert fill.slippage_bps == pytest.approx(50.0)

    def test_order_count(self):
        adapter = SimBrokerAdapter(SimBrokerConfig())
        assert adapter.order_count == 0

        adapter.fill_order("AAPL", OrderSide.BUY, 1000.0, 150.0)
        assert adapter.order_count == 1

        adapter.fill_order("MSFT", OrderSide.SELL, 2000.0, 300.0)
        assert adapter.order_count == 2


# ---------------------------------------------------------------------------
# BrokerError tests
# ---------------------------------------------------------------------------


class TestBrokerError:
    def test_error_attrs(self):
        err = BrokerError("test error", code="TEST", retryable=True)
        assert str(err) == "test error"
        assert err.code == "TEST"
        assert err.retryable is True

    def test_defaults(self):
        err = BrokerError("default")
        assert err.code == ""
        assert err.retryable is False


# ---------------------------------------------------------------------------
# Domain types tests
# ---------------------------------------------------------------------------


class TestDomainTypes:
    def test_order_request(self):
        req = OrderRequest(symbol="AAPL", side=OrderSide.BUY, notional=1500.0)
        assert req.symbol == "AAPL"
        assert req.client_order_id is None

    def test_fill(self):
        fill = Fill(
            symbol="AAPL",
            side=OrderSide.BUY,
            filled_qty=10.0,
            filled_avg_price=150.0,
            filled_at=datetime(2024, 1, 15, tzinfo=timezone.utc),
        )
        assert fill.commission == 0.0

    def test_order_result(self):
        result = OrderResult(
            order_id="abc",
            client_order_id="def",
            symbol="AAPL",
            side=OrderSide.BUY,
            status=OrderStatus.FILLED,
            submitted_at=datetime(2024, 1, 15, tzinfo=timezone.utc),
        )
        assert result.filled_qty == 0.0
        assert result.reject_reason == ""


# ---------------------------------------------------------------------------
# AlpacaStreamAdapter tests
# ---------------------------------------------------------------------------


class TestAlpacaStreamAdapter:
    def test_init(self):
        cfg = AlpacaBrokerConfig(api_key="k", api_secret="s", paper=True)
        bars_received = []
        adapter = AlpacaStreamAdapter(cfg, ["AAPL"], bars_received.append)
        assert adapter.is_running is False
        assert adapter._symbols == ["AAPL"]

    def test_stop_without_start(self):
        cfg = AlpacaBrokerConfig(api_key="k", api_secret="s", paper=True)
        adapter = AlpacaStreamAdapter(cfg, ["AAPL"], lambda x: None)
        adapter.stop()  # should not raise
        assert adapter.is_running is False
