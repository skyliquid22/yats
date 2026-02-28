"""Tests for research.execution.order_translator — signal-to-order translation."""

from __future__ import annotations

from datetime import datetime, timezone

from research.execution.broker_alpaca import OrderSide
from research.execution.order_translator import (
    TranslationResult,
    translate_signal,
    translate_signals,
)
from research.execution.signal import Signal
from research.execution.state import Position


def _make_signal(
    symbol: str = "AAPL",
    target_pct: float = 0.1,
    ts: datetime | None = None,
) -> Signal:
    return Signal(
        timestamp=ts or datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc),
        symbol=symbol,
        target_position_pct=target_pct,
        model_version="test_v1",
    )


# ---------------------------------------------------------------------------
# translate_signal tests
# ---------------------------------------------------------------------------


class TestTranslateSignal:
    def test_buy_from_flat(self):
        """No position → buy order."""
        sig = _make_signal(target_pct=0.1)
        result = translate_signal(
            sig,
            nav=100_000.0,
            positions={},
            current_prices={"AAPL": 150.0},
        )
        assert result.order is not None
        assert result.order.side == OrderSide.BUY
        assert result.order.notional == 10_000.0  # 100k * 0.1
        assert result.order.symbol == "AAPL"
        assert result.reason == "order_generated"

    def test_sell_to_reduce(self):
        """Existing long → sell to reduce."""
        sig = _make_signal(target_pct=0.05)
        pos = Position(symbol="AAPL", quantity=100.0, avg_entry_price=150.0)
        result = translate_signal(
            sig,
            nav=100_000.0,
            positions={"AAPL": pos},
            current_prices={"AAPL": 150.0},
        )
        assert result.order is not None
        assert result.order.side == OrderSide.SELL
        # desired = 5000, current = 100*150 = 15000, delta = -10000
        assert result.order.notional == 10_000.0
        assert result.reason == "order_generated"

    def test_sell_to_flat(self):
        """Target 0% → sell entire position."""
        sig = _make_signal(target_pct=0.0)
        pos = Position(symbol="AAPL", quantity=50.0, avg_entry_price=200.0)
        result = translate_signal(
            sig,
            nav=100_000.0,
            positions={"AAPL": pos},
            current_prices={"AAPL": 200.0},
        )
        assert result.order is not None
        assert result.order.side == OrderSide.SELL
        assert result.order.notional == 10_000.0  # 50 * 200

    def test_zero_target_no_position(self):
        """Target 0% with no position → no order."""
        sig = _make_signal(target_pct=0.0)
        result = translate_signal(
            sig,
            nav=100_000.0,
            positions={},
            current_prices={"AAPL": 150.0},
        )
        assert result.order is None
        assert result.reason == "zero_target"

    def test_below_threshold(self):
        """Delta below min_order_notional → no order."""
        sig = _make_signal(target_pct=0.1)
        # Position already near target: 100k * 0.1 = 10000, position = 66.6 * 150 = 9990
        pos = Position(symbol="AAPL", quantity=66.6, avg_entry_price=150.0)
        result = translate_signal(
            sig,
            nav=100_000.0,
            positions={"AAPL": pos},
            current_prices={"AAPL": 150.0},
            min_order_notional=100.0,
        )
        # delta = 10000 - 9990 = 10, below 100
        assert result.order is None
        assert result.reason == "below_threshold"

    def test_buy_to_increase(self):
        """Existing long → buy more to increase."""
        sig = _make_signal(target_pct=0.2)
        pos = Position(symbol="AAPL", quantity=50.0, avg_entry_price=150.0)
        result = translate_signal(
            sig,
            nav=100_000.0,
            positions={"AAPL": pos},
            current_prices={"AAPL": 150.0},
        )
        assert result.order is not None
        assert result.order.side == OrderSide.BUY
        # desired = 20000, current = 50*150 = 7500, delta = 12500
        assert result.order.notional == 12_500.0

    def test_negative_target_from_flat(self):
        """Negative target from flat → sell (short)."""
        sig = _make_signal(target_pct=-0.1)
        result = translate_signal(
            sig,
            nav=100_000.0,
            positions={},
            current_prices={"AAPL": 150.0},
        )
        assert result.order is not None
        assert result.order.side == OrderSide.SELL
        assert result.order.notional == 10_000.0

    def test_result_captures_notionals(self):
        """TranslationResult includes desired/current/delta notionals."""
        sig = _make_signal(target_pct=0.15)
        pos = Position(symbol="AAPL", quantity=20.0, avg_entry_price=150.0)
        result = translate_signal(
            sig,
            nav=100_000.0,
            positions={"AAPL": pos},
            current_prices={"AAPL": 200.0},
        )
        assert result.desired_notional == 15_000.0  # 100k * 0.15
        assert result.current_notional == 4_000.0  # 20 * 200
        assert result.delta_notional == 11_000.0

    def test_client_order_id_generated(self):
        """Each order gets a unique client_order_id."""
        sig = _make_signal(target_pct=0.1)
        r1 = translate_signal(sig, 100_000.0, {}, {"AAPL": 150.0})
        r2 = translate_signal(sig, 100_000.0, {}, {"AAPL": 150.0})
        assert r1.order is not None and r2.order is not None
        assert r1.order.client_order_id != r2.order.client_order_id


# ---------------------------------------------------------------------------
# translate_signals (batch) tests
# ---------------------------------------------------------------------------


class TestTranslateSignals:
    def test_multiple_symbols(self):
        """Translates signals for different symbols independently."""
        signals = [
            _make_signal(symbol="AAPL", target_pct=0.1),
            _make_signal(symbol="MSFT", target_pct=0.05),
        ]
        results = translate_signals(
            signals,
            nav=100_000.0,
            positions={},
            current_prices={"AAPL": 150.0, "MSFT": 300.0},
        )
        assert len(results) == 2
        symbols = {r.signal.symbol for r in results}
        assert symbols == {"AAPL", "MSFT"}

    def test_nets_per_symbol_keeps_latest(self):
        """Multiple signals for same symbol: only latest is used."""
        early = datetime(2024, 1, 15, 14, 0, tzinfo=timezone.utc)
        late = datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc)
        signals = [
            _make_signal(symbol="AAPL", target_pct=0.1, ts=early),
            _make_signal(symbol="AAPL", target_pct=0.2, ts=late),
        ]
        results = translate_signals(
            signals,
            nav=100_000.0,
            positions={},
            current_prices={"AAPL": 150.0},
        )
        assert len(results) == 1
        assert results[0].desired_notional == 20_000.0  # 0.2 * 100k

    def test_empty_signals(self):
        """Empty signal list produces empty results."""
        results = translate_signals([], nav=100_000.0, positions={}, current_prices={})
        assert results == []
