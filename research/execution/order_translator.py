"""Order translator — converts target position signals into order requests.

Implements PRD §13.2 Order Translator:
- desired_notional = NAV × target_position_pct
- order_notional = desired_notional - current_position_notional
- Market orders only (v1)
- Orders netted per symbol — one open position per symbol
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass

from research.execution.broker_alpaca import OrderRequest, OrderSide
from research.execution.signal import Signal
from research.execution.state import Position

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TranslationResult:
    """Result of translating a signal into an order request.

    If no order is needed (delta below threshold), order is None.
    """

    signal: Signal
    order: OrderRequest | None
    desired_notional: float
    current_notional: float
    delta_notional: float
    reason: str  # "order_generated", "below_threshold", "zero_target"


def translate_signal(
    signal: Signal,
    nav: float,
    positions: dict[str, Position],
    current_prices: dict[str, float],
    *,
    min_order_notional: float = 1.0,
) -> TranslationResult:
    """Translate a single signal into an order request.

    Args:
        signal: Target position signal from model.
        nav: Current net asset value.
        positions: Current positions keyed by symbol.
        current_prices: Current market prices keyed by symbol.
        min_order_notional: Minimum order size in USD (filters noise).

    Returns:
        TranslationResult with the order (or None if below threshold).
    """
    desired_notional = nav * signal.target_position_pct

    # Current position notional (0 if no position)
    pos = positions.get(signal.symbol)
    price = current_prices.get(signal.symbol, 0.0)
    current_notional = pos.notional(price) if pos else 0.0

    delta_notional = desired_notional - current_notional

    # Zero target with no position — nothing to do
    if abs(desired_notional) < 1e-10 and pos is None:
        return TranslationResult(
            signal=signal,
            order=None,
            desired_notional=desired_notional,
            current_notional=current_notional,
            delta_notional=delta_notional,
            reason="zero_target",
        )

    # Delta below minimum order size — skip
    if abs(delta_notional) < min_order_notional:
        return TranslationResult(
            signal=signal,
            order=None,
            desired_notional=desired_notional,
            current_notional=current_notional,
            delta_notional=delta_notional,
            reason="below_threshold",
        )

    side = OrderSide.BUY if delta_notional > 0 else OrderSide.SELL

    order = OrderRequest(
        symbol=signal.symbol,
        side=side,
        notional=abs(delta_notional),
        client_order_id=str(uuid.uuid4()),
    )

    logger.info(
        "Signal → Order: %s %s $%.2f (target=%.4f, nav=%.2f, delta=$%.2f)",
        signal.symbol,
        side.value,
        order.notional,
        signal.target_position_pct,
        nav,
        delta_notional,
    )

    return TranslationResult(
        signal=signal,
        order=order,
        desired_notional=desired_notional,
        current_notional=current_notional,
        delta_notional=delta_notional,
        reason="order_generated",
    )


def translate_signals(
    signals: list[Signal],
    nav: float,
    positions: dict[str, Position],
    current_prices: dict[str, float],
    *,
    min_order_notional: float = 1.0,
) -> list[TranslationResult]:
    """Translate a batch of signals into order requests.

    Nets signals per symbol — if multiple signals arrive for the same symbol,
    only the last one (by timestamp) is used.

    Args:
        signals: List of target position signals.
        nav: Current net asset value.
        positions: Current positions keyed by symbol.
        current_prices: Current market prices keyed by symbol.
        min_order_notional: Minimum order size in USD.

    Returns:
        List of TranslationResults (one per unique symbol).
    """
    # Net per symbol: keep latest signal per symbol
    latest_by_symbol: dict[str, Signal] = {}
    for sig in signals:
        existing = latest_by_symbol.get(sig.symbol)
        if existing is None or sig.timestamp > existing.timestamp:
            latest_by_symbol[sig.symbol] = sig

    results = []
    for sig in latest_by_symbol.values():
        result = translate_signal(
            sig,
            nav,
            positions,
            current_prices,
            min_order_notional=min_order_notional,
        )
        results.append(result)

    return results
