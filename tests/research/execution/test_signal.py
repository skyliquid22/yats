"""Tests for research.execution.signal — Signal dataclass."""

from __future__ import annotations

from datetime import datetime, timezone

from research.execution.signal import Signal


def test_signal_creation():
    """Signal can be created with all required fields."""
    ts = datetime(2024, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
    sig = Signal(
        timestamp=ts,
        symbol="AAPL",
        target_position_pct=0.15,
        model_version="policy_v1",
    )
    assert sig.timestamp == ts
    assert sig.symbol == "AAPL"
    assert sig.target_position_pct == 0.15
    assert sig.model_version == "policy_v1"
    assert sig.confidence_score is None


def test_signal_with_confidence():
    """Signal accepts optional confidence_score."""
    sig = Signal(
        timestamp=datetime(2024, 1, 15, tzinfo=timezone.utc),
        symbol="MSFT",
        target_position_pct=-0.1,
        model_version="policy_v2",
        confidence_score=0.85,
    )
    assert sig.confidence_score == 0.85


def test_signal_frozen():
    """Signal is immutable."""
    sig = Signal(
        timestamp=datetime(2024, 1, 15, tzinfo=timezone.utc),
        symbol="AAPL",
        target_position_pct=0.1,
        model_version="v1",
    )
    try:
        sig.symbol = "MSFT"  # type: ignore[misc]
        assert False, "Should have raised"
    except AttributeError:
        pass


def test_signal_negative_target():
    """Signal supports negative targets (short positions)."""
    sig = Signal(
        timestamp=datetime(2024, 1, 15, tzinfo=timezone.utc),
        symbol="SPY",
        target_position_pct=-0.3,
        model_version="v1",
    )
    assert sig.target_position_pct == -0.3
