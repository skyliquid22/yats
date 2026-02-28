"""Tests for research.execution.heartbeat — heartbeat writer."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from research.execution.heartbeat import HeartbeatWriter


class TestHeartbeatWriter:
    def _make_writer(self) -> HeartbeatWriter:
        return HeartbeatWriter(
            ilp_host="localhost",
            ilp_port=9009,
            experiment_id="exp-001",
            mode="paper",
        )

    @patch("research.execution.heartbeat.Sender")
    def test_write_heartbeat_basic(self, mock_sender_cls):
        """Write a basic heartbeat with loop iteration and pending orders."""
        mock_sender = MagicMock()
        mock_sender_cls.return_value.__enter__ = MagicMock(return_value=mock_sender)
        mock_sender_cls.return_value.__exit__ = MagicMock(return_value=False)

        writer = self._make_writer()
        writer.write(loop_iteration=42, orders_pending=3)

        mock_sender.row.assert_called_once()
        call_args = mock_sender.row.call_args
        assert call_args[0][0] == "trading_heartbeat"
        assert call_args[1]["symbols"]["experiment_id"] == "exp-001"
        assert call_args[1]["symbols"]["mode"] == "paper"
        assert call_args[1]["columns"]["loop_iteration"] == 42
        assert call_args[1]["columns"]["orders_pending"] == 3
        mock_sender.flush.assert_called_once()

    @patch("research.execution.heartbeat.Sender")
    def test_write_heartbeat_with_timestamps(self, mock_sender_cls):
        """Write heartbeat with bar and fill timestamps."""
        mock_sender = MagicMock()
        mock_sender_cls.return_value.__enter__ = MagicMock(return_value=mock_sender)
        mock_sender_cls.return_value.__exit__ = MagicMock(return_value=False)

        writer = self._make_writer()
        bar_ts = datetime(2024, 6, 15, 14, 30, tzinfo=timezone.utc)
        fill_ts = datetime(2024, 6, 15, 14, 29, tzinfo=timezone.utc)

        writer.write(
            loop_iteration=100,
            orders_pending=0,
            last_bar_received=bar_ts,
            last_fill_received=fill_ts,
        )

        call_args = mock_sender.row.call_args
        columns = call_args[1]["columns"]
        assert "last_bar_received" in columns
        assert "last_fill_received" in columns

    @patch("research.execution.heartbeat.Sender")
    def test_write_heartbeat_no_optional_timestamps(self, mock_sender_cls):
        """Heartbeat without optional timestamps omits them from columns."""
        mock_sender = MagicMock()
        mock_sender_cls.return_value.__enter__ = MagicMock(return_value=mock_sender)
        mock_sender_cls.return_value.__exit__ = MagicMock(return_value=False)

        writer = self._make_writer()
        writer.write(loop_iteration=1, orders_pending=0)

        call_args = mock_sender.row.call_args
        columns = call_args[1]["columns"]
        assert "last_bar_received" not in columns
        assert "last_fill_received" not in columns
