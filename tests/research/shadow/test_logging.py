"""Tests for research.shadow.logging — StepLogger."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from research.shadow.logging import StepLogger, build_step_entry


class TestStepLogger:
    def test_writes_jsonl(self, tmp_path: Path):
        log_path = tmp_path / "logs" / "steps.jsonl"
        logger = StepLogger(log_path)
        logger.log_step({"step": 0, "value": 100.0})
        logger.log_step({"step": 1, "value": 101.0})

        lines = log_path.read_text().strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0])["step"] == 0
        assert json.loads(lines[1])["value"] == 101.0

    def test_appends_on_resume(self, tmp_path: Path):
        log_path = tmp_path / "steps.jsonl"
        log_path.write_text('{"step": 0}\n')

        logger = StepLogger(log_path)
        logger.log_step({"step": 1})

        lines = log_path.read_text().strip().split("\n")
        assert len(lines) == 2

    def test_creates_parent_dirs(self, tmp_path: Path):
        log_path = tmp_path / "deep" / "nested" / "steps.jsonl"
        logger = StepLogger(log_path)
        logger.log_step({"test": True})
        assert log_path.exists()


class TestBuildStepEntry:
    def test_all_fields(self):
        entry = build_step_entry(
            step_index=5,
            timestamp="2023-06-15T00:00:00",
            weights=[0.5, 0.5],
            previous_weights=[0.4, 0.6],
            symbols=("AAPL", "MSFT"),
            returns_per_symbol=[0.01, -0.005],
            weighted_return=0.0025,
            cost=0.0001,
            portfolio_value=1002500.0,
            peak_value=1002500.0,
            drawdown=0.0,
        )
        assert entry["step_index"] == 5
        assert entry["timestamp"] == "2023-06-15T00:00:00"
        assert entry["weights"] == [0.5, 0.5]
        assert entry["symbols"] == ["AAPL", "MSFT"]
        assert entry["portfolio_value"] == 1002500.0
