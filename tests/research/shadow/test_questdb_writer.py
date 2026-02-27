"""Tests for research.shadow.questdb_writer — ExecutionTableWriter."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest

from research.shadow.questdb_writer import (
    ExecutionTableWriter,
    QuestDBWriterConfig,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    return QuestDBWriterConfig(ilp_host="localhost", ilp_port=9009)


@pytest.fixture
def writer(config):
    return ExecutionTableWriter(
        config=config,
        experiment_id="exp_001",
        run_id="run_abc",
        mode="shadow",
        dagster_run_id="dagster_123",
    )


@pytest.fixture
def mock_sender():
    """Patch Sender so no real QuestDB connection is needed."""
    with patch("research.shadow.questdb_writer.Sender") as MockSender:
        instance = MagicMock()
        MockSender.return_value.__enter__ = MagicMock(return_value=instance)
        MockSender.return_value.__exit__ = MagicMock(return_value=False)
        yield instance


# ---------------------------------------------------------------------------
# write_step tests
# ---------------------------------------------------------------------------


class TestWriteStep:
    def test_writes_per_symbol_rows(self, writer, mock_sender):
        ts = datetime(2023, 6, 15, tzinfo=timezone.utc)
        writer.write_step(
            timestamp=ts,
            step=1,
            symbols=("AAPL", "MSFT"),
            target_weights=[0.5, 0.5],
            realized_weights=[0.48, 0.48],
            fill_prices=[150.0, 300.0],
            slippage_bps=[0.0, 0.0],
            fees_per_symbol=[0.001, 0.001],
            rejected=[False, False],
            reject_reasons=["", ""],
            portfolio_value=1_000_000.0,
            cash=40_000.0,
            regime_bucket="normal",
        )

        # Should have 2 row calls (one per symbol) + 1 flush
        assert mock_sender.row.call_count == 2
        mock_sender.flush.assert_called_once()

        # Check first row is AAPL
        first_call = mock_sender.row.call_args_list[0]
        assert first_call[0][0] == "execution_log"
        assert first_call[1]["symbols"]["symbol"] == "AAPL"
        assert first_call[1]["symbols"]["experiment_id"] == "exp_001"
        assert first_call[1]["symbols"]["mode"] == "shadow"
        assert first_call[1]["columns"]["step"] == 1
        assert first_call[1]["columns"]["target_weight"] == 0.5
        assert first_call[1]["columns"]["portfolio_value"] == 1_000_000.0

        # Check second row is MSFT
        second_call = mock_sender.row.call_args_list[1]
        assert second_call[1]["symbols"]["symbol"] == "MSFT"

    def test_accumulates_stats(self, writer, mock_sender):
        ts = datetime(2023, 6, 15, tzinfo=timezone.utc)
        writer.write_step(
            timestamp=ts,
            step=1,
            symbols=("AAPL",),
            target_weights=[0.5],
            realized_weights=[0.48],
            fill_prices=[150.0],
            slippage_bps=[2.5],
            fees_per_symbol=[0.001],
            rejected=[False],
            reject_reasons=[""],
            portfolio_value=1_000_000.0,
            cash=520_000.0,
            regime_bucket="normal",
        )

        assert writer._acc.fills == 1
        assert writer._acc.rejects == 0
        assert writer._acc.total_fees == pytest.approx(0.001)
        assert writer._acc.slippage_bps_values == [2.5]

    def test_rejected_orders_counted(self, writer, mock_sender):
        ts = datetime(2023, 6, 15, tzinfo=timezone.utc)
        writer.write_step(
            timestamp=ts,
            step=1,
            symbols=("AAPL", "MSFT"),
            target_weights=[0.5, 0.5],
            realized_weights=[0.48, 0.0],
            fill_prices=[150.0, 0.0],
            slippage_bps=[0.0, 0.0],
            fees_per_symbol=[0.001, 0.0],
            rejected=[False, True],
            reject_reasons=["", "risk_limit"],
            portfolio_value=1_000_000.0,
            cash=520_000.0,
            regime_bucket="normal",
        )

        assert writer._acc.fills == 1
        assert writer._acc.rejects == 1


# ---------------------------------------------------------------------------
# write_metrics tests
# ---------------------------------------------------------------------------


class TestWriteMetrics:
    def test_writes_summary_row(self, writer, mock_sender):
        # Simulate some accumulated stats
        ts = datetime(2023, 6, 15, tzinfo=timezone.utc)
        writer.write_step(
            timestamp=ts,
            step=1,
            symbols=("AAPL",),
            target_weights=[0.5],
            realized_weights=[0.48],
            fill_prices=[150.0],
            slippage_bps=[1.0],
            fees_per_symbol=[0.001],
            rejected=[False],
            reject_reasons=[""],
            portfolio_value=1_000_000.0,
            cash=520_000.0,
            regime_bucket="normal",
        )

        mock_sender.reset_mock()

        writer.write_metrics(
            timestamp=ts,
            sharpe=1.5,
            max_drawdown=-0.05,
            total_return=0.12,
        )

        mock_sender.row.assert_called_once()
        row_call = mock_sender.row.call_args
        assert row_call[0][0] == "execution_metrics"
        assert row_call[1]["symbols"]["experiment_id"] == "exp_001"
        assert row_call[1]["symbols"]["mode"] == "shadow"
        assert row_call[1]["columns"]["sharpe"] == 1.5
        assert row_call[1]["columns"]["total_return"] == 0.12
        assert row_call[1]["columns"]["fill_rate"] == 1.0
        assert row_call[1]["columns"]["reject_rate"] == 0.0
        mock_sender.flush.assert_called_once()

    def test_fill_rate_with_rejects(self, writer, mock_sender):
        ts = datetime(2023, 6, 15, tzinfo=timezone.utc)
        # 2 fills, 1 reject across 2 steps
        writer.write_step(
            timestamp=ts,
            step=1,
            symbols=("AAPL", "MSFT"),
            target_weights=[0.5, 0.5],
            realized_weights=[0.48, 0.0],
            fill_prices=[150.0, 0.0],
            slippage_bps=[0.0, 0.0],
            fees_per_symbol=[0.001, 0.0],
            rejected=[False, True],
            reject_reasons=["", "risk_limit"],
            portfolio_value=1_000_000.0,
            cash=520_000.0,
            regime_bucket="normal",
        )
        writer.write_step(
            timestamp=ts,
            step=2,
            symbols=("AAPL",),
            target_weights=[0.6],
            realized_weights=[0.58],
            fill_prices=[151.0],
            slippage_bps=[0.5],
            fees_per_symbol=[0.002],
            rejected=[False],
            reject_reasons=[""],
            portfolio_value=1_001_000.0,
            cash=419_420.0,
            regime_bucket="normal",
        )

        mock_sender.reset_mock()
        writer.write_metrics(timestamp=ts, sharpe=1.0)

        row_call = mock_sender.row.call_args
        cols = row_call[1]["columns"]
        assert cols["fill_rate"] == pytest.approx(2 / 3)
        assert cols["reject_rate"] == pytest.approx(1 / 3)

    def test_no_steps_defaults(self, writer, mock_sender):
        """Metrics with no steps should use safe defaults."""
        ts = datetime(2023, 6, 15, tzinfo=timezone.utc)
        writer.write_metrics(timestamp=ts)

        row_call = mock_sender.row.call_args
        cols = row_call[1]["columns"]
        assert cols["fill_rate"] == 1.0
        assert cols["avg_slippage_bps"] == 0.0
        assert cols["total_fees"] == 0.0


# ---------------------------------------------------------------------------
# Engine integration (no real QuestDB)
# ---------------------------------------------------------------------------


class TestEngineQuestDBIntegration:
    """Test that ShadowEngine calls the writer correctly."""

    def test_engine_writes_execution_log(self, tmp_path):
        from research.experiments.spec import CostConfig, ExperimentSpec, RiskConfig
        from research.shadow.data_source import Snapshot
        from research.shadow.engine import ShadowEngine, ShadowRunConfig

        spec = ExperimentSpec(
            experiment_name="test_qdb",
            symbols=("AAPL", "MSFT"),
            start_date=datetime(2023, 1, 1).date(),
            end_date=datetime(2023, 12, 31).date(),
            interval="daily",
            feature_set="core_v1",
            policy="equal_weight",
            policy_params={},
            cost_config=CostConfig(transaction_cost_bp=10.0),
            seed=42,
            risk_config=RiskConfig(),
        )

        snapshots = []
        for i in range(5):
            panel = {
                "AAPL": {"close": 100.0 + i},
                "MSFT": {"close": 200.0 + i},
            }
            snapshots.append(Snapshot(
                as_of=datetime(2023, 1, 3 + i, tzinfo=timezone.utc),
                symbols=("AAPL", "MSFT"),
                panel=panel,
                regime_features=(),
                regime_feature_names=(),
                observation_columns=("close",),
            ))

        config = ShadowRunConfig(
            experiment_id="test_qdb",
            run_id="run_qdb",
            output_dir=tmp_path / "shadow" / "test_qdb" / "run_qdb",
        )

        from research.policies.equal_weight_policy import EqualWeightPolicy
        policy = EqualWeightPolicy(2)

        mock_writer = MagicMock(spec=ExecutionTableWriter)

        engine = ShadowEngine(
            spec, policy, snapshots, config,
            questdb_writer=mock_writer,
        )
        summary = engine.run()

        # 4 steps (5 snapshots - 1), each with 2 symbols
        assert mock_writer.write_step.call_count == 4
        assert mock_writer.write_metrics.call_count == 1

        # Check write_step call args
        first_call = mock_writer.write_step.call_args_list[0]
        assert first_call.kwargs["step"] == 1
        assert first_call.kwargs["symbols"] == ("AAPL", "MSFT")
        assert len(first_call.kwargs["target_weights"]) == 2
        assert len(first_call.kwargs["fill_prices"]) == 2

        # Check write_metrics
        metrics_call = mock_writer.write_metrics.call_args
        assert "sharpe" in metrics_call.kwargs
        assert "max_drawdown" in metrics_call.kwargs
        assert "total_return" in metrics_call.kwargs

    def test_engine_works_without_writer(self, tmp_path):
        """Engine should work fine with no QuestDB writer (backwards compat)."""
        from research.experiments.spec import CostConfig, ExperimentSpec, RiskConfig
        from research.shadow.data_source import Snapshot
        from research.shadow.engine import ShadowEngine, ShadowRunConfig

        spec = ExperimentSpec(
            experiment_name="test_no_qdb",
            symbols=("AAPL",),
            start_date=datetime(2023, 1, 1).date(),
            end_date=datetime(2023, 12, 31).date(),
            interval="daily",
            feature_set="core_v1",
            policy="equal_weight",
            policy_params={},
            cost_config=CostConfig(transaction_cost_bp=10.0),
            seed=42,
            risk_config=RiskConfig(),
        )

        snapshots = []
        for i in range(3):
            snapshots.append(Snapshot(
                as_of=datetime(2023, 1, 3 + i, tzinfo=timezone.utc),
                symbols=("AAPL",),
                panel={"AAPL": {"close": 100.0 + i}},
                regime_features=(),
                regime_feature_names=(),
                observation_columns=("close",),
            ))

        config = ShadowRunConfig(
            experiment_id="test_no_qdb",
            run_id="run_no_qdb",
            output_dir=tmp_path / "shadow" / "test_no_qdb" / "run_no_qdb",
        )

        from research.policies.equal_weight_policy import EqualWeightPolicy
        engine = ShadowEngine(spec, EqualWeightPolicy(1), snapshots, config)
        summary = engine.run()
        assert summary["experiment_id"] == "test_no_qdb"
        assert "sharpe" in summary

    def test_summary_includes_sharpe(self, tmp_path):
        """Verify that _build_summary now includes sharpe."""
        from research.experiments.spec import CostConfig, ExperimentSpec, RiskConfig
        from research.shadow.data_source import Snapshot
        from research.shadow.engine import ShadowEngine, ShadowRunConfig

        spec = ExperimentSpec(
            experiment_name="test_sharpe",
            symbols=("AAPL", "MSFT"),
            start_date=datetime(2023, 1, 1).date(),
            end_date=datetime(2023, 12, 31).date(),
            interval="daily",
            feature_set="core_v1",
            policy="equal_weight",
            policy_params={},
            cost_config=CostConfig(transaction_cost_bp=0.0),
            seed=42,
            risk_config=RiskConfig(),
        )

        snapshots = []
        for i in range(10):
            panel = {
                "AAPL": {"close": 100.0 + i * 0.5},
                "MSFT": {"close": 200.0 + i * 0.3},
            }
            snapshots.append(Snapshot(
                as_of=datetime(2023, 1, 3 + i, tzinfo=timezone.utc),
                symbols=("AAPL", "MSFT"),
                panel=panel,
                regime_features=(),
                regime_feature_names=(),
                observation_columns=("close",),
            ))

        config = ShadowRunConfig(
            experiment_id="test_sharpe",
            run_id="run_sharpe",
            output_dir=tmp_path / "shadow" / "test_sharpe" / "run_sharpe",
        )

        from research.policies.equal_weight_policy import EqualWeightPolicy
        engine = ShadowEngine(spec, EqualWeightPolicy(2), snapshots, config)
        summary = engine.run()

        assert "sharpe" in summary
        # With steadily rising prices and no costs, Sharpe should be positive
        assert summary["sharpe"] > 0
