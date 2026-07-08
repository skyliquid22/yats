"""Unit tests for dashboard/queries.py — mocked PG cursor, no live DB needed.

Guards ya-v3bp2: all query functions return the correct shape and handle
empty-result / error cases gracefully.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Cursor / connection mock factory
# ---------------------------------------------------------------------------


def _make_cursor(rows=None, description=None):
    """Return a mock psycopg2 cursor."""
    cur = MagicMock()
    cur.description = description
    cur.fetchall.return_value = rows or []
    cur.fetchone.return_value = rows[0] if rows else None
    return cur


def _make_conn(cursor=None):
    """Return a mock psycopg2 connection that yields a cursor."""
    conn = MagicMock()
    conn.cursor.return_value = cursor or _make_cursor()
    return conn


# ---------------------------------------------------------------------------
# health_checks
# ---------------------------------------------------------------------------


class TestHealthChecks:
    def test_returns_four_services(self):
        from dashboard.queries import health_checks

        # QuestDB cursor succeeds
        cur = _make_cursor(rows=[(1,)])
        conn = _make_conn(cur)
        with (
            patch("dashboard.queries.requests.get") as mock_get,
        ):
            mock_get.return_value = MagicMock(status_code=200)
            results = health_checks(conn)

        assert len(results) == 4
        services = {r["service"] for r in results}
        assert services == {"questdb", "thetadata", "alpaca", "financialdatasets"}

    def test_all_have_required_keys(self):
        from dashboard.queries import health_checks

        cur = _make_cursor(rows=[(1,)])
        # second call for FD error count
        conn = MagicMock()
        conn.cursor.side_effect = [cur, _make_cursor(rows=[(0,)])]
        with patch("dashboard.queries.requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=200)
            results = health_checks(conn)

        for r in results:
            assert "service" in r
            assert "status" in r
            assert r["status"] in ("ok", "warn", "crit")
            assert "latency_ms" in r
            assert isinstance(r["latency_ms"], int)

    def test_questdb_crit_when_cursor_raises(self):
        from dashboard.queries import health_checks

        conn = MagicMock()
        conn.cursor.side_effect = Exception("connection refused")
        with patch("dashboard.queries.requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=200)
            results = health_checks(conn)

        qdb = next(r for r in results if r["service"] == "questdb")
        assert qdb["status"] == "crit"

    def test_theta_crit_when_request_raises(self):
        from dashboard.queries import health_checks

        cur = _make_cursor(rows=[(1,)])
        conn = MagicMock()
        conn.cursor.side_effect = [cur, _make_cursor(rows=[(0,)])]
        with patch("dashboard.queries.requests.get") as mock_get:
            mock_get.side_effect = [
                MagicMock(status_code=200),  # Theta
                Exception("timeout"),         # Alpaca
            ]
            # Re-raise on second call
            mock_get.side_effect = Exception("timeout")
            results = health_checks(conn)

        theta = next(r for r in results if r["service"] == "thetadata")
        assert theta["status"] == "crit"

    def test_fd_warn_when_recent_errors(self):
        from dashboard.queries import health_checks

        qdb_cur = _make_cursor(rows=[(1,)])
        fd_cur = _make_cursor(rows=[(2,)])  # 2 recent failures → warn
        conn = MagicMock()
        conn.cursor.side_effect = [qdb_cur, fd_cur]
        with patch("dashboard.queries.requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=200)
            results = health_checks(conn)

        fd = next(r for r in results if r["service"] == "financialdatasets")
        assert fd["status"] == "warn"

    def test_fd_crit_when_many_errors(self):
        from dashboard.queries import health_checks

        qdb_cur = _make_cursor(rows=[(1,)])
        fd_cur = _make_cursor(rows=[(5,)])  # 5 recent failures → crit
        conn = MagicMock()
        conn.cursor.side_effect = [qdb_cur, fd_cur]
        with patch("dashboard.queries.requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=200)
            results = health_checks(conn)

        fd = next(r for r in results if r["service"] == "financialdatasets")
        assert fd["status"] == "crit"


class TestKillSwitchesLatest:
    def _desc(self):
        return [
            (col,) + (None,) * 6
            for col in ("timestamp", "experiment_id", "mode", "trigger", "action",
                        "triggered_by", "details", "resolved", "resolved_at", "resolved_by")
        ]

    def test_returns_list_of_dicts(self):
        from dashboard.queries import kill_switches_latest

        cur = _make_cursor(
            rows=[("2026-01-01", "exp-1", "live", "drawdown", "halt", "sensor", "", False, None, None)],
            description=self._desc(),
        )
        conn = _make_conn(cur)
        result = kill_switches_latest(conn)
        assert isinstance(result, list)
        assert result[0]["experiment_id"] == "exp-1"

    def test_empty_result(self):
        from dashboard.queries import kill_switches_latest

        cur = _make_cursor(rows=[], description=self._desc())
        conn = _make_conn(cur)
        result = kill_switches_latest(conn)
        assert result == []


# ---------------------------------------------------------------------------
# coverage
# ---------------------------------------------------------------------------


class TestCoverage:
    def _sym_desc(self):
        return [(c,) + (None,) * 6 for c in ("symbol", "from_ts", "to_ts", "row_count")]

    def test_returns_all_domains(self):
        from dashboard.queries import coverage, _COVERAGE_DOMAINS

        conn = MagicMock()
        conn.cursor.return_value = _make_cursor(rows=[], description=self._sym_desc())
        result = coverage(conn)
        for domain in _COVERAGE_DOMAINS:
            assert domain in result

    def test_empty_table_returns_zero_symbol_count(self):
        from dashboard.queries import coverage

        conn = MagicMock()
        conn.cursor.return_value = _make_cursor(rows=[], description=self._sym_desc())
        result = coverage(conn)
        assert result["equity_ohlcv"]["symbol_count"] == 0

    def test_mode_count_and_exceptions(self):
        from dashboard.queries import coverage

        # 3 symbols with 1000, 1000, 100 rows → mode=1000, exceptions=[ABNB]
        rows = [
            ("AAPL", "2020-01-01", "2024-12-31", 1000),
            ("MSFT", "2020-01-01", "2024-12-31", 1000),
            ("ABNB", "2023-01-01", "2024-12-31", 100),
        ]
        cursors = [_make_cursor(rows=rows, description=self._sym_desc())] * 5
        conn = MagicMock()
        conn.cursor.side_effect = cursors

        result = coverage(conn)
        eq = result["equity_ohlcv"]
        assert eq["mode_rows"] == 1000
        assert "ABNB" in eq["exceptions"]
        assert "AAPL" not in eq["exceptions"]
        assert eq["symbol_count"] == 3

    def test_stock_only_domains_report_etf_absent(self):
        from dashboard.queries import coverage

        rows = [("AAPL", "2020-01-01", "2024-12-31", 10)]
        conn = MagicMock()
        conn.cursor.return_value = _make_cursor(rows=rows, description=self._sym_desc())
        result = coverage(conn)
        # fundamentals is stock_only=True → ETF_SYMBOLS not in data → etf_absent
        etf_absent = result["fundamentals"]["etf_absent"]
        assert "SPY" in etf_absent or "QQQ" in etf_absent

    def test_equity_ohlcv_freshness_ok(self):
        from dashboard.queries import coverage

        import datetime
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
        rows = [("AAPL", "2020-01-01", yesterday, 1000)]
        conn = MagicMock()
        conn.cursor.return_value = _make_cursor(rows=rows, description=self._sym_desc())
        result = coverage(conn)
        eq = result["equity_ohlcv"]
        assert eq["sessions_behind"] >= 0

    def test_error_table_returns_error_key(self):
        from dashboard.queries import coverage

        conn = MagicMock()
        conn.cursor.return_value = MagicMock()
        conn.cursor.return_value.execute.side_effect = Exception("table not found")
        result = coverage(conn)
        # At least one domain should have an error key
        assert any("error" in v for v in result.values())


# ---------------------------------------------------------------------------
# storage
# ---------------------------------------------------------------------------


class TestStorage:
    def test_returns_grouped_dict(self):
        from dashboard.queries import storage

        storage_cur = _make_cursor(
            rows=[("canonical_equity_ohlcv", 1000, 500000)],
            description=[(c,) + (None,) * 6 for c in ("tableName", "rowCount", "diskSize")],
        )
        tables_cur = _make_cursor(
            rows=[("canonical_equity_ohlcv", True, True)],
            description=[(c,) + (None,) * 6 for c in ("tableName", "walEnabled", "dedup")],
        )
        conn = MagicMock()
        conn.cursor.side_effect = [storage_cur, tables_cur]
        result = storage(conn)
        assert "canonical" in result
        row = result["canonical"][0]
        assert row["table"] == "canonical_equity_ohlcv"
        assert row["row_count"] == 1000
        assert row["wal_enabled"] is True

    def test_unknown_table_grouped_as_other(self):
        from dashboard.queries import storage

        storage_cur = _make_cursor(
            rows=[("some_unknown_table", 0, 0)],
            description=[(c,) + (None,) * 6 for c in ("tableName", "rowCount", "diskSize")],
        )
        tables_cur = _make_cursor(rows=[], description=[(c,) + (None,) * 6 for c in ("tableName", "walEnabled", "dedup")])
        conn = MagicMock()
        conn.cursor.side_effect = [storage_cur, tables_cur]
        result = storage(conn)
        assert "other" in result


# ---------------------------------------------------------------------------
# storage_tickers
# ---------------------------------------------------------------------------


class TestStorageTickers:
    def _desc(self):
        return [(c,) + (None,) * 6 for c in ("symbol", "from_ts", "to_ts", "row_count")]

    def test_equity_domain_returns_rows(self):
        from dashboard.queries import storage_tickers

        rows = [("AAPL", "2020-01-01", "2024-12-31", 1000)]
        conn = _make_conn(_make_cursor(rows=rows, description=self._desc()))
        result = storage_tickers(conn, "equity_ohlcv")
        assert len(result) == 1
        assert result[0]["symbol"] == "AAPL"

    def test_unknown_domain_returns_empty(self):
        from dashboard.queries import storage_tickers

        conn = _make_conn(_make_cursor(rows=[], description=self._desc()))
        result = storage_tickers(conn, "nonexistent_domain")
        assert result == []

    def test_features_domain_includes_feature_set(self):
        from dashboard.queries import storage_tickers

        rows = [("core_v1", "AAPL", "2020-01-01", "2024-12-31", 1000)]
        desc = [(c,) + (None,) * 6 for c in ("feature_set", "symbol", "from_ts", "to_ts", "row_count")]
        conn = _make_conn(_make_cursor(rows=rows, description=desc))
        result = storage_tickers(conn, "features")
        assert result[0]["feature_set"] == "core_v1"


# ---------------------------------------------------------------------------
# runs
# ---------------------------------------------------------------------------


class TestRuns:
    def _desc(self):
        return [(c,) + (None,) * 6 for c in (
            "started_at", "job_name", "dagster_run_id", "status",
            "finished_at", "duration_s", "rows_written", "detail", "failure_cause",
        )]

    def test_returns_list_of_dicts(self):
        from dashboard.queries import runs

        rows = [("2026-01-01", "ingest_alpaca", "run-001", "success", "2026-01-01", 5.0, 100, "", None)]
        conn = _make_conn(_make_cursor(rows=rows, description=self._desc()))
        result = runs(conn, hours=24)
        assert len(result) == 1
        assert result[0]["job_name"] == "ingest_alpaca"

    def test_empty_returns_empty_list(self):
        from dashboard.queries import runs

        conn = _make_conn(_make_cursor(rows=[], description=self._desc()))
        result = runs(conn, hours=24)
        assert result == []

    def test_hours_parameter_used_in_query(self):
        from dashboard.queries import runs

        cur = _make_cursor(rows=[], description=self._desc())
        conn = _make_conn(cur)
        runs(conn, hours=48)
        sql = cur.execute.call_args[0][0]
        assert "48" in sql


# ---------------------------------------------------------------------------
# experiments_search
# ---------------------------------------------------------------------------


class TestExperimentsSearch:
    def _desc(self):
        return [(c,) + (None,) * 6 for c in (
            "experiment_id", "universe", "feature_set", "policy_type",
            "reward_version", "sharpe", "calmar", "max_drawdown", "total_return",
            "qualification_status", "promotion_tier", "created_at", "stale",
        )]

    @pytest.fixture(autouse=True)
    def _isolate_data_root(self, tmp_path, monkeypatch):
        """Search also scans YATS_DATA_ROOT/wfo_sweeps — point it at an empty
        tmp dir so real local sweep summaries can't leak into assertions
        (observed: 8 real sweep configs made count tests fail on dev machines
        while passing in clean polecat clones)."""
        monkeypatch.setenv("YATS_DATA_ROOT", str(tmp_path))

    def test_returns_results_and_deflation_clock(self):
        from dashboard.queries import experiments_search

        rows = [("exp-001", "sp500", "core_v1", "ppo", None, 1.2, 0.5, -0.1, 0.3, "passed", None, "2026-01-01", False)]
        conn = _make_conn(_make_cursor(rows=rows, description=self._desc()))
        result = experiments_search(conn, "")
        assert "results" in result
        assert "deflation_clock" in result
        assert isinstance(result["deflation_clock"], int)

    def test_empty_search_returns_all(self):
        from dashboard.queries import experiments_search

        rows = [("exp-001", "sp500", "core_v1", "ppo", None, 1.2, 0.5, -0.1, 0.3, "passed", None, "2026-01-01", False)]
        conn = _make_conn(_make_cursor(rows=rows, description=self._desc()))
        result = experiments_search(conn, "")
        assert len(result["results"]) == 1

    def test_search_filters_by_policy(self):
        from dashboard.queries import experiments_search

        rows = [
            ("exp-001", "sp500", "core_v1", "ppo", None, 1.2, 0.5, -0.1, 0.3, "passed", None, "2026-01-01", False),
            ("exp-002", "sp500", "core_v1", "sac", None, 0.9, 0.3, -0.2, 0.2, "failed", None, "2026-01-02", False),
        ]
        conn = _make_conn(_make_cursor(rows=rows, description=self._desc()))
        result = experiments_search(conn, "ppo")
        ids = [r["experiment_id"] for r in result["results"]]
        assert "exp-001" in ids
        assert "exp-002" not in ids

    def test_results_carry_evaluation_regime_split(self):
        from dashboard.queries import experiments_search

        rows = [("exp-001", "sp500", "core_v1", "ppo", None, 1.2, 0.5, -0.1, 0.3, "passed", None, "2026-01-01", False)]
        conn = _make_conn(_make_cursor(rows=rows, description=self._desc()))
        result = experiments_search(conn, "")
        assert result["results"][0]["evaluation_regime"] == "split"

    def test_wfo_sweeps_loaded_when_present(self, tmp_path):
        from dashboard.queries import experiments_search

        sweep_dir = tmp_path / "wfo_sweeps" / "test_sweep"
        sweep_dir.mkdir(parents=True)
        summary = {
            "sweep": "test_sweep",
            "grid_size": 10,
            "configs": [
                {"policy": "ppo", "feature_set": "core_v1", "sharpe": 1.5, "dsr": 0.8, "dsr_significant": True}
            ],
        }
        (sweep_dir / "sweep_summary.json").write_text(json.dumps(summary))

        conn = _make_conn(_make_cursor(rows=[], description=self._desc()))
        with patch("dashboard.queries._get_data_root", return_value=tmp_path):
            result = experiments_search(conn, "ppo")

        assert result["deflation_clock"] == 10
        wfo_results = [r for r in result["results"] if r.get("evaluation_regime") == "wfo-oos"]
        assert len(wfo_results) == 1
        assert wfo_results[0]["dsr"] == 0.8


# ---------------------------------------------------------------------------
# experiment_detail
# ---------------------------------------------------------------------------


class TestExperimentDetail:
    def _index_desc(self):
        return [(c,) + (None,) * 6 for c in (
            "experiment_id", "universe", "feature_set", "policy_type",
            "sharpe", "calmar", "max_drawdown", "total_return",
            "qualification_status", "promotion_tier", "created_at",
        )]

    def test_returns_experiment_id(self):
        from dashboard.queries import experiment_detail

        conn = _make_conn(_make_cursor(rows=[], description=self._index_desc()))
        with patch("dashboard.queries._get_data_root", return_value=Path("/nonexistent")):
            result = experiment_detail(conn, "exp-001")
        assert result["experiment_id"] == "exp-001"
        assert result["spec"] is None
        assert result["performance"] is None
        assert result["qualification_report"] is None

    def test_loads_spec_and_metrics_when_present(self, tmp_path):
        from dashboard.queries import experiment_detail

        exp_dir = tmp_path / "experiments" / "exp-001"
        (exp_dir / "spec").mkdir(parents=True)
        (exp_dir / "evaluation").mkdir(parents=True)
        (exp_dir / "promotion").mkdir(parents=True)

        spec = {"experiment_id": "exp-001", "policy": "ppo"}
        (exp_dir / "spec" / "experiment_spec.json").write_text(json.dumps(spec))

        metrics = {
            "performance": {"sharpe": 1.5},
            "trading": {"win_rate": 0.6},
            "safety": {},
            "config": {},
            "inputs_used": {},
            "series": {"equity_curve": list(range(300))},
        }
        (exp_dir / "evaluation" / "metrics.json").write_text(json.dumps(metrics))

        qual = {"passed": True}
        (exp_dir / "promotion" / "qualification_report.json").write_text(json.dumps(qual))

        conn = _make_conn(_make_cursor(rows=[], description=self._index_desc()))
        with patch("dashboard.queries._get_data_root", return_value=tmp_path):
            result = experiment_detail(conn, "exp-001")

        assert result["spec"]["policy"] == "ppo"
        assert result["performance"]["sharpe"] == 1.5
        assert result["qualification_report"]["passed"] is True
        # Equity curve downsampled to <=200 points
        assert len(result["equity_curve"]) <= 200

    def test_equity_curve_under_200_not_truncated(self, tmp_path):
        from dashboard.queries import experiment_detail

        exp_dir = tmp_path / "experiments" / "exp-002"
        (exp_dir / "evaluation").mkdir(parents=True)
        metrics = {"series": {"equity_curve": list(range(50))}}
        (exp_dir / "evaluation" / "metrics.json").write_text(json.dumps(metrics))

        conn = _make_conn(_make_cursor(rows=[], description=self._index_desc()))
        with patch("dashboard.queries._get_data_root", return_value=tmp_path):
            result = experiment_detail(conn, "exp-002")

        assert len(result["equity_curve"]) == 50


# ---------------------------------------------------------------------------
# _downsample
# ---------------------------------------------------------------------------


class TestDownsample:
    def test_no_op_when_under_limit(self):
        from dashboard.queries import _downsample
        s = list(range(100))
        assert _downsample(s, 200) == s

    def test_truncates_to_max_points(self):
        from dashboard.queries import _downsample
        s = list(range(1000))
        result = _downsample(s, 200)
        assert len(result) <= 200

    def test_preserves_first_element(self):
        from dashboard.queries import _downsample
        s = list(range(500))
        result = _downsample(s, 200)
        assert result[0] == 0


# ---------------------------------------------------------------------------
# trading
# ---------------------------------------------------------------------------


class TestTrading:
    def _portfolio_desc(self):
        return [(c,) + (None,) * 6 for c in (
            "timestamp", "experiment_id", "mode", "nav", "cash",
            "gross_exposure", "net_exposure", "leverage", "num_positions",
            "daily_pnl", "peak_nav", "drawdown",
        )]

    def _orders_desc(self):
        return [(c,) + (None,) * 6 for c in ("experiment_id", "mode", "open_count")]

    def _risk_desc(self):
        return [(c,) + (None,) * 6 for c in (
            "timestamp", "rule_id", "experiment_id", "mode",
            "decision", "action_taken", "original_size", "reduced_size",
        )]

    def _hb_desc(self):
        return [(c,) + (None,) * 6 for c in (
            "timestamp", "experiment_id", "mode", "loop_iteration",
            "orders_pending", "last_bar_received",
        )]

    def _promo_desc(self):
        return [(c,) + (None,) * 6 for c in (
            "promoted_at", "experiment_id", "tier", "promoted_by",
            "qualification_passed", "sharpe", "max_drawdown",
        )]

    def test_returns_expected_keys(self):
        from dashboard.queries import trading

        conn = MagicMock()
        conn.cursor.side_effect = [
            _make_cursor(rows=[], description=self._portfolio_desc()),
            _make_cursor(rows=[], description=self._orders_desc()),
            _make_cursor(rows=[], description=self._risk_desc()),
            _make_cursor(rows=[], description=self._hb_desc()),
            _make_cursor(rows=[], description=self._promo_desc()),
        ]
        result = trading(conn)
        assert set(result.keys()) == {"portfolio_state", "open_orders", "risk_decisions", "heartbeat", "promotions"}

    def test_heartbeat_age_computed(self):
        from dashboard.queries import trading
        import datetime

        ts = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(seconds=30)
        conn = MagicMock()
        conn.cursor.side_effect = [
            _make_cursor(rows=[], description=self._portfolio_desc()),
            _make_cursor(rows=[], description=self._orders_desc()),
            _make_cursor(rows=[], description=self._risk_desc()),
            _make_cursor(
                rows=[(ts, "exp-1", "live", 10, 0, None)],
                description=self._hb_desc(),
            ),
            _make_cursor(rows=[], description=self._promo_desc()),
        ]
        result = trading(conn)
        hb = result["heartbeat"][0]
        assert hb["heartbeat_status"] == "ok"
        assert hb["age_s"] < 120

    def test_live_experiment_allocation_100pct(self):
        from dashboard.queries import trading
        import datetime

        ts = datetime.datetime.now(tz=datetime.timezone.utc)
        conn = MagicMock()
        conn.cursor.side_effect = [
            _make_cursor(rows=[], description=self._portfolio_desc()),
            _make_cursor(rows=[], description=self._orders_desc()),
            _make_cursor(rows=[], description=self._risk_desc()),
            _make_cursor(rows=[], description=self._hb_desc()),
            _make_cursor(
                rows=[(ts, "exp-live", "live", "system", True, 1.5, -0.1)],
                description=self._promo_desc(),
            ),
        ]
        result = trading(conn)
        promos = result["promotions"]
        live = [p for p in promos if p.get("tier") == "live"]
        assert live[0]["allocation"] == 1.0
