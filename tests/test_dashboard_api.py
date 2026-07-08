"""API contract tests for the YATS Dashboard FastAPI app.

All queries.* functions are monkeypatched so these tests need no live DB.
One @pytest.mark.live_db test exercises the health endpoint against real QuestDB.

Guards ya-v3bp2: all 8 endpoints return HTTP 200 with expected response shapes.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def clear_api_cache():
    """Reset the in-process TTL cache before each test so tests don't share state."""
    import dashboard.api as _api
    _api._cache.clear()


@pytest.fixture()
def client():
    from dashboard.api import app
    return TestClient(app, raise_server_exceptions=True)


# ---------------------------------------------------------------------------
# Helper: a context manager that yields a mock connection
# ---------------------------------------------------------------------------


def _mock_pg_conn(conn):
    """Patch queries.pg_conn to yield *conn* as the context manager value."""
    from contextlib import contextmanager

    @contextmanager
    def _fake():
        yield conn

    return patch("dashboard.queries.pg_conn", _fake)


# ---------------------------------------------------------------------------
# GET /api/health
# ---------------------------------------------------------------------------


class TestHealthEndpoint:
    def test_returns_200(self, client):
        conn = MagicMock()
        with (
            _mock_pg_conn(conn),
            patch("dashboard.queries.health_checks", return_value=[
                {"service": "questdb", "status": "ok", "latency_ms": 1},
            ]),
            patch("dashboard.queries.kill_switches_latest", return_value=[]),
        ):
            r = client.get("/api/health")
        assert r.status_code == 200

    def test_response_has_services_and_kill_switches(self, client):
        conn = MagicMock()
        with (
            _mock_pg_conn(conn),
            patch("dashboard.queries.health_checks", return_value=[
                {"service": "questdb", "status": "ok", "latency_ms": 1},
            ]),
            patch("dashboard.queries.kill_switches_latest", return_value=[]),
        ):
            r = client.get("/api/health")
        body = r.json()
        assert "services" in body
        assert "kill_switches" in body
        assert isinstance(body["services"], list)


# ---------------------------------------------------------------------------
# GET /api/coverage
# ---------------------------------------------------------------------------


class TestCoverageEndpoint:
    def test_returns_200(self, client):
        conn = MagicMock()
        with (
            _mock_pg_conn(conn),
            patch("dashboard.queries.coverage", return_value={
                "equity_ohlcv": {"symbol_count": 50, "freshness_status": "ok"},
            }),
        ):
            r = client.get("/api/coverage")
        assert r.status_code == 200

    def test_response_is_dict(self, client):
        conn = MagicMock()
        with (
            _mock_pg_conn(conn),
            patch("dashboard.queries.coverage", return_value={"equity_ohlcv": {}}),
        ):
            r = client.get("/api/coverage")
        assert isinstance(r.json(), dict)


# ---------------------------------------------------------------------------
# GET /api/storage
# ---------------------------------------------------------------------------


class TestStorageEndpoint:
    def test_returns_200(self, client):
        conn = MagicMock()
        with (
            _mock_pg_conn(conn),
            patch("dashboard.queries.storage", return_value={"canonical": []}),
        ):
            r = client.get("/api/storage")
        assert r.status_code == 200

    def test_response_is_grouped_dict(self, client):
        conn = MagicMock()
        payload = {"canonical": [{"table": "canonical_equity_ohlcv", "row_count": 1000}]}
        with (
            _mock_pg_conn(conn),
            patch("dashboard.queries.storage", return_value=payload),
        ):
            r = client.get("/api/storage")
        body = r.json()
        assert "canonical" in body


# ---------------------------------------------------------------------------
# GET /api/storage/{domain}/tickers
# ---------------------------------------------------------------------------


class TestStorageTickersEndpoint:
    def test_returns_200_for_known_domain(self, client):
        conn = MagicMock()
        with (
            _mock_pg_conn(conn),
            patch("dashboard.queries.storage_tickers", return_value=[
                {"symbol": "AAPL", "from_ts": "2020-01-01", "to_ts": "2024-12-31", "row_count": 1000},
            ]),
        ):
            r = client.get("/api/storage/equity_ohlcv/tickers")
        assert r.status_code == 200

    def test_response_is_list(self, client):
        conn = MagicMock()
        with (
            _mock_pg_conn(conn),
            patch("dashboard.queries.storage_tickers", return_value=[]),
        ):
            r = client.get("/api/storage/options_eod/tickers")
        assert isinstance(r.json(), list)

    def test_domain_passed_to_query(self, client):
        conn = MagicMock()
        captured = {}
        def fake_tickers(c, domain):
            captured["domain"] = domain
            return []
        with (
            _mock_pg_conn(conn),
            patch("dashboard.queries.storage_tickers", fake_tickers),
        ):
            client.get("/api/storage/fundamentals/tickers")
        assert captured["domain"] == "fundamentals"


# ---------------------------------------------------------------------------
# GET /api/runs
# ---------------------------------------------------------------------------


class TestRunsEndpoint:
    def test_returns_200(self, client):
        conn = MagicMock()
        with (
            _mock_pg_conn(conn),
            patch("dashboard.queries.runs", return_value=[]),
        ):
            r = client.get("/api/runs")
        assert r.status_code == 200

    def test_default_hours_24(self, client):
        conn = MagicMock()
        captured = {}
        def fake_runs(c, hours=24):
            captured["hours"] = hours
            return []
        with _mock_pg_conn(conn), patch("dashboard.queries.runs", fake_runs):
            client.get("/api/runs")
            assert captured["hours"] == 24

    def test_custom_hours_param(self, client):
        conn = MagicMock()
        captured = {}
        def fake_runs(c, hours=24):
            captured["hours"] = hours
            return []
        with _mock_pg_conn(conn), patch("dashboard.queries.runs", fake_runs):
            client.get("/api/runs?hours=48")
            assert captured["hours"] == 48

    def test_returns_list(self, client):
        conn = MagicMock()
        payload = [{"job_name": "ingest_alpaca", "status": "success"}]
        with _mock_pg_conn(conn), patch("dashboard.queries.runs", return_value=payload):
            r = client.get("/api/runs")
        assert isinstance(r.json(), list)
        assert r.json()[0]["job_name"] == "ingest_alpaca"


# ---------------------------------------------------------------------------
# GET /api/experiments
# ---------------------------------------------------------------------------


class TestExperimentsEndpoint:
    def test_returns_200(self, client):
        conn = MagicMock()
        with (
            _mock_pg_conn(conn),
            patch("dashboard.queries.experiments_search", return_value={"results": [], "deflation_clock": 0}),
        ):
            r = client.get("/api/experiments")
        assert r.status_code == 200

    def test_response_has_results_and_deflation_clock(self, client):
        conn = MagicMock()
        with (
            _mock_pg_conn(conn),
            patch("dashboard.queries.experiments_search", return_value={"results": [], "deflation_clock": 42}),
        ):
            r = client.get("/api/experiments")
        body = r.json()
        assert "results" in body
        assert "deflation_clock" in body
        assert body["deflation_clock"] == 42

    def test_q_param_passed_to_query(self, client):
        conn = MagicMock()
        captured = {}
        def fake_search(c, q=""):
            captured["q"] = q
            return {"results": [], "deflation_clock": 0}
        with (
            _mock_pg_conn(conn),
            patch("dashboard.queries.experiments_search", fake_search),
        ):
            client.get("/api/experiments?q=ppo")
        assert captured["q"] == "ppo"


# ---------------------------------------------------------------------------
# GET /api/experiments/{id}
# ---------------------------------------------------------------------------


class TestExperimentDetailEndpoint:
    def test_returns_200(self, client):
        conn = MagicMock()
        with (
            _mock_pg_conn(conn),
            patch("dashboard.queries.experiment_detail", return_value={
                "experiment_id": "exp-001",
                "spec": None,
                "performance": None,
                "trading": None,
                "safety": None,
                "equity_curve": [],
                "qualification_report": None,
            }),
        ):
            r = client.get("/api/experiments/exp-001")
        assert r.status_code == 200

    def test_experiment_id_in_response(self, client):
        conn = MagicMock()
        with (
            _mock_pg_conn(conn),
            patch("dashboard.queries.experiment_detail", return_value={
                "experiment_id": "exp-abc",
            }),
        ):
            r = client.get("/api/experiments/exp-abc")
        assert r.json()["experiment_id"] == "exp-abc"

    def test_equity_curve_present(self, client):
        conn = MagicMock()
        with (
            _mock_pg_conn(conn),
            patch("dashboard.queries.experiment_detail", return_value={
                "experiment_id": "exp-001",
                "equity_curve": [1.0, 1.05, 1.1],
            }),
        ):
            r = client.get("/api/experiments/exp-001")
        assert "equity_curve" in r.json()


# ---------------------------------------------------------------------------
# GET /api/trading
# ---------------------------------------------------------------------------


class TestTradingEndpoint:
    def test_returns_200(self, client):
        conn = MagicMock()
        with (
            _mock_pg_conn(conn),
            patch("dashboard.queries.trading", return_value={
                "portfolio_state": [],
                "open_orders": [],
                "risk_decisions": [],
                "heartbeat": [],
                "promotions": [],
            }),
        ):
            r = client.get("/api/trading")
        assert r.status_code == 200

    def test_response_has_expected_keys(self, client):
        conn = MagicMock()
        payload = {
            "portfolio_state": [],
            "open_orders": [],
            "risk_decisions": [],
            "heartbeat": [],
            "promotions": [],
        }
        with (
            _mock_pg_conn(conn),
            patch("dashboard.queries.trading", return_value=payload),
        ):
            r = client.get("/api/trading")
        body = r.json()
        for key in ("portfolio_state", "open_orders", "risk_decisions", "heartbeat", "promotions"):
            assert key in body


# ---------------------------------------------------------------------------
# Live DB smoke test
# ---------------------------------------------------------------------------


@pytest.mark.live_db
def test_health_endpoint_live():
    """Smoke test: /api/health connects to real QuestDB and returns 200."""
    from dashboard.api import app
    client = TestClient(app)
    r = client.get("/api/health")
    assert r.status_code == 200
    body = r.json()
    assert "services" in body
    services = {s["service"] for s in body["services"]}
    assert "questdb" in services
    qdb = next(s for s in body["services"] if s["service"] == "questdb")
    assert qdb["status"] == "ok"
