"""Dashboard query layer — all SQL lives here, one function per panel.

Functions accept a psycopg2 connection so they are unit-testable with mocked
cursors without a live DB. HTTP health probes are also here (imported from api
is circular).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import time
from contextlib import contextmanager
from pathlib import Path
from statistics import mode as stat_mode
from typing import Any

import psycopg2
import requests

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def _pg_params() -> dict:
    return {
        "host": os.environ.get("QDB_PG_HOST", "localhost"),
        "port": int(os.environ.get("QDB_PG_PORT", "8812")),
        "user": os.environ.get("QDB_PG_USER", "admin"),
        "password": os.environ.get("QDB_PG_PASSWORD", "quest"),
        "database": os.environ.get("QDB_PG_DATABASE", "qdb"),
    }


@contextmanager
def pg_conn():
    """Open a short-lived autocommit PG wire connection to QuestDB."""
    conn = psycopg2.connect(**_pg_params())
    conn.autocommit = True
    try:
        yield conn
    finally:
        conn.close()


def _rows_as_dicts(cur) -> list[dict]:
    if cur.description is None:
        return []
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

_THETA_BASE = os.environ.get("THETADATA_BASE_URL", "http://127.0.0.1:25503/v3")
_ALPACA_BROKER_BASE = os.environ.get("APCA_BROKER_BASE_URL", "https://paper-api.alpaca.markets/v2")
_HTTP_TIMEOUT = 5  # seconds


def health_checks(conn) -> list[dict]:
    """Timed checks for each monitored service.

    Returns [{service, status ok|warn|crit, latency_ms}].
    """
    results: list[dict] = []

    # QuestDB — SELECT 1 via PG wire
    t0 = time.monotonic()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        qdb_status = "ok"
    except Exception:
        qdb_status = "crit"
    results.append({
        "service": "questdb",
        "status": qdb_status,
        "latency_ms": int((time.monotonic() - t0) * 1000),
    })

    # Theta Terminal
    t0 = time.monotonic()
    try:
        r = requests.get(
            f"{_THETA_BASE}/option/list/expirations",
            params={"symbol": "SPY"},
            timeout=_HTTP_TIMEOUT,
        )
        theta_status = "ok" if r.status_code < 400 else "warn"
    except Exception:
        theta_status = "crit"
    results.append({
        "service": "thetadata",
        "status": theta_status,
        "latency_ms": int((time.monotonic() - t0) * 1000),
    })

    # Alpaca clock
    t0 = time.monotonic()
    try:
        r = requests.get(
            f"{_ALPACA_BROKER_BASE}/clock",
            headers={
                "APCA-API-KEY-ID": os.environ.get("APCA_API_KEY_ID", ""),
                "APCA-API-SECRET-KEY": os.environ.get("APCA_API_SECRET_KEY", ""),
            },
            timeout=_HTTP_TIMEOUT,
        )
        alpaca_status = "ok" if r.status_code < 400 else "warn"
    except Exception:
        alpaca_status = "crit"
    results.append({
        "service": "alpaca",
        "status": alpaca_status,
        "latency_ms": int((time.monotonic() - t0) * 1000),
    })

    # FinancialDatasets — recent error counter from job_runs
    t0 = time.monotonic()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT count(*) FROM job_runs
            WHERE job_name = 'ingest_financialdatasets'
              AND status = 'failed'
              AND started_at >= dateadd('h', -1, now())
            """
        )
        row = cur.fetchone()
        cur.close()
        err_count = row[0] if row else 0
        fd_status = "ok" if err_count == 0 else ("warn" if err_count < 3 else "crit")
    except Exception:
        fd_status = "warn"
    results.append({
        "service": "financialdatasets",
        "status": fd_status,
        "latency_ms": int((time.monotonic() - t0) * 1000),
    })

    return results


def kill_switches_latest(conn) -> list[dict]:
    """Latest kill_switch row per experiment_id."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT timestamp, experiment_id, mode, trigger, action,
               triggered_by, details, resolved, resolved_at, resolved_by
        FROM kill_switches
        LATEST ON timestamp PARTITION BY experiment_id
        """
    )
    rows = _rows_as_dicts(cur)
    cur.close()
    return rows


# ---------------------------------------------------------------------------
# Coverage
# ---------------------------------------------------------------------------

ETF_SYMBOLS: frozenset[str] = frozenset({"SPY", "QQQ"})

_COVERAGE_DOMAINS: dict[str, dict] = {
    "equity_ohlcv": {
        "table": "canonical_equity_ohlcv",
        "ts_col": "timestamp",
        "sym_col": "symbol",
        "where": "",
        "stock_only": False,
    },
    "options_eod": {
        "table": "canonical_options_chain",
        "ts_col": "quote_date",
        "sym_col": "underlying",
        "where": "WHERE source_vendor = 'thetadata_eod'",
        "stock_only": False,
    },
    "fundamentals": {
        "table": "canonical_fundamentals",
        "ts_col": "report_date",
        "sym_col": "symbol",
        "where": "",
        "stock_only": True,
    },
    "insider_trades": {
        "table": "canonical_insider_trades",
        "ts_col": "filing_date",
        "sym_col": "symbol",
        "where": "",
        "stock_only": True,
    },
    "institutional_holdings": {
        "table": "canonical_institutional_holdings",
        "ts_col": "filing_date",
        "sym_col": "symbol",
        "where": "",
        "stock_only": True,
    },
}


def _business_days_behind(max_ts) -> int:
    """Business days between max_ts date and today (0 if up to date)."""
    import numpy as np
    if max_ts is None:
        return -1
    if hasattr(max_ts, "date"):
        from_date = max_ts.date()
    elif isinstance(max_ts, str):
        try:
            from_date = dt.date.fromisoformat(max_ts[:10])
        except ValueError:
            return -1
    else:
        from_date = max_ts
    today = dt.date.today()
    if from_date >= today:
        return 0
    return int(np.busday_count(from_date, today))


def coverage(conn) -> dict:
    """Per-domain coverage stats.

    Returns {domain: {symbol_count, from, to, mode_rows, exceptions,
                       etf_absent, sessions_behind, freshness_status}}.
    """
    result: dict[str, Any] = {}

    for domain, cfg in _COVERAGE_DOMAINS.items():
        table = cfg["table"]
        ts_col = cfg["ts_col"]
        sym_col = cfg["sym_col"]
        where = cfg["where"]

        try:
            cur = conn.cursor()
            cur.execute(
                f"""
                SELECT {sym_col} AS symbol,
                       min({ts_col}) AS from_ts,
                       max({ts_col}) AS to_ts,
                       count(*) AS row_count
                FROM {table}
                {where}
                GROUP BY {sym_col}
                """
            )
            rows = cur.fetchall()
            cur.close()
        except Exception as exc:
            result[domain] = {"error": str(exc)}
            continue

        if not rows:
            result[domain] = {
                "symbol_count": 0,
                "from": None,
                "to": None,
                "mode_rows": 0,
                "exceptions": [],
                "etf_absent": list(ETF_SYMBOLS) if cfg["stock_only"] else [],
                "sessions_behind": -1,
                "freshness_status": "crit",
            }
            continue

        symbols = {r[0] for r in rows}
        counts = [r[3] for r in rows]
        from_dates = [r[1] for r in rows if r[1] is not None]
        to_dates = [r[2] for r in rows if r[2] is not None]

        try:
            mode_count = stat_mode(counts)
        except Exception:
            mode_count = max(counts) if counts else 0

        # Exceptions: symbols with < 80% of modal row count
        threshold = mode_count * 0.8 if mode_count > 0 else 0
        exceptions = sorted([r[0] for r in rows if r[3] < threshold])

        etf_absent = sorted([s for s in ETF_SYMBOLS if s not in symbols]) if cfg["stock_only"] else []

        global_from = min(from_dates, default=None)
        global_to = max(to_dates, default=None)

        sessions_behind = -1
        freshness_status = "ok"
        if domain == "equity_ohlcv":
            sessions_behind = _business_days_behind(global_to)
            if sessions_behind > 7:
                freshness_status = "crit"
            elif sessions_behind > 3:
                freshness_status = "warn"

        result[domain] = {
            "symbol_count": len(symbols),
            "from": str(global_from) if global_from is not None else None,
            "to": str(global_to) if global_to is not None else None,
            "mode_rows": mode_count,
            "exceptions": exceptions,
            "etf_absent": etf_absent,
            "sessions_behind": sessions_behind,
            "freshness_status": freshness_status,
        }

    return result


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

_TABLE_DOMAINS: dict[str, str] = {
    "raw_alpaca_equity_ohlcv": "raw",
    "raw_fd_fundamentals": "raw",
    "raw_fd_financial_metrics": "raw",
    "raw_fd_insider_trades": "raw",
    "raw_fd_analyst_estimates": "raw",
    "raw_fd_earnings": "raw",
    "raw_fd_institutional_holdings": "raw",
    "raw_thetadata_options_chain": "raw",
    "raw_thetadata_options_eod": "raw",
    "canonical_equity_ohlcv": "canonical",
    "canonical_fundamentals": "canonical",
    "canonical_financial_metrics": "canonical",
    "canonical_options_chain": "canonical",
    "canonical_insider_trades": "canonical",
    "canonical_institutional_holdings": "canonical",
    "canonical_inst_ownership": "canonical",
    "features": "features",
    "feature_watermarks": "features",
    "experiment_index": "experiments",
    "execution_log": "experiments",
    "execution_metrics": "experiments",
    "positions": "experiments",
    "orders": "experiments",
    "portfolio_state": "experiments",
    "risk_decisions": "ops",
    "kill_switches": "ops",
    "promotions": "ops",
    "audit_trail": "ops",
    "trading_heartbeat": "ops",
    "reconciliation_log": "ops",
    "canonical_pins": "ops",
    "canonical_hashes": "ops",
    "job_runs": "ops",
}


def storage(conn) -> dict:
    """Table storage grouped by domain with WAL/dedup write-mode flags."""
    storage_map: dict[str, dict] = {}

    cur = conn.cursor()
    cur.execute("SELECT tableName, rowCount, diskSize FROM table_storage()")
    for row in cur.fetchall():
        name, row_count, disk_size = row
        storage_map[name] = {
            "row_count": row_count or 0,
            "disk_size": disk_size or 0,
            "wal_enabled": False,
            "dedup": False,
        }
    cur.close()

    try:
        cur = conn.cursor()
        cur.execute("SELECT tableName, walEnabled, dedup FROM tables()")
        for row in cur.fetchall():
            name, wal_enabled, dedup = row
            if name in storage_map:
                storage_map[name]["wal_enabled"] = bool(wal_enabled)
                storage_map[name]["dedup"] = bool(dedup)
        cur.close()
    except Exception:
        pass  # older QuestDB may not expose dedup column

    grouped: dict[str, list] = {}
    for tname, info in storage_map.items():
        domain = _TABLE_DOMAINS.get(tname, "other")
        grouped.setdefault(domain, []).append({"table": tname, **info})

    return grouped


def storage_tickers(conn, domain: str) -> list[dict]:
    """Per-symbol rows/range for a coverage domain (lazy drill-down)."""
    if domain == "features":
        cur = conn.cursor()
        cur.execute(
            """
            SELECT feature_set, symbol,
                   min(timestamp) AS from_ts,
                   max(timestamp) AS to_ts,
                   count(*) AS row_count
            FROM features
            GROUP BY feature_set, symbol
            ORDER BY feature_set, symbol
            """
        )
        rows = _rows_as_dicts(cur)
        cur.close()
        return rows

    if domain not in _COVERAGE_DOMAINS:
        return []

    cfg = _COVERAGE_DOMAINS[domain]
    table = cfg["table"]
    ts_col = cfg["ts_col"]
    sym_col = cfg["sym_col"]
    where = cfg["where"]

    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT {sym_col} AS symbol,
               min({ts_col}) AS from_ts,
               max({ts_col}) AS to_ts,
               count(*) AS row_count
        FROM {table}
        {where}
        GROUP BY {sym_col}
        ORDER BY {sym_col}
        """
    )
    rows = _rows_as_dicts(cur)
    cur.close()
    return rows


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------


def runs(conn, hours: int = 24) -> list[dict]:
    """Job runs feed for the last N hours, newest first."""
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT started_at, job_name, dagster_run_id, status,
               finished_at, duration_s, rows_written, detail, failure_cause
        FROM job_runs
        WHERE started_at >= dateadd('h', -{int(hours)}, now())
        ORDER BY started_at DESC
        """
    )
    rows = _rows_as_dicts(cur)
    cur.close()
    return rows


# ---------------------------------------------------------------------------
# Experiments
# ---------------------------------------------------------------------------


def _get_data_root() -> Path:
    return Path(os.environ.get("YATS_DATA_ROOT", ".yats_data"))


def _downsample(series: list, max_points: int = 200) -> list:
    """Uniform-stride downsample to at most max_points."""
    if len(series) <= max_points:
        return series
    stride = max(1, len(series) // max_points)
    return series[::stride][:max_points]


def experiments_search(conn, q: str = "") -> dict:
    """Search experiment_index + wfo sweep configs.

    Returns {results: [...], deflation_clock: int}.
    deflation_clock = total trials across all sweep summaries.
    """
    q_lower = q.lower().strip()
    results: list[dict] = []

    cur = conn.cursor()
    cur.execute(
        """
        SELECT experiment_id, universe, feature_set, policy_type,
               reward_version, sharpe, calmar, max_drawdown, total_return,
               qualification_status, promotion_tier, created_at, stale
        FROM experiment_index
        ORDER BY created_at DESC
        """
    )
    index_rows = _rows_as_dicts(cur)
    cur.close()

    for row in index_rows:
        if not q_lower or any(
            q_lower in str(row.get(f) or "").lower()
            for f in ("experiment_id", "feature_set", "policy_type", "promotion_tier")
        ):
            results.append({**row, "evaluation_regime": "split"})

    deflation_clock = 0
    sweeps_dir = _get_data_root() / "wfo_sweeps"
    if sweeps_dir.exists():
        for summary_path in sweeps_dir.glob("*/sweep_summary.json"):
            try:
                summary = json.loads(summary_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            deflation_clock += summary.get("grid_size", 0)
            sweep_name = summary.get("sweep", summary_path.parent.name)
            for cfg in summary.get("configs", []):
                policy = cfg.get("policy", "")
                fs = cfg.get("feature_set", "")
                cfg_id = f"{sweep_name}:{policy}:{fs}"
                if not q_lower or any(
                    q_lower in s.lower()
                    for s in (cfg_id, policy, fs, sweep_name)
                    if s
                ):
                    results.append({
                        "experiment_id": cfg_id,
                        "feature_set": fs,
                        "policy_type": policy,
                        "sharpe": cfg.get("sharpe"),
                        "dsr": cfg.get("dsr"),
                        "dsr_significant": cfg.get("dsr_significant"),
                        "evaluation_regime": "wfo-oos",
                        "sweep": sweep_name,
                    })

    return {"results": results, "deflation_clock": deflation_clock}


def experiment_detail(conn, experiment_id: str) -> dict:
    """Artifact bundle for an experiment: spec + metrics + equity_curve + qual report."""
    data_root = _get_data_root()
    exp_dir = data_root / "experiments" / experiment_id
    result: dict[str, Any] = {"experiment_id": experiment_id}

    # Spec
    spec_path = exp_dir / "spec" / "experiment_spec.json"
    if spec_path.exists():
        try:
            result["spec"] = json.loads(spec_path.read_text(encoding="utf-8"))
        except Exception:
            result["spec"] = None
    else:
        result["spec"] = None

    # Evaluation metrics + equity curve
    metrics_path = exp_dir / "evaluation" / "metrics.json"
    if metrics_path.exists():
        try:
            metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
            result["performance"] = metrics.get("performance")
            result["trading"] = metrics.get("trading")
            result["safety"] = metrics.get("safety")
            result["config"] = metrics.get("config")
            result["inputs_used"] = metrics.get("inputs_used")
            eq = metrics.get("series", {}).get("equity_curve", [])
            result["equity_curve"] = _downsample(eq)
        except Exception:
            result["performance"] = None
            result["trading"] = None
            result["safety"] = None
            result["equity_curve"] = []
    else:
        result["performance"] = None
        result["trading"] = None
        result["safety"] = None
        result["equity_curve"] = []

    # Qualification report
    qual_path = exp_dir / "promotion" / "qualification_report.json"
    if qual_path.exists():
        try:
            result["qualification_report"] = json.loads(
                qual_path.read_text(encoding="utf-8")
            )
        except Exception:
            result["qualification_report"] = None
    else:
        result["qualification_report"] = None

    # experiment_index row
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT experiment_id, universe, feature_set, policy_type,
                   sharpe, calmar, max_drawdown, total_return,
                   qualification_status, promotion_tier, created_at
            FROM experiment_index
            WHERE experiment_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (experiment_id,),
        )
        cols = [d[0] for d in cur.description]
        row = cur.fetchone()
        cur.close()
        if row:
            result["index"] = dict(zip(cols, row))
    except Exception:
        pass

    return result


# ---------------------------------------------------------------------------
# Trading
# ---------------------------------------------------------------------------


def trading(conn) -> dict:
    """Live trading state: portfolio, open orders, risk, heartbeat, promotions."""
    result: dict[str, Any] = {}

    # Portfolio state — latest per experiment_id
    cur = conn.cursor()
    cur.execute(
        """
        SELECT timestamp, experiment_id, mode, nav, cash, gross_exposure,
               net_exposure, leverage, num_positions, daily_pnl, peak_nav, drawdown
        FROM portfolio_state
        LATEST ON timestamp PARTITION BY experiment_id
        """
    )
    result["portfolio_state"] = _rows_as_dicts(cur)
    cur.close()

    # Open orders grouped by experiment_id
    cur = conn.cursor()
    cur.execute(
        """
        SELECT experiment_id, mode, count(*) AS open_count
        FROM orders
        WHERE status IN ('submitted', 'partial')
        GROUP BY experiment_id, mode
        """
    )
    result["open_orders"] = _rows_as_dicts(cur)
    cur.close()

    # Risk decisions last 24h
    cur = conn.cursor()
    cur.execute(
        """
        SELECT timestamp, rule_id, experiment_id, mode,
               decision, action_taken, original_size, reduced_size
        FROM risk_decisions
        WHERE timestamp >= dateadd('h', -24, now())
        ORDER BY timestamp DESC
        """
    )
    result["risk_decisions"] = _rows_as_dicts(cur)
    cur.close()

    # Trading heartbeat age
    cur = conn.cursor()
    cur.execute(
        """
        SELECT timestamp, experiment_id, mode, loop_iteration,
               orders_pending, last_bar_received
        FROM trading_heartbeat
        LATEST ON timestamp PARTITION BY experiment_id
        """
    )
    hb_rows = _rows_as_dicts(cur)
    cur.close()

    now_utc = dt.datetime.now(tz=dt.timezone.utc)
    for row in hb_rows:
        ts = row.get("timestamp")
        if ts is not None:
            if hasattr(ts, "tzinfo") and ts.tzinfo is None:
                ts = ts.replace(tzinfo=dt.timezone.utc)
            age_s = (now_utc - ts).total_seconds()
            row["age_s"] = round(age_s, 1)
            row["heartbeat_status"] = "ok" if age_s < 120 else "crit"
    result["heartbeat"] = hb_rows

    # Promotions — latest per experiment_id, compute live allocations
    cur = conn.cursor()
    cur.execute(
        """
        SELECT promoted_at, experiment_id, tier, promoted_by,
               qualification_passed, sharpe, max_drawdown
        FROM promotions
        LATEST ON promoted_at PARTITION BY experiment_id
        """
    )
    promotions = _rows_as_dicts(cur)
    cur.close()

    live_exps = [p for p in promotions if p.get("tier") == "live"]
    n = len(live_exps)
    for p in live_exps:
        p["allocation"] = round(1.0 / n, 4) if n > 0 else 0.0
    result["promotions"] = promotions

    return result
