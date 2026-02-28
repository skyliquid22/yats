"""Streaming canonicalization — Alpaca WebSocket bars → canonical_equity_ohlcv.

Receives real-time bars via Alpaca WebSocket, validates, and writes to
canonical_equity_ohlcv via QuestDB ILP with reconcile_method="streaming".

Streaming canonical data:
- Used ONLY by live execution loops
- NOT used by research pipelines (they filter reconcile_method="batch")
- Does NOT carry reproducibility guarantees
"""

import asyncio
import json
import logging
import os
import signal
from datetime import datetime, timezone

from dagster import Config, OpExecutionContext, job, op
from questdb.ingress import Protocol, Sender, TimestampNanos

from yats_pipelines.resources.questdb import QuestDBResource

logger = logging.getLogger(__name__)

# Alpaca WebSocket endpoints by feed type
WS_BASE = "wss://stream.data.alpaca.markets/v2"
DEFAULT_FEED = "iex"

# Reconnection policy
INITIAL_BACKOFF_S = 1.0
MAX_BACKOFF_S = 60.0
BACKOFF_MULTIPLIER = 2.0
RECONNECT_ALERT_THRESHOLD_S = 300  # 5 minutes

# OHLCV price fields for validation
_OHLCV_PRICE_FIELDS = ("open", "high", "low", "close")


# ---------------------------------------------------------------------------
# Helpers (same ILP patterns as canonicalize.py)
# ---------------------------------------------------------------------------


def _ilp_sender(qdb: QuestDBResource):
    """Create a QuestDB ILP Sender context manager."""
    return Sender(Protocol.Tcp, qdb.ilp_host, qdb.ilp_port)


def _ts_nanos(dt: datetime) -> TimestampNanos:
    """Convert datetime to ILP TimestampNanos."""
    return TimestampNanos(int(dt.timestamp() * 1_000_000_000))


def _row(sender, table: str, symbols: dict, columns: dict, at: datetime) -> None:
    """Write an ILP row, filtering out None values from columns."""
    sender.row(
        table,
        symbols=symbols,
        columns={k: v for k, v in columns.items() if v is not None},
        at=_ts_nanos(at),
    )


# ---------------------------------------------------------------------------
# Validation (lightweight streaming variant — no rolling stats)
# ---------------------------------------------------------------------------


def _validate_bar(bar: dict) -> list[str]:
    """Validate a single OHLCV bar. Returns list of warning strings.

    Streaming validation is simpler than batch: no rolling outlier detection
    (no pre-loaded history). Checks missing fields and non-positive prices.
    """
    warnings = []

    for field in ("open", "high", "low", "close", "volume"):
        if bar.get(field) is None:
            warnings.append(f"missing_{field}")

    for field in _OHLCV_PRICE_FIELDS:
        val = bar.get(field)
        if val is not None and val <= 0:
            warnings.append(f"non_positive_{field}={val}")

    return warnings


def _validation_status(warnings: list[str]) -> str:
    """Determine validation status from warnings."""
    if any(w.startswith("missing_") or w.startswith("non_positive_") for w in warnings):
        return "failed"
    if warnings:
        return "warning"
    return "passed"


# ---------------------------------------------------------------------------
# Wire format normalization
# ---------------------------------------------------------------------------


def _normalize_ws_bar(msg: dict) -> dict:
    """Convert Alpaca WebSocket bar message to YATS schema dict.

    Alpaca WebSocket bar fields:
    S=symbol, t=timestamp, o=open, h=high, l=low, c=close,
    v=volume, vw=vwap, n=trade_count
    """
    return {
        "symbol": msg["S"],
        "timestamp": msg["t"],
        "open": float(msg["o"]),
        "high": float(msg["h"]),
        "low": float(msg["l"]),
        "close": float(msg["c"]),
        "volume": int(msg["v"]),
        "vwap": float(msg.get("vw", 0)),
        "trade_count": int(msg.get("n", 0)),
    }


# ---------------------------------------------------------------------------
# ILP write
# ---------------------------------------------------------------------------


def _write_canonical_bar(sender, bar: dict, now: datetime) -> str:
    """Write a validated bar to canonical_equity_ohlcv and reconciliation_log.

    Returns the validation status.
    """
    warnings = _validate_bar(bar)
    status = _validation_status(warnings)

    ts_str = bar["timestamp"]
    if isinstance(ts_str, str):
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    else:
        ts = ts_str

    _row(sender, "canonical_equity_ohlcv",
         symbols={
             "symbol": bar["symbol"],
             "source_vendor": "alpaca",
             "reconcile_method": "streaming",
             "validation_status": status,
         },
         columns={
             "open": bar.get("open"),
             "high": bar.get("high"),
             "low": bar.get("low"),
             "close": bar.get("close"),
             "volume": bar.get("volume"),
             "vwap": bar.get("vwap"),
             "trade_count": bar.get("trade_count"),
             "canonicalized_at": _ts_nanos(now),
         },
         at=ts)

    _row(sender, "reconciliation_log",
         symbols={
             "domain": "equity_ohlcv",
             "symbol": bar["symbol"],
             "primary_vendor": "alpaca",
         },
         columns={
             "fallback_used": False,
             "validation_warnings": json.dumps(warnings) if warnings else "[]",
             "dagster_run_id": "",
             "reconciled_at": _ts_nanos(now),
         },
         at=ts)

    return status


def _write_audit_alert(sender, message: str, now: datetime) -> None:
    """Write a reconnect failure alert to audit_trail."""
    _row(sender, "audit_trail",
         symbols={
             "tool_name": "stream_canonical",
             "invoker": "streaming_pipeline",
             "mode": "streaming",
             "result_status": "error",
         },
         columns={
             "result_summary": message,
             "duration_ms": 0,
             "dagster_run_id": "",
         },
         at=now)
    sender.flush()


# ---------------------------------------------------------------------------
# WebSocket streaming loop
# ---------------------------------------------------------------------------


async def _stream_bars(
    symbols: list[str],
    feed: str,
    qdb: QuestDBResource,
    duration_s: int,
    log,
) -> dict:
    """Connect to Alpaca WebSocket and stream bars to canonical table.

    Returns stats dict with counts.
    """
    import websockets

    api_key = os.environ.get("APCA_API_KEY_ID", "")
    api_secret = os.environ.get("APCA_API_SECRET_KEY", "")
    if not api_key or not api_secret:
        raise RuntimeError("APCA_API_KEY_ID and APCA_API_SECRET_KEY must be set")

    ws_url = f"{WS_BASE}/{feed}"
    stats = {"bars_received": 0, "bars_written": 0, "reconnects": 0, "errors": 0}
    stop_event = asyncio.Event()

    # Handle graceful shutdown
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    # Set deadline if duration specified
    if duration_s > 0:
        loop.call_later(duration_s, stop_event.set)

    backoff = INITIAL_BACKOFF_S
    disconnect_start: float | None = None

    with _ilp_sender(qdb) as sender:
        while not stop_event.is_set():
            try:
                async with websockets.connect(ws_url) as ws:
                    # Authenticate
                    auth_msg = await ws.recv()
                    auth_data = json.loads(auth_msg)
                    if not isinstance(auth_data, list) or not any(
                        m.get("T") == "success" and m.get("msg") == "connected"
                        for m in auth_data
                    ):
                        log.error("Unexpected connect message: %s", auth_data)
                        raise RuntimeError(f"WebSocket connect failed: {auth_data}")

                    await ws.send(json.dumps({
                        "action": "auth",
                        "key": api_key,
                        "secret": api_secret,
                    }))

                    auth_resp = await ws.recv()
                    auth_resp_data = json.loads(auth_resp)
                    if not isinstance(auth_resp_data, list) or not any(
                        m.get("T") == "success" and m.get("msg") == "authenticated"
                        for m in auth_resp_data
                    ):
                        raise RuntimeError(f"WebSocket auth failed: {auth_resp_data}")

                    log.info("Authenticated with Alpaca WebSocket (%s feed)", feed)

                    # Subscribe to bars
                    await ws.send(json.dumps({
                        "action": "subscribe",
                        "bars": symbols,
                    }))

                    sub_resp = await ws.recv()
                    log.info("Subscribed to bars: %s", sub_resp)

                    # Reset backoff and disconnect timer on successful connection
                    backoff = INITIAL_BACKOFF_S
                    if disconnect_start is not None:
                        log.info("Reconnected after %.1fs", asyncio.get_event_loop().time() - disconnect_start)
                        disconnect_start = None

                    # Receive loop
                    while not stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        except asyncio.TimeoutError:
                            # No data in 30s — connection health check (send ping)
                            continue

                        messages = json.loads(raw)
                        if not isinstance(messages, list):
                            messages = [messages]

                        now = datetime.now(timezone.utc)
                        for msg in messages:
                            if msg.get("T") != "b":
                                # Not a bar message (could be trade, quote, etc.)
                                continue

                            stats["bars_received"] += 1
                            try:
                                bar = _normalize_ws_bar(msg)
                                status = _write_canonical_bar(sender, bar, now)
                                stats["bars_written"] += 1

                                if status == "failed":
                                    log.warning(
                                        "Bar validation failed: %s at %s",
                                        bar["symbol"], bar["timestamp"],
                                    )
                            except (KeyError, ValueError, TypeError) as exc:
                                stats["errors"] += 1
                                log.error("Error processing bar: %s — %s", msg, exc)

                        # Flush after each batch of messages
                        sender.flush()

            except Exception as exc:
                if stop_event.is_set():
                    break

                stats["reconnects"] += 1
                log.warning("WebSocket disconnected: %s — reconnecting in %.1fs", exc, backoff)

                # Track disconnect duration for alerting
                if disconnect_start is None:
                    disconnect_start = asyncio.get_event_loop().time()
                else:
                    elapsed = asyncio.get_event_loop().time() - disconnect_start
                    if elapsed > RECONNECT_ALERT_THRESHOLD_S:
                        alert_msg = (
                            f"Streaming reconnect failed for {elapsed:.0f}s "
                            f"(threshold: {RECONNECT_ALERT_THRESHOLD_S}s): {exc}"
                        )
                        log.error(alert_msg)
                        _write_audit_alert(sender, alert_msg, datetime.now(timezone.utc))

                await asyncio.sleep(backoff)
                backoff = min(backoff * BACKOFF_MULTIPLIER, MAX_BACKOFF_S)

    return stats


# ---------------------------------------------------------------------------
# Dagster op + job
# ---------------------------------------------------------------------------


class StreamCanonicalConfig(Config):
    """Run config for the stream_canonical job."""

    symbols: list[str] = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]
    feed: str = DEFAULT_FEED
    duration_s: int = 0  # 0 = run until signal; >0 = run for N seconds then stop


@op
def stream_canonical_op(context: OpExecutionContext, config: StreamCanonicalConfig):
    """Stream Alpaca WebSocket bars to canonical_equity_ohlcv via ILP."""
    qdb = QuestDBResource()

    context.log.info(
        "Starting streaming canonicalization: symbols=%s, feed=%s, duration=%s",
        config.symbols,
        config.feed,
        f"{config.duration_s}s" if config.duration_s > 0 else "indefinite",
    )

    stats = asyncio.run(_stream_bars(
        symbols=config.symbols,
        feed=config.feed,
        qdb=qdb,
        duration_s=config.duration_s,
        log=context.log,
    ))

    context.log.info(
        "Streaming canonicalization stopped: received=%d, written=%d, "
        "reconnects=%d, errors=%d",
        stats["bars_received"],
        stats["bars_written"],
        stats["reconnects"],
        stats["errors"],
    )


@job
def stream_canonical():
    """Dagster job: stream Alpaca WebSocket bars to canonical_equity_ohlcv."""
    stream_canonical_op()
