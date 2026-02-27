"""Dagster ingest job — Alpaca historical OHLCV bars to QuestDB via ILP."""

import logging
from datetime import datetime

from dagster import Config, OpExecutionContext, job, op
from questdb.ingress import Protocol, Sender, TimestampNanos

from yats_pipelines.resources.alpaca import AlpacaResource
from yats_pipelines.resources.questdb import QuestDBResource

logger = logging.getLogger(__name__)


class IngestAlpacaConfig(Config):
    """Run config for the Alpaca ingest job."""

    ticker_list: list[str]
    start_date: str  # ISO-8601, e.g. "2024-01-01"
    end_date: str  # ISO-8601, e.g. "2024-12-31"
    bar_frequency: str = "1Day"


@op
def fetch_alpaca_bars(
    context: OpExecutionContext, config: IngestAlpacaConfig
) -> dict:
    """Fetch historical OHLCV bars from Alpaca Data API v2."""
    alpaca = AlpacaResource()

    if not alpaca.api_key or not alpaca.api_secret:
        raise RuntimeError(
            "Missing Alpaca credentials: set APCA_API_KEY_ID and APCA_API_SECRET_KEY"
        )

    context.log.info(
        "Fetching %s bars for %d tickers: %s (%s to %s)",
        config.bar_frequency,
        len(config.ticker_list),
        ", ".join(config.ticker_list),
        config.start_date,
        config.end_date,
    )

    raw_bars = alpaca.get_historical_bars(
        symbols=config.ticker_list,
        start=config.start_date,
        end=config.end_date,
        timeframe=config.bar_frequency,
    )

    rows = alpaca.normalize_bars(raw_bars)
    context.log.info("Normalized %d rows for QuestDB ingestion", len(rows))
    return {"rows": rows, "count": len(rows)}


@op
def write_bars_to_questdb(
    context: OpExecutionContext, bars_result: dict
) -> None:
    """Write OHLCV bars to raw_alpaca_equity_ohlcv via QuestDB ILP."""
    rows = bars_result["rows"]
    if not rows:
        context.log.warning("No rows to write — skipping QuestDB ingestion")
        return

    qdb = QuestDBResource()
    table = "raw_alpaca_equity_ohlcv"

    context.log.info(
        "Writing %d rows to %s via ILP (%s:%d)",
        len(rows),
        table,
        qdb.ilp_host,
        qdb.ilp_port,
    )

    with Sender(Protocol.Tcp, qdb.ilp_host, qdb.ilp_port) as sender:
        for row in rows:
            ts = row["timestamp"]
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            ingested_at = row["ingested_at"]
            if isinstance(ingested_at, str):
                ingested_at = datetime.fromisoformat(
                    ingested_at.replace("Z", "+00:00")
                )

            sender.row(
                table,
                symbols={"symbol": row["symbol"]},
                columns={
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "vwap": row["vwap"],
                    "trade_count": row["trade_count"],
                    "ingested_at": TimestampNanos(
                        int(ingested_at.timestamp() * 1_000_000_000)
                    ),
                },
                at=TimestampNanos(int(ts.timestamp() * 1_000_000_000)),
            )
        sender.flush()

    context.log.info("Successfully wrote %d rows to %s", len(rows), table)


@job
def ingest_alpaca():
    """Dagster job: ingest Alpaca historical OHLCV bars into QuestDB."""
    bars = fetch_alpaca_bars()
    write_bars_to_questdb(bars)
