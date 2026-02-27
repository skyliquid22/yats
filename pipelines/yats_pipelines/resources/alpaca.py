"""Alpaca Data API v2 vendor adapter — historical OHLCV bars."""

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

RETRY_DELAYS = [1, 5, 30]  # seconds — exponential backoff
DATA_BASE_URL = "https://data.alpaca.markets/v2"


@dataclass
class AlpacaResource:
    """Alpaca resource — API key from environment.

    Auth uses APCA-API-KEY-ID + APCA-API-SECRET-KEY headers.
    Data API v2 at data.alpaca.markets/v2.
    """

    api_key: str = field(
        default_factory=lambda: os.environ.get("APCA_API_KEY_ID", "")
    )
    api_secret: str = field(
        default_factory=lambda: os.environ.get("APCA_API_SECRET_KEY", "")
    )
    data_base_url: str = DATA_BASE_URL
    max_concurrency: int = 5
    request_delay: float = 0.2  # seconds between requests for rate limiting

    def _headers(self) -> dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.api_secret,
        }

    def _request_with_retry(self, url: str, params: dict) -> dict:
        """Execute GET request with exponential backoff retry (3 attempts)."""
        last_exc: Exception | None = None
        for attempt, delay in enumerate(RETRY_DELAYS):
            try:
                resp = requests.get(
                    url, headers=self._headers(), params=params, timeout=30
                )
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", delay))
                    logger.warning(
                        "Rate limited (429), waiting %ds (attempt %d/%d)",
                        retry_after,
                        attempt + 1,
                        len(RETRY_DELAYS),
                    )
                    time.sleep(retry_after)
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as exc:
                last_exc = exc
                if attempt < len(RETRY_DELAYS) - 1:
                    logger.warning(
                        "Request failed (attempt %d/%d): %s — retrying in %ds",
                        attempt + 1,
                        len(RETRY_DELAYS),
                        exc,
                        delay,
                    )
                    time.sleep(delay)
        raise RuntimeError(
            f"Alpaca API request failed after {len(RETRY_DELAYS)} attempts: {last_exc}"
        ) from last_exc

    def get_historical_bars(
        self,
        symbols: list[str],
        start: str,
        end: str,
        timeframe: str = "1Day",
    ) -> dict[str, list[dict]]:
        """Fetch historical OHLCV bars for multiple symbols.

        Args:
            symbols: List of ticker symbols (e.g. ["AAPL", "MSFT"]).
            start: Start date as ISO-8601 string (e.g. "2024-01-01").
            end: End date as ISO-8601 string (e.g. "2024-12-31").
            timeframe: Bar frequency — "1Day", "1Hour", etc.

        Returns:
            Dict mapping symbol to list of bar dicts with keys:
            t, o, h, l, c, v, vw, n (Alpaca wire format).
        """
        url = f"{self.data_base_url}/stocks/bars"
        all_bars: dict[str, list[dict]] = {s: [] for s in symbols}
        page_token: str | None = None

        while True:
            params: dict[str, str] = {
                "symbols": ",".join(symbols),
                "timeframe": timeframe,
                "start": start,
                "end": end,
                "limit": "10000",
                "adjustment": "raw",
            }
            if page_token:
                params["page_token"] = page_token

            data = self._request_with_retry(url, params)

            bars = data.get("bars", {})
            for sym, bar_list in bars.items():
                all_bars.setdefault(sym, []).extend(bar_list)

            page_token = data.get("next_page_token")
            if not page_token:
                break

            time.sleep(self.request_delay)

        total = sum(len(v) for v in all_bars.values())
        logger.info(
            "Fetched %d bars for %d symbols (%s to %s, %s)",
            total,
            len(symbols),
            start,
            end,
            timeframe,
        )
        return all_bars

    def normalize_bars(
        self, raw_bars: dict[str, list[dict]]
    ) -> list[dict]:
        """Convert Alpaca wire-format bars to YATS schema dicts.

        Returns list of dicts with keys matching raw_alpaca_equity_ohlcv columns:
        timestamp, symbol, open, high, low, close, volume, vwap, trade_count, ingested_at.
        """
        now = datetime.utcnow()
        rows: list[dict] = []
        for symbol, bars in raw_bars.items():
            for bar in bars:
                rows.append(
                    {
                        "timestamp": bar["t"],
                        "symbol": symbol,
                        "open": float(bar["o"]),
                        "high": float(bar["h"]),
                        "low": float(bar["l"]),
                        "close": float(bar["c"]),
                        "volume": int(bar["v"]),
                        "vwap": float(bar.get("vw", 0)),
                        "trade_count": int(bar.get("n", 0)),
                        "ingested_at": now,
                    }
                )
        return rows
