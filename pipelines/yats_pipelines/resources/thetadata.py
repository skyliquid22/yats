"""ThetaData REST API v2 vendor adapter — options chains, greeks, IV, open interest."""

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import requests

logger = logging.getLogger(__name__)

RETRY_DELAYS = [1, 5, 30]  # seconds — exponential backoff
# ThetaData v2 REST is served by the local Theta Terminal process.
# Override with THETADATA_BASE_URL env var if the terminal runs on a non-default port.
BASE_URL = os.environ.get("THETADATA_BASE_URL", "http://127.0.0.1:25510/v2")


@dataclass
class ThetaDataResource:
    """ThetaData REST API v2 resource — proxied through local Theta Terminal.

    The Theta Terminal must be running at base_url before any calls are made.
    No auth headers are sent; the terminal authenticates upstream at launch.
    Strikes are in milli-dollars (ThetaData internal) — divide by 1000 for USD.
    """

    base_url: str = field(
        default_factory=lambda: os.environ.get("THETADATA_BASE_URL", "http://127.0.0.1:25510/v2")
    )
    request_delay: float = 0.3  # seconds between requests for rate limiting

    def _request_with_retry(self, url: str, params: dict) -> dict:
        """Execute GET request with exponential backoff retry (3 attempts)."""
        last_exc: Exception | None = None
        for attempt, delay in enumerate(RETRY_DELAYS):
            try:
                resp = requests.get(url, params=params, timeout=30)
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
            except requests.exceptions.ConnectionError as exc:
                raise RuntimeError(
                    "Theta Terminal not running — start it with your ThetaData credentials; see README"
                ) from exc
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
            f"ThetaData request failed after {len(RETRY_DELAYS)} attempts: {last_exc}"
        ) from last_exc

    @staticmethod
    def _parse_response(data: dict) -> list[dict]:
        """Convert ThetaData v2 columnar response to list of row dicts."""
        fields = data["header"]["format"]
        return [dict(zip(fields, row)) for row in data.get("response", [])]

    def list_expirations(self, root: str) -> list[str]:
        """List all available expiration dates for a root.

        Returns sorted list of YYYYMMDD strings.
        """
        url = f"{self.base_url}/list/expirations"
        raw = self._request_with_retry(url, {"root": root})
        rows = self._parse_response(raw)
        exps = []
        for row in rows:
            # ThetaData returns expirations as integers YYYYMMDD in first field
            val = row.get("expirations") or next(iter(row.values()), None)
            if val is not None:
                exps.append(str(int(val)))
        return sorted(exps)

    def get_option_chain_snapshot(self, root: str, exp: str) -> list[dict]:
        """Get option chain snapshot for a root + expiry.

        Calls greeks, quote, open_interest, and ohlc bulk snapshot endpoints
        and joins them by (strike_raw, right) key.

        Args:
            root: Underlying ticker (e.g. "AAPL").
            exp: Expiration date as YYYYMMDD string (e.g. "20240119").

        Returns:
            List of dicts per contract with fields: root, exp, strike, right,
            bid, ask, last, iv, delta, gamma, theta, vega, rho,
            open_interest, volume, quote_ts.
        """
        params = {"root": root, "exp": exp}

        greeks_rows = self._parse_response(
            self._request_with_retry(
                f"{self.base_url}/bulk_snapshot/option/greeks", params
            )
        )
        time.sleep(self.request_delay)

        quote_rows = self._parse_response(
            self._request_with_retry(
                f"{self.base_url}/bulk_snapshot/option/quote", params
            )
        )
        time.sleep(self.request_delay)

        oi_rows = self._parse_response(
            self._request_with_retry(
                f"{self.base_url}/bulk_snapshot/option/open_interest", params
            )
        )
        time.sleep(self.request_delay)

        ohlc_rows = self._parse_response(
            self._request_with_retry(
                f"{self.base_url}/bulk_snapshot/option/ohlc", params
            )
        )
        time.sleep(self.request_delay)

        def _key(row: dict) -> tuple:
            return (row.get("strike"), row.get("right"))

        greeks_by_key = {_key(r): r for r in greeks_rows}
        quote_by_key = {_key(r): r for r in quote_rows}
        oi_by_key = {_key(r): r for r in oi_rows}
        ohlc_by_key = {_key(r): r for r in ohlc_rows}

        rows = []
        for key, gk in greeks_by_key.items():
            qt = quote_by_key.get(key, {})
            oi = oi_by_key.get(key, {})
            oh = ohlc_by_key.get(key, {})

            ms = gk.get("ms_of_day") or qt.get("ms_of_day") or 0
            date_int = gk.get("date") or qt.get("date") or 0

            rows.append({
                "root": root,
                "exp": exp,
                "strike": _parse_strike(gk.get("strike")),
                "right": gk.get("right", ""),
                "bid": _f(qt.get("bid")),
                "ask": _f(qt.get("ask")),
                "last": _f(oh.get("close")),
                "iv": _f(gk.get("implied_vol")),
                "delta": _f(gk.get("delta")),
                "gamma": _f(gk.get("gamma")),
                "theta": _f(gk.get("theta")),
                "vega": _f(gk.get("vega")),
                "rho": _f(gk.get("rho")),
                "open_interest": _i(oi.get("open_interest")),
                "volume": _i(oh.get("volume")),
                "quote_ts": _parse_thetadata_datetime(date_int, ms),
            })

        logger.info(
            "Fetched %d option contracts for %s exp=%s", len(rows), root, exp
        )
        return rows

    def get_historical_eod(
        self, root: str, exp: str, start_date: str, end_date: str
    ) -> list[dict]:
        """Get historical EOD option data for all contracts of a root + expiry.

        Args:
            root: Underlying ticker (e.g. "AAPL").
            exp: Expiration date as YYYYMMDD string.
            start_date: Start date as YYYYMMDD string.
            end_date: End date as YYYYMMDD string.

        Returns:
            List of dicts per contract per day with fields: root, exp, strike,
            right, open, high, low, close, volume, trade_count, quote_date.
        """
        url = f"{self.base_url}/bulk_hist/option/eod"
        params = {
            "root": root,
            "exp": exp,
            "start_date": start_date,
            "end_date": end_date,
        }
        raw = self._request_with_retry(url, params)
        raw_rows = self._parse_response(raw)

        rows = []
        for row in raw_rows:
            date_int = row.get("date", 0)
            rows.append({
                "root": root,
                "exp": exp,
                "strike": _parse_strike(row.get("strike")),
                "right": row.get("right", ""),
                "open": _f(row.get("open")),
                "high": _f(row.get("high")),
                "low": _f(row.get("low")),
                "close": _f(row.get("close")),
                "volume": _i(row.get("volume")),
                "trade_count": _i(row.get("count")),
                "quote_date": _parse_thetadata_date(date_int),
            })

        logger.info(
            "Fetched %d EOD rows for %s exp=%s (%s to %s)",
            len(rows),
            root,
            exp,
            start_date,
            end_date,
        )
        return rows

    def normalize_chain_snapshot(
        self,
        raw_rows: list[dict],
        ingested_at: datetime,
        dagster_run_id: str,
    ) -> list[dict]:
        """Add ingestion metadata to chain snapshot rows for raw table write."""
        return [
            {**row, "ingested_at": ingested_at, "dagster_run_id": dagster_run_id}
            for row in raw_rows
        ]

    def normalize_eod(
        self,
        raw_rows: list[dict],
        ingested_at: datetime,
        dagster_run_id: str,
    ) -> list[dict]:
        """Add ingestion metadata to EOD rows for raw table write."""
        return [
            {**row, "ingested_at": ingested_at, "dagster_run_id": dagster_run_id}
            for row in raw_rows
        ]


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _f(val) -> float | None:
    """Safe float conversion."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _i(val) -> int | None:
    """Safe int conversion."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _parse_strike(val) -> float | None:
    """Convert ThetaData strike (milli-dollars) to USD dollars."""
    if val is None:
        return None
    try:
        return float(val) / 1000.0
    except (ValueError, TypeError):
        return None


def _parse_thetadata_date(date_int: int) -> datetime | None:
    """Convert ThetaData YYYYMMDD integer to UTC midnight datetime."""
    if not date_int:
        return None
    try:
        s = str(int(date_int))
        return datetime(int(s[:4]), int(s[4:6]), int(s[6:8]), tzinfo=timezone.utc)
    except (ValueError, IndexError):
        return None


def _parse_thetadata_datetime(date_int: int, ms_of_day: int) -> datetime | None:
    """Convert ThetaData date + ms_of_day offset to UTC datetime."""
    base = _parse_thetadata_date(date_int)
    if base is None:
        return None
    return base + timedelta(milliseconds=int(ms_of_day or 0))
