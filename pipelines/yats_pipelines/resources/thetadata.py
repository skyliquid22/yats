"""ThetaData REST API v3 vendor adapter — options chains, greeks, IV, open interest."""

import csv
import io
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import requests

logger = logging.getLogger(__name__)

RETRY_DELAYS = [1, 5, 30]  # seconds — exponential backoff
# ThetaData v3 REST is served by the local Theta Terminal v3 process.
# Override with THETADATA_BASE_URL env var if the terminal runs on a non-default address.
BASE_URL = os.environ.get("THETADATA_BASE_URL", "http://127.0.0.1:25503/v3")


@dataclass
class ThetaDataResource:
    """ThetaData REST API v3 resource — proxied through local Theta Terminal v3.

    The Theta Terminal v3 must be running at base_url before any calls are made.
    No auth headers are sent; the terminal authenticates upstream at launch.
    Strikes are in dollars (v3 native format — no milli-dollar conversion).
    Expiration dates use YYYYMMDD at the interface boundary; v3 API also accepts this.
    """

    base_url: str = field(
        default_factory=lambda: os.environ.get("THETADATA_BASE_URL", "http://127.0.0.1:25503/v3")
    )
    request_delay: float = 0.3  # seconds between requests for rate limiting

    def _request_with_retry(self, url: str, params: dict) -> str:
        """Execute GET request with exponential backoff retry (3 attempts). Returns CSV text.

        HTTP 472 (no data for request) is treated as a valid empty result: returns ""
        immediately with no retries. Retry logic applies only to 5xx and connection errors.
        """
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
                if resp.status_code == 472:
                    logger.debug("ThetaData 472 (no data) for %s — returning empty result", url)
                    return ""
                resp.raise_for_status()
                return resp.text
            except requests.exceptions.ConnectionError as exc:
                raise RuntimeError(
                    "Theta Terminal not running — start it with: "
                    "java -jar ThetaTerminalv3.jar --api-key <key>; see README"
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

    def _try_get_second_order_greeks(self, params: dict) -> dict[tuple, float | None]:
        """Fetch second-order greeks snapshot and return gamma keyed by (strike, right).

        Returns {} immediately (no retries) when:
        - Subscription does not include second-order greeks (403)
        - No data available for the request (472)
        - Any connection or HTTP error

        This is a best-effort call — callers treat a missing key as gamma=None.
        """
        url = f"{self.base_url}/option/snapshot/greeks/second_order"
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code in (403, 472):
                logger.debug(
                    "Second-order greeks not available (HTTP %d) for %s",
                    resp.status_code,
                    params,
                )
                return {}
            resp.raise_for_status()
        except requests.exceptions.ConnectionError:
            logger.debug("Second-order greeks fetch skipped: Theta Terminal not reachable")
            return {}
        except requests.RequestException as exc:
            logger.debug("Second-order greeks fetch failed: %s", exc)
            return {}
        rows = self._parse_csv_response(resp.text)
        return {
            (r.get("strike", ""), r.get("right", "")): _f(r.get("gamma"))
            for r in rows
        }

    @staticmethod
    def _parse_csv_response(text: str) -> list[dict]:
        """Parse ThetaData v3 CSV response into list of row dicts."""
        reader = csv.DictReader(io.StringIO(text))
        return [dict(row) for row in reader]

    def list_expirations(self, root: str) -> list[str]:
        """List all available expiration dates for a root symbol.

        Returns sorted list of YYYYMMDD strings.
        """
        url = f"{self.base_url}/option/list/expirations"
        text = self._request_with_retry(url, {"symbol": root})
        rows = self._parse_csv_response(text)
        exps = []
        for row in rows:
            exp = row.get("expiration", "")
            if exp:
                # v3 returns "YYYY-MM-DD"; strip dashes for interface compatibility
                exps.append(exp.replace("-", ""))
        return sorted(exps)

    def get_option_chain_snapshot(self, root: str, exp: str) -> list[dict]:
        """Get option chain snapshot for a root + expiry.

        Calls greeks/first_order, open_interest, and ohlc endpoints and joins
        them by (strike, right) key.

        Args:
            root: Underlying ticker (e.g. "AAPL").
            exp: Expiration date as YYYYMMDD string (e.g. "20260710").

        Returns:
            List of dicts per contract with fields: root, exp, strike, right,
            bid, ask, last, iv, delta, gamma, theta, vega, rho,
            open_interest, volume, quote_ts.

            Note: gamma is populated from the second-order greeks endpoint when
            available (requires professional subscription); None on STANDARD plan.
        """
        params = {"symbol": root, "expiration": exp}

        greeks_rows = self._parse_csv_response(
            self._request_with_retry(
                f"{self.base_url}/option/snapshot/greeks/first_order", params
            )
        )
        time.sleep(self.request_delay)

        oi_rows = self._parse_csv_response(
            self._request_with_retry(
                f"{self.base_url}/option/snapshot/open_interest", params
            )
        )
        time.sleep(self.request_delay)

        ohlc_rows = self._parse_csv_response(
            self._request_with_retry(
                f"{self.base_url}/option/snapshot/ohlc", params
            )
        )
        time.sleep(self.request_delay)

        second_order = self._try_get_second_order_greeks(params)
        time.sleep(self.request_delay)

        def _key(row: dict) -> tuple:
            return (row.get("strike", ""), row.get("right", ""))

        oi_by_key = {_key(r): r for r in oi_rows}
        ohlc_by_key = {_key(r): r for r in ohlc_rows}

        rows = []
        for gk in greeks_rows:
            key = _key(gk)
            oi = oi_by_key.get(key, {})
            oh = ohlc_by_key.get(key, {})

            rows.append({
                "root": root,
                "exp": exp,
                "strike": _f(gk.get("strike")),   # v3: already in dollars
                "right": gk.get("right", ""),      # v3: "CALL" or "PUT"
                "bid": _f(gk.get("bid")),
                "ask": _f(gk.get("ask")),
                "last": _f(oh.get("close")),
                "iv": _f(gk.get("implied_vol")),
                "delta": _f(gk.get("delta")),
                "gamma": second_order.get(key),    # None when not subscribed or no data
                "theta": _f(gk.get("theta")),
                "vega": _f(gk.get("vega")),
                "rho": _f(gk.get("rho")),
                "open_interest": _i(oi.get("open_interest")),
                "volume": _i(oh.get("volume")),
                "quote_ts": _parse_iso_timestamp(gk.get("timestamp")),
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
        url = f"{self.base_url}/option/history/eod"
        params = {
            "symbol": root,
            "expiration": exp,
            "start_date": start_date,
            "end_date": end_date,
        }
        text = self._request_with_retry(url, params)
        raw_rows = self._parse_csv_response(text)

        rows = []
        for row in raw_rows:
            rows.append({
                "root": root,
                "exp": exp,
                "strike": _f(row.get("strike")),   # v3: already in dollars
                "right": row.get("right", ""),      # v3: "CALL" or "PUT"
                "open": _f(row.get("open")),
                "high": _f(row.get("high")),
                "low": _f(row.get("low")),
                "close": _f(row.get("close")),
                "volume": _i(row.get("volume")),
                "trade_count": _i(row.get("count")),
                "quote_date": _parse_iso_timestamp(row.get("last_trade")),
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
        return int(float(val))
    except (ValueError, TypeError):
        return None


def _parse_iso_timestamp(ts: str | None) -> datetime | None:
    """Parse ThetaData v3 ISO 8601 timestamp string to UTC datetime."""
    if not ts:
        return None
    try:
        # v3 uses format like "2026-07-02T16:00:00.003" (no tz suffix = ET session time)
        # Store as-is treating as UTC for consistency with existing pipeline convention.
        return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
    except (ValueError, AttributeError):
        return None


# ---------------------------------------------------------------------------
# v2 compatibility helpers (kept for existing tests; not called by adapter)
# ---------------------------------------------------------------------------


def _parse_strike(val) -> float | None:
    """Convert ThetaData v2 strike (milli-dollars) to USD dollars. Not used in v3."""
    if val is None:
        return None
    try:
        return float(val) / 1000.0
    except (ValueError, TypeError):
        return None


def _parse_thetadata_date(date_int: int) -> datetime | None:
    """Convert ThetaData v2 YYYYMMDD integer to UTC midnight datetime."""
    if not date_int:
        return None
    try:
        s = str(int(date_int))
        return datetime(int(s[:4]), int(s[4:6]), int(s[6:8]), tzinfo=timezone.utc)
    except (ValueError, IndexError):
        return None


def _parse_thetadata_datetime(date_int: int, ms_of_day: int) -> datetime | None:
    """Convert ThetaData v2 date + ms_of_day offset to UTC datetime."""
    base = _parse_thetadata_date(date_int)
    if base is None:
        return None
    return base + timedelta(milliseconds=int(ms_of_day or 0))
