"""financialdatasets.ai REST vendor adapter.

Covers 5 data domains:
1. Fundamentals (income statements — annual/quarterly/TTM)
2. Financial metrics (50+ pre-computed ratios)
3. Earnings (results, estimates, surprise)
4. Insider trades (Form 3/4/5 transactions)
5. Analyst estimates (consensus forecasts)

Auth via X-API-KEY header. Exponential backoff retry (3 attempts).
Configurable request-per-second throttle.
"""

import logging
import os
import time
from dataclasses import dataclass, field

import requests

logger = logging.getLogger(__name__)


@dataclass
class FinancialDatasetsResource:
    """financialdatasets.ai resource — REST client with retry and throttle."""

    api_key: str = field(default_factory=lambda: os.environ.get("FINANCIALDATASETS_API_KEY", ""))
    base_url: str = "https://api.financialdatasets.ai"
    max_retries: int = 3
    requests_per_second: float = 5.0
    _last_request_time: float = field(default=0.0, repr=False)

    def _headers(self) -> dict[str, str]:
        return {"X-API-KEY": self.api_key}

    def _throttle(self) -> None:
        if self.requests_per_second <= 0:
            return
        min_interval = 1.0 / self.requests_per_second
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request_time = time.monotonic()

    def _get(self, path: str, params: dict | None = None) -> dict:
        """GET with exponential backoff retry."""
        url = f"{self.base_url}{path}"
        for attempt in range(self.max_retries):
            self._throttle()
            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=30)
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as exc:
                if attempt == self.max_retries - 1:
                    raise
                wait = 2 ** attempt
                logger.warning("Request failed (attempt %d/%d): %s — retrying in %ds",
                               attempt + 1, self.max_retries, exc, wait)
                time.sleep(wait)
        raise RuntimeError("unreachable")

    # ------------------------------------------------------------------
    # Domain: Fundamentals (income statements)
    # ------------------------------------------------------------------

    def get_income_statements(
        self, ticker: str, period: str = "annual", limit: int = 100
    ) -> list[dict]:
        """Fetch income statements for a ticker.

        Args:
            ticker: Stock symbol (e.g. "AAPL").
            period: "annual", "quarterly", or "ttm".
            limit: Max records to return.
        """
        data = self._get("/financials/income-statements", {
            "ticker": ticker, "period": period, "limit": limit,
        })
        return data.get("income_statements", [])

    # ------------------------------------------------------------------
    # Domain: Financial Metrics
    # ------------------------------------------------------------------

    def get_financial_metrics(
        self, ticker: str, period: str = "annual", limit: int = 100
    ) -> list[dict]:
        """Fetch financial metrics (PE, ROE, margins, shares_outstanding, etc.)."""
        data = self._get("/financial-metrics", {
            "ticker": ticker, "period": period, "limit": limit,
        })
        return data.get("financial_metrics", [])

    # ------------------------------------------------------------------
    # Domain: Earnings
    # ------------------------------------------------------------------

    def get_earnings(self, ticker: str, limit: int = 100) -> list[dict]:
        """Fetch earnings results (actual vs estimate, surprise)."""
        data = self._get("/earnings", {"ticker": ticker, "limit": limit})
        return data.get("earnings", [])

    # ------------------------------------------------------------------
    # Domain: Insider Trades
    # ------------------------------------------------------------------

    def get_insider_trades(self, ticker: str, limit: int = 500) -> list[dict]:
        """Fetch insider trades (Form 3/4/5 transactions)."""
        data = self._get("/insider-trades", {"ticker": ticker, "limit": limit})
        return data.get("insider_trades", [])

    # ------------------------------------------------------------------
    # Domain: Analyst Estimates
    # ------------------------------------------------------------------

    def get_analyst_estimates(
        self, ticker: str, period: str = "annual", limit: int = 100
    ) -> list[dict]:
        """Fetch analyst consensus estimates."""
        data = self._get("/analyst-estimates", {
            "ticker": ticker, "period": period, "limit": limit,
        })
        return data.get("analyst_estimates", [])
