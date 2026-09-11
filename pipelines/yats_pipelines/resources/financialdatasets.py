"""financialdatasets.ai REST vendor adapter.

Covers 6 data domains:
1. Fundamentals (income statements — annual/quarterly/TTM)
2. Financial metrics (50+ pre-computed ratios)
3. Earnings (results, estimates, surprise)
4. Insider trades (Form 3/4/5 transactions)
5. Analyst estimates (consensus forecasts)
6. Institutional holdings (Form 13F filings)

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

    def get_insider_trades(
        self, ticker: str, limit: int = 500, start_date: str | None = None
    ) -> list[dict]:
        """Fetch insider trades (Form 3/4/5 transactions).

        VERIFIED LIVE (2026-09-11): the endpoint hard-caps each response at
        ~10 rows regardless of ``limit`` — a single call returns only the
        most recent filing dates. ``filing_date_lte`` works as a cursor, so
        when ``start_date`` is given we page backwards until the response is
        exhausted or older than the bound. Without ``start_date`` a single
        page is fetched (legacy behavior).
        """
        if start_date is None:
            data = self._get("/insider-trades", {"ticker": ticker, "limit": limit})
            return data.get("insider_trades", [])

        from datetime import date as _date, timedelta as _timedelta

        all_trades: list[dict] = []
        seen: set[tuple] = set()
        lte: str | None = None
        while True:
            params = {"ticker": ticker, "limit": limit}
            if lte is not None:
                params["filing_date_lte"] = lte
            page = self._get("/insider-trades", params).get("insider_trades", [])
            if not page:
                break
            fresh = 0
            oldest: str | None = None
            for rec in page:
                fd = (rec.get("filing_date") or rec.get("transaction_date") or "")[:10]
                key = (fd, rec.get("name"), rec.get("transaction_date"),
                       rec.get("transaction_shares"), rec.get("transaction_price_per_share"))
                if key not in seen:
                    seen.add(key)
                    all_trades.append(rec)
                    fresh += 1
                if fd and (oldest is None or fd < oldest):
                    oldest = fd
            if oldest is None or oldest <= start_date:
                break
            if fresh:
                # keep same-day siblings reachable: cursor to the oldest date
                lte = oldest
            else:
                # page was all duplicates — step past the exhausted date
                d = _date.fromisoformat(oldest) - _timedelta(days=1)
                lte = d.isoformat()
        return all_trades

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

    # ------------------------------------------------------------------
    # Domain: Institutional Holdings (Form 13F)
    # ------------------------------------------------------------------

    def get_institutional_holdings(
        self, ticker: str, report_periods: list[str] | str | None = None
    ) -> list[dict]:
        """Fetch institutional holdings (Form 13F filings) for a ticker.

        VERIFIED LIVE (2026-07-08): the endpoint IGNORES the offset param — an
        offset loop refetches the same page forever (observed: 3.8 GB RSS
        runaway). It also hard-caps responses at 200 rows regardless of limit.
        The working cursor is report_period=YYYY-MM-DD (exact quarter filter);
        the <=200 rows returned per quarter sum to ~75% of market cap for
        mega-caps, i.e. the top of the book — sufficient for ownership-level
        features, NOT for raw holder counts (breadth saturates at the cap).

        Args:
            ticker: Underlying ticker.
            report_periods: Quarter-end dates as YYYY-MM-DD. Defaults to the
                last 10 calendar quarter-ends.

        Returns:
            List of holding dicts across all requested quarters.
            subsidiaries detail is omitted (v1).
        """
        if report_periods is None:
            n_quarters = int(os.environ.get("FD_13F_QUARTERS", "10"))
            report_periods = _recent_quarter_ends(n_quarters)
        elif isinstance(report_periods, str):
            # "YYYY-MM-DD:YYYY-MM-DD" window — all quarter-ends inside it
            start_s, _, end_s = report_periods.partition(":")
            report_periods = _quarter_ends_between(start_s, end_s or None)
        all_holdings: list[dict] = []
        for rp in report_periods:
            data = self._get("/institutional-holdings", {
                "ticker": ticker, "limit": 1000, "report_period": rp,
            })
            all_holdings.extend(data.get("institutional_holdings", []))
        return all_holdings

def _quarter_ends_between(start: str, end: str | None = None) -> list[str]:
    """All calendar quarter-end dates in [start, end] (YYYY-MM-DD), oldest first."""
    from datetime import date
    start_d = date.fromisoformat(start)
    end_d = date.fromisoformat(end) if end else date.today()
    ends: list[str] = []
    y, q = start_d.year, (start_d.month - 1) // 3 + 1
    while True:
        m = q * 3
        last_day = {3: 31, 6: 30, 9: 30, 12: 31}[m]
        qe = date(y, m, last_day)
        if qe > end_d:
            break
        if qe >= start_d:
            ends.append(qe.isoformat())
        q += 1
        if q == 5:
            q, y = 1, y + 1
    return ends


def _recent_quarter_ends(n: int) -> list[str]:
    """Last n calendar quarter-end dates (YYYY-MM-DD), oldest first."""
    from datetime import date
    today = date.today()
    ends: list[str] = []
    y, q = today.year, (today.month - 1) // 3  # most recent COMPLETED quarter
    if q == 0:
        y, q = y - 1, 4
    for _ in range(n):
        month, day = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}[q]
        ends.append(f"{y:04d}-{month:02d}-{day:02d}")
        q -= 1
        if q == 0:
            y, q = y - 1, 4
    return list(reversed(ends))
