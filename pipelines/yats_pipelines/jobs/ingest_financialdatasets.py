"""Dagster ingest job for financialdatasets.ai data.

Ingests 6 data domains into raw_fd_* QuestDB tables via ILP:
- raw_fd_fundamentals
- raw_fd_financial_metrics
- raw_fd_earnings
- raw_fd_insider_trades
- raw_fd_analyst_estimates
- raw_fd_institutional_holdings

Config: ticker_list + data_domains.
"""

import logging
from datetime import datetime, timezone
from typing import Any

from dagster import Config, OpExecutionContext, job, op

from yats_pipelines.resources.financialdatasets import FinancialDatasetsResource
from yats_pipelines.resources.questdb import QuestDBResource
from yats_pipelines.utils.run_recorder import record_finish, record_start

logger = logging.getLogger(__name__)

ALL_DOMAINS = ("fundamentals", "metrics", "earnings", "insider_trades", "analyst_estimates", "institutional_holdings")


class IngestFinancialdatasetsConfig(Config):
    """Run config for ingest_financialdatasets job."""

    ticker_list: list[str]
    data_domains: list[str] = list(ALL_DOMAINS)


def _ilp_sender(qdb: QuestDBResource):
    """Create a QuestDB ILP Sender context manager."""
    from questdb.ingress import Sender, Protocol

    return Sender(Protocol.Tcp, qdb.ilp_host, qdb.ilp_port)


def _ts(val: str | None) -> datetime | None:
    """Parse ISO date/datetime string to UTC datetime."""
    if not val:
        return None
    if len(val) == 10:
        val = val + "T00:00:00Z"
    if val.endswith("Z"):
        val = val[:-1] + "+00:00"
    return datetime.fromisoformat(val).astimezone(timezone.utc)


def _f(val: Any) -> float | None:
    """Safe float conversion."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _row(sender, table: str, symbols: dict, columns: dict, at: datetime) -> None:
    """Write an ILP row, filtering out None values from columns."""
    sender.row(
        table,
        symbols=symbols,
        columns={k: v for k, v in columns.items() if v is not None},
        at=at,
    )


# ---------------------------------------------------------------------------
# Domain ingestion functions
# ---------------------------------------------------------------------------


def _ingest_fundamentals(fd: FinancialDatasetsResource, sender, tickers: list[str], now: datetime) -> int:
    rows = 0
    for ticker in tickers:
        for period in ("annual", "quarterly", "ttm"):
            try:
                records = fd.get_income_statements(ticker, period=period)
            except Exception:
                logger.warning("Failed fundamentals %s/%s", ticker, period, exc_info=True)
                continue
            skipped = 0
            for rec in records:
                report_date = _ts(rec.get("report_period"))
                if not report_date:
                    skipped += 1
                    continue
                _row(sender, "raw_fd_fundamentals",
                     symbols={
                         "symbol": ticker,
                         "fiscal_period": rec.get("fiscal_period", ""),
                         "period": rec.get("period", period),
                         "currency": rec.get("currency", "USD"),
                     },
                     columns={
                         "revenue": _f(rec.get("revenue")),
                         "cost_of_revenue": _f(rec.get("cost_of_revenue")),
                         "gross_profit": _f(rec.get("gross_profit")),
                         "operating_expense": _f(rec.get("operating_expense")),
                         "operating_income": _f(rec.get("operating_income")),
                         "interest_expense": _f(rec.get("interest_expense")),
                         "ebit": _f(rec.get("ebit")),
                         "net_income": _f(rec.get("net_income")),
                         "net_income_common_stock": _f(rec.get("net_income_common_stock")),
                         "eps": _f(rec.get("earnings_per_share")),
                         "eps_diluted": _f(rec.get("earnings_per_share_diluted")),
                         "weighted_average_shares": _f(rec.get("weighted_average_shares")),
                         "weighted_average_shares_diluted": _f(rec.get("weighted_average_shares_diluted")),
                         "dividends_per_share": _f(rec.get("dividends_per_share")),
                         "ingested_at": now,
                     },
                     at=report_date)
                rows += 1
            if skipped:
                logger.warning(
                    "Skipped %d/%d fundamentals records for %s/%s — missing report_period",
                    skipped, len(records), ticker, period,
                )
    return rows


def _ingest_metrics(fd: FinancialDatasetsResource, sender, tickers: list[str], now: datetime) -> int:
    rows = 0
    for ticker in tickers:
        for period in ("annual", "quarterly", "ttm"):
            try:
                records = fd.get_financial_metrics(ticker, period=period)
            except Exception:
                logger.warning("Failed metrics %s/%s", ticker, period, exc_info=True)
                continue
            skipped = 0
            for rec in records:
                ts = _ts(rec.get("report_period")) or _ts(rec.get("period_end_date"))
                if not ts:
                    skipped += 1
                    continue
                _row(sender, "raw_fd_financial_metrics",
                     symbols={"symbol": ticker},
                     columns={
                         "market_cap": _f(rec.get("market_capitalization")),
                         "pe_ratio": _f(rec.get("price_to_earnings_ratio")),
                         "ps_ratio": _f(rec.get("price_to_sales_ratio")),
                         "pb_ratio": _f(rec.get("price_to_book_ratio")),
                         "ev_ebitda": _f(rec.get("enterprise_value_to_ebitda")),
                         "roe": _f(rec.get("return_on_equity")),
                         "roa": _f(rec.get("return_on_assets")),
                         "gross_margin": _f(rec.get("gross_margin")),
                         "operating_margin": _f(rec.get("operating_margin")),
                         "net_margin": _f(rec.get("net_margin")),
                         "fcf_margin": _f(rec.get("free_cash_flow_margin")),
                         "debt_to_equity": _f(rec.get("debt_to_equity")),
                         "current_ratio": _f(rec.get("current_ratio")),
                         "revenue_growth_yoy": _f(rec.get("revenue_growth")),
                         "eps_growth_yoy": _f(rec.get("earnings_per_share_growth")),
                         "dividend_yield": _f(rec.get("dividend_yield")),
                         "shares_outstanding": _f(rec.get("shares_outstanding")),
                         "ingested_at": now,
                     },
                     at=ts)
                rows += 1
            if skipped:
                logger.warning(
                    "Skipped %d/%d metrics records for %s/%s — missing report_period/period_end_date",
                    skipped, len(records), ticker, period,
                )
    return rows


def _ingest_earnings(fd: FinancialDatasetsResource, sender, tickers: list[str], now: datetime) -> int:
    rows = 0
    for ticker in tickers:
        try:
            records = fd.get_earnings(ticker)
        except Exception:
            logger.warning("Failed earnings %s", ticker, exc_info=True)
            continue
        skipped = 0
        for rec in records:
            report_date = _ts(rec.get("report_period"))
            if not report_date:
                skipped += 1
                continue
            _row(sender, "raw_fd_earnings",
                 symbols={
                     "symbol": ticker,
                     "fiscal_period": rec.get("fiscal_period", ""),
                 },
                 columns={
                     "eps_actual": _f(rec.get("earnings_per_share")),
                     "eps_estimate": _f(rec.get("estimated_earnings_per_share")),
                     "eps_surprise": _f(rec.get("earnings_surprise")),
                     "eps_surprise_pct": _f(rec.get("earnings_surprise_pct")),
                     "revenue_actual": _f(rec.get("revenue")),
                     "revenue_estimate": _f(rec.get("estimated_revenue")),
                     "ingested_at": now,
                 },
                 at=report_date)
            rows += 1
        if skipped:
            logger.warning(
                "Skipped %d/%d earnings records for %s — missing report_period",
                skipped, len(records), ticker,
            )
    return rows


def _ingest_insider_trades(fd: FinancialDatasetsResource, sender, tickers: list[str], now: datetime) -> int:
    rows = 0
    for ticker in tickers:
        try:
            records = fd.get_insider_trades(ticker)
        except Exception:
            logger.warning("Failed insider_trades %s", ticker, exc_info=True)
            continue
        skipped = 0
        for rec in records:
            filing_date = _ts(rec.get("filing_date"))
            transaction_date = _ts(rec.get("transaction_date"))
            # Point-in-time key: market only knows at filing, not transaction.
            # Fall back to transaction_date only if filing_date is absent.
            filed_at = filing_date
            if filed_at is None:
                filed_at = transaction_date
                if filed_at is not None:
                    logger.warning(
                        "insider_trade missing filing_date for %s — falling back to transaction_date",
                        ticker,
                    )
            if filed_at is None:
                skipped += 1
                continue
            _row(sender, "raw_fd_insider_trades",
                 symbols={
                     "symbol": ticker,
                     "transaction_type": rec.get("transaction_type", ""),
                 },
                 columns={
                     "insider_name": rec.get("name", ""),
                     "insider_title": rec.get("title", ""),
                     "shares": _f(rec.get("transaction_shares")),
                     "price_per_share": _f(rec.get("transaction_price_per_share")),
                     "total_value": _f(rec.get("transaction_value")),
                     "shares_owned_after": _f(rec.get("shares_owned_after_transaction")),
                     "filing_date": filing_date,
                     "transaction_date": transaction_date,
                     "is_board_director": rec.get("is_board_director"),
                     "shares_owned_before": _f(rec.get("shares_owned_before_transaction")),
                     "security_title": rec.get("security_title", ""),
                     "issuer": rec.get("issuer", ""),
                     "ingested_at": now,
                 },
                 at=filed_at)
            rows += 1
        if skipped:
            logger.warning(
                "Skipped %d/%d insider_trades records for %s — missing filing_date and transaction_date",
                skipped, len(records), ticker,
            )
    return rows


def _ingest_institutional_holdings(
    fd: FinancialDatasetsResource, sender, tickers: list[str], now: datetime,
    run_id: str = "",
) -> int:
    rows = 0
    for ticker in tickers:
        try:
            records = fd.get_institutional_holdings(ticker)
        except Exception:
            logger.warning("Failed institutional_holdings %s", ticker, exc_info=True)
            continue
        skipped = 0
        for rec in records:
            filing_date = _ts(rec.get("filing_date"))
            if not filing_date:
                skipped += 1
                continue
            report_period = _ts(rec.get("report_period"))
            _row(sender, "raw_fd_institutional_holdings",
                 symbols={
                     "symbol": ticker,
                     "filer_cik": rec.get("filer_cik", ""),
                     "cusip": rec.get("cusip", ""),
                     "title_of_class": rec.get("title_of_class", ""),
                 },
                 columns={
                     "report_period": report_period,
                     "accession_number": rec.get("accession_number", ""),
                     "filer_name": rec.get("filer_name", ""),
                     "shares": _f(rec.get("shares")),
                     "value_usd": _f(rec.get("value_usd")),
                     "reported_price": _f(rec.get("reported_price")),
                     "ingested_at": now,
                     "dagster_run_id": run_id,
                 },
                 at=filing_date)
            rows += 1
        if skipped:
            logger.warning(
                "Skipped %d/%d institutional_holdings records for %s — missing filing_date",
                skipped, len(records), ticker,
            )
    return rows


def _ingest_analyst_estimates(fd: FinancialDatasetsResource, sender, tickers: list[str], now: datetime) -> int:
    rows = 0
    for ticker in tickers:
        for period in ("annual", "quarterly"):
            try:
                records = fd.get_analyst_estimates(ticker, period=period)
            except Exception:
                logger.warning("Failed analyst_estimates %s/%s", ticker, period, exc_info=True)
                continue
            skipped = 0
            for rec in records:
                ts = _ts(rec.get("report_period")) or _ts(rec.get("period_end_date"))
                if not ts:
                    skipped += 1
                    continue
                num = rec.get("number_of_analysts")
                _row(sender, "raw_fd_analyst_estimates",
                     symbols={
                         "symbol": ticker,
                         "period": rec.get("period", period),
                     },
                     columns={
                         "eps_estimate_mean": _f(rec.get("estimated_earnings_per_share")),
                         "eps_estimate_high": _f(rec.get("estimated_earnings_per_share_high")),
                         "eps_estimate_low": _f(rec.get("estimated_earnings_per_share_low")),
                         "eps_actual": _f(rec.get("actual_earnings_per_share")),
                         "revenue_estimate_mean": _f(rec.get("estimated_revenue")),
                         "revenue_actual": _f(rec.get("actual_revenue")),
                         "num_analysts": int(num) if num is not None else None,
                         "ingested_at": now,
                     },
                     at=ts)
                rows += 1
            if skipped:
                logger.warning(
                    "Skipped %d/%d analyst_estimates records for %s/%s — missing report_period/period_end_date",
                    skipped, len(records), ticker, period,
                )
    return rows


_DOMAIN_FNS = {
    "fundamentals": _ingest_fundamentals,
    "metrics": _ingest_metrics,
    "earnings": _ingest_earnings,
    "insider_trades": _ingest_insider_trades,
    "analyst_estimates": _ingest_analyst_estimates,
    "institutional_holdings": _ingest_institutional_holdings,
}


# ---------------------------------------------------------------------------
# Dagster op + job
# ---------------------------------------------------------------------------


@op
def ingest_financialdatasets_op(context: OpExecutionContext, config: IngestFinancialdatasetsConfig):
    """Fetch data from financialdatasets.ai and write to raw_fd_* tables via ILP."""
    detail = f"domains={config.data_domains} tickers={len(config.ticker_list)}"
    record_start("ingest_financialdatasets", context.run_id, detail)
    _exc: Exception | None = None
    _total_rows = 0
    try:
        fd = FinancialDatasetsResource()
        qdb = QuestDBResource()
        now = datetime.now(timezone.utc)
        tickers = config.ticker_list
        domains = config.data_domains

        context.log.info("Ingesting domains=%s for tickers=%s", domains, tickers)

        with _ilp_sender(qdb) as sender:
            for domain in domains:
                fn = _DOMAIN_FNS.get(domain)
                if fn is None:
                    context.log.warning("Unknown domain: %s — skipping", domain)
                    continue
                extra = {"run_id": context.run_id} if domain == "institutional_holdings" else {}
                rows = fn(fd, sender, tickers, now, **extra)
                _total_rows += rows
                context.log.info("Domain %s: %d rows ingested", domain, rows)
            sender.flush()

        context.log.info("ingest_financialdatasets complete")
    except Exception as exc:
        _exc = exc
        raise
    finally:
        record_finish(
            "ingest_financialdatasets", context.run_id,
            "failed" if _exc else "success",
            rows_written=None if _exc else _total_rows,
            failure_cause=str(_exc)[:200] if _exc else None,
        )


@job(tags={"yats/concurrency_pool": "ingest", "dagster/priority": "10"})
def ingest_financialdatasets():
    """Ingest financialdatasets.ai data into raw_fd_* QuestDB tables."""
    ingest_financialdatasets_op()
