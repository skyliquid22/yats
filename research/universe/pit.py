"""Point-in-time universe construction — candidate filter, bar cache, builder.

The membership artifact is built from the union of Alpaca active AND
inactive us_equity assets, so names that later delisted participate in
every rebalance up to their delisting date. PIT discipline: the screen for
rebalance date D uses only daily bars with session date <= D — never after.

COMMON-STOCK FILTER (documented contract, see :func:`is_common_stock`):
Alpaca's /v2/assets carries no share-class taxonomy, so common stock is
approximated by symbol pattern + asset-name heuristics:

* Symbol must be 1-5 uppercase letters (``^[A-Z]{1,5}$``). This rejects
  suffixed listings quoted with ``.`` or ``/`` (preferred series, when-issued,
  warrants — e.g. ``BAC.PRA``, ``ABC/WS``) and numeric escrow/CUSIP junk
  (``693ESC030``). Known limitation: dotted class shares (``BRK.B``) are
  excluded with them; acceptable for a liquidity-ranked top-N.
* 5-letter symbols whose 5th letter is W (warrant), R (rights) or U (unit)
  are rejected per the NASDAQ 5th-letter suffix convention.
* Name heuristics reject warrants, rights, units, preferreds (incl.
  depositary shares *representing* preferred — plain ADSs are kept),
  exchange-traded debt ("Notes due", "% ..."), escrow shares, and
  funds/ETFs/ETNs by name. Major ETFs whose names dodge the heuristics
  (e.g. "Invesco QQQ Trust, Series 1") are caught by an explicit denylist.
* OTC-exchange assets are excluded (no SIP bar coverage; not screenable).
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd

from research.universe.screen import (
    EXCLUDED_ETFS,
    MIN_BARS,
    median_dollar_volumes,
    rank_universe,
)

logger = logging.getLogger(__name__)

# Default on-disk bar cache location (gitignored via .yats_data/).
DEFAULT_CACHE_DIR = Path(".yats_data") / "pit_cache"

# Membership artifact schema — the loader (research/universe/membership.py)
# and every consumer build against exactly these columns.
MEMBERSHIP_COLUMNS = ("rebalance_date", "symbol", "rank", "median_dollar_volume")

# Plain-letter symbol, 1-5 chars. Anything with '.', '/', digits or other
# punctuation is not a screenable common-stock listing for our purposes.
_SYMBOL_RE = re.compile(r"^[A-Z]{1,5}$")

# NASDAQ 5th-letter suffixes that mark non-common listings.
_FIFTH_LETTER_REJECT = frozenset("WRU")  # warrant, rights, unit

# Asset-name patterns that mark non-common listings (case-insensitive).
_NAME_REJECT_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in (
        r"\bwarrants?\b",
        r"\brights?\b",
        r"\bunits?\b",
        r"\bpreferred\b",
        r"\bpfd\b",
        r"\bnotes\b",            # exchange-traded debt: "7.5% Notes due 2028"
        r"\bdebentures?\b",
        r"\bbonds?\b",
        r"\d+(\.\d+)?\s*%",      # coupon in name => debt or preferred series
        r"\bescrow\b",
        r"\betf\b",
        r"\betn\b",
        r"\bexchange[- ]traded\b",
        r"\bfund\b",             # open/closed-end funds
        r"\bwhen[- ]issued\b",
    )
]

# Major ETFs/ETNs whose asset names dodge the word heuristics (e.g.
# "Invesco QQQ Trust, Series 1" contains neither "ETF" nor "Fund") are
# caught by the shared denylist. Rationale (see research/universe/screen.py):
# fundamentals and insider data are structurally null for funds.
ETF_DENYLIST: frozenset[str] = EXCLUDED_ETFS


def is_common_stock(symbol: str, name: str, exchange: str = "") -> bool:
    """Heuristic common-stock test for an Alpaca asset record.

    See the module docstring for the documented filter contract.

    Args:
        symbol: Asset ticker as listed by Alpaca.
        name: Asset display name ("Apple Inc. Common Stock", ...).
        exchange: Listing exchange; "OTC" is rejected.

    Returns:
        True if the asset looks like an exchange-listed common stock
        (or common-equity ADS) eligible for the candidate pool.
    """
    if exchange.upper() == "OTC":
        return False
    if not _SYMBOL_RE.match(symbol):
        return False
    if len(symbol) == 5 and symbol[-1] in _FIFTH_LETTER_REJECT:
        return False
    if symbol in ETF_DENYLIST:
        return False
    name = name or ""
    for pattern in _NAME_REJECT_PATTERNS:
        if pattern.search(name):
            return False
    return True


def filter_common_stock(assets: Iterable[Mapping]) -> list[str]:
    """Apply :func:`is_common_stock` to Alpaca asset records; return symbols.

    Deduplicates (active + inactive listings can share a symbol) and sorts
    for deterministic downstream ordering.
    """
    keep: set[str] = set()
    total = 0
    for asset in assets:
        total += 1
        symbol = (asset.get("symbol") or "").strip()
        if is_common_stock(
            symbol,
            asset.get("name") or "",
            asset.get("exchange") or "",
        ):
            keep.add(symbol)
    logger.info(
        "Common-stock filter: %d unique symbols kept from %d asset records",
        len(keep), total,
    )
    return sorted(keep)


# ---------------------------------------------------------------------------
# Rebalance schedule
# ---------------------------------------------------------------------------

def quarterly_rebalance_dates(
    start: date = date(2020, 1, 2),
    end: date | None = None,
) -> list[date]:
    """Quarterly rebalance dates from ``start`` through ``end`` (inclusive).

    Dates fall on calendar quarter starts (Jan/Apr/Jul/Oct 1st), except the
    first, which is clamped forward to ``start`` (so the default schedule
    begins 2020-01-02, the first trading day of 2020). A rebalance date
    need not be a trading day: the screen uses bars with session date <= D,
    so a holiday D simply screens on data through the prior session.
    """
    if end is None:
        end = date.today()
    dates: list[date] = []
    year = start.year
    month = ((start.month - 1) // 3) * 3 + 1
    current = date(year, month, 1)
    while current <= end:
        dates.append(max(current, start))
        month += 3
        if month > 12:
            month = 1
            year += 1
        current = date(year, month, 1)
    return dates


# ---------------------------------------------------------------------------
# On-disk bar cache
# ---------------------------------------------------------------------------

_CACHE_COLUMNS = ("date", "close", "volume")


def _bar_session_date(raw_timestamp: str) -> pd.Timestamp:
    """Session date (tz-naive, midnight) from a wire-format bar timestamp.

    Alpaca daily bars are stamped at the UTC session open (e.g.
    ``2020-01-02T05:00:00Z``); the UTC calendar date is the session date.
    """
    ts = pd.Timestamp(raw_timestamp)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.normalize()


def _wire_bars_to_frame(bars: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": [_bar_session_date(bar["t"]) for bar in bars],
            "close": [float(bar["c"]) for bar in bars],
            "volume": [int(bar["v"]) for bar in bars],
        }
    )


@dataclass
class BarCache:
    """Per-symbol parquet cache of full daily bar history.

    Layout: ``<root>/<SYMBOL>.parquet`` with columns ``date`` (datetime64,
    session date), ``close`` (float), ``volume`` (int). Symbols reaching the
    cache have already passed the plain-letter symbol filter, so the symbol
    is filename-safe. An empty file is written for symbols with no bar
    history so reruns do not refetch them.
    """

    root: Path = DEFAULT_CACHE_DIR

    def __post_init__(self) -> None:
        self.root = Path(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, symbol: str) -> Path:
        return self.root / f"{symbol}.parquet"

    def has(self, symbol: str) -> bool:
        return self.path_for(symbol).exists()

    def store_wire_bars(self, symbol: str, bars: list[dict]) -> None:
        """Convert Alpaca wire-format bars to the cache schema and write."""
        frame = _wire_bars_to_frame(bars) if bars else _empty_cache_frame()
        self.store_frame(symbol, frame)

    def store_frame(self, symbol: str, frame: pd.DataFrame) -> None:
        frame = (
            frame.loc[:, list(_CACHE_COLUMNS)]
            .drop_duplicates(subset="date", keep="last")
            .sort_values("date")
            .reset_index(drop=True)
        )
        frame.to_parquet(self.path_for(symbol), index=False)

    def append_wire_bars(self, symbol: str, bars: list[dict]) -> None:
        """Extend an existing symbol file with newer wire-format bars."""
        if not bars:
            return
        new = _wire_bars_to_frame(bars)
        existing = self.load(symbol) if self.has(symbol) else _empty_cache_frame()
        self.store_frame(symbol, pd.concat([existing, new], ignore_index=True))

    def load(self, symbol: str) -> pd.DataFrame:
        return pd.read_parquet(self.path_for(symbol))

    def load_all(self, symbols: Iterable[str]) -> dict[str, pd.DataFrame]:
        """Load cached frames for ``symbols``; empty/missing symbols skipped."""
        out: dict[str, pd.DataFrame] = {}
        for symbol in symbols:
            if not self.has(symbol):
                continue
            frame = self.load(symbol)
            if not frame.empty:
                out[symbol] = frame
        return out


def _empty_cache_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.Series(dtype="datetime64[ns]"),
            "close": pd.Series(dtype="float64"),
            "volume": pd.Series(dtype="int64"),
        }
    )


# ---------------------------------------------------------------------------
# Membership construction
# ---------------------------------------------------------------------------

def build_membership(
    bars_by_symbol: Mapping[str, pd.DataFrame],
    rebalance_dates: Iterable[date],
    top_n: int = 250,
    window_days: int = 90,
    min_bars: int = MIN_BARS,
) -> pd.DataFrame:
    """Build the point-in-time membership table.

    For each rebalance date D, ranks symbols by median daily dollar volume
    over daily bars with session date in ``[D - window_days, D]`` — never
    after D (PIT discipline) — and keeps the top ``top_n``. A symbol whose
    first bar postdates D has zero bars in the window and cannot appear at
    D; a delisted symbol drops out once its window bar count falls below
    ``min_bars``. Ranking math is the shared
    :func:`research.universe.screen.median_dollar_volumes` /
    :func:`research.universe.screen.rank_universe` used by the liquid50
    generator; ties break alphabetically, so output is deterministic.

    Args:
        bars_by_symbol: Symbol -> DataFrame with columns ``date``
            (datetime64), ``close``, ``volume`` (the BarCache schema).
        rebalance_dates: Rebalance dates D (ascending recommended).
        top_n: Members to keep per rebalance.
        window_days: Trailing calendar-day screen window ending at D.
        min_bars: Minimum bars in window to be rankable.

    Returns:
        DataFrame with columns ``rebalance_date`` (datetime64), ``symbol``,
        ``rank`` (1-based), ``median_dollar_volume``; sorted by
        (rebalance_date, rank).
    """
    rows: list[dict] = []
    for rebalance in rebalance_dates:
        d_end = pd.Timestamp(rebalance)
        d_start = d_end - pd.Timedelta(days=window_days)
        windowed: dict[str, list[dict]] = {}
        for symbol, frame in bars_by_symbol.items():
            mask = (frame["date"] >= d_start) & (frame["date"] <= d_end)
            if int(mask.sum()) < min_bars:
                continue  # pre-filter: IPO'd after D, delisted, halted, sparse
            sub = frame.loc[mask]
            windowed[symbol] = [
                {"c": c, "v": v}
                for c, v in zip(sub["close"], sub["volume"])
            ]
        volumes = median_dollar_volumes(windowed, min_bars=min_bars)
        ranking = rank_universe(volumes, top=top_n)
        if len(ranking) < top_n:
            logger.warning(
                "Rebalance %s: only %d of %d requested members survived",
                rebalance, len(ranking), top_n,
            )
        for rank, (symbol, dollar_volume) in enumerate(ranking, start=1):
            rows.append(
                {
                    "rebalance_date": d_end,
                    "symbol": symbol,
                    "rank": rank,
                    "median_dollar_volume": dollar_volume,
                }
            )
    frame = pd.DataFrame(rows, columns=list(MEMBERSHIP_COLUMNS))
    frame["rebalance_date"] = pd.to_datetime(frame["rebalance_date"])
    frame["rank"] = frame["rank"].astype("int64")
    frame["median_dollar_volume"] = frame["median_dollar_volume"].astype("float64")
    return frame.sort_values(["rebalance_date", "rank"]).reset_index(drop=True)


def default_history_start(first_rebalance: date, window_days: int = 90) -> date:
    """History fetch must begin early enough to cover the first window."""
    return first_rebalance - timedelta(days=window_days + 7)
