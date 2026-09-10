"""Shared liquidity-screen helpers: median-dollar-volume ranking + chunked fetch.

Extracted from scripts/generate_universe.py so the as-of-generation
generator (liquid50) and the point-in-time membership builder
(scripts/build_pit_universe.py) rank symbols with the exact same math.
"""
from __future__ import annotations

import logging
import statistics

logger = logging.getLogger(__name__)

# Minimum daily bars required within a screen window for a symbol to be
# ranked. Guards against halted/delisted/recently-listed names producing a
# median over a handful of prints.
MIN_BARS = 20

# Symbols are fetched in chunks to keep request URLs well under length limits.
FETCH_CHUNK_SIZE = 100

# ETFs excluded from candidate pools. Fundamentals and insider-transaction
# data are structurally null for funds (no 10-K/10-Q, no Form 4 filers), so
# ETFs would rank highly on dollar volume yet contribute nothing but nulls to
# fundamental/insider features. Explicit denylist so names whose asset-name
# text dodges word heuristics (e.g. "Invesco QQQ Trust, Series 1") still
# fail loudly.
EXCLUDED_ETFS: frozenset[str] = frozenset({
    "SPY", "QQQ", "QQQM", "IWM", "DIA", "VOO", "IVV", "VTI", "RSP",
    "VEA", "VWO", "IEFA", "IEMG", "EEM", "EFA", "EWZ", "FXI", "KWEB",
    "VUG", "VTV", "SCHD", "JEPI", "JEPQ",
    "XLF", "XLK", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLB",
    "XLRE", "XLC", "KRE", "XBI", "IBB", "SMH", "SOXX", "GDX",
    "GLD", "SLV", "USO", "UNG",
    "TLT", "HYG", "LQD", "AGG", "BND",
    "ARKK", "SOXL", "TQQQ", "SQQQ", "UVXY", "VXX",
})


def median_dollar_volumes(
    bars_by_symbol: dict[str, list[dict]],
    min_bars: int = MIN_BARS,
) -> dict[str, float]:
    """Compute median daily dollar volume (close * volume) per symbol.

    Symbols with fewer than ``min_bars`` bars in the window are dropped
    (halted, recently listed, or bad symbol) with a warning.

    Args:
        bars_by_symbol: Symbol -> list of Alpaca wire-format bars
            (keys ``c`` close, ``v`` volume).
        min_bars: Minimum bar count required to be ranked.

    Returns:
        Symbol -> median daily dollar volume.
    """
    result: dict[str, float] = {}
    for symbol, bars in bars_by_symbol.items():
        if len(bars) < min_bars:
            logger.warning(
                "Skipping %s: only %d bars in window (min %d)",
                symbol, len(bars), min_bars,
            )
            continue
        result[symbol] = statistics.median(
            float(bar["c"]) * float(bar["v"]) for bar in bars
        )
    return result


def rank_universe(
    dollar_volumes: dict[str, float], top: int
) -> list[tuple[str, float]]:
    """Rank symbols by median daily dollar volume, descending; take top N.

    Ties break alphabetically for deterministic output.
    """
    ranked = sorted(dollar_volumes.items(), key=lambda kv: (-kv[1], kv[0]))
    return ranked[:top]


def fetch_bars(
    alpaca,
    symbols: list[str],
    start: str,
    end: str,
    chunk_size: int = FETCH_CHUNK_SIZE,
) -> dict[str, list[dict]]:
    """Fetch daily bars for all symbols in chunks via the Alpaca adapter."""
    all_bars: dict[str, list[dict]] = {}
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i : i + chunk_size]
        all_bars.update(
            alpaca.get_historical_bars(
                symbols=chunk, start=start, end=end, timeframe="1Day"
            )
        )
    return all_bars
