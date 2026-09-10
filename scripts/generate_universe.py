"""Generate a liquidity-screened universe config (configs/universes/liquid50.yml).

Screens a committed seed pool of ~150 large-cap US common stocks by median
daily dollar volume (close * volume) over a trailing window of daily bars
fetched from Alpaca Data API v2, then writes the top N as a universe YAML.

ETFs (SPY, QQQ, IWM, ...) are excluded from the candidate pool entirely:
fundamentals and insider-transaction datasets are structurally null for
funds, so including them would poison downstream feature coverage stats.

SURVIVORSHIP NOTE: the seed pool and the resulting constituency are
as-of-generation. Historical backfills over this list carry survivorship
bias — acceptable and documented for the pilot; to be replaced by
point-in-time constituency at the 250-name stage.

Credentials are read from the environment only:
    APCA_API_KEY_ID, APCA_API_SECRET_KEY

Usage:
    python scripts/generate_universe.py                # writes configs/universes/liquid50.yml
    python scripts/generate_universe.py --dry-run      # print ranking, no file write
    python scripts/generate_universe.py --top 25 --window-days 60
"""
from __future__ import annotations

import argparse
import logging
import pathlib
import statistics
import sys
from datetime import datetime, timedelta, timezone

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "pipelines"))

from yats_pipelines.resources.alpaca import AlpacaResource  # noqa: E402

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = REPO_ROOT / "configs" / "universes"

# Minimum daily bars required within the window for a symbol to be ranked.
# Guards against halted/delisted/recently-listed names producing a median
# over a handful of prints.
MIN_BARS = 20

# Symbols are fetched in chunks to keep request URLs well under length limits.
FETCH_CHUNK_SIZE = 100

# ---------------------------------------------------------------------------
# Candidate pool
# ---------------------------------------------------------------------------
# ETFs excluded from the candidate pool. Fundamentals and insider-transaction
# data are structurally null for funds (no 10-K/10-Q, no Form 4 filers), so
# ETFs would rank highly on dollar volume yet contribute nothing but nulls to
# fundamental/insider features. Kept as an explicit denylist so any future
# seed-list edit that reintroduces one fails loudly.
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

# Seed pool: S&P 100 constituents plus ~50 additional large/liquid US common
# stocks (as of generation-tool authoring). US-listed common stock only — no
# ETFs (see EXCLUDED_ETFS above). This is a static pilot list; see the
# survivorship note in the module docstring.
SEED_TICKERS: tuple[str, ...] = (
    # --- S&P 100 constituents ---
    "AAPL", "ABBV", "ABT", "ACN", "ADBE", "AIG", "AMD", "AMGN", "AMT",
    "AMZN", "AVGO", "AXP", "BA", "BAC", "BK", "BKNG", "BLK", "BMY",
    "BRK.B", "C", "CAT", "CHTR", "CL", "CMCSA", "COF", "COP", "COST",
    "CRM", "CSCO", "CVS", "CVX", "DE", "DHR", "DIS", "DUK", "EMR",
    "ETN", "F", "FDX", "GD", "GE", "GILD", "GM", "GOOG", "GOOGL",
    "GS", "HD", "HON", "IBM", "INTC", "INTU", "ISRG", "JNJ", "JPM",
    "KO", "LIN", "LLY", "LMT", "LOW", "MA", "MCD", "MDLZ", "MDT",
    "MET", "META", "MMM", "MO", "MRK", "MS", "MSFT", "NEE", "NFLX",
    "NKE", "NOW", "NVDA", "ORCL", "PEP", "PFE", "PG", "PLTR", "PM",
    "PYPL", "QCOM", "RTX", "SBUX", "SCHW", "SO", "SPG", "T", "TGT",
    "TMO", "TMUS", "TSLA", "TXN", "UNH", "UNP", "UPS", "USB", "V",
    "VZ", "WFC", "WMT", "XOM",
    # --- Additional large/liquid US common stocks ---
    "ABNB", "ADI", "ADP", "AMAT", "ANET", "AZO", "BIIB", "BSX",
    "CDNS", "CI", "CMG", "CME", "COIN", "CRWD", "DAL", "DASH",
    "DDOG", "DELL", "DVN", "ELV", "EOG", "FCX", "FTNT", "HAL",
    "HLT", "HOOD", "HUM", "ICE", "KLAC", "LRCX", "LULU", "MAR",
    "MCK", "MCO", "MMC", "MPC", "MRVL", "MU", "OXY", "PANW", "PGR",
    "REGN", "SLB", "SNOW", "SNPS", "SPGI", "SYK", "UBER", "VLO",
    "VRTX", "WDAY",
)


# ---------------------------------------------------------------------------
# Screening
# ---------------------------------------------------------------------------

def screen_candidates(seed: tuple[str, ...] = SEED_TICKERS) -> list[str]:
    """Return the deduplicated candidate pool with ETFs excluded."""
    seen: set[str] = set()
    candidates: list[str] = []
    for symbol in seed:
        if symbol in seen or symbol in EXCLUDED_ETFS:
            continue
        seen.add(symbol)
        candidates.append(symbol)
    return candidates


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
    alpaca: AlpacaResource,
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


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def render_universe_yaml(
    name: str,
    tickers: list[str],
    generated_at: str,
    window_start: str,
    window_end: str,
    window_days: int,
    pool_size: int,
) -> str:
    """Render the universe YAML with a provenance header comment.

    Body format matches the existing universe configs
    (configs/universes/*.yml): name, description, tickers list.
    """
    header = "\n".join([
        "# Generated by scripts/generate_universe.py — do not edit by hand;",
        "# re-run the generator instead.",
        f"# Generated: {generated_at} (UTC)",
        f"# Screen window: {window_start} to {window_end}"
        f" ({window_days} calendar days of daily bars)",
        "# Method: median daily dollar volume (close * volume) per symbol over",
        f"#   the window, ranked descending across a {pool_size}-name large-cap",
        f"#   seed pool; top {len(tickers)} selected.",
        "# ETFs are excluded from the candidate pool entirely: fundamentals and",
        "#   insider-transaction data are structurally null for funds.",
        "# SURVIVORSHIP NOTE: constituency is as-of-generation. Historical",
        "#   backfills over this list carry survivorship bias — acceptable and",
        "#   documented for the pilot; to be replaced by point-in-time",
        "#   constituency at the 250-name stage.",
    ])
    description = (
        f"top {len(tickers)} US large-caps by median daily dollar volume "
        f"({window_days}d screen ending {window_end}; ETFs excluded; "
        "as-of-generation constituency)"
    )
    lines = [header, f"name: {name}", f"description: {description}", "tickers:"]
    lines.extend(f"  - {ticker}" for ticker in tickers)
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate a liquidity-screened universe config"
    )
    parser.add_argument(
        "--top", type=int, default=50,
        help="number of symbols to select (default: 50)",
    )
    parser.add_argument(
        "--window-days", type=int, default=90,
        help="trailing calendar-day screen window (default: 90)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="print the ranking without writing the config file",
    )
    parser.add_argument(
        "--output", type=pathlib.Path, default=None,
        help="output path (default: configs/universes/liquid<TOP>.yml)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = build_arg_parser().parse_args(argv)

    alpaca = AlpacaResource()
    if not alpaca.api_key or not alpaca.api_secret:
        print(
            "Missing Alpaca credentials: set APCA_API_KEY_ID and "
            "APCA_API_SECRET_KEY in the environment",
            file=sys.stderr,
        )
        return 1

    now = datetime.now(timezone.utc)
    # End at T-1: free-tier Alpaca returns 403 for current-day SIP data.
    window_end = now.date() - timedelta(days=1)
    window_start = window_end - timedelta(days=args.window_days)

    candidates = screen_candidates()
    logger.info(
        "Screening %d candidates (%s to %s)",
        len(candidates), window_start, window_end,
    )

    bars = fetch_bars(
        alpaca, candidates, start=str(window_start), end=str(window_end)
    )
    volumes = median_dollar_volumes(bars)
    ranking = rank_universe(volumes, top=args.top)

    if len(ranking) < args.top:
        print(
            f"warning: only {len(ranking)} of requested {args.top} symbols "
            "survived the screen",
            file=sys.stderr,
        )

    if args.dry_run:
        print(f"{'rank':>4}  {'symbol':<8}  median daily $ volume")
        for rank, (symbol, dollar_volume) in enumerate(ranking, start=1):
            print(f"{rank:>4}  {symbol:<8}  {dollar_volume:>20,.0f}")
        print(f"\n(dry run — no file written; {len(ranking)} symbols)")
        return 0

    name = f"liquid{args.top}"
    output = args.output or (DEFAULT_OUTPUT_DIR / f"{name}.yml")
    content = render_universe_yaml(
        name=name,
        tickers=[symbol for symbol, _ in ranking],
        generated_at=now.strftime("%Y-%m-%d %H:%M:%S"),
        window_start=str(window_start),
        window_end=str(window_end),
        window_days=args.window_days,
        pool_size=len(candidates),
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(content)
    print(f"Wrote {len(ranking)} symbols to {output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
