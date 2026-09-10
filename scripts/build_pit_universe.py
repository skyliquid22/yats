"""Build the point-in-time (PIT) top-N universe membership artifact.

Replaces the as-of-generation liquid50 constituency (survivorship-biased)
with a quarterly-rebalanced, point-in-time membership table:

1. Candidate pool: Alpaca /v2/assets, BOTH active and inactive us_equity
   (inactive includes delisted names — Alpaca's free tier serves their
   historical daily bars), filtered to plausible common stock. The filter
   contract (symbol pattern + asset-name heuristics + ETF denylist + OTC
   exclusion) is documented in research/universe/pit.py.
2. For each quarterly rebalance date D from 2020-01-02 to present: rank by
   median daily dollar volume over the trailing 90 calendar days of daily
   bars ending at D — never after (PIT discipline) — and keep the top N
   (default 250). Delisted names participate until their delisting date.
3. Output: configs/universes/pit250_membership.parquet
   (columns: rebalance_date, symbol, rank, median_dollar_volume) plus a
   YAML sidecar with generation metadata. Commit the parquet only if it is
   under 5 MB; otherwise commit just the sidecar and regenerate on demand.

BAR CACHE: each candidate symbol's FULL daily history (first-window start
through yesterday) is fetched ONCE and cached as parquet under
.yats_data/pit_cache/ (gitignored); rebalance windows are sliced locally.
Reruns are incremental: symbols already cached are skipped, and when the
cache manifest (_meta.yml) shows an older fetched-through date, only the
missing tail is refetched. Requests are chunked at 100 symbols and paced
under the free-tier 200 requests/minute limit. If a first run was
interrupted before the manifest was written, pass --refresh (or delete the
cache dir) for exact end-date consistency.

Credentials are read from the environment only:
    APCA_API_KEY_ID, APCA_API_SECRET_KEY

Usage:
    PYTHONPATH=.:pipelines uv run python scripts/build_pit_universe.py
    ... --dry-run                # summary only, no files written
    ... --top 250 --window-days 90 --start 2020-01-02
    ... --refresh                # ignore existing bar cache, refetch all
"""
from __future__ import annotations

import argparse
import logging
import pathlib
import sys
import time
from datetime import date, datetime, timedelta, timezone

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "pipelines"))
sys.path.insert(0, str(REPO_ROOT))

import yaml  # noqa: E402

from yats_pipelines.resources.alpaca import AlpacaResource  # noqa: E402
from research.universe.pit import (  # noqa: E402
    BarCache,
    build_membership,
    default_history_start,
    filter_common_stock,
    quarterly_rebalance_dates,
)
from research.universe.screen import FETCH_CHUNK_SIZE, MIN_BARS  # noqa: E402

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT = REPO_ROOT / "configs" / "universes" / "pit250_membership.parquet"
DEFAULT_CACHE_DIR = REPO_ROOT / ".yats_data" / "pit_cache"
MANIFEST_NAME = "_meta.yml"

# Free tier allows 200 requests/minute; pace at ~150/min for headroom
# (retries and pagination also consume budget).
REQUEST_INTERVAL_SECONDS = 0.4

# Artifact commit policy: parquet goes into git only under this size.
COMMIT_SIZE_LIMIT_BYTES = 5 * 1024 * 1024


# ---------------------------------------------------------------------------
# Cache population
# ---------------------------------------------------------------------------

def _read_manifest(cache: BarCache) -> date | None:
    path = cache.root / MANIFEST_NAME
    if not path.exists():
        return None
    data = yaml.safe_load(path.read_text()) or {}
    raw = data.get("fetched_through")
    return date.fromisoformat(str(raw)) if raw else None


def _write_manifest(cache: BarCache, fetched_through: date) -> None:
    (cache.root / MANIFEST_NAME).write_text(
        yaml.safe_dump(
            {
                "fetched_through": str(fetched_through),
                "written_at": datetime.now(timezone.utc).isoformat(),
            },
            sort_keys=True,
        )
    )


def _fetch_chunks(
    alpaca: AlpacaResource,
    cache: BarCache,
    symbols: list[str],
    start: date,
    end: date,
    append: bool,
    label: str,
) -> None:
    """Fetch daily bars for ``symbols`` in chunks and write to the cache."""
    total_chunks = (len(symbols) + FETCH_CHUNK_SIZE - 1) // FETCH_CHUNK_SIZE
    for index in range(0, len(symbols), FETCH_CHUNK_SIZE):
        chunk = symbols[index : index + FETCH_CHUNK_SIZE]
        logger.info(
            "%s: chunk %d/%d (%d symbols, %s to %s)",
            label, index // FETCH_CHUNK_SIZE + 1, total_chunks,
            len(chunk), start, end,
        )
        bars = alpaca.get_historical_bars(
            symbols=chunk, start=str(start), end=str(end), timeframe="1Day"
        )
        for symbol in chunk:
            symbol_bars = bars.get(symbol, [])
            if append:
                cache.append_wire_bars(symbol, symbol_bars)
            else:
                # Empty histories are stored too, so reruns skip them.
                cache.store_wire_bars(symbol, symbol_bars)
        time.sleep(REQUEST_INTERVAL_SECONDS)


def ensure_cache(
    alpaca: AlpacaResource,
    cache: BarCache,
    symbols: list[str],
    history_start: date,
    history_end: date,
    refresh: bool = False,
) -> None:
    """Populate the per-symbol bar cache incrementally.

    * Symbols without a cache file get a full-history fetch.
    * If the manifest shows the cache was fetched through an earlier date,
      already-cached symbols get a tail fetch from that date forward.
    * ``refresh`` discards cache state and refetches everything.
    """
    fetched_through = None if refresh else _read_manifest(cache)
    missing = [s for s in symbols if refresh or not cache.has(s)]
    missing_set = set(missing)
    cached = [s for s in symbols if s not in missing_set]

    if missing:
        _fetch_chunks(
            alpaca, cache, missing, history_start, history_end,
            append=False, label="full history",
        )
    else:
        logger.info("Bar cache complete: all %d symbols present", len(symbols))

    if cached and fetched_through is not None and fetched_through < history_end:
        _fetch_chunks(
            alpaca, cache, cached,
            fetched_through + timedelta(days=1), history_end,
            append=True, label="tail extension",
        )

    _write_manifest(cache, history_end)


# ---------------------------------------------------------------------------
# Sidecar metadata
# ---------------------------------------------------------------------------

def render_sidecar(
    generated_at: str,
    top_n: int,
    window_days: int,
    rebalance_dates: list[date],
    pool_stats: dict[str, int],
    history_start: date,
    history_end: date,
    artifact_path: str,
    rows: int,
    size_bytes: int,
) -> str:
    """Render the YAML sidecar documenting how the artifact was generated."""
    header = "\n".join([
        "# Generated by scripts/build_pit_universe.py — do not edit by hand;",
        "# re-run the builder instead (requires APCA_API_KEY_ID /",
        "# APCA_API_SECRET_KEY in the environment):",
        "#   PYTHONPATH=.:pipelines uv run python scripts/build_pit_universe.py",
        "# POINT-IN-TIME: each rebalance uses only bars dated on or before the",
        "# rebalance date; the candidate pool includes delisted (inactive)",
        "# assets, so there is no survivorship bias.",
    ])
    body = yaml.safe_dump(
        {
            "name": f"pit{top_n}",
            "description": (
                f"point-in-time top {top_n} US common stocks by median daily "
                f"dollar volume; quarterly rebalance; {window_days}d trailing "
                "screen ending at each rebalance date; active + delisted "
                "candidate pool (no survivorship bias)"
            ),
            "generated_at": generated_at,
            "method": {
                "ranking": "median daily dollar volume (close * volume)",
                "window_days": window_days,
                "min_bars_in_window": MIN_BARS,
                "top_n": top_n,
                "tie_break": "alphabetical",
                "rebalance": "quarterly (calendar quarter starts)",
                "pit_rule": "bars with session date <= rebalance date only",
            },
            "rebalance_dates": {
                "first": str(rebalance_dates[0]),
                "last": str(rebalance_dates[-1]),
                "count": len(rebalance_dates),
            },
            "candidate_pool": pool_stats,
            "candidate_filter": (
                "us_equity assets (active + inactive) filtered to plausible "
                "common stock: 1-5 uppercase letters (no '.', '/', digits), "
                "no NASDAQ W/R/U 5th-letter suffixes, name heuristics reject "
                "warrants/rights/units/preferred/debt/escrow/funds, ETF "
                "denylist, OTC excluded; see research/universe/pit.py"
            ),
            "bar_history": {
                "start": str(history_start),
                "end": str(history_end),
                "cache": ".yats_data/pit_cache/ (gitignored, per-symbol parquet)",
            },
            "artifact": {
                "path": artifact_path,
                "rows": rows,
                "size_bytes": size_bytes,
                "columns": [
                    "rebalance_date", "symbol", "rank", "median_dollar_volume",
                ],
                "commit_policy": (
                    "committed only if under 5 MB; otherwise regenerate with "
                    "the builder command above"
                ),
            },
        },
        sort_keys=False,
    )
    return header + "\n" + body


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the point-in-time universe membership artifact"
    )
    parser.add_argument(
        "--top", type=int, default=250,
        help="members per rebalance (default: 250)",
    )
    parser.add_argument(
        "--window-days", type=int, default=90,
        help="trailing calendar-day screen window (default: 90)",
    )
    parser.add_argument(
        "--start", type=date.fromisoformat, default=date(2020, 1, 2),
        help="first rebalance date (default: 2020-01-02)",
    )
    parser.add_argument(
        "--cache-dir", type=pathlib.Path, default=DEFAULT_CACHE_DIR,
        help="bar cache directory (default: .yats_data/pit_cache)",
    )
    parser.add_argument(
        "--output", type=pathlib.Path, default=DEFAULT_OUTPUT,
        help="membership parquet path "
             "(default: configs/universes/pit250_membership.parquet)",
    )
    parser.add_argument(
        "--refresh", action="store_true",
        help="ignore the existing bar cache and refetch all history",
    )
    parser.add_argument(
        "--max-symbols", type=int, default=None,
        help="debug: cap the candidate pool at N symbols",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="fetch/build and print a summary without writing output files",
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
    # Pace paginated requests under the free-tier 200 req/min cap.
    alpaca.request_delay = REQUEST_INTERVAL_SECONDS

    now = datetime.now(timezone.utc)
    # End at T-1: free-tier Alpaca returns 403 for current-day SIP data.
    history_end = now.date() - timedelta(days=1)
    history_start = default_history_start(args.start, args.window_days)

    active = alpaca.get_assets(status="active", asset_class="us_equity")
    inactive = alpaca.get_assets(status="inactive", asset_class="us_equity")
    candidates = filter_common_stock(active + inactive)
    if args.max_symbols is not None:
        candidates = candidates[: args.max_symbols]
    pool_stats = {
        "assets_active": len(active),
        "assets_inactive": len(inactive),
        "after_common_stock_filter": len(candidates),
    }
    logger.info(
        "Candidate pool: %d active + %d inactive assets -> %d common-stock "
        "symbols", len(active), len(inactive), len(candidates),
    )

    cache = BarCache(root=args.cache_dir)
    ensure_cache(
        alpaca, cache, candidates, history_start, history_end,
        refresh=args.refresh,
    )

    bars = cache.load_all(candidates)
    pool_stats["with_bar_history"] = len(bars)
    logger.info("Loaded cached bar history for %d symbols", len(bars))

    rebalance_dates = quarterly_rebalance_dates(start=args.start, end=history_end)
    membership = build_membership(
        bars,
        rebalance_dates,
        top_n=args.top,
        window_days=args.window_days,
    )

    quarters = membership["rebalance_date"].nunique()
    print(
        f"Membership: {len(membership)} rows across {quarters} rebalances "
        f"({rebalance_dates[0]} to {rebalance_dates[-1]}), top {args.top}"
    )
    if args.dry_run:
        first = membership[
            membership["rebalance_date"] == membership["rebalance_date"].min()
        ]
        print("First rebalance, top 10:")
        print(first.head(10).to_string(index=False))
        print("(dry run — no files written)")
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    membership.to_parquet(args.output, index=False)
    size_bytes = args.output.stat().st_size

    sidecar_path = args.output.with_suffix(".yml")
    sidecar_path.write_text(
        render_sidecar(
            generated_at=now.strftime("%Y-%m-%d %H:%M:%S UTC"),
            top_n=args.top,
            window_days=args.window_days,
            rebalance_dates=rebalance_dates,
            pool_stats=pool_stats,
            history_start=history_start,
            history_end=history_end,
            artifact_path=str(args.output.relative_to(REPO_ROOT))
            if args.output.is_relative_to(REPO_ROOT) else str(args.output),
            rows=len(membership),
            size_bytes=size_bytes,
        )
    )

    print(f"Wrote {args.output} ({size_bytes / 1024:.0f} KiB) and {sidecar_path}")
    if size_bytes < COMMIT_SIZE_LIMIT_BYTES:
        print("Artifact is under 5 MB — commit the parquet + sidecar.")
    else:
        print(
            "Artifact exceeds 5 MB — commit only the sidecar and document "
            "regeneration (see sidecar commit_policy)."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
