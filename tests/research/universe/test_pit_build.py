"""Point-in-time universe builder tests — all synthetic, no live API calls.

Covers: the common-stock filter contract, the quarterly rebalance schedule,
the on-disk bar cache, PIT discipline (no post-date bars influence a
rebalance; late IPOs absent; delisted names exit), determinism, and the
mocked end-to-end builder CLI including incremental cache reruns.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest

from research.universe.pit import (
    BarCache,
    build_membership,
    default_history_start,
    filter_common_stock,
    is_common_stock,
    quarterly_rebalance_dates,
)
from research.universe.screen import EXCLUDED_ETFS, MIN_BARS


# ---------------------------------------------------------------------------
# Synthetic bar helpers
# ---------------------------------------------------------------------------

def _bars_frame(start: str, end: str, close: float, volume: int) -> pd.DataFrame:
    """Business-day bar frame in the BarCache schema (date, close, volume)."""
    dates = pd.bdate_range(start=start, end=end)
    return pd.DataFrame(
        {
            "date": dates,
            "close": [close] * len(dates),
            "volume": [volume] * len(dates),
        }
    )


def _wire_bars(start: str, end: str, close: float, volume: int) -> list[dict]:
    """Business-day bars in Alpaca wire format."""
    return [
        {
            "t": f"{d.date()}T05:00:00Z",
            "o": close, "h": close, "l": close, "c": close,
            "v": volume, "vw": close, "n": 10,
        }
        for d in pd.bdate_range(start=start, end=end)
    ]


# ---------------------------------------------------------------------------
# Common-stock filter contract
# ---------------------------------------------------------------------------

class TestCommonStockFilter:
    def test_plain_common_stock_kept(self):
        assert is_common_stock("AAPL", "Apple Inc. Common Stock", "NASDAQ")
        assert is_common_stock("F", "Ford Motor Company Common Stock", "NYSE")
        # 5-letter class shares whose 5th letter is not a W/R/U suffix.
        assert is_common_stock("GOOGL", "Alphabet Inc. Class A Common Stock",
                               "NASDAQ")

    def test_ads_of_foreign_common_kept(self):
        # Plain ADSs are common equity; only depositary-preferred is debt-like.
        assert is_common_stock(
            "BABA", "Alibaba Group Holding Limited American Depositary Shares",
            "NYSE",
        )

    def test_dotted_and_slashed_symbols_rejected(self):
        assert not is_common_stock("BRK.B", "Berkshire Hathaway Class B", "NYSE")
        assert not is_common_stock("BAC.PRA", "Bank of America Pfd A", "NYSE")
        assert not is_common_stock("ABC/WS", "AmerisourceBergen Warrants", "NYSE")

    def test_numeric_escrow_junk_rejected(self):
        assert not is_common_stock("693ESC030", "Escrow Shares", "NYSE")
        assert not is_common_stock("ABC1", "Some Listing", "NYSE")

    def test_fifth_letter_warrant_right_unit_suffixes_rejected(self):
        assert not is_common_stock("ABCDW", "ABCD Corp", "NASDAQ")
        assert not is_common_stock("ABCDR", "ABCD Corp", "NASDAQ")
        assert not is_common_stock("ABCDU", "ABCD Corp", "NASDAQ")

    def test_name_heuristics_reject_non_common_listings(self):
        rejected = [
            ("ABCD", "ABCD Corp Warrant"),
            ("ABCD", "ABCD Corp Rights"),
            ("ABCD", "ABCD Acquisition Corp Units"),
            ("ABCD", "ABCD Corp 7.5% Preferred Series A"),
            ("ABCD", "ABCD Corp Pfd Series B"),
            ("ABCD", "ABCD Corp 6.25% Notes due 2028"),
            ("ABCD", "ABCD Escrow Shares"),
            ("ABCD", "ABCD Large Cap ETF"),
            ("ABCD", "ABCD Closed End Fund"),
            ("ABCD", "ABCD Common Stock When-Issued"),
        ]
        for symbol, name in rejected:
            assert not is_common_stock(symbol, name, "NYSE"), name

    def test_name_heuristics_word_boundaries(self):
        # "Bright" must not trip \brights?\b; "Uniti" must not trip \bunits?\b.
        assert is_common_stock("BFAM", "Bright Horizons Family Solutions", "NYSE")
        assert is_common_stock("UNIT", "Uniti Group Inc. Common Stock", "NASDAQ")

    def test_etf_denylist_catches_evasive_fund_names(self):
        # "Invesco QQQ Trust, Series 1" contains neither "ETF" nor "Fund".
        assert not is_common_stock("QQQ", "Invesco QQQ Trust, Series 1", "NASDAQ")
        assert "SPY" in EXCLUDED_ETFS

    def test_otc_rejected(self):
        assert not is_common_stock("TCEHY", "Tencent Holdings ADR", "OTC")

    def test_filter_dedupes_and_sorts(self):
        assets = [
            {"symbol": "MSFT", "name": "Microsoft Common Stock",
             "exchange": "NASDAQ"},
            {"symbol": "AAPL", "name": "Apple Inc. Common Stock",
             "exchange": "NASDAQ"},
            {"symbol": "AAPL", "name": "Apple Inc. Common Stock",
             "exchange": "NASDAQ"},  # inactive duplicate listing
            {"symbol": "ABCDW", "name": "ABCD Corp", "exchange": "NASDAQ"},
            {"symbol": "693ESC030", "name": "Escrow", "exchange": "NYSE"},
        ]
        assert filter_common_stock(assets) == ["AAPL", "MSFT"]


# ---------------------------------------------------------------------------
# Rebalance schedule
# ---------------------------------------------------------------------------

class TestQuarterlyRebalanceDates:
    def test_default_start_is_first_trading_day_of_2020(self):
        dates = quarterly_rebalance_dates(end=date(2020, 12, 31))
        assert dates == [
            date(2020, 1, 2), date(2020, 4, 1),
            date(2020, 7, 1), date(2020, 10, 1),
        ]

    def test_end_inclusive_on_quarter_start(self):
        dates = quarterly_rebalance_dates(end=date(2020, 7, 1))
        assert dates[-1] == date(2020, 7, 1)

    def test_no_dates_after_end(self):
        dates = quarterly_rebalance_dates(end=date(2021, 6, 30))
        assert dates[-1] == date(2021, 4, 1)

    def test_arbitrary_start_clamps_only_first(self):
        dates = quarterly_rebalance_dates(
            start=date(2021, 2, 15), end=date(2021, 12, 31)
        )
        assert dates == [
            date(2021, 2, 15), date(2021, 4, 1),
            date(2021, 7, 1), date(2021, 10, 1),
        ]

    def test_history_start_covers_first_window(self):
        assert default_history_start(date(2020, 1, 2), 90) < date(2019, 10, 5)


# ---------------------------------------------------------------------------
# Bar cache
# ---------------------------------------------------------------------------

class TestBarCache:
    def test_wire_bar_roundtrip_and_session_date(self, tmp_path):
        cache = BarCache(root=tmp_path / "cache")
        cache.store_wire_bars(
            "AAPL", _wire_bars("2020-01-02", "2020-01-10", 100.0, 1000)
        )
        frame = cache.load("AAPL")
        assert list(frame.columns) == ["date", "close", "volume"]
        # 05:00Z stamp maps to the session calendar date, tz-naive.
        assert frame["date"].iloc[0] == pd.Timestamp("2020-01-02")
        assert frame["date"].dt.tz is None
        assert frame["close"].iloc[0] == 100.0
        assert frame["volume"].iloc[0] == 1000

    def test_empty_history_cached_and_skipped_by_load_all(self, tmp_path):
        cache = BarCache(root=tmp_path / "cache")
        cache.store_wire_bars("GHOST", [])
        assert cache.has("GHOST")  # rerun will not refetch
        assert cache.load_all(["GHOST", "MISSING"]) == {}

    def test_append_extends_and_dedupes(self, tmp_path):
        cache = BarCache(root=tmp_path / "cache")
        cache.store_wire_bars(
            "AAPL", _wire_bars("2020-01-02", "2020-01-10", 100.0, 1000)
        )
        # Overlapping tail: the 2020-01-10 bar repeats with a revised print.
        cache.append_wire_bars(
            "AAPL", _wire_bars("2020-01-10", "2020-01-17", 101.0, 2000)
        )
        frame = cache.load("AAPL")
        assert frame["date"].is_unique
        assert frame["date"].is_monotonic_increasing
        revised = frame.loc[frame["date"] == pd.Timestamp("2020-01-10")]
        assert revised["close"].iloc[0] == 101.0


# ---------------------------------------------------------------------------
# Membership construction — PIT discipline
# ---------------------------------------------------------------------------

REBALANCES = [date(2020, 1, 2), date(2020, 4, 1), date(2020, 7, 1)]


def _base_pool(n: int = 5) -> dict[str, pd.DataFrame]:
    """Symbols STK00..STK0n trading the whole period, distinct volumes."""
    return {
        f"STK{i:02d}": _bars_frame(
            "2019-10-01", "2020-12-31", 10.0, 1_000 * (i + 1)
        )
        for i in range(n)
    }


class TestBuildMembershipPit:
    def test_ipo_after_rebalance_date_cannot_appear(self):
        pool = _base_pool()
        # NEWCO's first bar postdates the 2020-04-01 rebalance.
        pool["NEWCO"] = _bars_frame("2020-04-15", "2020-12-31", 50.0, 10**9)
        result = build_membership(pool, REBALANCES, top_n=10)
        by_date = {
            d.date(): set(g["symbol"])
            for d, g in result.groupby("rebalance_date")
        }
        assert "NEWCO" not in by_date[date(2020, 1, 2)]
        assert "NEWCO" not in by_date[date(2020, 4, 1)]
        # By July it has a full window of bars and dominates on volume.
        assert "NEWCO" in by_date[date(2020, 7, 1)]

    def test_post_date_volume_cannot_influence_earlier_rank(self):
        pool = _base_pool(3)
        # SPIKE trades tiny volume before April, enormous volume after.
        before = _bars_frame("2019-10-01", "2020-03-31", 10.0, 1)
        after = _bars_frame("2020-04-01", "2020-12-31", 10.0, 10**10)
        pool["SPIKE"] = pd.concat([before, after], ignore_index=True)
        result = build_membership(pool, REBALANCES, top_n=2)
        by_date = {
            d.date(): set(g["symbol"])
            for d, g in result.groupby("rebalance_date")
        }
        # Only pre-D bars count at 2020-01-02: SPIKE ranks dead last.
        assert "SPIKE" not in by_date[date(2020, 1, 2)]
        assert "SPIKE" in by_date[date(2020, 7, 1)]

    def test_delisted_symbol_exits_after_delisting(self):
        pool = _base_pool()
        # GONE delists 2020-01-15: high volume until then, nothing after —
        # by 2020-04-01 only ~9 bars remain in the window (< MIN_BARS).
        pool["GONE"] = _bars_frame("2019-10-01", "2020-01-15", 20.0, 10**9)
        result = build_membership(pool, REBALANCES, top_n=10)
        by_date = {
            d.date(): set(g["symbol"])
            for d, g in result.groupby("rebalance_date")
        }
        assert "GONE" in by_date[date(2020, 1, 2)]      # participates until exit
        assert "GONE" not in by_date[date(2020, 4, 1)]  # gone after delisting
        assert "GONE" not in by_date[date(2020, 7, 1)]

    def test_min_bars_enforced_within_window(self):
        pool = {
            "THIN": _bars_frame("2019-12-10", "2019-12-31", 10.0, 10**9),
            "FULL": _bars_frame("2019-10-01", "2020-12-31", 10.0, 100),
        }
        assert len(pool["THIN"]) < MIN_BARS
        result = build_membership(pool, [date(2020, 1, 2)], top_n=10)
        assert set(result["symbol"]) == {"FULL"}

    def test_top_n_truncation_and_rank_order(self):
        result = build_membership(_base_pool(5), [date(2020, 1, 2)], top_n=3)
        assert len(result) == 3
        assert result["rank"].tolist() == [1, 2, 3]
        # Highest median dollar volume gets rank 1.
        assert result.iloc[0]["symbol"] == "STK04"
        assert result["median_dollar_volume"].is_monotonic_decreasing

    def test_deterministic_and_alphabetical_tiebreak(self):
        pool = {
            "ZZZ": _bars_frame("2019-10-01", "2020-12-31", 10.0, 500),
            "AAA": _bars_frame("2019-10-01", "2020-12-31", 10.0, 500),
            "MMM": _bars_frame("2019-10-01", "2020-12-31", 10.0, 500),
        }
        first = build_membership(pool, REBALANCES, top_n=2)
        second = build_membership(pool, REBALANCES, top_n=2)
        pd.testing.assert_frame_equal(first, second)
        jan = first[first["rebalance_date"] == pd.Timestamp("2020-01-02")]
        assert jan["symbol"].tolist() == ["AAA", "MMM"]

    def test_output_schema(self):
        result = build_membership(_base_pool(2), [date(2020, 1, 2)], top_n=2)
        assert list(result.columns) == [
            "rebalance_date", "symbol", "rank", "median_dollar_volume",
        ]
        assert str(result["rebalance_date"].dtype).startswith("datetime64")
        assert result["rank"].dtype == "int64"
        assert result["median_dollar_volume"].dtype == "float64"


# ---------------------------------------------------------------------------
# End-to-end CLI (mocked Alpaca — never hits the network)
# ---------------------------------------------------------------------------

_FAKE_ASSETS_ACTIVE = [
    {"symbol": "AAA", "name": "AAA Corp Common Stock", "exchange": "NYSE"},
    {"symbol": "BBB", "name": "BBB Inc. Common Stock", "exchange": "NASDAQ"},
    {"symbol": "BAD.W", "name": "Bad Warrant", "exchange": "NYSE"},
    {"symbol": "SPY", "name": "SPDR S&P 500 ETF Trust", "exchange": "ARCA"},
]
_FAKE_ASSETS_INACTIVE = [
    # Delisted mid-2020 but must participate at earlier rebalances.
    {"symbol": "DEAD", "name": "Dead Co Common Stock", "exchange": "NYSE"},
    {"symbol": "693ESC030", "name": "Escrow Shares", "exchange": "NYSE"},
]

_SERIES = {
    "AAA": ("2019-10-01", "2021-12-31", 10.0, 5_000),
    "BBB": ("2019-10-01", "2021-12-31", 10.0, 1_000),
    "DEAD": ("2019-10-01", "2020-05-15", 10.0, 9_000),
}


def _fake_get_historical_bars(symbols, start, end, timeframe="1Day"):
    """Serve synthetic wire bars windowed to the requested [start, end]."""
    out = {}
    for symbol in symbols:
        if symbol not in _SERIES:
            out[symbol] = []
            continue
        s0, s1, close, volume = _SERIES[symbol]
        lo, hi = max(pd.Timestamp(s0), pd.Timestamp(start)), min(
            pd.Timestamp(s1), pd.Timestamp(end)
        )
        out[symbol] = (
            _wire_bars(str(lo.date()), str(hi.date()), close, volume)
            if lo <= hi else []
        )
    return out


@pytest.fixture
def mock_alpaca():
    with patch("scripts.build_pit_universe.AlpacaResource") as cls, \
         patch("scripts.build_pit_universe.time.sleep"):
        resource = cls.return_value
        resource.api_key = "test-key"
        resource.api_secret = "test-secret"
        resource.get_assets.side_effect = lambda status, asset_class: (
            _FAKE_ASSETS_ACTIVE if status == "active" else _FAKE_ASSETS_INACTIVE
        )
        resource.get_historical_bars.side_effect = _fake_get_historical_bars
        yield resource


class TestBuilderCli:
    def test_end_to_end_writes_pit_artifact(self, mock_alpaca, tmp_path):
        from scripts.build_pit_universe import main

        output = tmp_path / "pit2_membership.parquet"
        rc = main([
            "--top", "2",
            "--cache-dir", str(tmp_path / "cache"),
            "--output", str(output),
        ])
        assert rc == 0
        assert output.exists()
        assert output.with_suffix(".yml").exists()

        from research.universe.membership import (
            load_membership, symbols_as_of,
        )
        membership = load_membership(output)
        # Candidate pool honored the filter: warrant/ETF/escrow never appear.
        assert set(membership["symbol"]) <= {"AAA", "BBB", "DEAD"}
        # DEAD (delisted 2020-05-15) participates early, exits later.
        assert symbols_as_of("2020-02-01", membership) == ["DEAD", "AAA"]
        assert "DEAD" not in symbols_as_of("2020-10-01", membership)
        assert "AAA" in symbols_as_of("2020-10-01", membership)

        sidecar = output.with_suffix(".yml").read_text()
        assert "POINT-IN-TIME" in sidecar
        assert "build_pit_universe.py" in sidecar

    def test_rerun_uses_cache_incrementally(self, mock_alpaca, tmp_path):
        from scripts.build_pit_universe import main

        output = tmp_path / "pit2_membership.parquet"
        args = [
            "--top", "2",
            "--cache-dir", str(tmp_path / "cache"),
            "--output", str(output),
        ]
        assert main(args) == 0
        first_calls = mock_alpaca.get_historical_bars.call_count
        assert first_calls > 0

        assert main(args) == 0
        # Cache manifest is current: the rerun fetches no bars at all.
        assert mock_alpaca.get_historical_bars.call_count == first_calls

    def test_dry_run_writes_nothing(self, mock_alpaca, tmp_path, capsys):
        from scripts.build_pit_universe import main

        output = tmp_path / "pit2_membership.parquet"
        rc = main([
            "--top", "2", "--dry-run",
            "--cache-dir", str(tmp_path / "cache"),
            "--output", str(output),
        ])
        assert rc == 0
        assert not output.exists()
        assert "dry run" in capsys.readouterr().out

    def test_missing_credentials_fails_fast(self, capsys):
        from scripts.build_pit_universe import main

        with patch("scripts.build_pit_universe.AlpacaResource") as cls:
            cls.return_value.api_key = ""
            cls.return_value.api_secret = ""
            rc = main([])
        assert rc == 1
        assert "APCA_API_KEY_ID" in capsys.readouterr().err
