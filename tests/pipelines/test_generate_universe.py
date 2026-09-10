"""Tests for the universe generator — ranking math, ETF exclusion, YAML format."""

from __future__ import annotations

import pathlib
from datetime import date
from unittest.mock import MagicMock, patch

import pytest
import yaml

from scripts.generate_universe import (
    EXCLUDED_ETFS,
    MIN_BARS,
    SEED_TICKERS,
    fetch_bars,
    main,
    median_dollar_volumes,
    rank_universe,
    render_universe_yaml,
    screen_candidates,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _bars(closes_and_volumes: list[tuple[float, int]]) -> list[dict]:
    """Build Alpaca wire-format daily bars from (close, volume) pairs."""
    return [
        {
            "t": f"2026-01-{i + 1:02d}T05:00:00Z",
            "o": c, "h": c, "l": c, "c": c,
            "v": v, "vw": c, "n": 100,
        }
        for i, (c, v) in enumerate(closes_and_volumes)
    ]


def _flat_bars(close: float, volume: int, n: int = 30) -> list[dict]:
    return _bars([(close, volume)] * n)


# ---------------------------------------------------------------------------
# Ranking math
# ---------------------------------------------------------------------------


class TestMedianDollarVolumes:
    def test_median_of_close_times_volume(self):
        # Dollar volumes: 100, 300, 200, ... median must be robust to the
        # ordering and to the outlier day.
        bars = _bars([(10.0, 10)] * 10 + [(10.0, 30)] * 10 + [(10.0, 20)] * 10)
        result = median_dollar_volumes({"AAPL": bars})
        assert result == {"AAPL": 200.0}

    def test_median_not_mean(self):
        # One huge outlier day must not drag the statistic.
        bars = _flat_bars(10.0, 10, n=29) + _bars([(10.0, 1_000_000)])
        result = median_dollar_volumes({"AAPL": bars})
        assert result["AAPL"] == 100.0

    def test_symbol_below_min_bars_dropped(self):
        sparse = _flat_bars(10.0, 10, n=MIN_BARS - 1)
        full = _flat_bars(10.0, 10, n=MIN_BARS)
        result = median_dollar_volumes({"HALTED": sparse, "AAPL": full})
        assert "HALTED" not in result
        assert "AAPL" in result

    def test_empty_bars_dropped(self):
        result = median_dollar_volumes({"BAD": []})
        assert result == {}


class TestRankUniverse:
    def test_descending_order_and_truncation(self):
        volumes = {"A": 100.0, "B": 300.0, "C": 200.0, "D": 50.0}
        ranked = rank_universe(volumes, top=3)
        assert ranked == [("B", 300.0), ("C", 200.0), ("A", 100.0)]

    def test_ties_break_alphabetically(self):
        volumes = {"ZZZ": 100.0, "AAA": 100.0, "MMM": 100.0}
        ranked = rank_universe(volumes, top=3)
        assert [s for s, _ in ranked] == ["AAA", "MMM", "ZZZ"]

    def test_top_larger_than_pool(self):
        ranked = rank_universe({"A": 1.0}, top=50)
        assert ranked == [("A", 1.0)]


# ---------------------------------------------------------------------------
# ETF exclusion
# ---------------------------------------------------------------------------


class TestEtfExclusion:
    def test_seed_pool_contains_no_etfs(self):
        assert set(SEED_TICKERS) & EXCLUDED_ETFS == set()

    def test_candidates_contain_no_etfs(self):
        assert set(screen_candidates()) & EXCLUDED_ETFS == set()

    def test_etf_injected_into_seed_is_filtered(self):
        seed = ("AAPL", "SPY", "QQQ", "MSFT", "IWM")
        assert screen_candidates(seed) == ["AAPL", "MSFT"]

    def test_duplicates_removed_order_preserved(self):
        seed = ("MSFT", "AAPL", "MSFT", "AAPL")
        assert screen_candidates(seed) == ["MSFT", "AAPL"]

    def test_pool_size_around_150(self):
        assert 140 <= len(screen_candidates()) <= 160


# ---------------------------------------------------------------------------
# Fetch chunking
# ---------------------------------------------------------------------------


class TestFetchBars:
    def test_symbols_fetched_in_chunks(self):
        alpaca = MagicMock()
        alpaca.get_historical_bars.side_effect = lambda symbols, **_: {
            s: _flat_bars(1.0, 1) for s in symbols
        }
        symbols = [f"S{i}" for i in range(250)]
        bars = fetch_bars(
            alpaca, symbols, start="2026-01-01", end="2026-03-31", chunk_size=100
        )
        assert alpaca.get_historical_bars.call_count == 3
        assert set(bars) == set(symbols)


# ---------------------------------------------------------------------------
# YAML output format
# ---------------------------------------------------------------------------


def _render_sample(tickers: list[str] | None = None) -> str:
    return render_universe_yaml(
        name="liquid50",
        tickers=tickers or ["NVDA", "TSLA", "AAPL"],
        generated_at="2026-09-10 12:00:00",
        window_start="2026-06-12",
        window_end="2026-09-10",
        window_days=90,
        pool_size=150,
    )


class TestYamlFormat:
    def test_matches_existing_universe_schema(self):
        existing = yaml.safe_load(
            (REPO_ROOT / "configs" / "universes" / "dev10.yml").read_text()
        )
        generated = yaml.safe_load(_render_sample())
        assert set(generated) == set(existing) == {"name", "description", "tickers"}
        assert isinstance(generated["tickers"], list)
        assert all(isinstance(t, str) for t in generated["tickers"])

    def test_name_and_ticker_order(self):
        generated = yaml.safe_load(_render_sample(["NVDA", "TSLA", "AAPL"]))
        assert generated["name"] == "liquid50"
        assert generated["tickers"] == ["NVDA", "TSLA", "AAPL"]

    def test_header_documents_provenance(self):
        text = _render_sample()
        assert "SURVIVORSHIP" in text
        assert "as-of-generation" in text
        assert "point-in-time" in text
        assert "2026-09-10" in text  # generation date
        assert "2026-06-12 to 2026-09-10" in text  # screen window
        assert "median daily dollar volume" in text  # method
        assert "ETFs are excluded" in text

    def test_header_is_yaml_comment_only(self):
        # Every non-body line must be a comment so parsers see a clean doc.
        text = _render_sample()
        body_start = text.index("name:")
        for line in text[:body_start].splitlines():
            assert line.startswith("#")


# ---------------------------------------------------------------------------
# CLI (mocked Alpaca — never hits the network)
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_alpaca():
    """AlpacaResource stand-in: distinct volume per symbol, later = bigger."""
    with patch("scripts.generate_universe.AlpacaResource") as cls:
        resource = cls.return_value
        resource.api_key = "test-key"
        resource.api_secret = "test-secret"

        def fake_fetch(symbols, start, end, timeframe):
            return {
                s: _flat_bars(100.0, 1000 * (i + 1))
                for i, s in enumerate(symbols)
            }

        resource.get_historical_bars.side_effect = fake_fetch
        yield resource


class TestCli:
    def test_dry_run_writes_nothing_and_prints_ranking(
        self, mock_alpaca, tmp_path, capsys
    ):
        output = tmp_path / "liquid50.yml"
        rc = main(["--dry-run", "--top", "5", "--output", str(output)])
        assert rc == 0
        assert not output.exists()
        out = capsys.readouterr().out
        assert "dry run" in out
        assert "median daily $ volume" in out
        # Rank 1 is the highest-volume symbol (last in the fetched chunk).
        assert "   1  " in out

    def test_writes_valid_universe_file(self, mock_alpaca, tmp_path):
        output = tmp_path / "liquid5.yml"
        rc = main(["--top", "5", "--output", str(output)])
        assert rc == 0
        data = yaml.safe_load(output.read_text())
        assert data["name"] == "liquid5"
        assert len(data["tickers"]) == 5
        assert set(data["tickers"]) & EXCLUDED_ETFS == set()
        assert "SURVIVORSHIP" in output.read_text()

    def test_window_days_controls_fetch_range(self, mock_alpaca, tmp_path):
        rc = main([
            "--top", "3", "--window-days", "60", "--dry-run",
            "--output", str(tmp_path / "x.yml"),
        ])
        assert rc == 0
        _, kwargs = mock_alpaca.get_historical_bars.call_args
        start = date.fromisoformat(kwargs["start"])
        end = date.fromisoformat(kwargs["end"])
        assert (end - start).days == 60

    def test_missing_credentials_fails_fast(self, capsys):
        with patch("scripts.generate_universe.AlpacaResource") as cls:
            cls.return_value.api_key = ""
            cls.return_value.api_secret = ""
            rc = main(["--dry-run"])
        assert rc == 1
        assert "APCA_API_KEY_ID" in capsys.readouterr().err
