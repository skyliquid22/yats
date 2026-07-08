"""Tests for insider_features_v1 — each feature computed correctly on synthetic data.

No live DB or external dependencies required.
Key invariant: filing_date > as_of_date must be excluded (no lookahead).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import research.features.insider_features_v1  # noqa: F401 — trigger @feature registration
from research.features.insider_features_v1 import (
    OPEN_MARKET_CODES,
    compute_insider_features,
)
from research.features.feature_registry import registry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(s: str) -> pd.Timestamp:
    return pd.Timestamp(s, tz="UTC")


def _make_trades(
    rows: list[dict],
    symbol: str = "AAPL",
) -> pd.DataFrame:
    """Build a minimal canonical_insider_trades DataFrame."""
    base = {
        "symbol": symbol,
        "insider_name": "John Doe",
        "insider_title": "SVP",
        "is_board_director": False,
        "transaction_type": "P",
        "total_value": 100_000.0,
    }
    records = []
    for r in rows:
        rec = dict(base)
        rec.update(r)
        if "filing_date" in rec and not isinstance(rec["filing_date"], pd.Timestamp):
            rec["filing_date"] = _ts(rec["filing_date"])
        records.append(rec)
    if not records:
        return pd.DataFrame(columns=[
            "filing_date", "symbol", "insider_name", "insider_title",
            "is_board_director", "transaction_type", "total_value",
        ])
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# No-lookahead gate
# ---------------------------------------------------------------------------

class TestNoLookahead:
    def test_filing_after_asof_excluded(self):
        """A filing dated after as_of_date must NOT affect the feature."""
        trades = _make_trades([
            {"filing_date": "2024-02-01", "transaction_type": "P", "total_value": 100_000},
        ])
        as_of = _ts("2024-01-31")  # one day before the filing
        result = compute_insider_features(trades, as_of, 1_000_000, 100.0)
        # All should be NaN — the only trade is in the future
        assert np.isnan(result["insider_net_buy_90d"])
        assert np.isnan(result["insider_buy_intensity_30d"])
        assert result["insider_cluster_30d"] == pytest.approx(0.0) or np.isnan(result["insider_cluster_30d"])
        assert np.isnan(result["exec_net_buy_90d"])

    def test_filing_on_asof_included(self):
        """A filing dated exactly on as_of_date must be included."""
        trades = _make_trades([
            {"filing_date": "2024-01-31", "transaction_type": "P", "total_value": 100_000},
        ])
        as_of = _ts("2024-01-31")
        result = compute_insider_features(trades, as_of, 1_000_000, 100.0)
        assert not np.isnan(result["insider_net_buy_90d"])
        assert result["insider_net_buy_90d"] == pytest.approx(1.0)  # pure buy


# ---------------------------------------------------------------------------
# Transaction type filtering
# ---------------------------------------------------------------------------

class TestOpenMarketFilter:
    def test_only_p_and_s_codes_included(self):
        """Award (A), exercise (M), gift (G), etc. must be excluded."""
        trades = _make_trades([
            {"filing_date": "2024-01-15", "transaction_type": "A", "total_value": 500_000},  # award
            {"filing_date": "2024-01-15", "transaction_type": "M", "total_value": 500_000},  # exercise
            {"filing_date": "2024-01-15", "transaction_type": "G", "total_value": 500_000},  # gift
            {"filing_date": "2024-01-15", "transaction_type": "P", "total_value": 100_000},  # open-market buy
        ])
        as_of = _ts("2024-01-31")
        result = compute_insider_features(trades, as_of, 10_000_000, 100.0)
        # Only the P trade is open-market; net = buy-only → ratio = 1.0
        assert result["insider_net_buy_90d"] == pytest.approx(1.0)

    def test_open_market_codes_set(self):
        assert "P" in OPEN_MARKET_CODES
        assert "S" in OPEN_MARKET_CODES
        assert "A" not in OPEN_MARKET_CODES
        assert "M" not in OPEN_MARKET_CODES
        assert "G" not in OPEN_MARKET_CODES


# ---------------------------------------------------------------------------
# insider_net_buy_90d
# ---------------------------------------------------------------------------

class TestInsiderNetBuy90d:
    def test_pure_buy_returns_one(self):
        trades = _make_trades([
            {"filing_date": "2024-01-10", "transaction_type": "P", "total_value": 200_000},
            {"filing_date": "2024-01-15", "transaction_type": "P", "total_value": 100_000},
        ])
        result = compute_insider_features(trades, _ts("2024-01-31"), 1_000_000, 100.0)
        assert result["insider_net_buy_90d"] == pytest.approx(1.0)

    def test_pure_sell_returns_minus_one(self):
        trades = _make_trades([
            {"filing_date": "2024-01-10", "transaction_type": "S", "total_value": 200_000},
        ])
        result = compute_insider_features(trades, _ts("2024-01-31"), 1_000_000, 100.0)
        assert result["insider_net_buy_90d"] == pytest.approx(-1.0)

    def test_equal_buy_sell_returns_zero(self):
        trades = _make_trades([
            {"filing_date": "2024-01-10", "transaction_type": "P", "total_value": 100_000},
            {"filing_date": "2024-01-10", "transaction_type": "S", "total_value": 100_000},
        ])
        result = compute_insider_features(trades, _ts("2024-01-31"), 1_000_000, 100.0)
        assert result["insider_net_buy_90d"] == pytest.approx(0.0)

    def test_90d_window_excludes_older_filings(self):
        as_of = _ts("2024-01-31")
        trades = _make_trades([
            # 91 days before as_of — outside 90d window
            {"filing_date": "2023-11-01", "transaction_type": "S", "total_value": 999_999},
            # 30 days before as_of — inside 90d window
            {"filing_date": "2024-01-01", "transaction_type": "P", "total_value": 100_000},
        ])
        result = compute_insider_features(trades, as_of, 1_000_000, 100.0)
        # Only the buy in the 90d window → ratio = 1.0 (sell too old)
        assert result["insider_net_buy_90d"] == pytest.approx(1.0)

    def test_nan_when_no_trades(self):
        result = compute_insider_features(pd.DataFrame(), _ts("2024-01-31"), 1_000_000, 100.0)
        assert np.isnan(result["insider_net_buy_90d"])


# ---------------------------------------------------------------------------
# insider_buy_intensity_30d
# ---------------------------------------------------------------------------

class TestInsiderBuyIntensity30d:
    def test_basic_computation(self):
        """net_buy_value / (shares_outstanding × close)."""
        net_buy = 1_000_000.0
        shares = 10_000_000.0
        close = 100.0
        trades = _make_trades([
            {"filing_date": "2024-01-20", "transaction_type": "P", "total_value": net_buy},
        ])
        result = compute_insider_features(trades, _ts("2024-01-31"), shares, close)
        expected = net_buy / (shares * close)
        assert result["insider_buy_intensity_30d"] == pytest.approx(expected, rel=1e-6)

    def test_net_buy_subtracts_sales(self):
        shares = 10_000_000.0
        close = 100.0
        trades = _make_trades([
            {"filing_date": "2024-01-20", "transaction_type": "P", "total_value": 2_000_000},
            {"filing_date": "2024-01-21", "transaction_type": "S", "total_value": 500_000},
        ])
        result = compute_insider_features(trades, _ts("2024-01-31"), shares, close)
        expected = 1_500_000 / (shares * close)
        assert result["insider_buy_intensity_30d"] == pytest.approx(expected, rel=1e-6)

    def test_30d_window_only(self):
        as_of = _ts("2024-01-31")
        shares = 10_000_000.0
        close = 100.0
        trades = _make_trades([
            # 35 days ago — outside 30d window but inside 90d
            {"filing_date": "2023-12-27", "transaction_type": "S", "total_value": 999_999},
            # 10 days ago — inside 30d window
            {"filing_date": "2024-01-21", "transaction_type": "P", "total_value": 100_000},
        ])
        result = compute_insider_features(trades, as_of, shares, close)
        # Only the buy inside 30d → intensity = 100_000 / (10M × 100)
        expected = 100_000 / (shares * close)
        assert result["insider_buy_intensity_30d"] == pytest.approx(expected, rel=1e-6)

    def test_nan_when_invalid_market_cap(self):
        trades = _make_trades([
            {"filing_date": "2024-01-20", "transaction_type": "P", "total_value": 100_000},
        ])
        result = compute_insider_features(trades, _ts("2024-01-31"), shares_outstanding=0.0, close=100.0)
        assert np.isnan(result["insider_buy_intensity_30d"])

    def test_nan_when_no_trades(self):
        result = compute_insider_features(pd.DataFrame(), _ts("2024-01-31"), 1_000_000, 100.0)
        assert np.isnan(result["insider_buy_intensity_30d"])


# ---------------------------------------------------------------------------
# insider_cluster_30d
# ---------------------------------------------------------------------------

class TestInsiderCluster30d:
    def test_counts_distinct_net_buyers(self):
        """Two insiders net-buying → cluster = 2."""
        trades = _make_trades([
            {"filing_date": "2024-01-20", "insider_name": "Alice", "transaction_type": "P", "total_value": 100_000},
            {"filing_date": "2024-01-21", "insider_name": "Bob", "transaction_type": "P", "total_value": 50_000},
        ])
        result = compute_insider_features(trades, _ts("2024-01-31"), 10_000_000, 100.0)
        assert result["insider_cluster_30d"] == pytest.approx(2.0)

    def test_net_seller_not_counted(self):
        """One net buyer, one net seller → cluster = 1."""
        trades = _make_trades([
            {"filing_date": "2024-01-20", "insider_name": "Alice", "transaction_type": "P", "total_value": 100_000},
            {"filing_date": "2024-01-21", "insider_name": "Bob", "transaction_type": "S", "total_value": 200_000},
        ])
        result = compute_insider_features(trades, _ts("2024-01-31"), 10_000_000, 100.0)
        assert result["insider_cluster_30d"] == pytest.approx(1.0)

    def test_same_insider_mixed_trades_net_is_correct(self):
        """Alice buys 300k and sells 100k → net = +200k → counted as buyer."""
        trades = _make_trades([
            {"filing_date": "2024-01-20", "insider_name": "Alice", "transaction_type": "P", "total_value": 300_000},
            {"filing_date": "2024-01-20", "insider_name": "Alice", "transaction_type": "S", "total_value": 100_000},
        ])
        result = compute_insider_features(trades, _ts("2024-01-31"), 10_000_000, 100.0)
        assert result["insider_cluster_30d"] == pytest.approx(1.0)

    def test_zero_when_all_sell(self):
        trades = _make_trades([
            {"filing_date": "2024-01-20", "insider_name": "Alice", "transaction_type": "S", "total_value": 100_000},
        ])
        result = compute_insider_features(trades, _ts("2024-01-31"), 10_000_000, 100.0)
        assert result["insider_cluster_30d"] == pytest.approx(0.0)

    def test_nan_when_empty(self):
        result = compute_insider_features(pd.DataFrame(), _ts("2024-01-31"), 1_000_000, 100.0)
        assert np.isnan(result["insider_cluster_30d"])


# ---------------------------------------------------------------------------
# exec_net_buy_90d
# ---------------------------------------------------------------------------

class TestExecNetBuy90d:
    def test_board_director_included(self):
        trades = _make_trades([
            {
                "filing_date": "2024-01-15",
                "insider_name": "Dir A",
                "is_board_director": True,
                "insider_title": "Director",
                "transaction_type": "P",
                "total_value": 100_000,
            },
        ])
        result = compute_insider_features(trades, _ts("2024-01-31"), 10_000_000, 100.0)
        assert result["exec_net_buy_90d"] == pytest.approx(1.0)

    def test_ceo_title_included(self):
        trades = _make_trades([
            {
                "filing_date": "2024-01-15",
                "insider_name": "CEO",
                "is_board_director": False,
                "insider_title": "Chief Executive Officer",
                "transaction_type": "P",
                "total_value": 500_000,
            },
        ])
        result = compute_insider_features(trades, _ts("2024-01-31"), 10_000_000, 100.0)
        assert result["exec_net_buy_90d"] == pytest.approx(1.0)

    def test_cfo_title_included(self):
        trades = _make_trades([
            {
                "filing_date": "2024-01-15",
                "insider_name": "CFO",
                "is_board_director": False,
                "insider_title": "Chief Financial Officer",
                "transaction_type": "P",
                "total_value": 500_000,
            },
        ])
        result = compute_insider_features(trades, _ts("2024-01-31"), 10_000_000, 100.0)
        assert result["exec_net_buy_90d"] == pytest.approx(1.0)

    def test_non_exec_excluded(self):
        """An SVP (non-exec) should be excluded from exec_net_buy_90d."""
        trades = _make_trades([
            {
                "filing_date": "2024-01-15",
                "insider_name": "SVP Sales",
                "is_board_director": False,
                "insider_title": "Senior VP Sales",
                "transaction_type": "P",
                "total_value": 100_000,
            },
        ])
        result = compute_insider_features(trades, _ts("2024-01-31"), 10_000_000, 100.0)
        assert np.isnan(result["exec_net_buy_90d"])

    def test_exec_and_non_exec_mixed(self):
        """Only exec trades should count for exec_net_buy_90d; all trades for net_buy_90d."""
        trades = _make_trades([
            {
                "filing_date": "2024-01-15",
                "insider_name": "CEO",
                "is_board_director": False,
                "insider_title": "ceo",
                "transaction_type": "P",
                "total_value": 200_000,
            },
            {
                "filing_date": "2024-01-16",
                "insider_name": "SVP",
                "is_board_director": False,
                "insider_title": "SVP",
                "transaction_type": "S",
                "total_value": 100_000,
            },
        ])
        result = compute_insider_features(trades, _ts("2024-01-31"), 10_000_000, 100.0)
        # Both in net_buy_90d: (200k - 100k) / (200k + 100k) = 1/3
        assert result["insider_net_buy_90d"] == pytest.approx(1 / 3, rel=1e-5)
        # Only CEO in exec: pure buy → 1.0
        assert result["exec_net_buy_90d"] == pytest.approx(1.0)

    def test_nan_when_empty(self):
        result = compute_insider_features(pd.DataFrame(), _ts("2024-01-31"), 1_000_000, 100.0)
        assert np.isnan(result["exec_net_buy_90d"])


# ---------------------------------------------------------------------------
# ETF handling (empty trades — structural NaN)
# ---------------------------------------------------------------------------

class TestEtfHandling:
    def test_all_nan_on_empty_trades(self):
        result = compute_insider_features(pd.DataFrame(), _ts("2024-01-31"), 1_000_000, 100.0)
        for key in ["insider_net_buy_90d", "insider_buy_intensity_30d",
                    "insider_cluster_30d", "exec_net_buy_90d"]:
            assert np.isnan(result[key]), f"Expected NaN for {key} on empty trades"


# ---------------------------------------------------------------------------
# Registry lookback assertions
# ---------------------------------------------------------------------------

class TestRegistryLookbacks:
    def test_insider_net_buy_90d_lookback(self):
        assert registry.feature_lookback("insider_net_buy_90d") == 90

    def test_insider_buy_intensity_30d_lookback(self):
        assert registry.feature_lookback("insider_buy_intensity_30d") == 30

    def test_insider_cluster_30d_lookback(self):
        assert registry.feature_lookback("insider_cluster_30d") == 30

    def test_exec_net_buy_90d_lookback(self):
        assert registry.feature_lookback("exec_net_buy_90d") == 90

    def test_all_insider_features_registered(self):
        for name in ["insider_net_buy_90d", "insider_buy_intensity_30d",
                     "insider_cluster_30d", "exec_net_buy_90d"]:
            assert name in registry.registered_features, f"'{name}' not registered"
