"""Tests for inst_features_v1 — inst_ownership_pct and inst_top10_share.

No live DB or external dependencies required.
Key invariant: filing_date > as_of_date must NOT affect the feature (no lookahead).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import research.features.inst_features_v1  # noqa: F401 — trigger @feature registration
from research.features.inst_features_v1 import compute_inst_features
from research.features.feature_registry import registry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(s: str) -> pd.Timestamp:
    return pd.Timestamp(s, tz="UTC")


def _make_ownership(rows: list[dict], symbol: str = "AAPL") -> pd.DataFrame:
    """Build a minimal canonical_inst_ownership DataFrame."""
    cols = ["filing_date", "symbol", "report_period", "total_shares", "filer_count"]
    base = {"symbol": symbol, "report_period": _ts("2024-01-01"), "filer_count": 50}
    records = []
    for r in rows:
        rec = dict(base)
        rec.update(r)
        if "filing_date" in rec and not isinstance(rec["filing_date"], pd.Timestamp):
            rec["filing_date"] = _ts(rec["filing_date"])
        if "report_period" in rec and not isinstance(rec["report_period"], pd.Timestamp):
            rec["report_period"] = _ts(rec["report_period"])
        records.append(rec)
    if not records:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(records)


def _make_holdings(rows: list[dict], symbol: str = "AAPL") -> pd.DataFrame:
    """Build a minimal canonical_institutional_holdings DataFrame."""
    cols = ["filing_date", "symbol", "report_period", "filer_cik", "filer_name", "shares", "value_usd"]
    base = {
        "symbol": symbol,
        "report_period": _ts("2024-01-01"),
        "filer_cik": "0001234567",
        "filer_name": "Fund A",
        "shares": 100_000.0,
    }
    records = []
    for r in rows:
        rec = dict(base)
        rec.update(r)
        if "filing_date" in rec and not isinstance(rec["filing_date"], pd.Timestamp):
            rec["filing_date"] = _ts(rec["filing_date"])
        if "report_period" in rec and not isinstance(rec["report_period"], pd.Timestamp):
            rec["report_period"] = _ts(rec["report_period"])
        records.append(rec)
    if not records:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(records)


EMPTY_OWN = _make_ownership([])
EMPTY_HOLD = _make_holdings([])


# ---------------------------------------------------------------------------
# inst_ownership_pct
# ---------------------------------------------------------------------------

class TestInstOwnershipPct:
    def test_basic_computation(self):
        """total_shares / shares_outstanding."""
        total_shares = 5_000_000.0
        shares_outstanding = 10_000_000.0
        own = _make_ownership([
            {"filing_date": "2024-01-15", "total_shares": total_shares},
        ])
        result = compute_inst_features(own, EMPTY_HOLD, shares_outstanding, _ts("2024-01-31"))
        expected = total_shares / shares_outstanding
        assert result["inst_ownership_pct"] == pytest.approx(expected, rel=1e-6)

    def test_no_lookahead_future_filing_excluded(self):
        """Filing dated after as_of_date must not be used."""
        own = _make_ownership([
            {"filing_date": "2024-02-01", "total_shares": 9_000_000},  # future
        ])
        result = compute_inst_features(own, EMPTY_HOLD, 10_000_000.0, _ts("2024-01-31"))
        assert np.isnan(result["inst_ownership_pct"])

    def test_uses_most_recent_eligible_filing(self):
        """When multiple filings exist, the latest eligible one is used."""
        own = _make_ownership([
            {"filing_date": "2023-10-15", "total_shares": 3_000_000},
            {"filing_date": "2024-01-15", "total_shares": 5_000_000},  # most recent
        ])
        result = compute_inst_features(own, EMPTY_HOLD, 10_000_000.0, _ts("2024-01-31"))
        assert result["inst_ownership_pct"] == pytest.approx(0.5, rel=1e-6)

    def test_filing_on_asof_included(self):
        own = _make_ownership([
            {"filing_date": "2024-01-31", "total_shares": 5_000_000},
        ])
        result = compute_inst_features(own, EMPTY_HOLD, 10_000_000.0, _ts("2024-01-31"))
        assert result["inst_ownership_pct"] == pytest.approx(0.5, rel=1e-6)

    def test_nan_on_empty_ownership(self):
        result = compute_inst_features(EMPTY_OWN, EMPTY_HOLD, 10_000_000.0, _ts("2024-01-31"))
        assert np.isnan(result["inst_ownership_pct"])

    def test_nan_when_shares_outstanding_zero(self):
        own = _make_ownership([{"filing_date": "2024-01-15", "total_shares": 5_000_000}])
        result = compute_inst_features(own, EMPTY_HOLD, 0.0, _ts("2024-01-31"))
        assert np.isnan(result["inst_ownership_pct"])

    def test_nan_when_shares_outstanding_nan(self):
        own = _make_ownership([{"filing_date": "2024-01-15", "total_shares": 5_000_000}])
        result = compute_inst_features(own, EMPTY_HOLD, float("nan"), _ts("2024-01-31"))
        assert np.isnan(result["inst_ownership_pct"])


# ---------------------------------------------------------------------------
# inst_top10_share
# ---------------------------------------------------------------------------

class TestInstTop10Share:
    def _make_ten_filers(self, filing_date: str, values: list[float]) -> pd.DataFrame:
        """Build holdings with the given per-filer values."""
        rows = [
            {
                "filing_date": filing_date,
                "filer_cik": f"CIK{i:04d}",
                "filer_name": f"Fund {i}",
                "value_usd": v,
                "shares": v / 100.0,
            }
            for i, v in enumerate(values)
        ]
        return _make_holdings(rows)

    def test_all_filers_within_10_returns_one(self):
        """With ≤10 filers, top10 = all → ratio = 1.0."""
        hold = self._make_ten_filers("2024-01-15", [1_000_000.0] * 5)
        result = compute_inst_features(EMPTY_OWN, hold, 10_000_000.0, _ts("2024-01-31"))
        assert result["inst_top10_share"] == pytest.approx(1.0, rel=1e-6)

    def test_top10_excludes_small_filers(self):
        """11 filers: top 10 sum / total sum < 1."""
        values = [100.0] + [10_000_000.0] * 10  # 1 tiny + 10 large
        hold = self._make_ten_filers("2024-01-15", values)
        result = compute_inst_features(EMPTY_OWN, hold, 100_000_000.0, _ts("2024-01-31"))
        total = sum(values)
        top10 = sum(sorted(values, reverse=True)[:10])
        assert result["inst_top10_share"] == pytest.approx(top10 / total, rel=1e-6)

    def test_no_lookahead(self):
        """Filing after as_of_date excluded."""
        hold = self._make_ten_filers("2024-02-01", [1_000_000.0] * 5)
        result = compute_inst_features(EMPTY_OWN, hold, 10_000_000.0, _ts("2024-01-31"))
        assert np.isnan(result["inst_top10_share"])

    def test_uses_latest_eligible_quarter(self):
        """When multiple quarters exist, latest eligible filing is used."""
        old_hold = _make_holdings([
            {"filing_date": "2023-10-15", "filer_cik": "CIK0001", "value_usd": 500_000},
            {"filing_date": "2023-10-15", "filer_cik": "CIK0002", "value_usd": 500_000},
        ])
        new_hold = _make_holdings([
            {"filing_date": "2024-01-15", "filer_cik": "CIK0001", "value_usd": 900_000},
            {"filing_date": "2024-01-15", "filer_cik": "CIK0002", "value_usd": 100_000},
        ])
        hold = pd.concat([old_hold, new_hold], ignore_index=True)
        result = compute_inst_features(EMPTY_OWN, hold, 10_000_000.0, _ts("2024-01-31"))
        # Latest quarter top10 = 900k + 100k = 1M total = 1M → ratio = 1.0
        assert result["inst_top10_share"] == pytest.approx(1.0, rel=1e-6)

    def test_nan_on_empty_holdings(self):
        result = compute_inst_features(EMPTY_OWN, EMPTY_HOLD, 10_000_000.0, _ts("2024-01-31"))
        assert np.isnan(result["inst_top10_share"])

    def test_nan_when_all_value_zero(self):
        hold = _make_holdings([
            {"filing_date": "2024-01-15", "filer_cik": "CIK001", "value_usd": 0.0, "shares": 1000},
        ])
        result = compute_inst_features(EMPTY_OWN, hold, 10_000_000.0, _ts("2024-01-31"))
        # value_usd=0 is filtered out → empty after filter
        assert np.isnan(result["inst_top10_share"])


# ---------------------------------------------------------------------------
# Registry lookbacks
# ---------------------------------------------------------------------------

class TestRegistryLookbacks:
    def test_inst_ownership_pct_lookback(self):
        assert registry.feature_lookback("inst_ownership_pct") == 105

    def test_inst_top10_share_lookback(self):
        assert registry.feature_lookback("inst_top10_share") == 105

    def test_all_inst_features_registered(self):
        for name in ["inst_ownership_pct", "inst_top10_share"]:
            assert name in registry.registered_features, f"'{name}' not registered"


# ---------------------------------------------------------------------------
# sweep_v2 max_lookback constraint
# ---------------------------------------------------------------------------

class TestSweepV2MaxLookback:
    def test_sweep_v2_max_lookback_within_105(self):
        """PURGE GEOMETRY: max lookback must stay <= ~105 on 502-bar span."""
        import research.features.ohlcv_features  # noqa: F401
        import research.features.cross_sectional_features  # noqa: F401
        import research.features.fundamental_features  # noqa: F401
        import research.features.regime_features_v1  # noqa: F401
        import research.features.options_features_v1  # noqa: F401
        from research.features.feature_registry import registry

        lb = registry.max_lookback("sweep_v2")
        assert lb <= 105, (
            f"sweep_v2 max_lookback={lb} exceeds 105-bar purge constraint "
            "— WFO folds become infeasible on 502-bar span"
        )

    def test_insider_v1_max_lookback_within_105(self):
        """insider_v1 standalone set also satisfies the 105-bar constraint."""
        from research.features.feature_registry import registry

        lb = registry.max_lookback("insider_v1")
        assert lb <= 105


# ---------------------------------------------------------------------------
# columns.py ordering test
# ---------------------------------------------------------------------------

class TestColumnsOrdering:
    def test_insider_before_institutional_in_observation(self):
        """Observation ordering: ohlcv -> cross_sectional -> fundamental -> options -> insider -> institutional."""
        from pathlib import Path
        from research.features.columns import load_feature_columns

        configs_dir = Path(__file__).resolve().parents[3] / "configs"
        obs_cols, _ = load_feature_columns("sweep_v2", configs_dir)

        insider_positions = [i for i, c in enumerate(obs_cols)
                              if c in ("insider_net_buy_90d", "insider_buy_intensity_30d",
                                       "insider_cluster_30d", "exec_net_buy_90d")]
        inst_positions = [i for i, c in enumerate(obs_cols)
                           if c in ("inst_ownership_pct", "inst_top10_share")]

        assert insider_positions, "No insider features in observation_columns"
        assert inst_positions, "No institutional features in observation_columns"
        assert max(insider_positions) < min(inst_positions), (
            "insider features must appear before institutional features"
        )

    def test_options_before_insider_in_observation(self):
        from pathlib import Path
        from research.features.columns import load_feature_columns

        configs_dir = Path(__file__).resolve().parents[3] / "configs"
        obs_cols, _ = load_feature_columns("sweep_v2", configs_dir)

        options_positions = [i for i, c in enumerate(obs_cols)
                              if c in ("atm_iv", "skew_25d", "iv_term_slope",
                                       "put_call_oi_ratio", "net_gamma_exposure")]
        insider_positions = [i for i, c in enumerate(obs_cols)
                              if c in ("insider_net_buy_90d",)]

        assert options_positions
        assert insider_positions
        assert max(options_positions) < min(insider_positions)

    def test_insider_v1_observation_columns_contain_all_6_features(self):
        from pathlib import Path
        from research.features.columns import load_feature_columns

        configs_dir = Path(__file__).resolve().parents[3] / "configs"
        obs_cols, _ = load_feature_columns("insider_v1", configs_dir)
        expected = {
            "insider_net_buy_90d", "insider_buy_intensity_30d",
            "insider_cluster_30d", "exec_net_buy_90d",
            "inst_ownership_pct", "inst_top10_share",
        }
        assert expected.issubset(set(obs_cols))
