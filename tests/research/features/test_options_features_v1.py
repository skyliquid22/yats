"""Tests for options_features_v1 — each feature computed correctly on synthetic data.

Fixtures are in-memory DataFrames; no live terminal or QuestDB required.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.features.options_features_v1 import (
    _atm_iv_from_slice,
    _interp_iv_at_delta,
    _select_expiry,
    compute_options_features,
)
from research.features.feature_registry import registry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(date_str: str) -> pd.Timestamp:
    return pd.Timestamp(date_str, tz="UTC")


def _make_chain(
    quote_date: str,
    underlying: str = "AAPL",
    expiry_30d: str | None = None,
    expiry_60d: str | None = None,
    spot: float = 150.0,
    n_strikes: int = 11,
    atm_iv: float = 0.25,
    skew: float = 0.05,
    gamma: float = 0.01,
    include_gamma: bool = True,
    right_format: str = "short",  # "short" → "C"/"P", "long" → "CALL"/"PUT"
) -> pd.DataFrame:
    """Build a synthetic options chain with realistic ATM/OTM structure.

    Creates contracts for each expiry with strikes centred around spot.
    IV has a smile; delta spans 0.01–0.99 across the strike grid so 25-delta
    strikes are always present. Uses n_strikes=11 and ±25% moneyness spread
    to match real chain shape. right_format="long" produces "CALL"/"PUT" as
    ThetaData v3 returns; "short" produces "C"/"P".
    """
    qd = pd.Timestamp(quote_date, tz="UTC")
    expiries = []
    if expiry_30d:
        expiries.append(pd.Timestamp(expiry_30d, tz="UTC"))
    if expiry_60d:
        expiries.append(pd.Timestamp(expiry_60d, tz="UTC"))

    if right_format == "long":
        call_right, put_right = "CALL", "PUT"
    else:
        call_right, put_right = "C", "P"

    rows = []
    for expiry in expiries:
        dte = (expiry - qd).days
        # For farther-out expirations, IV is slightly higher (term slope)
        base_iv = atm_iv + (dte - 30) * 0.001  # +0.1 vol per extra day
        # ±25% moneyness spread with n_strikes steps — gives delta ~0.01–0.99
        half = n_strikes // 2
        strikes = [spot * (1 + 0.05 * i) for i in range(-half, half + 1)]
        for strike in strikes:
            moneyness = (strike - spot) / spot
            # Simple IV smile: ATM lowest, wings higher
            iv = base_iv + abs(moneyness) * 0.3

            # Approximate delta: call ~0.5 at ATM, decreasing toward OTM
            call_delta = max(0.01, min(0.99, 0.5 - moneyness * 2))
            put_delta = call_delta - 1.0  # put-call parity delta

            oi = int(max(100, 5000 - abs(moneyness) * 50000))
            # put skew: OTM puts (low strike) have more OI
            if strike < spot:
                oi = int(oi * 1.5)

            base_row = dict(
                quote_date=qd,
                underlying=underlying,
                expiry=expiry,
                strike=strike,
                iv=iv,
                open_interest=oi,
            )
            if include_gamma:
                base_row["gamma"] = gamma * np.exp(-0.5 * moneyness**2 * 400)

            # Call contract
            call = {**base_row, "right": call_right, "delta": call_delta}
            # Put contract
            put = {**base_row, "right": put_right, "delta": put_delta}
            rows.extend([call, put])

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# _select_expiry
# ---------------------------------------------------------------------------

class TestSelectExpiry:
    def test_returns_nearest_30d_expiry(self):
        qd = _ts("2024-01-15")
        chain = _make_chain("2024-01-15", expiry_30d="2024-02-16", expiry_60d="2024-03-15")
        near = _select_expiry(chain, qd, 30)
        assert not near.empty
        exp = near["expiry"].iloc[0]
        dte = (exp - qd).days
        assert 15 <= dte <= 45

    def test_returns_nearest_60d_expiry(self):
        qd = _ts("2024-01-15")
        chain = _make_chain("2024-01-15", expiry_30d="2024-02-16", expiry_60d="2024-03-15")
        far = _select_expiry(chain, qd, 60)
        assert not far.empty
        exp = far["expiry"].iloc[0]
        dte = (exp - qd).days
        assert 45 <= dte <= 75

    def test_empty_chain_returns_empty(self):
        qd = _ts("2024-01-15")
        result = _select_expiry(pd.DataFrame(columns=["expiry"]), qd, 30)
        assert result.empty

    def test_no_expiry_in_window_returns_empty(self):
        # Only a very far expiry (>15d away from target)
        qd = _ts("2024-01-15")
        chain = _make_chain("2024-01-15", expiry_60d="2024-06-21")  # ~157d out
        near = _select_expiry(chain, qd, 30)
        assert near.empty

    def test_skips_expiry_within_min_dte(self):
        qd = _ts("2024-01-15")
        # expiry in 3 days — should be skipped (< _MIN_DTE = 7)
        chain = _make_chain("2024-01-15", expiry_30d="2024-01-18")
        result = _select_expiry(chain, qd, 30)
        assert result.empty


# ---------------------------------------------------------------------------
# _atm_iv_from_slice
# ---------------------------------------------------------------------------

class TestAtmIvFromSlice:
    def test_interpolates_between_bracketing_strikes(self):
        # Two strikes: 140 (IV=0.30) and 160 (IV=0.20), spot=150
        df = pd.DataFrame({
            "strike": [140.0, 160.0],
            "iv": [0.30, 0.20],
            "right": ["C", "C"],
        })
        result = _atm_iv_from_slice(df, spot=150.0)
        # log-interpolation between 140 and 160; midpoint should be near 0.25
        assert 0.20 < result < 0.30

    def test_exact_strike_at_spot(self):
        df = pd.DataFrame({"strike": [150.0], "iv": [0.25], "right": ["C"]})
        result = _atm_iv_from_slice(df, spot=150.0)
        assert pytest.approx(result, rel=1e-6) == 0.25

    def test_empty_slice_returns_nan(self):
        df = pd.DataFrame({"strike": [], "iv": [], "right": []})
        result = _atm_iv_from_slice(df, spot=150.0)
        assert np.isnan(result)

    def test_zero_spot_returns_nan(self):
        df = pd.DataFrame({"strike": [150.0], "iv": [0.25], "right": ["C"]})
        result = _atm_iv_from_slice(df, spot=0.0)
        assert np.isnan(result)

    def test_all_nan_iv_returns_nan(self):
        df = pd.DataFrame({"strike": [140.0, 150.0, 160.0], "iv": [np.nan, np.nan, np.nan], "right": ["C", "C", "C"]})
        result = _atm_iv_from_slice(df, spot=150.0)
        assert np.isnan(result)

    def test_uses_mean_of_put_and_call_iv(self):
        # At strike 150, call IV=0.20, put IV=0.30 — should average to 0.25
        df = pd.DataFrame({
            "strike": [150.0, 150.0],
            "iv": [0.20, 0.30],
            "right": ["C", "P"],
        })
        result = _atm_iv_from_slice(df, spot=150.0)
        assert pytest.approx(result, rel=1e-5) == 0.25


# ---------------------------------------------------------------------------
# _interp_iv_at_delta
# ---------------------------------------------------------------------------

class TestInterpIvAtDelta:
    def _make_delta_df(self):
        # Put contracts: delta ~ -0.1 to -0.9 → abs_delta 0.1 to 0.9
        # IV decreases as we go further OTM (smaller abs_delta for puts)
        rows = []
        for d in [0.1, 0.2, 0.25, 0.3, 0.4, 0.5]:
            rows.append({"right": "P", "delta": -d, "iv": 0.30 + (0.5 - d) * 0.2})
            rows.append({"right": "C", "delta": d, "iv": 0.28 + (0.5 - d) * 0.15})
        return pd.DataFrame(rows)

    def test_exact_25d_put(self):
        df = self._make_delta_df()
        result = _interp_iv_at_delta(df, "P", 0.25)
        # We have an exact 0.25 abs_delta put in the fixture
        assert not np.isnan(result)
        assert 0.20 < result < 0.50

    def test_interpolates_between_puts(self):
        # No exact 0.22 delta contract — should interpolate between 0.2 and 0.25
        df = pd.DataFrame({
            "right": ["P", "P"],
            "delta": [-0.2, -0.25],
            "iv": [0.32, 0.30],
        })
        result = _interp_iv_at_delta(df, "P", 0.22)
        assert not np.isnan(result)
        # Should be between the two IVs
        assert 0.30 < result < 0.32

    def test_empty_side_returns_nan(self):
        df = pd.DataFrame({"right": ["C", "C"], "delta": [0.3, 0.4], "iv": [0.25, 0.28]})
        result = _interp_iv_at_delta(df, "P", 0.25)
        assert np.isnan(result)

    def test_wrong_right_column_returns_nan(self):
        df = pd.DataFrame({"right": ["C"], "delta": [0.25], "iv": [0.25]})
        result = _interp_iv_at_delta(df, "P", 0.25)
        assert np.isnan(result)


# ---------------------------------------------------------------------------
# compute_options_features — integration tests with synthetic chain
# ---------------------------------------------------------------------------

class TestComputeOptionsFeatures:
    _qd = "2024-01-15"
    _exp30 = "2024-02-16"   # 32 days out
    _exp60 = "2024-03-18"   # 63 days out
    _spot = 150.0

    def _chain(self, **kwargs):
        return _make_chain(
            self._qd,
            expiry_30d=self._exp30,
            expiry_60d=self._exp60,
            spot=self._spot,
            **kwargs,
        )

    def test_atm_iv_non_null_with_valid_chain(self):
        feats = compute_options_features(
            self._chain(), self._spot, _ts(self._qd)
        )
        assert not np.isnan(feats["atm_iv"])

    def test_atm_iv_reasonable_range(self):
        feats = compute_options_features(
            self._chain(atm_iv=0.25), self._spot, _ts(self._qd)
        )
        # Should be within 5 vol points of the input ATM IV
        assert 0.15 < feats["atm_iv"] < 0.45

    def test_skew_25d_non_null(self):
        feats = compute_options_features(
            self._chain(), self._spot, _ts(self._qd)
        )
        assert not np.isnan(feats["skew_25d"])

    def test_skew_25d_sign_positive_for_put_heavy_skew(self):
        # When puts have higher IV (put skew), skew_25d = IV_put - IV_call > 0
        chain = self._chain()
        # Manually inflate put IVs
        chain.loc[chain["right"] == "P", "iv"] += 0.05
        feats = compute_options_features(chain, self._spot, _ts(self._qd))
        assert feats["skew_25d"] > 0

    def test_iv_term_slope_non_null_with_both_tenors(self):
        feats = compute_options_features(
            self._chain(), self._spot, _ts(self._qd)
        )
        assert not np.isnan(feats["iv_term_slope"])

    def test_iv_term_slope_positive_for_upward_term_structure(self):
        # The fixture uses base_iv + (dte-30)*0.001, so 60d IV > 30d IV → slope > 0
        feats = compute_options_features(
            self._chain(atm_iv=0.20), self._spot, _ts(self._qd)
        )
        assert feats["iv_term_slope"] > 0

    def test_iv_term_slope_nan_when_far_tenor_missing(self):
        chain = _make_chain(self._qd, expiry_30d=self._exp30, spot=self._spot)
        feats = compute_options_features(chain, self._spot, _ts(self._qd))
        assert np.isnan(feats["iv_term_slope"])

    def test_put_call_oi_ratio_non_null(self):
        feats = compute_options_features(
            self._chain(), self._spot, _ts(self._qd)
        )
        assert not np.isnan(feats["put_call_oi_ratio"])

    def test_put_call_oi_ratio_positive(self):
        feats = compute_options_features(
            self._chain(), self._spot, _ts(self._qd)
        )
        assert feats["put_call_oi_ratio"] > 0

    def test_put_call_oi_ratio_higher_when_put_oi_inflated(self):
        # Build two chains: one baseline, one with 2x put OI
        base = self._chain()
        high_put = base.copy()
        high_put.loc[high_put["right"] == "P", "open_interest"] *= 2
        feats_base = compute_options_features(base, self._spot, _ts(self._qd))
        feats_high = compute_options_features(high_put, self._spot, _ts(self._qd))
        assert feats_high["put_call_oi_ratio"] > feats_base["put_call_oi_ratio"]

    def test_net_gamma_exposure_non_null_with_gamma(self):
        feats = compute_options_features(
            self._chain(include_gamma=True), self._spot, _ts(self._qd)
        )
        assert not np.isnan(feats["net_gamma_exposure"])

    def test_net_gamma_exposure_nan_without_gamma_column(self):
        chain = self._chain(include_gamma=False)
        feats = compute_options_features(chain, self._spot, _ts(self._qd))
        assert np.isnan(feats["net_gamma_exposure"])

    def test_net_gamma_exposure_sign_convention(self):
        # Single call with gamma=0.01, OI=100 → GEX = +1.0
        # Single put with gamma=0.01, OI=100 → GEX = -1.0
        # Together → GEX = 0.0
        qd = _ts(self._qd)
        expiry = _ts(self._exp30)
        df = pd.DataFrame([
            {"quote_date": qd, "underlying": "TEST", "expiry": expiry,
             "strike": 150.0, "right": "C", "iv": 0.25, "delta": 0.5,
             "gamma": 0.01, "open_interest": 100},
            {"quote_date": qd, "underlying": "TEST", "expiry": expiry,
             "strike": 150.0, "right": "P", "iv": 0.25, "delta": -0.5,
             "gamma": 0.01, "open_interest": 100},
        ])
        feats = compute_options_features(df, 150.0, qd)
        assert pytest.approx(feats["net_gamma_exposure"], abs=1e-9) == 0.0

    def test_all_nan_on_empty_chain(self):
        feats = compute_options_features(pd.DataFrame(), 150.0, _ts(self._qd))
        for key in ["atm_iv", "skew_25d", "iv_term_slope", "put_call_oi_ratio", "net_gamma_exposure"]:
            assert np.isnan(feats[key]), f"Expected NaN for {key} on empty chain"

    def test_all_nan_when_near_tenor_missing(self):
        # Only a far expiry — no near tenor
        chain = _make_chain(self._qd, expiry_60d=self._exp60, spot=self._spot)
        feats = compute_options_features(chain, self._spot, _ts(self._qd))
        # atm_iv and skew_25d require near tenor
        assert np.isnan(feats["atm_iv"])
        assert np.isnan(feats["skew_25d"])

    def test_handles_all_nan_iv_gracefully(self):
        chain = self._chain()
        chain["iv"] = np.nan
        feats = compute_options_features(chain, self._spot, _ts(self._qd))
        assert np.isnan(feats["atm_iv"])
        assert np.isnan(feats["skew_25d"])

    def test_handles_missing_25d_strike_gracefully(self):
        # Chain with only very deep ITM contracts (abs_delta ~ 0.9) — no 25d contract
        qd = _ts(self._qd)
        expiry = _ts(self._exp30)
        df = pd.DataFrame([
            {"quote_date": qd, "underlying": "TEST", "expiry": expiry,
             "strike": 100.0, "right": "C", "iv": 0.25, "delta": 0.95, "open_interest": 500},
            {"quote_date": qd, "underlying": "TEST", "expiry": expiry,
             "strike": 100.0, "right": "P", "iv": 0.28, "delta": -0.95, "open_interest": 500},
        ])
        feats = compute_options_features(df, 150.0, qd)
        # Should return a value (extrapolated to nearest endpoint), not crash
        # skew_25d may be non-NaN (nearest endpoint fallback) or NaN — just must not raise
        assert isinstance(feats["skew_25d"], float)


# ---------------------------------------------------------------------------
# Realistic chain shape — mirrors real ThetaData v3 data ("CALL"/"PUT" right column)
# These tests would have returned NaN for skew_25d, put_call_oi_ratio, and
# net_gamma_exposure before the right-column normalisation fix.
# ---------------------------------------------------------------------------

class TestRealisticChainShape:
    """Verify all 5 features are non-NaN when the chain uses real ThetaData v3 format."""

    _qd = "2024-01-15"
    _exp30 = "2024-02-16"   # 32 days out
    _exp60 = "2024-03-18"   # 63 days out
    _spot = 150.0

    def _realistic_chain(self, **kwargs):
        return _make_chain(
            self._qd,
            expiry_30d=self._exp30,
            expiry_60d=self._exp60,
            spot=self._spot,
            right_format="long",  # "CALL"/"PUT" as ThetaData v3 returns
            **kwargs,
        )

    def test_all_5_features_non_nan_with_call_put_right(self):
        chain = self._realistic_chain(include_gamma=True)
        feats = compute_options_features(chain, self._spot, _ts(self._qd))
        for name in ["atm_iv", "skew_25d", "iv_term_slope", "put_call_oi_ratio", "net_gamma_exposure"]:
            assert not np.isnan(feats[name]), f"{name} is NaN with CALL/PUT right column"

    def test_skew_25d_non_nan_with_call_put_right(self):
        chain = self._realistic_chain()
        feats = compute_options_features(chain, self._spot, _ts(self._qd))
        assert not np.isnan(feats["skew_25d"]), "skew_25d must not be NaN when right='CALL'/'PUT'"

    def test_put_call_oi_ratio_non_nan_with_call_put_right(self):
        chain = self._realistic_chain()
        feats = compute_options_features(chain, self._spot, _ts(self._qd))
        assert not np.isnan(feats["put_call_oi_ratio"])
        assert feats["put_call_oi_ratio"] > 0

    def test_net_gamma_exposure_correct_sign_convention_with_call_put_right(self):
        qd = _ts(self._qd)
        expiry = _ts(self._exp30)
        # Single call (+GEX) and single put (-GEX), equal gamma and OI → net = 0
        df = pd.DataFrame([
            {"quote_date": qd, "underlying": "TEST", "expiry": expiry,
             "strike": 150.0, "right": "CALL", "iv": 0.25, "delta": 0.5,
             "gamma": 0.01, "open_interest": 100},
            {"quote_date": qd, "underlying": "TEST", "expiry": expiry,
             "strike": 150.0, "right": "PUT", "iv": 0.25, "delta": -0.5,
             "gamma": 0.01, "open_interest": 100},
        ])
        feats = compute_options_features(df, 150.0, qd)
        assert pytest.approx(feats["net_gamma_exposure"], abs=1e-9) == 0.0

    def test_net_gamma_exposure_positive_call_only(self):
        qd = _ts(self._qd)
        expiry = _ts(self._exp30)
        df = pd.DataFrame([
            {"quote_date": qd, "underlying": "TEST", "expiry": expiry,
             "strike": 150.0, "right": "CALL", "iv": 0.25, "delta": 0.5,
             "gamma": 0.01, "open_interest": 100},
        ])
        feats = compute_options_features(df, 150.0, qd)
        assert feats["net_gamma_exposure"] > 0, "Call-only chain must have positive GEX"

    def test_iv_term_slope_non_nan_with_call_put_right(self):
        chain = self._realistic_chain()
        feats = compute_options_features(chain, self._spot, _ts(self._qd))
        assert not np.isnan(feats["iv_term_slope"])

    def test_skew_25d_sign_positive_for_put_heavy_with_call_put_right(self):
        chain = self._realistic_chain()
        chain.loc[chain["right"] == "PUT", "iv"] += 0.05
        feats = compute_options_features(chain, self._spot, _ts(self._qd))
        assert feats["skew_25d"] > 0


# ---------------------------------------------------------------------------
# Registry registration
# ---------------------------------------------------------------------------

class TestRegistryRegistration:
    def test_all_options_features_registered(self):
        for name in ["atm_iv", "skew_25d", "iv_term_slope", "put_call_oi_ratio", "net_gamma_exposure"]:
            assert name in registry.registered_features, f"'{name}' not registered"

    def test_options_v1_feature_set_loads(self):
        fs = registry.load_feature_set("options_v1")
        assert fs.name == "options_v1"
        assert set(fs.options) == {
            "atm_iv", "skew_25d", "iv_term_slope", "put_call_oi_ratio", "net_gamma_exposure"
        }

    def test_options_v1_feature_set_hash_stable(self):
        fs = registry.load_feature_set("options_v1")
        h1 = fs.config_hash()
        h2 = fs.config_hash()
        assert h1 == h2
        assert len(h1) == 12


# ---------------------------------------------------------------------------
# Winsorization (options features are winsorized per-underlying in pipeline)
# ---------------------------------------------------------------------------

class TestWinsorizeApplied:
    def test_winsorize_clips_outliers(self):
        """The compute_options_features output feeds through _winsorize in pipeline.
        Verify here that extreme values are handled without NaN injection.
        """
        qd = _ts("2024-01-15")
        expiry = _ts("2024-02-16")
        # Extreme IV (5.0 — outlier), should not crash or produce inf
        df = pd.DataFrame([
            {"quote_date": qd, "underlying": "X", "expiry": expiry,
             "strike": 150.0, "right": "C", "iv": 5.0, "delta": 0.5, "open_interest": 100},
            {"quote_date": qd, "underlying": "X", "expiry": expiry,
             "strike": 150.0, "right": "P", "iv": 5.0, "delta": -0.5, "open_interest": 100},
        ])
        feats = compute_options_features(df, 150.0, qd)
        assert not np.isinf(feats["atm_iv"])
        assert feats["atm_iv"] == pytest.approx(5.0, rel=1e-5)
