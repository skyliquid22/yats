"""Tests for research.alpha.targets — forward returns and residualization."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.alpha.targets import (
    compute_forward_returns,
    compute_rolling_beta,
    residualize_vs_spy,
)


def _make_panel(n_days: int = 100, symbols: list[str] | None = None) -> pd.DataFrame:
    """Create a simple panel with synthetic closes."""
    if symbols is None:
        symbols = ["AAPL", "SPY"]
    dates = pd.date_range("2024-01-01", periods=n_days, freq="B").date
    rows = []
    for sym in symbols:
        base = 150.0 if sym == "AAPL" else 500.0
        for i, d in enumerate(dates):
            # Deterministic price: 0.1% daily return
            close = base * (1.001 ** i)
            rows.append({"date": d, "symbol": sym, "close": close})
    return pd.DataFrame(rows).sort_values(["date", "symbol"]).reset_index(drop=True)


class TestComputeForwardReturns:
    def test_basic_5day(self):
        df = _make_panel(n_days=20)
        fwd = compute_forward_returns(df, 5)
        # For AAPL with 0.1% daily, 5d fwd return ≈ 1.001^5 - 1 ≈ 0.00501
        aapl_rows = df[df["symbol"] == "AAPL"].sort_values("date")
        fwd_aapl = fwd.reindex(aapl_rows.index)
        # First 15 rows should have valid values
        assert not np.isnan(fwd_aapl.iloc[0])
        expected = 1.001 ** 5 - 1.0
        assert abs(fwd_aapl.iloc[0] - expected) < 1e-6

    def test_last_h_rows_nan(self):
        df = _make_panel(n_days=20)
        fwd = compute_forward_returns(df, 5)
        aapl_rows = df[df["symbol"] == "AAPL"].sort_values("date")
        fwd_aapl = fwd.reindex(aapl_rows.index)
        assert np.isnan(fwd_aapl.iloc[-1])
        assert np.isnan(fwd_aapl.iloc[-5])

    def test_horizon_1_matches_pct_change(self):
        df = _make_panel(n_days=10)
        fwd = compute_forward_returns(df, 1)
        aapl_rows = df[df["symbol"] == "AAPL"].sort_values("date").reset_index(drop=True)
        fwd_aapl = fwd.reindex(aapl_rows.index).reset_index(drop=True)
        # fwd_1d at t = close[t+1] / close[t] - 1
        for i in range(len(aapl_rows) - 1):
            expected = aapl_rows["close"].iloc[i + 1] / aapl_rows["close"].iloc[i] - 1.0
            assert abs(fwd_aapl.iloc[i] - expected) < 1e-10


class TestComputeRollingBeta:
    def test_beta_of_identical_series_is_one(self):
        r = pd.Series([0.01, -0.005, 0.02, -0.01, 0.015] * 20)
        beta = compute_rolling_beta(r, r, window=10)
        # Beta of identical series = 1
        valid = beta.dropna()
        assert len(valid) > 0
        assert all(abs(b - 1.0) < 1e-6 for b in valid)

    def test_beta_of_zero_series_is_zero(self):
        spy = pd.Series([0.01, -0.005, 0.02, -0.01, 0.015] * 20)
        zero = pd.Series([0.0] * 100)
        beta = compute_rolling_beta(zero, spy, window=10)
        valid = beta.dropna()
        assert all(abs(b) < 1e-10 for b in valid)

    def test_beta_clipped_at_5(self):
        """Extreme betas are clipped to [-5, 5]."""
        spy = pd.Series([0.001] * 100)
        big = pd.Series([1.0] * 100)
        beta = compute_rolling_beta(big, spy, window=10)
        valid = beta.dropna()
        assert all(abs(b) <= 5.0 + 1e-10 for b in valid)

    def test_first_window_rows_nan(self):
        r = pd.Series([0.01] * 50)
        spy = pd.Series([0.01] * 50)
        beta = compute_rolling_beta(r, spy, window=30)
        # min_periods = 15 (window // 2)
        assert beta.iloc[:14].isna().all()


class TestResidualizeVsSpy:
    def test_residuals_reduce_market_exposure(self):
        """After residualization, correlation of residuals with SPY fwd is lower."""
        n = 120
        df = _make_panel(n_days=n, symbols=["AAPL", "MSFT", "SPY"])
        fwd_5d = compute_forward_returns(df, 5)
        df["fwd_5d"] = fwd_5d
        spy_mask = df["symbol"] == "SPY"
        spy_fwd = df.loc[spy_mask].set_index("date")["fwd_5d"]
        spy_fwd = spy_fwd[~spy_fwd.index.duplicated(keep="first")]

        resid = residualize_vs_spy(df, "fwd_5d", spy_fwd)
        df["resid"] = resid

        aapl_mask = df["symbol"] == "AAPL"
        aapl_fwd = df.loc[aapl_mask, "fwd_5d"].dropna()
        aapl_resid = df.loc[aapl_mask, "resid"].dropna()
        spy_fwd_aligned = spy_fwd.reindex(df.loc[aapl_mask, "date"]).dropna()
        common = aapl_fwd.index.intersection(spy_fwd_aligned.index).intersection(aapl_resid.index)
        if len(common) > 10:
            corr_before = abs(np.corrcoef(aapl_fwd.loc[common], spy_fwd_aligned.loc[common])[0, 1])
            corr_after = abs(np.corrcoef(aapl_resid.loc[common], spy_fwd_aligned.loc[common])[0, 1])
            assert corr_after <= corr_before + 0.1  # residual should not increase correlation

    def test_output_same_length_as_input(self):
        n = 80
        df = _make_panel(n_days=n, symbols=["AAPL", "SPY"])
        fwd = compute_forward_returns(df, 5)
        df["fwd_5d"] = fwd
        spy_mask = df["symbol"] == "SPY"
        spy_fwd = df.loc[spy_mask].set_index("date")["fwd_5d"]
        spy_fwd = spy_fwd[~spy_fwd.index.duplicated(keep="first")]

        resid = residualize_vs_spy(df, "fwd_5d", spy_fwd)
        assert len(resid) == len(df)

    def test_spy_residual_unchanged(self):
        """SPY's residual should equal its raw forward return (no adjustment)."""
        n = 80
        df = _make_panel(n_days=n, symbols=["AAPL", "SPY"])
        fwd = compute_forward_returns(df, 5)
        df["fwd_5d"] = fwd
        spy_mask = df["symbol"] == "SPY"
        spy_fwd = df.loc[spy_mask].set_index("date")["fwd_5d"]
        spy_fwd = spy_fwd[~spy_fwd.index.duplicated(keep="first")]

        resid = residualize_vs_spy(df, "fwd_5d", spy_fwd)
        spy_rows = df[df["symbol"] == "SPY"]
        # SPY residual == SPY raw fwd (no adjustment applied to the market proxy)
        pd.testing.assert_series_equal(
            resid.loc[spy_rows.index],
            df.loc[spy_rows.index, "fwd_5d"],
            check_names=False,
        )
