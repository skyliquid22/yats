"""Tests for research.alpha.neutralize (SPAN-16 label variant N)."""

import numpy as np
import pandas as pd
import pytest

from research.alpha.neutralize import (
    pca_neutralize_column,
    trailing_pca_eigenvectors,
)

RNG = np.random.default_rng(7)


def _factor_market(n_days=600, n_sym=30, factor_vol=0.02, idio_vol=0.005):
    """Synthetic returns with one dominant common factor."""
    dates = pd.bdate_range("2020-01-01", periods=n_days)
    f = RNG.normal(0, factor_vol, n_days)
    loadings = RNG.uniform(0.5, 1.5, n_sym)
    idio = RNG.normal(0, idio_vol, (n_days, n_sym))
    rets = np.outer(f, loadings) + idio
    symbols = [f"S{i:02d}" for i in range(n_sym)]
    rets_df = pd.DataFrame(rets, index=dates, columns=symbols)
    closes = 100 * (1 + rets_df).cumprod()
    return dates, symbols, rets_df, closes


def _long_closes(closes):
    out = closes.stack(future_stack=True).rename("close").reset_index()
    out.columns = ["date", "symbol", "close"]
    return out


def _panel_with_fwd(closes, h=5):
    long = _long_closes(closes)
    long = long.sort_values(["symbol", "date"]).reset_index(drop=True)
    long["fwd"] = long.groupby("symbol")["close"].transform(
        lambda s: s.shift(-h) / s - 1.0
    )
    return long


class TestTrailingPCA:
    def test_causality_ignores_future(self):
        dates, _, rets, _ = _factor_market()
        asof = dates[400]
        v1 = trailing_pca_eigenvectors(rets, asof)
        poisoned = rets.copy()
        poisoned.loc[poisoned.index >= asof] = 99.0
        v2 = trailing_pca_eigenvectors(poisoned, asof)
        pd.testing.assert_frame_equal(v1, v2)

    def test_short_window_returns_none(self):
        dates, _, rets, _ = _factor_market()
        assert trailing_pca_eigenvectors(rets, dates[50]) is None

    def test_low_coverage_symbol_excluded(self):
        dates, symbols, rets, _ = _factor_market()
        rets.loc[:, "S00"] = np.nan
        v = trailing_pca_eigenvectors(rets, dates[400])
        assert "S00" not in v.index
        assert len(v.index) == len(symbols) - 1


class TestPcaNeutralizeColumn:
    def test_removes_factor_exposure(self):
        dates, symbols, rets, closes = _factor_market()
        panel = _panel_with_fwd(closes)
        rebalances = [dates[300], dates[450]]
        resid = pca_neutralize_column(
            panel, "fwd", _long_closes(closes), rebalances
        )
        # factor forward return proxy: equal-weight market fwd
        panel = panel.assign(resid=resid)
        blk = panel[panel["date"].isin(dates[300:590])].dropna(subset=["resid", "fwd"])
        mkt_fwd = blk.groupby("date")["fwd"].mean()
        sym_resid = blk.pivot_table(index="date", columns="symbol", values="resid")
        corrs = sym_resid.corrwith(mkt_fwd).abs()
        raw = blk.pivot_table(index="date", columns="symbol", values="fwd")
        raw_corrs = raw.corrwith(mkt_fwd).abs()
        # raw targets are dominated by the factor; residuals are not
        assert raw_corrs.median() > 0.9
        assert corrs.median() < 0.35

    def test_dates_before_first_rebalance_are_nan(self):
        dates, _, _, closes = _factor_market()
        panel = _panel_with_fwd(closes)
        resid = pca_neutralize_column(
            panel, "fwd", _long_closes(closes), [dates[300]]
        )
        early = panel["date"] < dates[300]
        assert resid[early].isna().all()

    def test_nan_fwd_stays_nan(self):
        dates, _, _, closes = _factor_market()
        panel = _panel_with_fwd(closes)
        resid = pca_neutralize_column(
            panel, "fwd", _long_closes(closes), [dates[300]]
        )
        nan_fwd = panel["fwd"].isna()
        assert resid[nan_fwd].isna().all()
