"""Tests for research/scripts/wfo_pilot_liquid50.py (pre-registered pilot runner).

Everything DB-side is out of scope: only pure functions are exercised —
no QuestDB connection is opened anywhere in this module.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[3]


def _load_module(name: str, rel_path: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


pilot = _load_module("wfo_pilot_liquid50_under_test", "research/scripts/wfo_pilot_liquid50.py")

from compute.stats.deflated_sharpe import _expected_max_sr_benchmark  # noqa: E402
from research.alpha.models import train_alpha_model  # noqa: E402

INSIDER_COLS = [
    "insider_net_buy_90d", "insider_buy_intensity_30d",
    "insider_cluster_30d", "exec_net_buy_90d",
]
INSTITUTIONAL_COLS = ["inst_ownership_pct", "inst_top10_share"]


# ---------------------------------------------------------------------------
# Arm column-subset derivation
# ---------------------------------------------------------------------------

class TestArmColumnDerivation:
    def test_arm_a_excludes_insider_and_institutional(self):
        groups = pilot.load_feature_groups()
        cols_a = pilot.arm_feature_cols(groups, "A")
        for col in INSIDER_COLS + INSTITUTIONAL_COLS:
            assert col not in cols_a, f"arm A must exclude {col}"

    def test_arm_b_includes_insider_and_institutional(self):
        groups = pilot.load_feature_groups()
        cols_b = pilot.arm_feature_cols(groups, "B")
        for col in INSIDER_COLS + INSTITUTIONAL_COLS:
            assert col in cols_b, f"arm B must include {col}"

    def test_arm_difference_is_exactly_the_two_groups(self):
        groups = pilot.load_feature_groups()
        cols_a = pilot.arm_feature_cols(groups, "A")
        cols_b = pilot.arm_feature_cols(groups, "B")
        assert set(cols_b) - set(cols_a) == set(INSIDER_COLS + INSTITUTIONAL_COLS)
        assert set(cols_a) <= set(cols_b)

    def test_arm_a_keeps_the_four_prereg_groups(self):
        # Prereg: arm A = ohlcv, cross_sectional, fundamental, and regime
        # groups only (regime included even though constant cross-sectionally).
        groups = pilot.load_feature_groups()
        cols_a = pilot.arm_feature_cols(groups, "A")
        for grp in ("ohlcv", "cross_sectional", "fundamental", "regime"):
            for col in groups[grp]:
                assert col in cols_a, f"arm A must keep {grp}:{col}"

    def test_no_duplicate_columns(self):
        groups = pilot.load_feature_groups()
        for arm in ("A", "B"):
            cols = pilot.arm_feature_cols(groups, arm)
            assert len(cols) == len(set(cols))

    def test_unknown_arm_raises(self):
        with pytest.raises(ValueError):
            pilot.arm_feature_cols({"ohlcv": ["ret_1d"]}, "C")

    def test_metadata_keys_skipped_by_group_loader(self):
        groups = pilot.load_feature_groups()
        assert "name" not in groups
        assert "description" not in groups
        # breadth_v1 as committed: 6 groups
        assert set(groups) == {
            "ohlcv", "cross_sectional", "fundamental", "regime",
            "insider", "institutional",
        }

    def test_derivation_from_synthetic_groups(self):
        groups = {
            "ohlcv": ["r1", "r2"],
            "insider": ["i1"],
            "institutional": ["n1"],
            "regime": ["g1"],
        }
        assert pilot.arm_feature_cols(groups, "A") == ["r1", "r2", "g1"]
        assert pilot.arm_feature_cols(groups, "B") == ["r1", "r2", "i1", "n1", "g1"]


# ---------------------------------------------------------------------------
# Grid construction: exactly 10 trials including overlays
# ---------------------------------------------------------------------------

class TestGridConstruction:
    def test_total_trials_is_ten(self):
        assert pilot.TOTAL_TRIALS == 10

    def test_base_grid_labels_and_regularization(self):
        labels = [c["label"] for c in pilot.BASE_GRID]
        assert labels == ["ridge_5d_a10", "ridge_21d_a10", "lgbm_5d", "lgbm_21d"]
        for cfg in pilot.BASE_GRID:
            if cfg["model"] == "ridge":
                assert cfg["reg"] == 10.0  # ALPHA-1 incumbent alpha
            else:
                assert cfg["reg"] == 0.1   # ALPHA-1 incumbent lambda

    def test_arm_grids_are_four_configs_each_plus_one_overlay(self):
        grids = pilot.build_arm_grids()
        assert set(grids) == {"A", "B"}
        for arm, grid in grids.items():
            assert len(grid) == 4
            assert all(c["arm"] == arm for c in grid)
        n_trials = sum(len(g) for g in grids.values()) + len(grids) * pilot.N_OVERLAYS_PER_ARM
        assert n_trials == pilot.TOTAL_TRIALS == 10

    def test_arm_grids_do_not_mutate_base_grid(self):
        grids = pilot.build_arm_grids()
        grids["A"][0]["reg"] = 999.0
        assert pilot.BASE_GRID[0]["reg"] == 10.0
        assert "arm" not in pilot.BASE_GRID[0]

    def test_wfo_geometry_matches_prereg(self):
        assert pilot.WFO_CFG.mode == "anchored"
        assert pilot.WFO_CFG.n_periods == 4
        assert pilot.WFO_CFG.label_horizon == 21
        assert pilot.WFO_CFG.purge_buffer == 253  # 1 + max lookback 252
        assert pilot.EXECUTION_LAG == 1
        assert pilot.FILL_TIMING == "next_close"
        assert pilot.COST_BP == 5.0
        assert pilot.VOL_TARGET == 0.10


# ---------------------------------------------------------------------------
# Fake per-config summaries (shape produced by summarize_config_result)
# ---------------------------------------------------------------------------

def _fake_summary(arm, label, sharpe, overlay=False):
    d = {
        "arm": arm,
        "model": "ridge",
        "horizon": 5,
        "reg": 10.0,
        "label": label,
        "sharpe": sharpe,
        "skewness": -0.1,
        "kurtosis": 5.0,
        "n_obs": 1200,
        "per_fold_oos_sharpe": [0.1, 0.4, 0.6, 0.3],
        "median_oos_sharpe": 0.35,
        "psr_vs_zero": 0.8,
    }
    if overlay:
        d["overlay"] = "vol_target_10pct"
        d["base_config"] = label.replace("_vt10", "")
    return d


def _fake_family():
    """8 base configs (4 per arm) + 2 overlays = the 10-trial family."""
    out = []
    for arm, bump in (("A", 0.0), ("B", 0.05)):
        for i, cfg in enumerate(pilot.BASE_GRID):
            out.append(_fake_summary(arm, cfg["label"], 0.4 + 0.1 * i + bump))
        out.append(_fake_summary(arm, "lgbm_21d_vt10", 0.9 + bump, overlay=True))
    return out


# ---------------------------------------------------------------------------
# Strict deflation: SR0 must use the 73-trial expected-max benchmark
# ---------------------------------------------------------------------------

class TestStrictDeflation:
    def test_uses_73_trials(self):
        result = pilot.strict_deflation(_fake_family())
        assert result["n_trials_emax"] == 73
        assert result["expected_max_sr"] == pytest.approx(_expected_max_sr_benchmark(73))

    def test_pool_includes_the_ten_new_trials(self):
        family = _fake_family()
        result = pilot.strict_deflation(family)
        assert len(result["pool_sharpes"]) == len(pilot.HONEST_FILL_POOL_PRE_PILOT) + 10
        assert len(pilot.HONEST_FILL_POOL_PRE_PILOT) == 15  # incumbent honest-fill pool
        for c in family:
            assert float(c["sharpe"]) in result["pool_sharpes"]

    def test_sr0_is_pool_std_times_emax_at_73(self):
        family = _fake_family()
        result = pilot.strict_deflation(family)
        pool = pilot.HONEST_FILL_POOL_PRE_PILOT + [c["sharpe"] for c in family]
        expected_sr0 = float(np.std(pool)) * _expected_max_sr_benchmark(73)
        assert result["sr0"] == pytest.approx(expected_sr0)
        # Guard against deflating at the family or pool size instead of 73:
        assert result["sr0"] != pytest.approx(
            float(np.std(pool)) * _expected_max_sr_benchmark(10)
        )
        assert result["sr0"] != pytest.approx(
            float(np.std(pool)) * _expected_max_sr_benchmark(len(pool))
        )

    def test_per_config_verdicts_shape(self):
        family = _fake_family()
        result = pilot.strict_deflation(family)
        assert len(result["per_config"]) == 10
        for c in result["per_config"]:
            assert c["arm"] in ("A", "B")
            assert 0.0 <= c["strict_dsr"] <= 1.0
            assert isinstance(c["certified"], bool)
        assert result["certification_bar"] == 0.95
        assert result["any_certified"] == any(c["certified"] for c in result["per_config"])

    def test_incumbent_pool_reproduces_registered_sr0_at_63(self):
        # Cross-check against the incumbent strict receipt
        # (docs/research/receipts/vt_lgbm21_result.json): sr0=0.508 at 63 trials.
        sr0_63 = float(np.std(pilot.HONEST_FILL_POOL_PRE_PILOT)) * _expected_max_sr_benchmark(63)
        assert sr0_63 == pytest.approx(0.508, abs=0.002)


# ---------------------------------------------------------------------------
# Secondary readout
# ---------------------------------------------------------------------------

class TestSecondaryReadout:
    def test_rows_cover_the_four_base_classes(self):
        rows = pilot.secondary_readout(_fake_family())
        assert [r["config"] for r in rows] == [c["label"] for c in pilot.BASE_GRID]

    def test_deltas_and_margins(self):
        rows = pilot.secondary_readout(_fake_family())
        for row in rows:
            dev10 = pilot.INCUMBENT_DEV10_SHARPES[row["config"]]
            assert row["sharpe_dev10_incumbent"] == dev10
            assert row["delta_a_vs_dev10"] == pytest.approx(
                row["sharpe_liquid50_arm_a"] - dev10
            )
            assert row["margin_b_minus_a"] == pytest.approx(0.05)

    def test_overlays_are_not_treated_as_base_classes(self):
        rows = pilot.secondary_readout(_fake_family())
        # The fake overlay label lgbm_21d_vt10 must not leak into the
        # lgbm_21d row (its sharpe would be 0.9/0.95, not 0.7/0.75).
        lgbm21 = next(r for r in rows if r["config"] == "lgbm_21d")
        assert lgbm21["sharpe_liquid50_arm_a"] == pytest.approx(0.7)
        assert lgbm21["sharpe_liquid50_arm_b"] == pytest.approx(0.75)


# ---------------------------------------------------------------------------
# Receipt path + output shape
# ---------------------------------------------------------------------------

class TestReceipt:
    def _build(self):
        family = _fake_family()
        deflation = pilot.strict_deflation(family)
        secondary = pilot.secondary_readout(family)
        return pilot.build_receipt(
            family, deflation, secondary,
            symbols=[f"S{i}" for i in range(50)],
            arm_cols={"A": ["ret_1d"], "B": ["ret_1d", "insider_net_buy_90d"]},
            rank_decay_by_arm={"A": 0.3, "B": 0.4},
            span_end=None,
        )

    def test_receipt_path_constant(self):
        assert str(pilot.RECEIPT_PATH).endswith("docs/research/receipts/pilot_liquid50.json")

    def test_receipt_shape(self):
        receipt = self._build()
        for key in (
            "sweep", "preregistration", "universe", "feature_set",
            "arm_feature_cols", "span", "wfo", "execution", "trials_charged",
            "deflation_clock", "configs", "strict_deflation",
            "secondary_readout", "rank_decay_by_arm",
        ):
            assert key in receipt, f"receipt missing {key}"
        assert receipt["universe"] == "liquid50"
        assert receipt["feature_set"] == "breadth_v1"
        assert receipt["trials_charged"] == 10
        assert receipt["deflation_clock"] == 73
        assert receipt["execution"] == {
            "execution_lag_days": 1,
            "fill_timing": "next_close",
            "transaction_cost_bp": 5.0,
        }
        assert receipt["wfo"] == {
            "mode": "anchored", "n_periods": 4, "train_window": 250,
            "label_horizon": 21, "purge_buffer": 253,
        }

    def test_receipt_configs_carry_required_fields(self):
        receipt = self._build()
        assert len(receipt["configs"]) == 10
        for c in receipt["configs"]:
            for key in ("arm", "label", "per_fold_oos_sharpe", "n_obs",
                        "skewness", "kurtosis", "sharpe"):
                assert key in c, f"config entry missing {key}"
            assert len(c["per_fold_oos_sharpe"]) == 4

    def test_receipt_is_json_serializable(self):
        receipt = self._build()
        parsed = json.loads(json.dumps(receipt, default=str))
        assert parsed["strict_deflation"]["n_trials_emax"] == 73


# ---------------------------------------------------------------------------
# Panel assembly: SPY sidecar + short-history (IPO) handling — DB mocked out
# ---------------------------------------------------------------------------

def _synthetic_frames(n_dates=60, feature_cols=("ret_1d", "mom_3m"), ipo_offset=30):
    rng = np.random.default_rng(7)
    dates = pd.bdate_range("2021-01-04", periods=n_dates).date
    rows_f, rows_p = [], []
    symbols = {"AAA": 0, "BBB": 0, "NEW": ipo_offset}  # NEW lists mid-sample
    for sym, start in symbols.items():
        price = 100.0
        for i, dt in enumerate(dates):
            if i < start:
                continue
            price *= 1.0 + rng.normal(0.0005, 0.01)
            rows_f.append({"date": dt, "symbol": sym,
                           **{c: rng.normal() for c in feature_cols}})
            rows_p.append({"date": dt, "symbol": sym, "open": price * 0.999, "close": price})
    spy = 400.0
    for dt in dates:
        spy *= 1.0 + rng.normal(0.0003, 0.008)
        rows_p.append({"date": dt, "symbol": "SPY", "open": spy * 0.999, "close": spy})
    return pd.DataFrame(rows_f), pd.DataFrame(rows_p), dates


class TestBuildPanel:
    def test_spy_sidecar_used_then_dropped(self):
        features_df, closes_df, _ = _synthetic_frames()
        panel = pilot.build_panel(features_df, closes_df, ["ret_1d", "mom_3m"])
        assert "SPY" not in set(panel["symbol"])
        for col in ("fwd_5d", "fwd_21d", "fwd_5d_resid", "fwd_21d_resid",
                    "ret_1d_realized"):
            assert col in panel.columns

    def test_missing_spy_raises(self):
        features_df, closes_df, _ = _synthetic_frames()
        closes_no_spy = closes_df[closes_df["symbol"] != "SPY"]
        with pytest.raises(ValueError, match="SPY"):
            pilot.build_panel(features_df, closes_no_spy, ["ret_1d", "mom_3m"])

    def test_short_history_symbol_is_nan_skipped_not_filled(self):
        # IPO-style names (HOOD/SNOW/PLTR): rows exist only from listing on;
        # earlier dates are absent entirely — never forward/back-filled.
        features_df, closes_df, dates = _synthetic_frames(ipo_offset=30)
        panel = pilot.build_panel(features_df, closes_df, ["ret_1d", "mom_3m"])
        new_rows = panel[panel["symbol"] == "NEW"]
        assert len(new_rows) == len(dates) - 30
        assert min(new_rows["date"]) == dates[30]
        # Cross-section width varies by date instead of carrying NaN symbols
        pre_ipo = panel[panel["date"] == dates[0]]
        post_ipo = panel[panel["date"] == dates[45]]
        assert set(pre_ipo["symbol"]) == {"AAA", "BBB"}
        assert set(post_ipo["symbol"]) == {"AAA", "BBB", "NEW"}

    def test_features_are_rank_normalized_per_date(self):
        features_df, closes_df, dates = _synthetic_frames()
        panel = pilot.build_panel(features_df, closes_df, ["ret_1d", "mom_3m"])
        # After percentile-rank + z-score, each date's cross-section of a
        # feature is mean-zero (dates with >=2 symbols).
        day = panel[panel["date"] == dates[45]]
        assert abs(float(day["ret_1d"].mean())) < 1e-9


# ---------------------------------------------------------------------------
# Cost + capture extensions of the shared ALPHA-1 eval (refactor regression)
# ---------------------------------------------------------------------------

def _tiny_eval_setup():
    rng = np.random.default_rng(11)
    dates = list(pd.bdate_range("2022-01-03", periods=12).date)
    symbols = ["X", "Y", "Z"]
    rows = []
    for dt in dates:
        for sym in symbols:
            rows.append({
                "date": dt, "symbol": sym,
                "f1": rng.normal(), "f2": rng.normal(),
                "fwd_5d_resid": rng.normal(scale=0.01),
                "ret_1d_realized": rng.normal(0.0005, 0.01),
            })
    panel = pd.DataFrame(rows)
    model = train_alpha_model(panel, ["f1", "f2"], "fwd_5d_resid",
                              model_type="ridge", horizon=5, reg=10.0)
    return panel, dates, symbols, model


class TestEvalCostAndCapture:
    def test_zero_cost_matches_default_gross_path(self):
        panel, dates, symbols, model = _tiny_eval_setup()
        idx = list(range(len(dates)))
        gross_fn = pilot.alpha1.make_eval_fn(
            panel, dates, symbols=symbols,
            fill_timing="next_close", execution_lag=1,
        )
        zero_cost_fn = pilot.alpha1.make_eval_fn(
            panel, dates, symbols=symbols,
            fill_timing="next_close", execution_lag=1, cost_bp=0.0,
        )
        r_gross, s_gross = gross_fn(idx, model)
        r_zero, s_zero = zero_cost_fn(idx, model)
        assert r_zero == r_gross
        assert s_zero == pytest.approx(s_gross)

    def test_cost_reduces_returns_by_turnover(self):
        panel, dates, symbols, model = _tiny_eval_setup()
        idx = list(range(len(dates)))
        gross_fn = pilot.alpha1.make_eval_fn(
            panel, dates, symbols=symbols,
            fill_timing="next_close", execution_lag=1,
        )
        cost_fn = pilot.alpha1.make_eval_fn(
            panel, dates, symbols=symbols,
            fill_timing="next_close", execution_lag=1, cost_bp=5.0,
        )
        r_gross, _ = gross_fn(idx, model)
        r_cost, _ = cost_fn(idx, model)
        assert len(r_cost) == len(r_gross)
        assert all(c <= g + 1e-15 for c, g in zip(r_cost, r_gross))
        # First fill charges full establishment turnover (|w| sums to 1):
        assert r_cost[0] == pytest.approx(r_gross[0] - 1.0 * 5.0 / 1e4)

    def test_fold_capture_shapes(self):
        panel, dates, symbols, model = _tiny_eval_setup()
        idx = list(range(len(dates)))
        capture = []
        fn = pilot.alpha1.make_eval_fn(
            panel, dates, symbols=symbols,
            fill_timing="next_close", execution_lag=1,
            cost_bp=5.0, fold_capture=capture,
        )
        returns, _ = fn(idx, model)
        assert len(capture) == 1  # one eval call = one fold
        weights, rets = capture[0]["weights"], capture[0]["returns"]
        assert list(weights.columns) == symbols
        assert weights.shape == rets.shape == (len(returns), len(symbols))
        # Long-only, fully invested book at every fill date
        assert np.allclose(weights.sum(axis=1), 1.0)


# ---------------------------------------------------------------------------
# Vol-target overlay
# ---------------------------------------------------------------------------

class TestVolTargetOverlay:
    def _captures(self, n_folds=2, n_bars=40):
        rng = np.random.default_rng(3)
        captures = []
        for f in range(n_folds):
            idx = list(pd.bdate_range("2023-01-02", periods=n_bars).date)
            weights = pd.DataFrame(
                np.full((n_bars, 2), 0.5), index=idx, columns=["X", "Y"],
            )
            returns = pd.DataFrame(
                rng.normal(0.0005, 0.03, size=(n_bars, 2)), index=idx, columns=["X", "Y"],
            )
            captures.append({"weights": weights, "returns": returns})
        return captures

    def test_overlay_summary_shape(self):
        base = _fake_summary("A", "lgbm_21d", 0.7)
        base.update({"model": "lgbm", "horizon": 21, "reg": 0.1})
        captures = self._captures()
        summary = pilot.apply_vol_target_overlay("A", base, captures)
        assert summary["label"] == "lgbm_21d_vt10"
        assert summary["overlay"] == "vol_target_10pct"
        assert summary["base_config"] == "lgbm_21d"
        assert summary["arm"] == "A"
        assert len(summary["per_fold_oos_sharpe"]) == 2
        assert summary["n_obs"] == 80
        for key in ("sharpe", "skewness", "kurtosis", "psr_vs_zero"):
            assert key in summary

    def test_overlay_requires_captured_folds(self):
        base = _fake_summary("A", "lgbm_21d", 0.7)
        with pytest.raises(ValueError, match="captured folds"):
            pilot.apply_vol_target_overlay("A", base, [])

    def test_overlay_never_amplifies_exposure(self):
        # High-vol synthetic returns (30%+ annualized) with a 10% target:
        # scaled gross exposure must be <= the raw 1.0 book everywhere after
        # the vol window warms up, so |overlay returns| shrink vs base.
        captures = self._captures(n_folds=1, n_bars=60)
        base = _fake_summary("A", "lgbm_21d", 0.7)
        base.update({"model": "lgbm", "horizon": 21, "reg": 0.1})
        weights, returns = captures[0]["weights"], captures[0]["returns"]
        from research.experiments.spec import PortfolioRiskConfig
        from research.portfolio.risk_layer import apply_risk_layer_batch
        scaled = apply_risk_layer_batch(weights, returns, None, PortfolioRiskConfig(vol_target=0.10))
        assert (scaled.values <= weights.values + 1e-12).all()
        assert scaled.values[30:].sum() < weights.values[30:].sum()  # actually scaled down


# ---------------------------------------------------------------------------
# Universe config
# ---------------------------------------------------------------------------

class TestUniverse:
    def test_liquid50_has_fifty_symbols_and_no_spy(self):
        symbols = pilot.load_universe()
        assert len(symbols) == 50
        assert len(set(symbols)) == 50
        assert "SPY" not in symbols  # ETFs excluded; SPY is sidecar-only
