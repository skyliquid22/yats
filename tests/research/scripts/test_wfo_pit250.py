"""Tests for research/scripts/wfo_pit250.py (pre-registered pit250 runner).

Everything DB-side is out of scope: only pure functions are exercised — no
QuestDB connection is opened anywhere in this module. The membership module
(research/universe/membership.py, built in a parallel branch) is MOCKED by
its interface contract: symbols_as_of(date, membership=None),
all_symbols(membership=None), is_member(symbol, date); membership parquet
at configs/universes/pit250_membership.parquet.
"""
from __future__ import annotations

import importlib.util
import json
import sys
import types
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


pit = _load_module("wfo_pit250_under_test", "research/scripts/wfo_pit250.py")

from compute.stats.deflated_sharpe import _expected_max_sr_benchmark  # noqa: E402

INSIDER_COLS = [
    "insider_net_buy_90d", "insider_buy_intensity_30d",
    "insider_cluster_30d", "exec_net_buy_90d",
]
INSTITUTIONAL_COLS = ["inst_ownership_pct", "inst_top10_share"]


# ---------------------------------------------------------------------------
# Grid: exactly 5 pre-registered trials, pinned geometry
# ---------------------------------------------------------------------------

class TestGrid:
    def test_total_trials_is_five(self):
        assert pit.TOTAL_TRIALS == 5
        assert len(pit.BASE_GRID) == 3
        assert pit.N_OVERLAYS == len(pit.OVERLAYS) == 2

    def test_base_grid_labels_variants_and_regularization(self):
        labels = [c["label"] for c in pit.BASE_GRID]
        assert labels == ["lgbm_21d", "lgbm_5d", "lgbm_21d_no_insider"]
        for cfg in pit.BASE_GRID:
            assert cfg["model"] == "lgbm"       # ridge dropped per prereg
            assert cfg["reg"] == 0.1            # incumbent lambda only
        by_label = {c["label"]: c for c in pit.BASE_GRID}
        # Insider-inclusive is the DEFAULT (pilot evidence); the excluded
        # variant exists only as the lgbm_21d attribution control.
        assert by_label["lgbm_21d"]["variant"] == "insider_inclusive"
        assert by_label["lgbm_5d"]["variant"] == "insider_inclusive"
        assert by_label["lgbm_21d_no_insider"]["variant"] == "control"
        assert by_label["lgbm_21d"]["horizon"] == 21
        assert by_label["lgbm_5d"]["horizon"] == 5
        assert by_label["lgbm_21d_no_insider"]["horizon"] == 21

    def test_overlay_identifiers(self):
        assert pit.OVERLAYS == ("vol_target_10pct", "regime_conditioned_vol_target")

    def test_geometry_matches_prereg(self):
        assert pit.WFO_CFG.mode == "anchored"
        assert pit.WFO_CFG.n_periods == 4
        assert pit.WFO_CFG.train_window == 250
        assert pit.WFO_CFG.label_horizon == 21
        assert pit.WFO_CFG.purge_buffer == 253
        assert pit.EXECUTION_LAG == 1
        assert pit.FILL_TIMING == "next_close"
        assert pit.COST_BP == 5.0
        assert pit.VOL_TARGET == 0.10

    def test_regime_conditioning_uses_dataclass_defaults(self):
        rc = pit.RC_CONFIG
        assert rc.enabled is True
        assert rc.feature == "spy_iv_zscore_60d"
        assert rc.low_target == 0.05
        assert rc.high_target == 0.15
        assert rc.zscore_lo == -1.0
        assert rc.zscore_hi == 1.0

    def test_paths(self):
        assert str(pit.MEMBERSHIP_PARQUET).endswith(
            "configs/universes/pit250_membership.parquet"
        )
        assert str(pit.RECEIPT_PATH).endswith(
            "docs/research/receipts/pit250_sweep_run2.json"
        )


# ---------------------------------------------------------------------------
# Membership interface: mocked by the contract
# ---------------------------------------------------------------------------

class TestMembershipContract:
    def test_load_membership_imports_the_contract_module(self, monkeypatch):
        fake = types.ModuleType("research.universe.membership")
        fake.symbols_as_of = lambda date, membership=None: ["AAA"]
        fake.all_symbols = lambda membership=None: ["AAA", "BBB"]
        fake.is_member = lambda symbol, date: symbol == "AAA"
        pkg = types.ModuleType("research.universe")
        pkg.membership = fake
        monkeypatch.setitem(sys.modules, "research.universe", pkg)
        monkeypatch.setitem(sys.modules, "research.universe.membership", fake)

        mod = pit.load_membership()
        assert mod.all_symbols() == ["AAA", "BBB"]
        assert mod.symbols_as_of("2024-01-02") == ["AAA"]
        assert mod.is_member("AAA", "2024-01-02") is True
        assert mod.is_member("BBB", "2024-01-02") is False


# ---------------------------------------------------------------------------
# Column variants (derived from breadth_v1 groups via the pilot machinery)
# ---------------------------------------------------------------------------

class TestVariantColumns:
    def test_insider_inclusive_contains_the_block(self):
        cols = pit.variant_feature_cols()[pit.VARIANT_INSIDER_INCLUSIVE]
        for col in INSIDER_COLS + INSTITUTIONAL_COLS:
            assert col in cols

    def test_control_excludes_exactly_the_block(self):
        variants = pit.variant_feature_cols()
        inclusive = set(variants[pit.VARIANT_INSIDER_INCLUSIVE])
        control = set(variants[pit.VARIANT_CONTROL])
        assert inclusive - control == set(INSIDER_COLS + INSTITUTIONAL_COLS)
        assert control <= inclusive

    def test_synthetic_groups(self):
        groups = {
            "ohlcv": ["r1"], "insider": ["i1"],
            "institutional": ["n1"], "regime": ["g1"],
        }
        variants = pit.variant_feature_cols(groups)
        assert variants["insider_inclusive"] == ["r1", "i1", "n1", "g1"]
        assert variants["control"] == ["r1", "g1"]


# ---------------------------------------------------------------------------
# PIT eligibility in panel construction — synthetic membership fixture
# ---------------------------------------------------------------------------

N_DATES = 60
ENTRY_BBB = 10       # BBB is not a member before dates[10]
EXIT_CCC = 20        # CCC exits membership at dates[20] (member through 19)
REENTRY_CCC = 40     # ... and re-enters at dates[40]


def _synthetic_frames(feature_cols=("ret_1d", "mom_3m", "log_mkt_cap")):
    """Features + closes exist for ALL dates for all symbols — masking must
    be done by the membership rule, not by data absence."""
    rng = np.random.default_rng(17)
    dates = list(pd.bdate_range("2022-01-03", periods=N_DATES).date)
    rows_f, rows_p = [], []
    for sym in ("AAA", "BBB", "CCC"):
        price = 100.0
        for dt in dates:
            price *= 1.0 + rng.normal(0.0005, 0.01)
            rows_f.append({"date": dt, "symbol": sym,
                           **{c: rng.normal() for c in feature_cols}})
            rows_p.append({"date": dt, "symbol": sym,
                           "open": price * 0.999, "close": price})
    spy = 400.0
    for dt in dates:
        spy *= 1.0 + rng.normal(0.0003, 0.008)
        rows_p.append({"date": dt, "symbol": "SPY", "open": spy * 0.999, "close": spy})
    return pd.DataFrame(rows_f), pd.DataFrame(rows_p), dates


def _is_member_factory(dates):
    idx = {d: i for i, d in enumerate(dates)}

    def is_member(symbol, date):
        i = idx[date]
        if symbol == "AAA":
            return True
        if symbol == "BBB":
            return i >= ENTRY_BBB
        if symbol == "CCC":
            return i < EXIT_CCC or i >= REENTRY_CCC
        return False

    return is_member


@pytest.fixture(scope="module")
def pit_panel():
    features_df, closes_df, dates = _synthetic_frames()
    is_member = _is_member_factory(dates)
    panel = pit.build_pit_panel(
        features_df, closes_df, ["ret_1d", "mom_3m", "log_mkt_cap"], is_member
    )
    return panel, dates


class TestMembershipEligibility:
    def test_symbol_absent_before_entry(self, pit_panel):
        panel, dates = pit_panel
        bbb = panel[panel["symbol"] == "BBB"]
        # Data exists on every date; membership starts at dates[ENTRY_BBB].
        assert min(bbb["date"]) == dates[ENTRY_BBB]
        assert len(bbb) == N_DATES - ENTRY_BBB

    def test_symbol_absent_after_delist(self, pit_panel):
        panel, dates = pit_panel
        ccc_dates = set(panel.loc[panel["symbol"] == "CCC", "date"])
        gap = set(dates[EXIT_CCC:REENTRY_CCC])
        assert ccc_dates.isdisjoint(gap)
        assert dates[EXIT_CCC - 1] in ccc_dates       # last member date present
        assert dates[REENTRY_CCC] in ccc_dates        # re-entry present

    def test_cross_section_is_members_only_per_date(self, pit_panel):
        panel, dates = pit_panel
        by_date = panel.groupby("date")["symbol"].apply(set)
        assert by_date[dates[5]] == {"AAA", "CCC"}                # pre-BBB entry
        assert by_date[dates[25]] == {"AAA", "BBB"}               # CCC gap
        assert by_date[dates[45]] == {"AAA", "BBB", "CCC"}
        assert "SPY" not in set(panel["symbol"])                   # sidecar dropped

    def test_ranks_computed_over_members_only(self, pit_panel):
        panel, dates = pit_panel
        # Cross-sectional rank normalization ran AFTER masking: each date's
        # feature cross-section (>=2 members) is mean-zero over members.
        day = panel[panel["date"] == dates[5]]
        assert len(day) == 2
        assert abs(float(day["ret_1d"].mean())) < 1e-9

    def test_membership_spells_assigned(self, pit_panel):
        panel, _ = pit_panel
        assert "membership_spell" in panel.columns
        assert set(panel.loc[panel["symbol"] == "AAA", "membership_spell"]) == {1}
        assert set(panel.loc[panel["symbol"] == "BBB", "membership_spell"]) == {1}
        assert set(panel.loc[panel["symbol"] == "CCC", "membership_spell"]) == {1, 2}

    def test_unwind_at_last_available_close(self, pit_panel):
        panel, dates = pit_panel
        ccc = panel[panel["symbol"] == "CCC"].set_index("date")
        # The final in-spell bar has no next in-spell close: it must NOT be
        # credited the cross-gap return — NaN here, counted as 0.0 (cash
        # exit at last observed close) by portfolio construction.
        assert np.isnan(ccc.loc[dates[EXIT_CCC - 1], "ret_1d_realized"])
        # Inside the spell the realized return is intact.
        assert np.isfinite(ccc.loc[dates[EXIT_CCC - 3], "ret_1d_realized"])

    def test_targets_never_span_a_membership_gap(self, pit_panel):
        panel, dates = pit_panel
        ccc = panel[panel["symbol"] == "CCC"].set_index("date")
        # fwd_5d: rows whose +5th row (in the symbol's sequence) falls in
        # spell 2 are invalidated ...
        for i in range(EXIT_CCC - 5, EXIT_CCC):
            assert np.isnan(ccc.loc[dates[i], "fwd_5d"]), f"fwd_5d must be NaN at row {i}"
            assert np.isnan(ccc.loc[dates[i], "fwd_5d_resid"])
        # ... while a row fully inside spell 1 keeps its target.
        assert np.isfinite(ccc.loc[dates[EXIT_CCC - 6], "fwd_5d"])
        # Continuous-membership symbols are untouched by invalidation.
        aaa = panel[panel["symbol"] == "AAA"].set_index("date")
        assert np.isfinite(aaa.loc[dates[EXIT_CCC - 1], "ret_1d_realized"])
        assert np.isfinite(aaa.loc[dates[EXIT_CCC - 3], "fwd_5d"])

    def test_empty_membership_raises(self):
        features_df, closes_df, _ = _synthetic_frames()
        with pytest.raises(ValueError, match="member rows"):
            pit.build_pit_panel(
                features_df, closes_df, ["ret_1d", "mom_3m", "log_mkt_cap"],
                lambda symbol, date: False,
            )


# ---------------------------------------------------------------------------
# Fake per-config summaries (shape produced by summarize_config_result)
# ---------------------------------------------------------------------------

def _fake_summary(label, sharpe, variant="insider_inclusive", overlay=None):
    d = {
        "model": "lgbm",
        "horizon": 21,
        "reg": 0.1,
        "variant": variant,
        "label": label,
        "sharpe": sharpe,
        "skewness": -0.1,
        "kurtosis": 5.0,
        "n_obs": 860,
        "per_fold_oos_sharpe": [None, 0.4, 0.6, 0.3],
        "median_oos_sharpe": 0.4,
        "psr_vs_zero": 0.8,
    }
    if overlay:
        d["overlay"] = overlay
        d["base_config"] = "lgbm_21d"
    return d


def _fake_family():
    """3 base configs + 2 overlays = the 5-trial family."""
    return [
        _fake_summary("lgbm_21d", 0.9),
        _fake_summary("lgbm_5d", 0.7),
        _fake_summary("lgbm_21d_no_insider", 0.6, variant="control"),
        _fake_summary("lgbm_21d_vt10", 1.1, overlay=pit.OVERLAY_VT10),
        _fake_summary("lgbm_21d_rcvt", 1.0, overlay=pit.OVERLAY_RCVT),
    ]


# ---------------------------------------------------------------------------
# Strict deflation: run 2 (amendment 3) — SR0 uses the 83-trial benchmark
# ---------------------------------------------------------------------------

class TestStrictDeflation:
    def test_uses_83_trials(self):
        result = pit.strict_deflation(_fake_family())
        assert result["n_trials_emax"] == 83
        assert result["expected_max_sr"] == pytest.approx(_expected_max_sr_benchmark(83))

    def test_pool_is_registered_30_plus_the_5_new_trials(self):
        family = _fake_family()
        result = pit.strict_deflation(family)
        assert len(pit.HONEST_FILL_POOL_PRE_PIT250) == 30
        assert len(result["pool_sharpes"]) == 35
        for c in family:
            assert float(c["sharpe"]) in result["pool_sharpes"]

    def test_registered_pool_matches_run1_receipt(self):
        # first 25 = pilot receipt pool; last 5 = run 1's per-config sharpes
        receipt = json.loads(
            (ROOT / "docs/research/receipts/pit250_sweep.json").read_text()
        )
        assert pit.HONEST_FILL_POOL_PRE_PIT250[:25] == pytest.approx(
            receipt["strict_deflation"]["pool_sharpes"][:25]
        )
        run1 = [c["sharpe"] for c in receipt["strict_deflation"]["per_config"]]
        assert pit.HONEST_FILL_POOL_PRE_PIT250[25:] == pytest.approx(run1)

    def test_sr0_is_pool_std_times_emax_at_83(self):
        family = _fake_family()
        result = pit.strict_deflation(family)
        pool = pit.HONEST_FILL_POOL_PRE_PIT250 + [c["sharpe"] for c in family]
        expected_sr0 = float(np.std(pool)) * _expected_max_sr_benchmark(83)
        assert result["sr0"] == pytest.approx(expected_sr0)
        # Guard against deflating at the family size, the pool size, or the
        # stale pre-family clock instead of 78:
        for wrong_n in (5, len(pool), 73):
            assert result["sr0"] != pytest.approx(
                float(np.std(pool)) * _expected_max_sr_benchmark(wrong_n)
            )

    def test_per_config_verdicts_shape(self):
        result = pit.strict_deflation(_fake_family())
        assert len(result["per_config"]) == 5
        for c in result["per_config"]:
            assert set(c) == {"config", "sharpe", "strict_dsr", "certified"}
            assert 0.0 <= c["strict_dsr"] <= 1.0
            assert isinstance(c["certified"], bool)
        assert result["certification_bar"] == 0.95
        assert result["any_certified"] == any(c["certified"] for c in result["per_config"])


# ---------------------------------------------------------------------------
# Secondary readouts
# ---------------------------------------------------------------------------

class TestInsiderAttribution:
    def test_margin_is_inclusive_minus_control(self):
        out = pit.insider_attribution_margin(_fake_family())
        assert out["sharpe_insider_inclusive"] == pytest.approx(0.9)
        assert out["sharpe_control"] == pytest.approx(0.6)
        assert out["margin"] == pytest.approx(0.3)
        assert out["gradient_reference"]["margin_50_names_pilot_mean"] == 0.10

    def test_overlays_do_not_leak_into_the_margin(self):
        # The vt10/rcvt overlays sit on lgbm_21d with higher sharpes; the
        # margin must come from the BASE configs only.
        out = pit.insider_attribution_margin(_fake_family())
        assert out["sharpe_insider_inclusive"] != pytest.approx(1.1)

    def test_missing_config_yields_none(self):
        out = pit.insider_attribution_margin([_fake_summary("lgbm_21d", 0.9)])
        assert out["margin"] is None


class TestCapTercileReadout:
    def _setup(self, with_cap=True):
        rng = np.random.default_rng(5)
        dates = list(pd.bdate_range("2023-01-02", periods=30).date)
        symbols = [f"S{i}" for i in range(6)]
        rows = []
        for dt in dates:
            for j, sym in enumerate(symbols):
                row = {"date": dt, "symbol": sym, "ret_1d_realized": rng.normal(0, 0.01)}
                if with_cap:
                    row["log_mkt_cap"] = float(j)  # S0..S1 small, S2..S3 mid, S4..S5 large
                rows.append(row)
        panel = pd.DataFrame(rows)
        weights = pd.DataFrame(1.0 / 6.0, index=dates, columns=symbols)
        returns = pd.DataFrame(
            rng.normal(0.0005, 0.01, size=(len(dates), 6)), index=dates, columns=symbols
        )
        return [{"weights": weights, "returns": returns}], panel

    def test_tercile_keys_and_weight_shares(self):
        captures, panel = self._setup()
        out = pit.cap_tercile_readout(captures, panel)
        assert set(out) == {"small", "mid", "large"}
        for name in ("small", "mid", "large"):
            # 2 of 6 equal-weight symbols per tercile
            assert out[name]["mean_weight_share"] == pytest.approx(1.0 / 3.0)
            assert isinstance(out[name]["contribution_sharpe"], float)

    def test_returns_none_when_market_cap_unavailable(self):
        captures, panel = self._setup(with_cap=False)
        assert pit.cap_tercile_readout(captures, panel) is None


class TestCoverageReadout:
    def test_splits_by_span_end_membership(self):
        groups = {"insider": ["i1"], "institutional": ["n1"], "fundamental": ["f1"]}
        dates = list(pd.bdate_range("2023-01-02", periods=4).date)
        rows = []
        for dt in dates:
            rows.append({"date": dt, "symbol": "LIVE", "i1": 1.0, "n1": 1.0, "f1": 1.0})
            rows.append({"date": dt, "symbol": "DEAD", "i1": np.nan, "n1": 1.0, "f1": 1.0})
        df = pd.DataFrame(rows)
        out = pit.coverage_readout(
            df, groups, lambda s, d: s == "LIVE", dates[-1]
        )
        assert out["active_at_span_end"]["n_symbols"] == 1
        assert out["inactive_at_span_end"]["n_symbols"] == 1
        assert out["active_at_span_end"]["insider"]["share_all_non_null"] == 1.0
        # Delisted-name insider coverage absent -> reported, not hidden.
        assert out["inactive_at_span_end"]["insider"]["share_all_non_null"] == 0.0
        assert out["inactive_at_span_end"]["fundamental"]["share_all_non_null"] == 1.0


# ---------------------------------------------------------------------------
# Overlays
# ---------------------------------------------------------------------------

def _overlay_setup(n_bars=60, daily_vol=0.005):
    rng = np.random.default_rng(3)
    idx = list(pd.bdate_range("2023-01-02", periods=n_bars).date)
    weights = pd.DataFrame(0.5, index=idx, columns=["X", "Y"])
    returns = pd.DataFrame(
        rng.normal(0.0005, daily_vol, size=(n_bars, 2)), index=idx, columns=["X", "Y"]
    )
    base = _fake_summary("lgbm_21d", 0.9)
    return base, [{"weights": weights, "returns": returns}], idx


class TestOverlays:
    def test_vt10_overlay_labeling(self):
        base, captures, _ = _overlay_setup()
        out = pit.apply_vt10_overlay(base, captures)
        assert out["label"] == "lgbm_21d_vt10"
        assert out["overlay"] == "vol_target_10pct"
        assert out["base_config"] == "lgbm_21d"
        assert out["variant"] == "insider_inclusive"
        for key in ("sharpe", "skewness", "kurtosis", "n_obs", "per_fold_oos_sharpe"):
            assert key in out

    def test_rcvt_overlay_labeling(self):
        base, captures, idx = _overlay_setup()
        regime = pd.Series(2.0, index=idx)  # stressed throughout
        out = pit.apply_rcvt_overlay(base, captures, regime)
        assert out["label"] == "lgbm_21d_rcvt"
        assert out["overlay"] == "regime_conditioned_vol_target"
        assert out["base_config"] == "lgbm_21d"

    def test_rcvt_requires_regime_series(self):
        base, captures, _ = _overlay_setup()
        with pytest.raises(ValueError, match="regime series"):
            pit.apply_rcvt_overlay(base, captures, pd.Series(dtype=float))
        with pytest.raises(ValueError, match="regime series"):
            pit.apply_rcvt_overlay(base, captures, None)

    def test_rcvt_with_no_readings_falls_back_to_vt10(self):
        # All-NaN regime values -> the risk layer's documented fallback to
        # the unconditioned 10% target: bit-identical to the vt10 overlay.
        base, captures, idx = _overlay_setup()
        nan_regime = pd.Series(np.nan, index=idx)
        vt = pit.apply_vt10_overlay(base, captures)
        rc = pit.apply_rcvt_overlay(base, captures, nan_regime)
        assert rc["sharpe"] == pytest.approx(vt["sharpe"])
        assert rc["n_obs"] == vt["n_obs"]
        assert rc["per_fold_oos_sharpe"] == pytest.approx(vt["per_fold_oos_sharpe"])

    def test_rcvt_conditioning_changes_the_result(self):
        # ~8% annualized vol: vt10 is often unbinding (scale clipped at 1)
        # while the stressed 5% target binds -> results must differ.
        base, captures, idx = _overlay_setup(daily_vol=0.005)
        stressed = pd.Series(2.0, index=idx)
        vt = pit.apply_vt10_overlay(base, captures)
        rc = pit.apply_rcvt_overlay(base, captures, stressed)
        assert rc["sharpe"] != pytest.approx(vt["sharpe"])

    def test_overlay_requires_captured_folds(self):
        base, _, _ = _overlay_setup()
        with pytest.raises(ValueError, match="captured folds"):
            pit.apply_vt10_overlay(base, [])


# ---------------------------------------------------------------------------
# Receipt shape
# ---------------------------------------------------------------------------

class TestReceipt:
    def _build(self):
        family = _fake_family()
        deflation = pit.strict_deflation(family)
        return pit.build_receipt(
            family, deflation,
            insider_attribution=pit.insider_attribution_margin(family),
            cap_terciles={"lgbm_21d": None},
            coverage={"active_at_span_end": {}, "inactive_at_span_end": {}},
            symbols=[f"S{i}" for i in range(250)],
            variant_cols={"insider_inclusive": ["ret_1d", "insider_net_buy_90d"],
                          "control": ["ret_1d"]},
            rank_decay=0.4,
            span_end=None,
        )

    def test_receipt_shape(self):
        receipt = self._build()
        for key in (
            "sweep", "preregistration", "universe", "membership_parquet",
            "membership_interface", "pit_eligibility_rule", "feature_set",
            "variant_feature_cols", "span", "wfo", "execution",
            "trials_charged", "deflation_clock", "configs",
            "strict_deflation", "insider_attribution", "cap_tercile_readout",
            "delisted_coverage", "rank_decay", "regime_overlay",
        ):
            assert key in receipt, f"receipt missing {key}"
        assert receipt["sweep"] == "pit250_sweep"
        assert receipt["universe"] == "pit250"
        assert receipt["membership_parquet"] == "configs/universes/pit250_membership.parquet"
        assert receipt["feature_set"] == "breadth_v1"
        assert receipt["trials_charged"] == 5
        assert receipt["deflation_clock"] == 83
        assert receipt["preregistration"].endswith("2026-09-11_pit250_sweep.md")
        assert "is_member" in receipt["pit_eligibility_rule"]
        assert "last available close" in receipt["pit_eligibility_rule"]

    def test_receipt_execution_and_wfo_pinned(self):
        receipt = self._build()
        assert receipt["execution"] == {
            "execution_lag_days": 1,
            "fill_timing": "next_close",
            "transaction_cost_bp": 5.0,
        }
        assert receipt["wfo"] == {
            "mode": "anchored", "n_periods": 4, "train_window": 250,
            "label_horizon": 21, "purge_buffer": 253,
        }
        assert receipt["regime_overlay"] == {
            "feature": "spy_iv_zscore_60d",
            "low_target": 0.05, "high_target": 0.15,
            "zscore_lo": -1.0, "zscore_hi": 1.0,
        }

    def test_receipt_configs_carry_required_fields(self):
        receipt = self._build()
        assert len(receipt["configs"]) == 5
        for c in receipt["configs"]:
            for key in ("label", "variant", "per_fold_oos_sharpe", "n_obs",
                        "skewness", "kurtosis", "sharpe"):
                assert key in c, f"config entry missing {key}"

    def test_receipt_is_json_serializable(self):
        receipt = self._build()
        parsed = json.loads(json.dumps(receipt, default=str))
        assert parsed["strict_deflation"]["n_trials_emax"] == 83
