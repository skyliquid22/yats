"""Registration pins for the SPAN-16 runner (no-DB structural tests)."""

import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture(scope="module")
def span16():
    spec = importlib.util.spec_from_file_location(
        "wfo_span16", ROOT / "research/scripts/wfo_span16.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["wfo_span16"] = mod
    spec.loader.exec_module(mod)
    return mod


class TestRegistrationPins:
    def test_clock_and_bar(self, span16):
        assert span16.N_TRIALS_DEFLATION == 88
        assert span16.CERTIFICATION_BAR == 0.95
        assert span16.TOTAL_TRIALS == 5

    def test_pool_is_35_from_run2_receipt(self, span16):
        pool = span16.registered_pool()
        assert len(pool) == 35
        r2 = json.loads(
            (ROOT / "docs/research/receipts/pit250_sweep_run2.json").read_text()
        )
        assert pool == pytest.approx(
            [float(x) for x in r2["strict_deflation"]["pool_sharpes"]]
        )

    def test_geometry_matches_prereg(self, span16):
        assert span16.TEST_WINDOW == 289
        assert span16.LABEL_PURGE_BUFFER == 253
        assert span16.MIN_TRAIN_BARS == 300
        assert span16.PCA_N_FACTORS == 5
        assert span16.PCA_WINDOW == 252

    def test_grid_is_2x2_clean(self, span16):
        labels = [c["label"] for c in span16.BASE_GRID]
        assert labels == [
            "lgbm_21d_clean_R", "lgbm_5d_clean_R",
            "lgbm_21d_clean_N", "lgbm_5d_clean_N",
        ]

    def test_clean_cols_exclude_insider_institutional(self, span16):
        cols = span16.clean_feature_cols()
        assert len(cols) == 27
        assert not any("insider" in c or "inst_" in c or "exec_" in c for c in cols)

    def test_receipt_path_is_span16(self, span16):
        assert str(span16.RECEIPT_PATH).endswith("docs/research/receipts/span16_sweep.json")
        assert str(span16.MEMBERSHIP_PARQUET).endswith("span16_membership.parquet")


class TestFoldRule:
    def _fold(self, idx, train_bars, returns):
        return SimpleNamespace(
            fold_index=idx, train_start=0, train_end=train_bars,
            oos_returns=returns, oos_sharpe=0.5 if returns else None,
        )

    def test_short_train_fold_excluded_from_primary(self, span16):
        good = [0.001] * 100
        result = SimpleNamespace(
            folds=[self._fold(0, 1, [0.0] * 50), self._fold(1, 299, good),
                   self._fold(2, 600, good)],
            concatenated_oos_returns=[0.0] * 50 + good + good,
            per_fold_oos_sharpe=[0.0, 1.0, 1.0],
            median_oos_sharpe=1.0,
        )
        summary = span16.summarize_with_fold_rule(
            {"label": "t", "model": "lgbm", "horizon": 21, "reg": 0.1}, result
        )
        assert summary["n_obs_primary"] == 100
        assert [e["fold_index"] for e in summary["excluded_folds"]] == [0, 1]
        assert summary["sharpe_all_folds"] is not None

    def test_capture_index_mapping_skips_untrained(self, span16):
        result = SimpleNamespace(
            folds=[self._fold(0, 1, []),          # untrained -> no capture
                   self._fold(1, 299, [0.1]),      # capture 0, excluded
                   self._fold(2, 600, [0.1])],     # capture 1, included
        )
        assert span16.included_capture_indices(result) == [1]
