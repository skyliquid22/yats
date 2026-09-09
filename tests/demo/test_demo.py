"""Demo pipeline tests: bundle schema, size budget, and end-to-end determinism."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
for _p in (str(ROOT), str(ROOT / "pipelines")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from demo.generate_data import (  # noqa: E402
    SYMBOLS,
    generate_panel,
    panel_content_hash,
    write_bundle,
)
from demo.run_demo import DEMO_GRID, run_demo  # noqa: E402

SWEEP_V1_FEATURES = [
    "ret_1d", "ret_5d", "ret_21d", "rv_21d", "rv_63d",
    "dist_20d_high", "dist_20d_low",
    "mom_3m", "log_mkt_cap", "size_rank", "value_rank",
    "pe_ttm", "ps_ttm", "pb", "ev_ebitda", "roe",
    "gross_margin", "operating_margin", "fcf_margin",
    "debt_equity", "eps_growth_1y", "revenue_growth_1y",
    "market_vol_20d", "market_trend_20d", "dispersion_20d", "corr_mean_20d",
    "atm_iv", "skew_25d", "iv_term_slope", "put_call_oi_ratio",
    "net_gamma_exposure",
]


def test_generator_is_deterministic_and_seed_sensitive():
    p1 = generate_panel(seed=42, n_days=140)
    p2 = generate_panel(seed=42, n_days=140)
    p3 = generate_panel(seed=7, n_days=140)
    assert panel_content_hash(p1) == panel_content_hash(p2)
    assert panel_content_hash(p1) != panel_content_hash(p3)


def test_bundle_schema_and_size(tmp_path):
    out = write_bundle(tmp_path / "panel.parquet", seed=42)
    assert out.exists()
    assert out.stat().st_size < 5_000_000, "bundle must stay under the 5 MB budget"

    import pandas as pd

    panel = pd.read_parquet(out)
    for col in ["date", "symbol", "open", "close", *SWEEP_V1_FEATURES]:
        assert col in panel.columns, f"missing column: {col}"
    assert sorted(panel["symbol"].unique()) == sorted(SYMBOLS)
    assert "SPY" in SYMBOLS  # residualization anchor must exist
    # Regime structure present
    assert set(panel["regime"].unique()) == {"calm", "spike", "trend"}


def test_demo_verdict_is_deterministic(tmp_path):
    """Same bundle + same grid -> byte-identical verdict hash."""
    bundle = write_bundle(tmp_path / "panel.parquet", seed=42)
    s1 = run_demo(bundle, grid=DEMO_GRID)
    s2 = run_demo(bundle, grid=DEMO_GRID)

    assert s1["verdict_hash"] == s2["verdict_hash"]
    # Sanity: a coherent verdict was produced
    assert len(s1["configs"]) == len(DEMO_GRID)
    for cfg in s1["configs"]:
        assert cfg["n_obs"] > 50
        assert len(cfg["per_fold_oos_sharpe"]) >= 2
        assert 0.0 <= cfg["dsr"] <= 1.0
    assert 0.0 <= s1["rank_decay"] <= 1.0
    assert s1["best_label"] in {c["label"] for c in DEMO_GRID}
