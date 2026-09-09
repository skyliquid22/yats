"""Deterministic synthetic market data for the offline demo.

Generates ~2 years of daily bars for 10 fake-but-realistic symbols (9 stocks
plus a synthetic market index named "SPY" so the sweep's SPY-residualization
works unchanged), with explicit regime structure:

    calm   [first ~45%]: low vol, mild positive drift
    spike  [next ~15%] : high vol, negative drift, correlations up
    trend  [last ~40%] : steady uptrend, moderate vol

Every feature column of configs/feature_sets/sweep_v1.yml is produced.
Price-derived features (returns, realized vol, 20d high/low distance,
momentum) are computed from the generated closes exactly as defined; slower
"fundamental" columns are quarterly-stepped series; options columns follow
the vol regime. A small persistent cross-sectional alpha is embedded in
returns and partially reflected in the quality fundamentals (roe,
eps_growth_1y, revenue_growth_1y, fcf_margin) so the demo models have a real
— but modest — signal to find.

No network, no database, no vendor keys. Same seed -> identical bundle.

Usage:
    PYTHONPATH=.:pipelines uv run python demo/generate_data.py [--seed 42] [--force]
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

STOCKS = ["ARBRK", "BLTZR", "CNDRA", "DRFTN", "EMBRX", "FLXON", "GRPHL", "HLIOS", "NMBUS"]
MARKET = "SPY"  # synthetic market index proxy; name kept for the residualizer
SYMBOLS = STOCKS + [MARKET]

N_DAYS = 522  # ~2 years of business days
START_DATE = "2024-01-02"
DEFAULT_SEED = 42

DEFAULT_OUT = Path(__file__).resolve().parent / "data" / "demo_panel.parquet"

# Regime schedule as fractions of the sample: (name, start_frac, end_frac, params)
REGIMES = [
    ("calm",  0.00, 0.45, dict(mkt_mu=0.00035, mkt_sigma=0.0060, idio_sigma=0.0110)),
    ("spike", 0.45, 0.60, dict(mkt_mu=-0.0018, mkt_sigma=0.0260, idio_sigma=0.0230)),
    ("trend", 0.60, 1.00, dict(mkt_mu=0.00095, mkt_sigma=0.0105, idio_sigma=0.0130)),
]

ANNUALIZE = np.sqrt(252.0)


def _regime_arrays(n_days: int) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Per-day market drift, market vol, idio vol, and regime label."""
    mkt_mu = np.empty(n_days)
    mkt_sigma = np.empty(n_days)
    idio_sigma = np.empty(n_days)
    label = np.empty(n_days, dtype=object)
    for name, lo, hi, p in REGIMES:
        i0, i1 = int(lo * n_days), int(hi * n_days)
        mkt_mu[i0:i1] = p["mkt_mu"]
        mkt_sigma[i0:i1] = p["mkt_sigma"]
        idio_sigma[i0:i1] = p["idio_sigma"]
        label[i0:i1] = name
    return mkt_mu, mkt_sigma, idio_sigma, label


def _quarterly_step(values_per_quarter: np.ndarray, n_days: int) -> np.ndarray:
    """Expand one value per quarter (63 trading days) to a daily series."""
    daily = np.repeat(values_per_quarter, 63, axis=0)
    return daily[:n_days]


def generate_panel(seed: int = DEFAULT_SEED, n_days: int = N_DAYS) -> pd.DataFrame:
    """Build the long (date, symbol) panel. Fully deterministic given seed."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(START_DATE, periods=n_days)
    n_stk = len(STOCKS)
    n_qtr = n_days // 63 + 2

    mkt_mu, mkt_sigma, idio_sigma, regime = _regime_arrays(n_days)

    # --- return generation -------------------------------------------------
    r_mkt = rng.normal(mkt_mu, mkt_sigma)

    beta = rng.uniform(0.7, 1.5, n_stk)
    # Persistent latent alpha per stock (AR(1), ~10%/yr scale at peak).
    alpha = np.empty((n_days, n_stk))
    a = rng.normal(0.0, 4e-4, n_stk)
    for t in range(n_days):
        a = 0.995 * a + rng.normal(0.0, 5e-5, n_stk)
        alpha[t] = a
    idio = rng.standard_normal((n_days, n_stk)) * idio_sigma[:, None]
    r_stk = alpha + beta[None, :] * r_mkt[:, None] + idio

    # --- prices ------------------------------------------------------------
    base_px = rng.uniform(25.0, 380.0, n_stk)
    close_stk = base_px[None, :] * np.cumprod(1.0 + r_stk, axis=0)
    close_mkt = 400.0 * np.cumprod(1.0 + r_mkt)
    close = pd.DataFrame(
        np.column_stack([close_stk, close_mkt]), index=dates, columns=SYMBOLS
    )

    gap = rng.standard_normal((n_days, n_stk + 1)) * 0.15 * np.column_stack(
        [idio_sigma[:, None].repeat(n_stk, axis=1), mkt_sigma]
    )
    open_ = close.shift(1) * (1.0 + gap)
    open_.iloc[0] = close.iloc[0] / (1.0 + np.abs(gap[0]) + 0.001)

    volume = pd.DataFrame(
        np.exp(rng.normal(15.0, 0.4, (n_days, n_stk + 1)))
        * (1.0 + 1.5 * (regime == "spike")[:, None]),
        index=dates, columns=SYMBOLS,
    ).round(0)

    # --- price-derived features (exact definitions) ------------------------
    ret_1d = close.pct_change()
    feats: dict[str, pd.DataFrame] = {
        "ret_1d": ret_1d,
        "ret_5d": close.pct_change(5),
        "ret_21d": close.pct_change(21),
        "rv_21d": ret_1d.rolling(21).std() * ANNUALIZE,
        "rv_63d": ret_1d.rolling(63).std() * ANNUALIZE,
        "dist_20d_high": close / close.rolling(20).max() - 1.0,
        "dist_20d_low": close / close.rolling(20).min() - 1.0,
        "mom_3m": close.pct_change(63),
    }

    # --- size / value ------------------------------------------------------
    shares = rng.uniform(5e7, 4e9, n_stk)
    mkt_cap = close.copy()
    mkt_cap[STOCKS] = close[STOCKS] * shares
    mkt_cap[MARKET] = 5.2e11  # notional index "cap" (constant)
    feats["log_mkt_cap"] = np.log(mkt_cap)
    feats["size_rank"] = mkt_cap.rank(axis=1, pct=True)

    # --- fundamentals (quarterly stepped, ETFs = NaN) ----------------------
    quality = alpha / 4e-4  # normalized latent quality, links alpha -> fundamentals
    q_quality = np.stack([quality[np.minimum(q * 63, n_days - 1)] for q in range(n_qtr)])

    def fundamental(anchor_lo, anchor_hi, load, noise_sd):
        anchor = rng.uniform(anchor_lo, anchor_hi, n_stk)
        per_q = anchor[None, :] + load * q_quality + rng.normal(0, noise_sd, (n_qtr, n_stk))
        daily = _quarterly_step(per_q, n_days)
        df = pd.DataFrame(np.full((n_days, n_stk + 1), np.nan), index=dates, columns=SYMBOLS)
        df[STOCKS] = daily
        return df

    eps0 = base_px / rng.uniform(14.0, 45.0, n_stk)  # implied starting EPS
    eps_growth = fundamental(0.04, 0.14, 0.10, 0.02)
    q_growth = np.clip(
        rng.normal(0.02, 0.01, (n_qtr, n_stk)) + 0.02 * q_quality, -0.15, 0.20
    )
    eps_per_q = eps0[None, :] * np.cumprod(1.0 + q_growth, axis=0)
    eps_ttm = pd.DataFrame(np.full((n_days, n_stk + 1), np.nan), index=dates, columns=SYMBOLS)
    eps_ttm[STOCKS] = _quarterly_step(eps_per_q, n_days)

    pe = close / eps_ttm
    feats["pe_ttm"] = pe
    feats["ps_ttm"] = pe * _spread(rng, n_stk, dates, 0.15, 0.6)
    feats["pb"] = pe * _spread(rng, n_stk, dates, 0.10, 0.35)
    feats["ev_ebitda"] = pe * _spread(rng, n_stk, dates, 0.4, 0.9)
    feats["roe"] = fundamental(0.08, 0.24, 0.05, 0.01)
    feats["gross_margin"] = fundamental(0.30, 0.62, 0.02, 0.008)
    feats["operating_margin"] = fundamental(0.10, 0.30, 0.03, 0.008)
    feats["fcf_margin"] = fundamental(0.05, 0.22, 0.04, 0.008)
    feats["debt_equity"] = fundamental(0.2, 1.6, -0.05, 0.03).clip(lower=0.01)
    feats["eps_growth_1y"] = eps_growth
    feats["revenue_growth_1y"] = fundamental(0.03, 0.12, 0.06, 0.015)
    earnings_yield = 1.0 / pe
    feats["value_rank"] = earnings_yield.rank(axis=1, pct=True)

    # --- options surface (vol-regime linked) -------------------------------
    rv20 = ret_1d.rolling(20, min_periods=5).std() * ANNUALIZE
    iv_premium = pd.DataFrame(
        1.10 + 0.05 * rng.standard_normal((n_days, n_stk + 1))
        + 0.25 * (regime == "spike")[:, None],
        index=dates, columns=SYMBOLS,
    )
    feats["atm_iv"] = (rv20 * iv_premium + 0.02).clip(lower=0.05)
    spike_col = (regime == "spike")[:, None].astype(float)
    feats["skew_25d"] = pd.DataFrame(
        -0.04 - 0.06 * spike_col + 0.012 * rng.standard_normal((n_days, n_stk + 1)),
        index=dates, columns=SYMBOLS)
    feats["iv_term_slope"] = pd.DataFrame(
        0.012 - 0.035 * spike_col + 0.006 * rng.standard_normal((n_days, n_stk + 1)),
        index=dates, columns=SYMBOLS)
    feats["put_call_oi_ratio"] = pd.DataFrame(
        0.9 + 0.45 * spike_col + 0.15 * rng.standard_normal((n_days, n_stk + 1)),
        index=dates, columns=SYMBOLS).clip(lower=0.1)
    feats["net_gamma_exposure"] = pd.DataFrame(
        (1.0 - 2.2 * spike_col + 0.8 * rng.standard_normal((n_days, n_stk + 1))) * 1e8,
        index=dates, columns=SYMBOLS)

    # --- market regime features (same value for every symbol on a date) ----
    r_mkt_s = pd.Series(r_mkt, index=dates)
    cross_std = pd.DataFrame(r_stk, index=dates, columns=STOCKS).std(axis=1)
    regime_feats = {
        "market_vol_20d": r_mkt_s.rolling(20).std() * ANNUALIZE,
        "market_trend_20d": r_mkt_s.rolling(20).mean() * 252.0,
        "dispersion_20d": cross_std.rolling(20).mean(),
        "corr_mean_20d": (1.0 / (1.0 + (cross_std / r_mkt_s.rolling(20).std().clip(lower=1e-6)) ** 2)).rolling(20).mean(),
    }
    for name, series in regime_feats.items():
        feats[name] = pd.DataFrame(
            np.repeat(series.values[:, None], n_stk + 1, axis=1),
            index=dates, columns=SYMBOLS)

    # --- assemble long panel ----------------------------------------------
    wide = {"open": open_, "close": close, "volume": volume, **feats}
    long_parts = {}
    for name, df in wide.items():
        s = df.stack()
        s.index.names = ["date", "symbol"]
        long_parts[name] = s
    panel = pd.DataFrame(long_parts).reset_index()
    panel["regime"] = panel["date"].map(pd.Series(regime, index=dates))
    panel = panel.sort_values(["date", "symbol"]).reset_index(drop=True)
    return panel


def _spread(rng: np.random.Generator, n_stk: int, dates, lo: float, hi: float) -> pd.DataFrame:
    """Per-stock constant multiplier (NaN for the index) as a wide frame."""
    mult = rng.uniform(lo, hi, n_stk)
    df = pd.DataFrame(
        np.full((len(dates), n_stk + 1), np.nan), index=dates, columns=SYMBOLS
    )
    df[STOCKS] = np.broadcast_to(mult, (len(dates), n_stk))
    return df


def panel_content_hash(panel: pd.DataFrame) -> str:
    """Stable content hash of the panel (column order + values, 10 sig figs)."""
    csv_bytes = panel.to_csv(index=False, float_format="%.10g").encode()
    return hashlib.sha256(csv_bytes).hexdigest()


def write_bundle(out_path: Path = DEFAULT_OUT, seed: int = DEFAULT_SEED,
                 n_days: int = N_DAYS) -> Path:
    """Generate the panel and write parquet bundle + sidecar metadata."""
    panel = generate_panel(seed=seed, n_days=n_days)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(out_path, index=False)
    meta = {
        "seed": seed,
        "n_days": n_days,
        "symbols": SYMBOLS,
        "rows": len(panel),
        "start": str(panel["date"].min()),
        "end": str(panel["date"].max()),
        "content_sha256": panel_content_hash(panel),
        "regimes": [(name, lo, hi) for name, lo, hi, _ in REGIMES],
    }
    out_path.with_suffix(".meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    size_mb = out_path.stat().st_size / 1e6
    print(f"[DEMO-DATA] wrote {out_path} ({len(panel)} rows, {size_mb:.2f} MB, "
          f"seed={seed}, sha256={meta['content_sha256'][:12]}...)")
    if size_mb > 5:
        raise RuntimeError(f"bundle exceeds 5 MB budget: {size_mb:.2f} MB")
    return out_path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--n-days", type=int, default=N_DAYS)
    ap.add_argument("--force", action="store_true", help="overwrite existing bundle")
    args = ap.parse_args()
    if args.out.exists() and not args.force:
        print(f"[DEMO-DATA] {args.out} already exists (use --force to regenerate)")
        return 0
    write_bundle(args.out, seed=args.seed, n_days=args.n_days)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
