"""pit250 point-in-time universe sweep — PRE-REGISTERED runner.

Implements docs/research/preregistrations/2026-09-11_pit250_sweep.md
EXACTLY. The full family is 5 trials and moves the deflation clock
73 -> 78. Do not add configs, ablations, horizons, or regularization
values here — the prereg forbids it.

Design (all committed in advance in the prereg):
    Universe:     configs/universes/pit250_membership.parquet, accessed
                  ONLY through research/universe/membership.py
                  (symbols_as_of / all_symbols / is_member — built in a
                  parallel branch; this runner is written against that
                  interface contract).
    Feature set:  breadth_v1 (same stored features as the liquid50 pilot).
    Variants:     insider_inclusive = full breadth_v1 (the DEFAULT — pilot
                  evidence: the insider/institutional block flipped from
                  -1.0 Sharpe at 10 mega-caps to +0.10 mean at 50 names);
                  control = breadth_v1 minus insider + institutional
                  groups (the pilot's arm-A definition), retained only as
                  the attribution control.
    Grid:         lgbm_21d, lgbm_5d (insider_inclusive) +
                  lgbm_21d_no_insider (control), lgbm lambda=0.1,
                  plus TWO overlays on the best base by pooled OOS Sharpe:
                  _vt10 (10% vol target, incumbent ALPHA-3 layer) and
                  _rcvt (regime-conditioned vol target,
                  RegimeConditioningConfig defaults, enabled=True — the
                  regime-conditioning layer's FIRST pre-registered trial,
                  motivated by the twice-replicated fold-4 finding in
                  wfo_sweep_3d / wfo_sweep_4b).
                  3 + 2 = 5 trials.
    Geometry:     anchored 4-fold WFO, train_window=250, label_horizon=21,
                  purge_buffer=253 — identical to the pilot.
    Execution:    execution_lag_days=1, fill_timing='next_close',
                  transaction_cost_bp=5. Honest-fill, no same-bar fills.
    Span:         2020-01 .. YATS_SWEEP_END (default: latest available).
    Deflation:    STRICT — SR0 = std(honest-fill pool INCLUDING these 5
                  trials; 25 + 5 = 30 values) x expected-max benchmark at
                  78 trials; per-config DSR = PSR(sharpe; SR0). Bar: 0.95.

PIT ELIGIBILITY RULE (prereg, binding): a symbol participates in
cross-sectional ranks, training rows, and portfolio construction only on
dates where is_member(symbol, date). The feature panel is masked to member
rows BEFORE rank normalization; positions unwind at the last available
close (the final close-to-close return credited to a symbol is the last
one computable strictly inside its membership spell; the bar with no next
in-spell close contributes zero — cash-equivalent exit at the last
observed close). Forward-return targets are computed within membership
spells only; rows whose h-bar-forward close falls outside the spell get
NaN targets and drop from training. Re-entry starts a new spell. SPY stays
a non-tradable residualization sidecar, exempt from the mask.

Model math is imported from the liquid50 pilot runner (which itself
imports research/scripts/wfo_sweep_alpha1.py) — zero duplicated math.

Run (operator only — spends the 5 pre-registered trials):
    OMP_NUM_THREADS=1 PYTHONPATH=.:pipelines uv run python research/scripts/wfo_pit250.py

Receipt: docs/research/receipts/pit250_sweep.json
All progress lines carry the [PIT250] prefix.
"""
from __future__ import annotations

import importlib.util
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger("wfo_pit250")

# Repo root, resolved relative to this file so fresh checkouts work anywhere.
RIG = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RIG))
sys.path.insert(0, str(RIG / "pipelines"))

from compute.stats.deflated_sharpe import (  # noqa: E402
    _expected_max_sr_benchmark,
    probabilistic_sharpe_ratio,
)
from research.eval.metrics import compute_sharpe, compute_turnover  # noqa: E402
from research.eval.wfo import compute_sweep_wfo_rank_decay, run_wfo  # noqa: E402
from research.experiments.spec import (  # noqa: E402
    PortfolioRiskConfig,
    RegimeConditioningConfig,
)
from research.portfolio.risk_layer import apply_risk_layer_batch  # noqa: E402


def _load_pilot():
    """Import the liquid50 pilot runner — panel/eval/overlay machinery reuse."""
    path = RIG / "research" / "scripts" / "wfo_pilot_liquid50.py"
    spec = importlib.util.spec_from_file_location("wfo_pilot_liquid50_pit250", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


pilot = _load_pilot()
alpha1 = pilot.alpha1

# ---------------------------------------------------------------------------
# Pre-registered constants (2026-09-11_pit250_sweep.md)
# ---------------------------------------------------------------------------

FEATURE_SET = "breadth_v1"
SPY = pilot.SPY  # residualization sidecar only — NOT tradable, mask-exempt

MEMBERSHIP_PARQUET = RIG / "configs" / "universes" / "pit250_membership.parquet"

# Column variants derived from the breadth_v1 YAML groups exactly as in the
# pilot: insider_inclusive == pilot arm B (full set, the DEFAULT now),
# control == pilot arm A (minus insider + institutional groups).
VARIANT_INSIDER_INCLUSIVE = "insider_inclusive"
VARIANT_CONTROL = "control"
_VARIANT_TO_PILOT_ARM = {VARIANT_INSIDER_INCLUSIVE: "B", VARIANT_CONTROL: "A"}

# The 3 base trials. lgbm only (ridge was ~flat under breadth in the pilot);
# incumbent regularization lambda=0.1. No other values may be run.
BASE_GRID = [
    {"model": "lgbm", "horizon": 21, "reg": 0.1, "label": "lgbm_21d",
     "variant": VARIANT_INSIDER_INCLUSIVE},
    {"model": "lgbm", "horizon": 5, "reg": 0.1, "label": "lgbm_5d",
     "variant": VARIANT_INSIDER_INCLUSIVE},
    {"model": "lgbm", "horizon": 21, "reg": 0.1, "label": "lgbm_21d_no_insider",
     "variant": VARIANT_CONTROL},
]

# Exactly two overlays, both on the best base by pooled OOS Sharpe.
OVERLAY_VT10 = "vol_target_10pct"
OVERLAY_RCVT = "regime_conditioned_vol_target"
OVERLAYS = (OVERLAY_VT10, OVERLAY_RCVT)
N_OVERLAYS = len(OVERLAYS)
TOTAL_TRIALS = len(BASE_GRID) + N_OVERLAYS  # == 5

VOL_TARGET = pilot.VOL_TARGET  # 0.10 — incumbent ALPHA-3 target
# Regime-conditioned vol target: RegimeConditioningConfig DEFAULTS (feature
# spy_iv_zscore_60d, high 0.15 calm / low 0.05 stressed, z band [-1, +1]),
# enabled=True — its first pre-registered trial. Bars with no admissible
# lagged regime reading fall back to the unconditioned 10% target (risk
# layer convention).
RC_CONFIG = RegimeConditioningConfig(enabled=True)
REGIME_FEATURE = RC_CONFIG.feature  # spy_iv_zscore_60d (regime_features_v2)

# WFO geometry + execution: pinned identical to the pilot (prereg
# "Geometry and execution"); referencing the pilot's constants means this
# runner cannot silently deviate from the shared registration lineage.
WFO_CFG = pilot.WFO_CFG                # anchored 4-fold, train 250, purge 253
EXECUTION_LAG = pilot.EXECUTION_LAG    # 1
FILL_TIMING = pilot.FILL_TIMING        # next_close
COST_BP = pilot.COST_BP                # 5.0
START = pilot.START                    # 2020-01-01 (YATS_SWEEP_START)
END = pilot.END                        # None -> latest (YATS_SWEEP_END)

OUT_DIR = RIG / ".yats_data" / "wfo_sweeps" / "pit250_sweep_run2"
RECEIPT_PATH = RIG / "docs" / "research" / "receipts" / "pit250_sweep_run2.json"
PREREG_PATH = "docs/research/preregistrations/2026-09-11_pit250_sweep.md"

# ---------------------------------------------------------------------------
# Strict deflation inputs (prereg "Success and failure criteria")
# ---------------------------------------------------------------------------

# Honest-fill result pool at registration time — exactly the 25 values in
# docs/research/receipts/pilot_liquid50.json (strict_deflation.pool_sharpes):
# 15 pre-pilot (8 PPO champion rerun + 6 ALPHA-1 + vt overlay) + the pilot's
# 10 trials. Pinned here; a test cross-checks against the receipt.
HONEST_FILL_POOL_PRE_PIT250 = [
    0.29, 0.402, 0.41, 0.417, 0.508, 0.527, 0.602, 0.756,   # PPO champion rerun (8)
    0.367, 0.585, 0.624, 0.635, 0.73, 0.905,                # ALPHA-1 supervised (6)
    1.1339,                                                 # vt overlay, trial 63
    0.6280655686607264, 0.6988651548365146,                 # pilot arm A bases
    0.6004885846665868, 0.6621917322412088,
    0.9584642062476496,                                     # pilot arm A vt10
    0.7488736319006262, 0.6301957271370994,                 # pilot arm B bases
    0.759441569409167, 0.8482920555098233,
    1.1570143680299465,                                     # pilot arm B vt10
    # pit250 run 1 (trials 74-78; insider panel data-defective, amendment 3):
    0.44651729263704965, 0.6510368481239657,
    0.4373738981065142,                                     # lgbm_21d, lgbm_5d, no_insider
    0.6886630187263603, 0.7238646138022323,                 # vt10, rcvt overlays
]

# Every DSR under this registration deflates at the POST-family clock.
# Run 2 (amendment 3): same 5 configs on the corrected insider/13F panel,
# charged as trials 79-83 on top of run 1's 74-78.
N_TRIALS_DEFLATION = 83
CERTIFICATION_BAR = 0.95

# References for the insider-attribution gradient readout (display only).
INSIDER_MARGIN_10N_SWEEP3D = -0.99   # mean B-A, 10 mega-caps (wfo_sweep_3d)
INSIDER_MARGIN_50N_PILOT = 0.10      # mean B-A, 50 large caps (pilot)


def mark(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[PIT250] {ts} {msg}", flush=True)


# ---------------------------------------------------------------------------
# Membership interface (parallel branch — imported lazily, mocked in tests)
# ---------------------------------------------------------------------------

def load_membership():
    """Import research/universe/membership.py (built in a parallel branch).

    Contract this runner is written against:
        symbols_as_of(date, membership=None) -> list[str]
        all_symbols(membership=None) -> list[str]
        is_member(symbol, date) -> bool
    Default membership table: configs/universes/pit250_membership.parquet.
    """
    from research.universe import membership
    return membership


# ---------------------------------------------------------------------------
# Column variants (derived from the breadth_v1 YAML groups via the pilot)
# ---------------------------------------------------------------------------

def variant_feature_cols(groups: dict[str, list[str]] | None = None) -> dict[str, list[str]]:
    """Model-input columns per variant, from the breadth_v1 group definitions."""
    if groups is None:
        groups = pilot.load_feature_groups()
    return {
        variant: pilot.arm_feature_cols(groups, arm)
        for variant, arm in _VARIANT_TO_PILOT_ARM.items()
    }


# ---------------------------------------------------------------------------
# PIT eligibility: membership mask, spells, and cross-spell invalidation
# ---------------------------------------------------------------------------

def eligibility_mask(df: pd.DataFrame, is_member_fn, exempt: tuple[str, ...] = (SPY,)) -> pd.Series:
    """Boolean Series (df.index): row (date, symbol) is PIT-eligible.

    A row is eligible iff is_member(symbol, date), except exempt symbols
    (the SPY residualization sidecar) which always pass. Results are
    memoized per (symbol, date) so membership backends are consulted once
    per pair.
    """
    cache: dict[tuple, bool] = {}

    def ok(sym, dt) -> bool:
        if sym in exempt:
            return True
        key = (sym, dt)
        if key not in cache:
            cache[key] = bool(is_member_fn(sym, dt))
        return cache[key]

    return pd.Series(
        [ok(s, d) for s, d in zip(df["symbol"], df["date"])],
        index=df.index,
    )


def assign_membership_spells(panel: pd.DataFrame, is_member_fn, all_dates: list) -> pd.Series:
    """Per-row membership spell id (int, 1-based per symbol).

    A spell is a maximal run of consecutive GLOBAL trading dates on which
    the symbol is a member. Contiguity is judged on the global trading
    calendar (all_dates), so a missing data row inside continuous
    membership does NOT break a spell (pilot convention: absent history is
    skipped), while a membership exit followed by re-entry DOES.
    """
    date_pos = {d: i for i, d in enumerate(all_dates)}
    spells = pd.Series(0, index=panel.index, dtype=int)
    for sym, grp in panel.groupby("symbol", sort=False):
        member = [bool(is_member_fn(sym, d)) for d in all_dates]
        run_id = np.cumsum([
            1 if (m and (i == 0 or not member[i - 1])) else 0
            for i, m in enumerate(member)
        ])
        spells.loc[grp.index] = [int(run_id[date_pos[d]]) for d in grp["date"]]
    return spells


# Target columns invalidated when the row h steps ahead (within the
# symbol's row sequence) belongs to a DIFFERENT membership spell — no
# return is ever computed across a membership gap. ret_1d_realized at a
# spell's final row goes NaN -> counted as 0.0 by portfolio construction:
# the position unwinds at the last available close (prereg convention).
_CROSS_SPELL_INVALIDATION = (
    (1, ("ret_1d_realized",)),
    (5, ("fwd_5d", "fwd_5d_resid")),
    (21, ("fwd_21d", "fwd_21d_resid")),
)


def invalidate_cross_spell_targets(panel: pd.DataFrame, spells: pd.Series) -> pd.DataFrame:
    """NaN-out targets/realized returns that would span a membership gap."""
    out = panel.copy()
    out["membership_spell"] = spells
    ordered = out.sort_values(["symbol", "date"])
    for h, cols in _CROSS_SPELL_INVALIDATION:
        shifted = ordered.groupby("symbol", sort=False)["membership_spell"].shift(-h)
        bad = shifted.notna() & (shifted != ordered["membership_spell"])
        cols_present = [c for c in cols if c in out.columns]
        out.loc[ordered.index[bad], cols_present] = np.nan
    return out


def build_pit_panel(
    features_df: pd.DataFrame,
    closes_df: pd.DataFrame,
    feature_cols: list[str],
    is_member_fn,
) -> pd.DataFrame:
    """Membership-aware panel: PIT eligibility enforced BEFORE ranking.

    1. Mask feature rows to member (date, symbol) pairs — non-member rows
       do not exist downstream (not in ranks, training, or weights).
    2. Delegate to the pilot's build_panel (inner join, SPY sidecar,
       cross-sectional rank normalization over members only, forward
       returns, SPY residualization, sidecar drop) — identical math.
    3. Segment membership spells on the global trading calendar and
       invalidate any target/realized return spanning a spell boundary.
    """
    elig = eligibility_mask(features_df, is_member_fn)
    member_features = features_df[elig].reset_index(drop=True)
    if member_features.empty:
        raise ValueError("No member rows after PIT eligibility mask — check membership table")
    panel = pilot.build_panel(member_features, closes_df, feature_cols)
    all_dates = sorted(closes_df["date"].unique())
    spells = assign_membership_spells(panel, is_member_fn, all_dates)
    return invalidate_cross_spell_targets(panel, spells)


# ---------------------------------------------------------------------------
# Regime series (overlay b) — spy_iv_zscore_60d, SPY, regime_features_v2
# ---------------------------------------------------------------------------

def fetch_regime_series_questdb() -> pd.Series:
    """SPY spy_iv_zscore_60d from the features table, indexed by date.

    The column is populated only on rows carrying regime_features_v2
    output, so filtering on non-null suffices; duplicates (if a date was
    written under multiple feature sets) keep the first.
    """
    import psycopg2

    from yats_pipelines.resources.questdb import QuestDBResource

    qdb = QuestDBResource()
    conn = psycopg2.connect(
        host=qdb.pg_host, port=qdb.pg_port, user=qdb.pg_user,
        password=qdb.pg_password, database=qdb.pg_database,
    )
    conn.autocommit = True
    cur = conn.cursor()
    end_clause = " AND timestamp <= %s" if END else ""
    params: tuple = (SPY, START) + ((END,) if END else ())
    cur.execute(
        f"SELECT timestamp, {REGIME_FEATURE} FROM features"
        f" WHERE symbol = %s AND {REGIME_FEATURE} IS NOT NULL"
        " AND timestamp >= %s" + end_clause + " ORDER BY timestamp",
        params,
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    if not rows:
        raise ValueError(
            f"No {REGIME_FEATURE} rows for {SPY} — the _rcvt overlay cannot "
            "silently degrade to the unconditioned vol target"
        )
    ser = pd.Series(
        [float(v) for _, v in rows],
        index=[pd.Timestamp(ts).date() for ts, _ in rows],
        name=REGIME_FEATURE,
    )
    return ser[~ser.index.duplicated(keep="first")].sort_index()


# ---------------------------------------------------------------------------
# Overlays on the best base's captured WFO folds
# ---------------------------------------------------------------------------

def apply_overlay(
    base_summary: dict,
    fold_captures: list[dict],
    *,
    label_suffix: str,
    overlay_name: str,
    risk_cfg: PortfolioRiskConfig,
    regime_series: pd.Series | None = None,
    cost_bp: float = COST_BP,
) -> dict:
    """Apply a risk-layer overlay to a base config's captured WFO folds.

    Per fold (strictly causal, no cross-fold state): scale the fill-date
    weights via research.portfolio.risk_layer.apply_risk_layer_batch
    (regime conditioning uses the t-1 reading of regime_series when the
    config enables it), recompute portfolio returns, and re-charge the 5bp
    cost on the SCALED weights' turnover. Same per-fold mechanics as the
    pilot's vt10 overlay, generalized to carry a regime series.
    """
    if not fold_captures:
        raise ValueError("No captured folds — cannot apply overlay")

    from types import SimpleNamespace

    concatenated: list[float] = []
    per_fold_sharpe: list[float | None] = []
    for fold in fold_captures:
        weights, returns = fold["weights"], fold["returns"]
        scaled = apply_risk_layer_batch(
            weights, returns, None, risk_cfg, regime_series=regime_series,
        )
        gross = (scaled * returns).sum(axis=1)
        net = gross - compute_turnover(scaled) * (cost_bp / 1e4)
        per_fold_sharpe.append(compute_sharpe(net) if len(net) >= 2 else None)
        concatenated.extend(float(r) for r in net.tolist())

    valid = [s for s in per_fold_sharpe if s is not None]
    result = SimpleNamespace(
        concatenated_oos_returns=concatenated,
        per_fold_oos_sharpe=per_fold_sharpe,
        median_oos_sharpe=float(np.median(valid)) if valid else None,
    )
    cfg = {
        "model": base_summary["model"],
        "horizon": base_summary["horizon"],
        "reg": base_summary["reg"],
        "variant": base_summary["variant"],
        "label": f"{base_summary['label']}{label_suffix}",
        "overlay": overlay_name,
        "base_config": base_summary["label"],
    }
    return alpha1.summarize_config_result(cfg, result)


def apply_vt10_overlay(base_summary: dict, fold_captures: list[dict]) -> dict:
    """Overlay (a): the incumbent 10% vol target."""
    return apply_overlay(
        base_summary, fold_captures,
        label_suffix="_vt10",
        overlay_name=OVERLAY_VT10,
        risk_cfg=PortfolioRiskConfig(vol_target=VOL_TARGET),
    )


def apply_rcvt_overlay(
    base_summary: dict, fold_captures: list[dict], regime_series: pd.Series,
) -> dict:
    """Overlay (b): regime-conditioned vol target — first prereg'd trial."""
    if regime_series is None or len(regime_series) == 0:
        raise ValueError("_rcvt overlay requires a non-empty regime series")
    return apply_overlay(
        base_summary, fold_captures,
        label_suffix="_rcvt",
        overlay_name=OVERLAY_RCVT,
        risk_cfg=PortfolioRiskConfig(vol_target=VOL_TARGET, regime_conditioning=RC_CONFIG),
        regime_series=regime_series,
    )


# ---------------------------------------------------------------------------
# Strict deflation (prereg: expected-max @ 78 over the honest-fill pool)
# ---------------------------------------------------------------------------

def strict_deflation(per_config: list[dict]) -> dict:
    """STRICT DSR per the prereg: pool includes these 5 trials; E[max] @ 78.

    SR0 = std(honest-fill pool INCLUDING the pit250 trials, population std)
          x expected-max of N_TRIALS_DEFLATION=78 standard normals
    — the pilot's computation, one clock tick later. Per-config
    DSR = PSR(sharpe; SR0); certification bar 0.95.
    """
    pool = list(HONEST_FILL_POOL_PRE_PIT250) + [float(c["sharpe"]) for c in per_config]
    emax = _expected_max_sr_benchmark(N_TRIALS_DEFLATION)
    sr0 = float(np.std(pool)) * emax  # population std (ddof=0), as incumbent

    per_config_out = []
    for c in per_config:
        psr = probabilistic_sharpe_ratio(
            observed_sharpe=float(c["sharpe"]),
            benchmark_sharpe=sr0,
            n_observations=int(c["n_obs"]),
            returns_skewness=float(c["skewness"]),
            returns_kurtosis=float(c["kurtosis"]),
        )
        per_config_out.append({
            "config": c["label"],
            "sharpe": float(c["sharpe"]),
            "strict_dsr": psr["dsr"],
            "certified": bool(psr["dsr"] > CERTIFICATION_BAR),
        })

    return {
        "method": (
            f"expected-max benchmark scaled to all {N_TRIALS_DEFLATION} trials; "
            "std over the full-span honest-fill pool including this family's 5 trials"
        ),
        "n_trials_emax": N_TRIALS_DEFLATION,
        "expected_max_sr": float(emax),
        "pool_sharpes": [float(s) for s in pool],
        "sr0": sr0,
        "certification_bar": CERTIFICATION_BAR,
        "per_config": per_config_out,
        "any_certified": any(c["certified"] for c in per_config_out),
    }


# ---------------------------------------------------------------------------
# Secondary readouts (committed in the prereg; display-only, not trials)
# ---------------------------------------------------------------------------

def insider_attribution_margin(per_config: list[dict]) -> dict:
    """Sharpe(lgbm_21d insider-inclusive) - Sharpe(lgbm_21d_no_insider).

    The third point on the down-cap gradient: -1.0 (10 names, sweep 3d),
    +0.10 (50 names, pilot), measured here at 250 PIT names.
    """
    by_label = {c["label"]: float(c["sharpe"]) for c in per_config if "overlay" not in c}
    inclusive = by_label.get("lgbm_21d")
    control = by_label.get("lgbm_21d_no_insider")
    margin = None if inclusive is None or control is None else inclusive - control
    return {
        "sharpe_insider_inclusive": inclusive,
        "sharpe_control": control,
        "margin": margin,
        "gradient_reference": {
            "margin_10_names_sweep3d_mean": INSIDER_MARGIN_10N_SWEEP3D,
            "margin_50_names_pilot_mean": INSIDER_MARGIN_50N_PILOT,
        },
    }


TERCILE_NAMES = ("small", "mid", "large")


def cap_tercile_readout(fold_captures: list[dict], panel: pd.DataFrame) -> dict | None:
    """Display-only: Sharpe of per-cap-tercile P&L contributions.

    Terciles come from the per-date cross-section of the log_mkt_cap
    feature (rank-normalized log_mkt_cap is monotone in raw market cap per
    date, so terciles are identical to raw-cap terciles). Contribution of
    tercile T on fill date t is sum(w_i(t) * r_i(t)) over members i in T;
    the three contribution series are Sharpe'd over the concatenated OOS
    folds. NOT a trial — it reports where the alpha lives.
    """
    if "log_mkt_cap" not in panel.columns:
        return None
    caps_by_date = {
        dt: grp.set_index("symbol")["log_mkt_cap"].dropna()
        for dt, grp in panel.groupby("date", sort=True)
    }
    contrib: dict[str, list[float]] = {name: [] for name in TERCILE_NAMES}
    weight_share: dict[str, list[float]] = {name: [] for name in TERCILE_NAMES}
    for fold in fold_captures:
        weights, returns = fold["weights"], fold["returns"]
        for dt in weights.index:
            caps = caps_by_date.get(dt)
            if caps is None or len(caps) < 3:
                continue
            pct = caps.rank(pct=True)
            w_row, r_row = weights.loc[dt], returns.loc[dt]
            for name, lo, hi in (
                ("small", 0.0, 1.0 / 3.0),
                ("mid", 1.0 / 3.0, 2.0 / 3.0),
                ("large", 2.0 / 3.0, 1.01),
            ):
                syms = pct[(pct > lo) & (pct <= hi)].index
                w = w_row.reindex(syms).fillna(0.0)
                contrib[name].append(float((w * r_row.reindex(syms).fillna(0.0)).sum()))
                weight_share[name].append(float(w.sum()))
    return {
        name: {
            "contribution_sharpe": (
                compute_sharpe(pd.Series(contrib[name])) if len(contrib[name]) >= 2 else None
            ),
            "mean_weight_share": (
                float(np.mean(weight_share[name])) if weight_share[name] else None
            ),
        }
        for name in TERCILE_NAMES
    }


def coverage_readout(
    features_df: pd.DataFrame,
    groups: dict[str, list[str]],
    is_member_fn,
    last_date,
) -> dict:
    """Realized per-group data coverage on member rows (prereg commitment).

    Splits by whether the symbol is still a member on the final date of the
    span (proxy for listed vs delisted/exited). Per feature group, reports
    the share of member rows with at least one and with all columns
    non-null. If delisted-name insider coverage is systematically absent,
    these numbers say so — reported either way.
    """
    out: dict[str, dict] = {}
    active = {
        sym: bool(is_member_fn(sym, last_date))
        for sym in features_df["symbol"].unique()
    }
    status = features_df["symbol"].map(active)
    for status_name, mask in (
        ("active_at_span_end", status),
        ("inactive_at_span_end", ~status),
    ):
        subset = features_df[mask]
        entry: dict[str, dict] = {
            "n_symbols": int(subset["symbol"].nunique()),
            "n_rows": int(len(subset)),
        }
        for group_name in ("insider", "institutional", "fundamental"):
            cols = [c for c in groups.get(group_name, []) if c in subset.columns]
            if not cols or subset.empty:
                entry[group_name] = {"share_any_non_null": None, "share_all_non_null": None}
                continue
            notna = subset[cols].notna()
            entry[group_name] = {
                "share_any_non_null": float(notna.any(axis=1).mean()),
                "share_all_non_null": float(notna.all(axis=1).mean()),
            }
        out[status_name] = entry
    return out


# ---------------------------------------------------------------------------
# Receipt
# ---------------------------------------------------------------------------

PIT_CONVENTION = (
    "A symbol participates in ranks/training/portfolio only on dates where "
    "is_member(symbol, date); the panel is masked before cross-sectional "
    "rank normalization; positions unwind at the last available close (the "
    "bar with no next in-spell close contributes zero return); targets are "
    "computed within membership spells only; re-entry starts a new spell; "
    "SPY is a mask-exempt, non-tradable residualization sidecar."
)


def build_receipt(
    per_config: list[dict],
    deflation: dict,
    *,
    insider_attribution: dict,
    cap_terciles: dict,
    coverage: dict,
    symbols: list[str],
    variant_cols: dict[str, list[str]],
    rank_decay: float | None,
    span_end: str | None,
    elapsed_hours: float | None = None,
) -> dict:
    """Assemble the receipt dict written to RECEIPT_PATH."""
    return {
        "sweep": "pit250_sweep",
        "preregistration": PREREG_PATH,
        "universe": "pit250",
        "membership_parquet": "configs/universes/pit250_membership.parquet",
        "membership_interface": "research/universe/membership.py",
        "pit_eligibility_rule": PIT_CONVENTION,
        "n_symbols_all_time": len(symbols),
        "symbols": list(symbols),
        "feature_set": FEATURE_SET,
        "variant_feature_cols": {v: list(cols) for v, cols in variant_cols.items()},
        "span": [START, span_end or "latest"],
        "wfo": {
            "mode": "anchored",
            "n_periods": WFO_CFG.n_periods,
            "train_window": WFO_CFG.train_window,
            "label_horizon": WFO_CFG.label_horizon,
            "purge_buffer": WFO_CFG.purge_buffer,
        },
        "execution": {
            "execution_lag_days": EXECUTION_LAG,
            "fill_timing": FILL_TIMING,
            "transaction_cost_bp": COST_BP,
        },
        "trials_charged": TOTAL_TRIALS,
        "deflation_clock": N_TRIALS_DEFLATION,
        "configs": per_config,
        "strict_deflation": deflation,
        "insider_attribution": insider_attribution,
        "cap_tercile_readout": cap_terciles,
        "delisted_coverage": coverage,
        "rank_decay": rank_decay,
        "regime_overlay": {
            "feature": REGIME_FEATURE,
            "low_target": RC_CONFIG.low_target,
            "high_target": RC_CONFIG.high_target,
            "zscore_lo": RC_CONFIG.zscore_lo,
            "zscore_hi": RC_CONFIG.zscore_hi,
        },
        "elapsed_hours": elapsed_hours,
    }


# ---------------------------------------------------------------------------
# Sweep driver
# ---------------------------------------------------------------------------

def print_verdict(deflation: dict, insider_attribution: dict) -> None:
    """Print the strict-deflation verdict + the insider-attribution readout."""
    mark(
        f"STRICT DEFLATION: SR0={deflation['sr0']:.4f} "
        f"(= std(pool n={len(deflation['pool_sharpes'])}) x "
        f"E[max]@{deflation['n_trials_emax']}={deflation['expected_max_sr']:.3f}) "
        f"bar={deflation['certification_bar']}"
    )
    for c in sorted(deflation["per_config"], key=lambda x: -x["strict_dsr"]):
        mark(
            f"  {c['config']}: sharpe={c['sharpe']:.3f} "
            f"strict_dsr={c['strict_dsr']:.3f} certified={c['certified']}"
        )
    best = max(deflation["per_config"], key=lambda x: x["strict_dsr"])
    verdict = "CERTIFIED" if deflation["any_certified"] else "NOT CERTIFIED"
    mark(
        f"VERDICT: {verdict} — best {best['config']} "
        f"strict_dsr={best['strict_dsr']:.3f} vs bar {CERTIFICATION_BAR} "
        f"at {N_TRIALS_DEFLATION} trials"
    )
    m = insider_attribution["margin"]
    mark(
        "INSIDER ATTRIBUTION (250 PIT names): margin="
        f"{'n/a' if m is None else f'{m:+.3f}'} "
        f"(gradient: {INSIDER_MARGIN_10N_SWEEP3D:+.2f} @10 names, "
        f"{INSIDER_MARGIN_50N_PILOT:+.2f} @50 names)"
    )


def main() -> int:
    t0 = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    membership = load_membership()
    symbols = sorted(membership.all_symbols())
    is_member = membership.is_member
    groups = pilot.load_feature_groups()
    variant_cols = variant_feature_cols(groups)
    mark(
        f"UNIVERSE pit250 all-time symbols={len(symbols)} "
        f"({MEMBERSHIP_PARQUET.name}); feature_set={FEATURE_SET}; "
        f"insider_inclusive cols={len(variant_cols[VARIANT_INSIDER_INCLUSIVE])}, "
        f"control cols={len(variant_cols[VARIANT_CONTROL])}"
    )
    mark(
        f"PREREGISTERED FAMILY: {TOTAL_TRIALS} trials, deflation clock "
        f"73 -> {N_TRIALS_DEFLATION} ({PREREG_PATH})"
    )

    # Fetch once with the full (insider-inclusive) column set; the control
    # config consumes a column subset of the same stored features.
    features_df, closes_df = pilot._fetch_frames_questdb(
        symbols, variant_cols[VARIANT_INSIDER_INCLUSIVE]
    )
    mark(
        f"DATA features rows={len(features_df)} px rows={len(closes_df)} "
        f"span=[{START}, {END or 'latest'}]"
    )
    panel = build_pit_panel(
        features_df, closes_df, variant_cols[VARIANT_INSIDER_INCLUSIVE], is_member
    )
    dates = sorted(panel["date"].unique())
    mark(
        f"PIT PANEL rows={len(panel)} dates={len(dates)} "
        f"symbols={panel['symbol'].nunique()} "
        f"spells={int(panel['membership_spell'].max())} max/symbol"
    )
    mark(
        f"WFO geometry: anchored {WFO_CFG.n_periods} folds, "
        f"train_window={WFO_CFG.train_window}, label_horizon={WFO_CFG.label_horizon}, "
        f"purge_buffer={WFO_CFG.purge_buffer}, lag={EXECUTION_LAG}, "
        f"fill={FILL_TIMING}, cost={COST_BP}bp"
    )

    regime_series = fetch_regime_series_questdb()
    mark(f"REGIME series {REGIME_FEATURE}: {len(regime_series)} dates")

    coverage = coverage_readout(
        features_df[eligibility_mask(features_df, is_member)],
        groups, is_member, dates[-1],
    )
    inactive = coverage["inactive_at_span_end"]
    mark(
        f"COVERAGE inactive-at-span-end: n_symbols={inactive['n_symbols']} "
        f"insider all-non-null share="
        f"{inactive['insider']['share_all_non_null']}"
    )

    per_config: list[dict] = []
    captures: dict[str, list[dict]] = {}
    data = list(range(len(dates)))
    for i, cfg in enumerate(BASE_GRID):
        cfg_t0 = time.time()
        cols = variant_cols[cfg["variant"]]
        mark(
            f"CONFIG {i + 1}/{len(BASE_GRID)}: {cfg['label']} "
            f"({cfg['model']} h={cfg['horizon']}d reg={cfg['reg']}, "
            f"variant={cfg['variant']}, {len(cols)} input cols)"
        )
        capture: list[dict] = []
        train_fn = alpha1.make_train_fn(panel, cfg, dates, feature_cols=cols)
        eval_fn = alpha1.make_eval_fn(
            panel, dates,
            symbols=symbols,
            fill_timing=FILL_TIMING,
            execution_lag=EXECUTION_LAG,
            cost_bp=COST_BP,
            fold_capture=capture,
        )
        result = run_wfo(data, WFO_CFG, train_fn=train_fn, eval_fn=eval_fn)
        summary = alpha1.summarize_config_result(cfg, result)
        summary["elapsed_s"] = time.time() - cfg_t0
        per_config.append(summary)
        captures[cfg["label"]] = capture
        mark(
            f"{cfg['label']} done: OOS sharpe={summary['sharpe']:.3f} "
            f"folds={['%.2f' % (s or 0) for s in summary['per_fold_oos_sharpe']]} "
            f"n={summary['n_obs']} ({summary['elapsed_s']:.1f}s)"
        )

    # Overlay choice rule (committed in the prereg): best BASE config by
    # pooled OOS Sharpe gets BOTH overlays — mechanical, control included.
    best = max(per_config, key=lambda c: c["sharpe"])
    mark(f"BEST BASE by OOS Sharpe: {best['label']} ({best['sharpe']:.3f}) — applying overlays")
    for overlay_fn, name in (
        (lambda b, c: apply_vt10_overlay(b, c), OVERLAY_VT10),
        (lambda b, c: apply_rcvt_overlay(b, c, regime_series), OVERLAY_RCVT),
    ):
        overlay = overlay_fn(best, captures[best["label"]])
        per_config.append(overlay)
        mark(
            f"{overlay['label']} ({name}) done: OOS sharpe={overlay['sharpe']:.3f} "
            f"folds={['%.2f' % (s or 0) for s in overlay['per_fold_oos_sharpe']]}"
        )

    if len(per_config) != TOTAL_TRIALS:
        mark(
            f"WARNING trial count {len(per_config)} != preregistered {TOTAL_TRIALS} "
            "— receipt records actual configs run; the clock still charges 5"
        )

    deflation = strict_deflation(per_config)
    for c, d in zip(per_config, deflation["per_config"]):
        c["strict_dsr"] = d["strict_dsr"]
        c["certified"] = d["certified"]

    insider_attr = insider_attribution_margin(per_config)
    terciles = {
        label: cap_tercile_readout(capture, panel)
        for label, capture in captures.items()
    }
    rank_decay = compute_sweep_wfo_rank_decay(
        [c["per_fold_oos_sharpe"] for c in per_config if "overlay" not in c]
    )

    receipt = build_receipt(
        per_config, deflation,
        insider_attribution=insider_attr,
        cap_terciles=terciles,
        coverage=coverage,
        symbols=symbols,
        variant_cols=variant_cols,
        rank_decay=rank_decay,
        span_end=END,
        elapsed_hours=(time.time() - t0) / 3600,
    )
    RECEIPT_PATH.parent.mkdir(parents=True, exist_ok=True)
    RECEIPT_PATH.write_text(json.dumps(receipt, indent=2, default=str))
    mark(f"RECEIPT written to {RECEIPT_PATH}")
    raw_out = OUT_DIR / "pit250_summary.json"
    raw_out.write_text(json.dumps(receipt, indent=2, default=str))
    mark(f"RAW SUMMARY written to {raw_out}")

    print_verdict(deflation, insider_attr)
    mark(
        f"SWEEP COMPLETE ({(time.time() - t0) / 60:.1f}m) "
        f"trials={len(per_config)}/{TOTAL_TRIALS} clock={N_TRIALS_DEFLATION} "
        f"rank_decay={rank_decay}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
