"""Dagster feature pipeline — full recompute.

Reads from canonical tables, computes all v1 features, writes to the
features table via ILP. Full recompute only (incremental is P2.8).

Input: universe, feature_set, date_range
Output: rows written to features table
Dependencies: canonical tables populated (P1.6)
"""

import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import psycopg2
import yaml
from dagster import Config, OpExecutionContext, job, op
from questdb.ingress import Protocol, Sender, TimestampNanos

from yats_pipelines.resources.questdb import QuestDBResource
from yats_pipelines.utils.run_recorder import record_finish, record_start

# Import feature modules to trigger @feature registrations
import research.features.ohlcv_features  # noqa: F401
import research.features.cross_sectional_features  # noqa: F401
import research.features.fundamental_features  # noqa: F401
import research.features.regime_features_v1  # noqa: F401
import research.features.options_features_v1  # noqa: F401
import research.features.insider_features_v1  # noqa: F401
import research.features.inst_features_v1  # noqa: F401
from research.features.feature_registry import registry
from research.features.regime_features_v1 import (
    DEFAULT_REGIME_UNIVERSE,
    compute_regime_features,
)
from research.features.options_features_v1 import compute_options_features
from research.features.insider_features_v1 import compute_insider_features
from research.features.inst_features_v1 import compute_inst_features

logger = logging.getLogger(__name__)


class FeaturePipelineConfig(Config):
    """Run config for the feature pipeline."""

    universe: str = "sp500"  # YAML universe name under configs/universes/
    feature_set: str = "core_v1"  # Feature set name under configs/feature_sets/
    feature_set_version: str = "1.0"
    start_date: str = ""  # ISO-8601; empty = all
    end_date: str = ""  # ISO-8601; empty = all
    regime_universe: list[str] = list(DEFAULT_REGIME_UNIVERSE)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pg_conn(qdb: QuestDBResource):
    return psycopg2.connect(
        host=qdb.pg_host, port=qdb.pg_port,
        user=qdb.pg_user, password=qdb.pg_password,
        database=qdb.pg_database,
    )


def _ilp_sender(qdb: QuestDBResource):
    return Sender(Protocol.Tcp, qdb.ilp_host, qdb.ilp_port)


def _ts_nanos(dt) -> TimestampNanos:
    return TimestampNanos(int(dt.timestamp() * 1_000_000_000))


def _date_clause(start_date: str, end_date: str, ts_col: str = "timestamp") -> tuple[str, list]:
    """Return (WHERE clause template, params list) for date filtering."""
    parts: list[str] = []
    params: list = []
    if start_date:
        parts.append(f"{ts_col} >= %s")
        params.append(f"{start_date}T00:00:00.000000Z")
    if end_date:
        parts.append(f"{ts_col} <= %s")
        params.append(f"{end_date}T23:59:59.999999Z")
    where = " WHERE " + " AND ".join(parts) if parts else ""
    return where, params


def _load_universe(name: str) -> list[str]:
    """Load universe tickers from YAML config."""
    import pathlib
    config_dir = pathlib.Path(__file__).resolve().parents[3] / "configs" / "universes"
    path = config_dir / f"{name}.yml"
    if not path.exists():
        raise FileNotFoundError(f"Universe config not found: {path}")
    with open(path) as f:
        data = yaml.safe_load(f)
    return data["tickers"]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def _load_ohlcv(conn, tickers: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """Load canonical OHLCV data for given tickers and date range."""
    where, params = _date_clause(start_date, end_date)
    if where:
        where += " AND symbol IN %s"
    else:
        where = " WHERE symbol IN %s"
    params.append(tuple(tickers))

    query = f"SELECT timestamp, symbol, open, high, low, close, volume FROM canonical_equity_ohlcv{where} ORDER BY symbol, timestamp"

    cur = conn.cursor()
    cur.execute(query, params)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    cur.close()

    if not rows:
        return pd.DataFrame(columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(rows, columns=columns)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


def _load_financial_metrics(conn, tickers: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """Load canonical financial metrics for given tickers and date range."""
    where, params = _date_clause(start_date, end_date)
    if where:
        where += " AND symbol IN %s"
    else:
        where = " WHERE symbol IN %s"
    params.append(tuple(tickers))

    fields = [
        "timestamp", "symbol", "pe_ratio", "ps_ratio", "pb_ratio",
        "ev_ebitda", "roe", "gross_margin", "operating_margin",
        "fcf_margin", "debt_to_equity", "eps_growth_yoy",
        "revenue_growth_yoy", "shares_outstanding",
    ]
    field_str = ", ".join(fields)
    query = f"SELECT {field_str} FROM canonical_financial_metrics{where} ORDER BY symbol, timestamp"

    cur = conn.cursor()
    cur.execute(query, params)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    cur.close()

    if not rows:
        return pd.DataFrame(columns=fields)

    df = pd.DataFrame(rows, columns=columns)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


# ---------------------------------------------------------------------------
# Feature computation
# ---------------------------------------------------------------------------


def _winsorize(series: pd.Series, lower: float = 0.01, upper: float = 0.99) -> pd.Series:
    """Winsorize at given percentiles."""
    lo = series.quantile(lower)
    hi = series.quantile(upper)
    return series.clip(lo, hi)


def _compute_ohlcv_features(ohlcv_by_symbol: dict[str, pd.DataFrame], feature_names: list[str]) -> dict[str, pd.DataFrame]:
    """Compute per-symbol OHLCV features."""
    results = {}
    for symbol, df in ohlcv_by_symbol.items():
        symbol_features = pd.DataFrame(index=df.index)
        symbol_features["timestamp"] = df["timestamp"]
        symbol_features["symbol"] = symbol

        for fname in feature_names:
            fn = registry.get(fname)
            symbol_features[fname] = fn(df).values

        results[symbol] = symbol_features
    return results


def _compute_fundamental_features(
    ohlcv_by_symbol: dict[str, pd.DataFrame],
    metrics_by_symbol: dict[str, pd.DataFrame],
    feature_names: list[str],
) -> dict[str, pd.DataFrame]:
    """Compute fundamental features per symbol by merging metrics onto OHLCV dates."""
    results = {}
    for symbol, ohlcv_df in ohlcv_by_symbol.items():
        metrics_df = metrics_by_symbol.get(symbol)
        if metrics_df is None or metrics_df.empty:
            # No metrics — all fundamentals will be NaN
            feat_df = pd.DataFrame(index=ohlcv_df.index)
            for fname in feature_names:
                feat_df[fname] = np.nan
            results[symbol] = feat_df
            continue

        # Merge metrics onto OHLCV dates using as-of join (forward-fill)
        ohlcv_dates = ohlcv_df[["timestamp"]].copy()
        metrics_sorted = metrics_df.sort_values("timestamp")

        merged = pd.merge_asof(
            ohlcv_dates.sort_values("timestamp"),
            metrics_sorted,
            on="timestamp",
            direction="backward",
        )

        feat_df = pd.DataFrame(index=ohlcv_df.index)
        for fname in feature_names:
            fn = registry.get(fname)
            feat_df[fname] = fn(merged).values

        results[symbol] = feat_df
    return results


def _compute_cross_sectional_features(
    ohlcv_by_symbol: dict[str, pd.DataFrame],
    metrics_by_symbol: dict[str, pd.DataFrame],
    feature_names: list[str],
) -> dict[str, pd.DataFrame]:
    """Compute cross-sectional features across all symbols per date.

    Per-symbol momentum features (mom_3m, mom_12m_excl_1m) are computed first,
    then cross-sectional rankings (size_rank, value_rank) are done per date.
    """
    # First compute per-symbol features that feed into cross-sectional
    per_symbol_cs = ["mom_3m", "mom_12m_excl_1m", "log_mkt_cap"]
    rank_features = ["size_rank", "value_rank"]

    results: dict[str, pd.DataFrame] = {}

    # Compute per-symbol features
    for symbol, ohlcv_df in ohlcv_by_symbol.items():
        feat_df = pd.DataFrame(index=ohlcv_df.index)
        feat_df["timestamp"] = ohlcv_df["timestamp"]
        feat_df["symbol"] = symbol

        for fname in per_symbol_cs:
            if fname not in feature_names:
                continue
            fn = registry.get(fname)
            if fname == "log_mkt_cap":
                # Needs shares_outstanding merged in
                metrics_df = metrics_by_symbol.get(symbol)
                if metrics_df is not None and not metrics_df.empty:
                    merged = pd.merge_asof(
                        ohlcv_df[["timestamp", "close"]].sort_values("timestamp"),
                        metrics_df[["timestamp", "shares_outstanding"]].sort_values("timestamp"),
                        on="timestamp",
                        direction="backward",
                    )
                    feat_df[fname] = fn(merged).values
                else:
                    feat_df[fname] = np.nan
            else:
                feat_df[fname] = fn(ohlcv_df).values

        results[symbol] = feat_df

    # Now compute cross-sectional ranks per date
    if any(f in feature_names for f in rank_features):
        # Build universe-wide DataFrame
        all_rows = []
        for symbol, feat_df in results.items():
            row_df = feat_df.copy()
            # Need pe_ttm for value_rank
            if "value_rank" in feature_names:
                metrics_df = metrics_by_symbol.get(symbol)
                ohlcv_df = ohlcv_by_symbol[symbol]
                if metrics_df is not None and not metrics_df.empty:
                    merged = pd.merge_asof(
                        ohlcv_df[["timestamp"]].sort_values("timestamp"),
                        metrics_df[["timestamp", "pe_ratio"]].sort_values("timestamp"),
                        on="timestamp",
                        direction="backward",
                    )
                    row_df["pe_ttm"] = merged["pe_ratio"].values
                else:
                    row_df["pe_ttm"] = np.nan
            all_rows.append(row_df)

        universe_df = pd.concat(all_rows, ignore_index=True)

        # Compute ranks per date
        for fname in rank_features:
            if fname not in feature_names:
                continue
            fn = registry.get(fname)
            universe_df[fname] = universe_df.groupby("timestamp").apply(
                lambda g: fn(g)
            ).reset_index(level=0, drop=True)

        # Z-score cross-sectional features per date (after winsorization)
        cs_numeric = [f for f in feature_names if f in universe_df.columns and f not in ("size_rank", "value_rank")]
        for fname in cs_numeric:
            # Winsorize per symbol
            universe_df[fname] = universe_df.groupby("symbol")[fname].transform(
                lambda s: _winsorize(s)
            )
            # Z-score per date
            def _zscore(s):
                std = s.std()
                if std > 0:
                    return (s - s.mean()) / std
                return s * 0.0  # Series of zeros, preserving index
            universe_df[fname] = universe_df.groupby("timestamp")[fname].transform(_zscore)

        # Split back to per-symbol
        for symbol in results:
            mask = universe_df["symbol"] == symbol
            sym_data = universe_df.loc[mask]
            for fname in feature_names:
                if fname in sym_data.columns:
                    results[symbol][fname] = sym_data[fname].values

    return results


def _compute_regime_features_pipeline(
    conn, regime_universe: list[str], start_date: str, end_date: str
) -> pd.DataFrame:
    """Compute regime features from regime universe close prices."""
    ohlcv = _load_ohlcv(conn, regime_universe, start_date, end_date)
    if ohlcv.empty:
        return pd.DataFrame()

    # Pivot to wide format: dates × symbols
    pivot = ohlcv.pivot_table(
        index="timestamp", columns="symbol", values="close"
    )

    regime_df = compute_regime_features(pivot)
    return regime_df


def _load_options_chain(
    conn, tickers: list[str], start_date: str, end_date: str
) -> pd.DataFrame:
    """Load canonical options chain for given underlyings and date range."""
    where, params = _date_clause(start_date, end_date, ts_col="quote_date")
    if where:
        where += " AND underlying IN %s"
    else:
        where = " WHERE underlying IN %s"
    params.append(tuple(tickers))

    fields = [
        "quote_date", "underlying", "expiry", "strike", "right",
        "iv", "delta", "gamma", "open_interest",
    ]
    field_str = ", ".join(fields)
    query = (
        f"SELECT {field_str} FROM canonical_options_chain{where} "
        f"ORDER BY underlying, quote_date, expiry, strike, right"
    )

    cur = conn.cursor()
    cur.execute(query, params)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    cur.close()

    if not rows:
        return pd.DataFrame(columns=fields)

    df = pd.DataFrame(rows, columns=columns)
    df["quote_date"] = pd.to_datetime(df["quote_date"], utc=True)
    df["expiry"] = pd.to_datetime(df["expiry"], utc=True)
    return df


def _compute_options_features_pipeline(
    options_df: pd.DataFrame,
    ohlcv_by_symbol: dict[str, pd.DataFrame],
    feature_names: list[str],
) -> dict[str, pd.DataFrame]:
    """Compute options features per (underlying, date).

    Returns dict: symbol → DataFrame with timestamp index and options feature columns.
    """
    results: dict[str, pd.DataFrame] = {}

    if options_df.empty:
        return results

    grouped = options_df.groupby("underlying")
    for symbol, chain_df in grouped:
        ohlcv_df = ohlcv_by_symbol.get(symbol)
        if ohlcv_df is None or ohlcv_df.empty:
            continue

        # Map close prices to dates for spot lookup
        spot_by_date = dict(
            zip(
                ohlcv_df["timestamp"].dt.normalize(),
                ohlcv_df["close"],
            )
        )

        rows = []
        for quote_date, day_chain in chain_df.groupby("quote_date"):
            spot = spot_by_date.get(pd.Timestamp(quote_date).normalize(), float("nan"))
            feats = compute_options_features(
                day_chain, float(spot), pd.Timestamp(quote_date), underlying=symbol
            )
            feats["timestamp"] = quote_date
            rows.append(feats)

        if not rows:
            continue

        feat_df = pd.DataFrame(rows).set_index(
            pd.RangeIndex(len(rows))
        )
        feat_df["timestamp"] = pd.to_datetime(feat_df["timestamp"], utc=True)

        # Winsorize each options feature per underlying
        for fname in feature_names:
            if fname in feat_df.columns:
                feat_df[fname] = _winsorize(feat_df[fname])

        results[symbol] = feat_df

    return results


def _load_insider_trades(
    conn, tickers: list[str], start_date: str, end_date: str, lookback_days: int = 90
) -> pd.DataFrame:
    """Load canonical insider trades for given symbols; extends range backward by lookback_days."""
    fields = [
        "filing_date", "symbol", "insider_name", "insider_title",
        "is_board_director", "transaction_type", "total_value",
    ]
    field_str = ", ".join(fields)

    params: list = [tuple(tickers)]
    where = "WHERE symbol IN %s"
    if start_date:
        # Pull extra lookback so 90d windows are fully populated at start_date
        where += " AND filing_date >= dateadd('d', %s, %s)"
        params.extend([-lookback_days, f"{start_date}T00:00:00.000000Z"])
    if end_date:
        where += " AND filing_date <= %s"
        params.append(f"{end_date}T23:59:59.999999Z")

    query = (
        f"SELECT {field_str} FROM canonical_insider_trades "
        f"{where} ORDER BY symbol, filing_date"
    )
    cur = conn.cursor()
    cur.execute(query, params)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    cur.close()

    if not rows:
        return pd.DataFrame(columns=fields)

    df = pd.DataFrame(rows, columns=columns)
    df["filing_date"] = pd.to_datetime(df["filing_date"], utc=True)
    return df


def _load_inst_ownership(
    conn, tickers: list[str], start_date: str, end_date: str, lookback_days: int = 150
) -> pd.DataFrame:
    """Load canonical_inst_ownership for given symbols."""
    fields = ["filing_date", "symbol", "report_period", "total_shares", "filer_count"]
    field_str = ", ".join(fields)

    params: list = [tuple(tickers)]
    where = "WHERE symbol IN %s"
    if start_date:
        where += " AND filing_date >= dateadd('d', %s, %s)"
        params.extend([-lookback_days, f"{start_date}T00:00:00.000000Z"])
    if end_date:
        where += " AND filing_date <= %s"
        params.append(f"{end_date}T23:59:59.999999Z")

    query = (
        f"SELECT {field_str} FROM canonical_inst_ownership "
        f"{where} ORDER BY symbol, filing_date"
    )
    cur = conn.cursor()
    cur.execute(query, params)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    cur.close()

    if not rows:
        return pd.DataFrame(columns=fields)

    df = pd.DataFrame(rows, columns=columns)
    df["filing_date"] = pd.to_datetime(df["filing_date"], utc=True)
    return df


def _load_inst_holdings(
    conn, tickers: list[str], start_date: str, end_date: str, lookback_days: int = 150
) -> pd.DataFrame:
    """Load canonical_institutional_holdings for given symbols."""
    fields = [
        "filing_date", "symbol", "report_period",
        "filer_cik", "filer_name", "shares", "value_usd",
    ]
    field_str = ", ".join(fields)

    params: list = [tuple(tickers)]
    where = "WHERE symbol IN %s"
    if start_date:
        where += " AND filing_date >= dateadd('d', %s, %s)"
        params.extend([-lookback_days, f"{start_date}T00:00:00.000000Z"])
    if end_date:
        where += " AND filing_date <= %s"
        params.append(f"{end_date}T23:59:59.999999Z")

    query = (
        f"SELECT {field_str} FROM canonical_institutional_holdings "
        f"{where} ORDER BY symbol, filing_date"
    )
    cur = conn.cursor()
    cur.execute(query, params)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    cur.close()

    if not rows:
        return pd.DataFrame(columns=fields)

    df = pd.DataFrame(rows, columns=columns)
    df["filing_date"] = pd.to_datetime(df["filing_date"], utc=True)
    return df


def _compute_insider_features_pipeline(
    insider_df: pd.DataFrame,
    ohlcv_by_symbol: dict[str, pd.DataFrame],
    metrics_by_symbol: dict[str, pd.DataFrame],
    feature_names: list[str],
) -> dict[str, pd.DataFrame]:
    """Compute insider features per (symbol, date).

    Returns dict: symbol → DataFrame with timestamp + insider feature columns.
    """
    results: dict[str, pd.DataFrame] = {}

    grouped = insider_df.groupby("symbol") if not insider_df.empty else {}

    for symbol, ohlcv_df in ohlcv_by_symbol.items():
        sym_trades = grouped.get_group(symbol) if (not insider_df.empty and symbol in insider_df["symbol"].values) else pd.DataFrame()
        metrics_df = metrics_by_symbol.get(symbol)

        # Merge shares_outstanding and close per OHLCV date
        if metrics_df is not None and not metrics_df.empty:
            merged_metrics = pd.merge_asof(
                ohlcv_df[["timestamp", "close"]].sort_values("timestamp"),
                metrics_df[["timestamp", "shares_outstanding"]].sort_values("timestamp"),
                on="timestamp",
                direction="backward",
            )
        else:
            merged_metrics = ohlcv_df[["timestamp", "close"]].copy()
            merged_metrics["shares_outstanding"] = np.nan

        rows = []
        for _, ohlcv_row in merged_metrics.iterrows():
            ts = ohlcv_row["timestamp"]
            feats = compute_insider_features(
                sym_trades,
                as_of_date=ts,
                shares_outstanding=float(ohlcv_row.get("shares_outstanding", np.nan) or np.nan),
                close=float(ohlcv_row["close"]),
                symbol=symbol,
            )
            feats["timestamp"] = ts
            rows.append(feats)

        if not rows:
            continue

        feat_df = pd.DataFrame(rows)
        feat_df["timestamp"] = pd.to_datetime(feat_df["timestamp"], utc=True)
        results[symbol] = feat_df

    return results


def _compute_inst_features_pipeline(
    ownership_df: pd.DataFrame,
    holdings_df: pd.DataFrame,
    ohlcv_by_symbol: dict[str, pd.DataFrame],
    metrics_by_symbol: dict[str, pd.DataFrame],
    feature_names: list[str],
) -> dict[str, pd.DataFrame]:
    """Compute institutional features per (symbol, date).

    Returns dict: symbol → DataFrame with timestamp + inst feature columns.
    """
    results: dict[str, pd.DataFrame] = {}

    own_grouped = ownership_df.groupby("symbol") if not ownership_df.empty else {}
    hold_grouped = holdings_df.groupby("symbol") if not holdings_df.empty else {}

    for symbol, ohlcv_df in ohlcv_by_symbol.items():
        sym_own = (
            own_grouped.get_group(symbol)
            if (not ownership_df.empty and symbol in ownership_df["symbol"].values)
            else pd.DataFrame()
        )
        sym_hold = (
            hold_grouped.get_group(symbol)
            if (not holdings_df.empty and symbol in holdings_df["symbol"].values)
            else pd.DataFrame()
        )
        metrics_df = metrics_by_symbol.get(symbol)

        if metrics_df is not None and not metrics_df.empty:
            merged_metrics = pd.merge_asof(
                ohlcv_df[["timestamp"]].sort_values("timestamp"),
                metrics_df[["timestamp", "shares_outstanding"]].sort_values("timestamp"),
                on="timestamp",
                direction="backward",
            )
        else:
            merged_metrics = ohlcv_df[["timestamp"]].copy()
            merged_metrics["shares_outstanding"] = np.nan

        rows = []
        for _, row in merged_metrics.iterrows():
            ts = row["timestamp"]
            feats = compute_inst_features(
                sym_own,
                sym_hold,
                shares_outstanding=float(row.get("shares_outstanding", np.nan) or np.nan),
                as_of_date=ts,
                symbol=symbol,
            )
            feats["timestamp"] = ts
            rows.append(feats)

        if not rows:
            continue

        feat_df = pd.DataFrame(rows)
        feat_df["timestamp"] = pd.to_datetime(feat_df["timestamp"], utc=True)
        results[symbol] = feat_df

    return results


# ---------------------------------------------------------------------------
# ILP writing
# ---------------------------------------------------------------------------

# All feature columns in the features table
_FEATURE_COLUMNS = [
    "ret_1d", "ret_5d", "ret_21d", "rv_21d", "rv_63d",
    "dist_20d_high", "dist_20d_low",
    "mom_3m", "mom_12m_excl_1m", "log_mkt_cap", "size_rank", "value_rank",
    "pe_ttm", "ps_ttm", "pb", "ev_ebitda", "roe", "gross_margin",
    "operating_margin", "fcf_margin", "debt_equity", "eps_growth_1y",
    "revenue_growth_1y",
    "market_vol_20d", "market_trend_20d", "dispersion_20d", "corr_mean_20d",
    "atm_iv", "skew_25d", "iv_term_slope", "put_call_oi_ratio", "net_gamma_exposure",
    "insider_net_buy_90d", "insider_buy_intensity_30d", "insider_cluster_30d", "exec_net_buy_90d",
    "inst_ownership_pct", "inst_top10_share",
]


def _write_features(
    sender,
    symbol: str,
    timestamps: pd.Series,
    features: dict[str, pd.Series],
    feature_set: str,
    feature_set_version: str,
    now: datetime,
) -> int:
    """Write feature rows to QuestDB via ILP."""
    written = 0
    skipped_all_nan = 0
    for i, ts in enumerate(timestamps):
        if pd.isna(ts):
            continue

        columns = {}
        has_any = False
        for col in _FEATURE_COLUMNS:
            if col in features:
                val = features[col].iloc[i] if hasattr(features[col], "iloc") else features[col][i]
                if isinstance(val, (float, np.floating)):
                    if not np.isnan(val):
                        columns[col] = float(val)
                        has_any = True
                elif val is not None:
                    columns[col] = float(val)
                    has_any = True

        if not has_any:
            skipped_all_nan += 1
            continue

        columns["computed_at"] = _ts_nanos(now)

        sender.row(
            "features",
            symbols={
                "symbol": symbol,
                "feature_set": feature_set,
                "feature_set_version": feature_set_version,
            },
            columns={k: v for k, v in columns.items() if v is not None},
            at=_ts_nanos(ts),
        )
        written += 1

    if skipped_all_nan > 0:
        logger.debug(
            "_write_features: skipped %d all-NaN rows for symbol %s (feature_set=%s)",
            skipped_all_nan, symbol, feature_set,
        )
    return written


# ---------------------------------------------------------------------------
# Dagster op + job
# ---------------------------------------------------------------------------


@op
def feature_pipeline_op(context: OpExecutionContext, config: FeaturePipelineConfig):
    """Full-recompute feature pipeline.

    Reads canonical tables, computes all features in the feature set,
    writes to the features table via ILP.
    """
    detail = f"universe={config.universe} feature_set={config.feature_set}"
    record_start("feature_pipeline", context.run_id, detail)
    _exc: Exception | None = None
    _total_written = 0

    qdb = QuestDBResource()
    now = datetime.now(timezone.utc)

    # Load feature set definition
    fs = registry.load_feature_set(config.feature_set)
    context.log.info(
        "Feature set '%s': %d features (%d ohlcv, %d cross-sectional, "
        "%d fundamental, %d regime, %d options, %d insider, %d institutional)",
        fs.name, len(fs.all_features),
        len(fs.ohlcv), len(fs.cross_sectional),
        len(fs.fundamental), len(fs.regime), len(fs.options),
        len(fs.insider), len(fs.institutional),
    )

    # Load universe
    tickers = _load_universe(config.universe)
    context.log.info("Universe '%s': %d tickers", config.universe, len(tickers))

    conn = None
    try:
        conn = _pg_conn(qdb)
        conn.autocommit = True
        # Load canonical data
        context.log.info("Loading canonical OHLCV data...")
        ohlcv_df = _load_ohlcv(conn, tickers, config.start_date, config.end_date)
        if ohlcv_df.empty:
            context.log.warning("No OHLCV data found — nothing to compute")
            return

        context.log.info("Loaded %d OHLCV rows for %d symbols",
                         len(ohlcv_df), ohlcv_df["symbol"].nunique())

        context.log.info("Loading canonical financial metrics...")
        metrics_df = _load_financial_metrics(conn, tickers, config.start_date, config.end_date)
        context.log.info("Loaded %d financial metrics rows", len(metrics_df))

        # Group by symbol
        ohlcv_by_symbol = {
            symbol: group.sort_values("timestamp").reset_index(drop=True)
            for symbol, group in ohlcv_df.groupby("symbol")
        }
        metrics_by_symbol = {
            symbol: group.sort_values("timestamp").reset_index(drop=True)
            for symbol, group in metrics_df.groupby("symbol")
        } if not metrics_df.empty else {}

        # Compute OHLCV features
        context.log.info("Computing OHLCV features...")
        ohlcv_features = _compute_ohlcv_features(ohlcv_by_symbol, fs.ohlcv)

        # Winsorize OHLCV features per symbol (1st-99th percentile)
        for symbol in ohlcv_features:
            for fname in fs.ohlcv:
                col = ohlcv_features[symbol][fname]
                ohlcv_features[symbol][fname] = _winsorize(col)

        # Compute fundamental features
        context.log.info("Computing fundamental features...")
        fundamental_features = _compute_fundamental_features(
            ohlcv_by_symbol, metrics_by_symbol, fs.fundamental
        )

        # Winsorize fundamental features per symbol
        for symbol in fundamental_features:
            for fname in fs.fundamental:
                col = fundamental_features[symbol][fname]
                fundamental_features[symbol][fname] = _winsorize(col)

        # Compute cross-sectional features
        context.log.info("Computing cross-sectional features...")
        cs_features = _compute_cross_sectional_features(
            ohlcv_by_symbol, metrics_by_symbol, fs.cross_sectional
        )

        # Compute regime features
        regime_df = pd.DataFrame()
        if fs.regime:
            context.log.info("Computing regime features for universe: %s",
                             config.regime_universe)
            regime_df = _compute_regime_features_pipeline(
                conn, config.regime_universe, config.start_date, config.end_date
            )
            context.log.info("Computed regime features: %d rows", len(regime_df))

        # Compute options features
        options_features: dict[str, pd.DataFrame] = {}
        if fs.options:
            context.log.info("Loading canonical options chain...")
            options_df = _load_options_chain(conn, tickers, config.start_date, config.end_date)
            context.log.info("Loaded %d options chain rows", len(options_df))
            options_features = _compute_options_features_pipeline(
                options_df, ohlcv_by_symbol, fs.options
            )
            context.log.info(
                "Computed options features for %d underlyings", len(options_features)
            )

        # Compute insider features
        insider_features: dict[str, pd.DataFrame] = {}
        if fs.insider:
            context.log.info("Loading canonical insider trades...")
            insider_df = _load_insider_trades(conn, tickers, config.start_date, config.end_date)
            context.log.info("Loaded %d insider trade rows", len(insider_df))
            insider_features = _compute_insider_features_pipeline(
                insider_df, ohlcv_by_symbol, metrics_by_symbol, fs.insider
            )
            context.log.info(
                "Computed insider features for %d symbols", len(insider_features)
            )

        # Compute institutional features
        inst_features: dict[str, pd.DataFrame] = {}
        if fs.institutional:
            context.log.info("Loading canonical inst_ownership and inst_holdings...")
            ownership_df = _load_inst_ownership(conn, tickers, config.start_date, config.end_date)
            holdings_df = _load_inst_holdings(conn, tickers, config.start_date, config.end_date)
            context.log.info(
                "Loaded %d inst_ownership rows, %d holdings rows",
                len(ownership_df), len(holdings_df),
            )
            inst_features = _compute_inst_features_pipeline(
                ownership_df, holdings_df, ohlcv_by_symbol, metrics_by_symbol, fs.institutional
            )
            context.log.info(
                "Computed inst features for %d symbols", len(inst_features)
            )

        # Write all features to QuestDB
        context.log.info("Writing features to QuestDB...")
        total_written = 0

        with _ilp_sender(qdb) as sender:
            for symbol, ohlcv_df_sym in ohlcv_by_symbol.items():
                # Merge all feature categories for this symbol
                all_features: dict[str, pd.Series] = {}

                # OHLCV
                if symbol in ohlcv_features:
                    for fname in fs.ohlcv:
                        all_features[fname] = ohlcv_features[symbol][fname]

                # Fundamental
                if symbol in fundamental_features:
                    for fname in fs.fundamental:
                        all_features[fname] = fundamental_features[symbol][fname]

                # Cross-sectional
                if symbol in cs_features:
                    for fname in fs.cross_sectional:
                        if fname in cs_features[symbol].columns:
                            all_features[fname] = cs_features[symbol][fname]

                # Regime (broadcast to all symbols)
                if not regime_df.empty:
                    for fname in fs.regime:
                        if fname in regime_df.columns:
                            # Align regime features to this symbol's timestamps
                            sym_ts = ohlcv_df_sym["timestamp"]
                            regime_aligned = regime_df[fname].reindex(
                                sym_ts, method="ffill"
                            )
                            all_features[fname] = regime_aligned

                # Options (per-symbol, join by timestamp)
                if symbol in options_features:
                    opt_df = options_features[symbol]
                    sym_ts = ohlcv_df_sym["timestamp"].reset_index(drop=True)
                    # Align options timestamps to OHLCV timestamps via merge
                    aligned = pd.merge_asof(
                        pd.DataFrame({"timestamp": sym_ts}),
                        opt_df.sort_values("timestamp"),
                        on="timestamp",
                        direction="nearest",
                        tolerance=pd.Timedelta("1d"),
                    )
                    for fname in fs.options:
                        if fname in aligned.columns:
                            all_features[fname] = aligned[fname].reset_index(drop=True)

                # Insider (per-symbol, aligned by timestamp)
                if symbol in insider_features:
                    ins_df = insider_features[symbol]
                    sym_ts = ohlcv_df_sym["timestamp"].reset_index(drop=True)
                    aligned_ins = pd.merge_asof(
                        pd.DataFrame({"timestamp": sym_ts}),
                        ins_df.sort_values("timestamp"),
                        on="timestamp",
                        direction="nearest",
                        tolerance=pd.Timedelta("1d"),
                    )
                    for fname in fs.insider:
                        if fname in aligned_ins.columns:
                            all_features[fname] = aligned_ins[fname].reset_index(drop=True)

                # Institutional (per-symbol, aligned by timestamp)
                if symbol in inst_features:
                    inst_df_sym = inst_features[symbol]
                    sym_ts = ohlcv_df_sym["timestamp"].reset_index(drop=True)
                    aligned_inst = pd.merge_asof(
                        pd.DataFrame({"timestamp": sym_ts}),
                        inst_df_sym.sort_values("timestamp"),
                        on="timestamp",
                        direction="nearest",
                        tolerance=pd.Timedelta("1d"),
                    )
                    for fname in fs.institutional:
                        if fname in aligned_inst.columns:
                            all_features[fname] = aligned_inst[fname].reset_index(drop=True)

                written = _write_features(
                    sender, symbol, ohlcv_df_sym["timestamp"],
                    all_features, config.feature_set,
                    config.feature_set_version, now,
                )
                total_written += written

            sender.flush()

        _total_written = total_written
        context.log.info(
            "Feature pipeline complete: %d rows written for %d symbols",
            total_written, len(ohlcv_by_symbol),
        )
    except Exception as exc:
        _exc = exc
        raise
    finally:
        if conn is not None:
            conn.close()
        record_finish(
            "feature_pipeline", context.run_id,
            "failed" if _exc else "success",
            rows_written=None if _exc else _total_written,
            failure_cause=str(_exc)[:200] if _exc else None,
        )


@job(tags={"yats/concurrency_pool": "feature", "dagster/priority": "20"})
def feature_pipeline():
    """Dagster job: compute features from canonical data (full recompute)."""
    feature_pipeline_op()
