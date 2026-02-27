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

# Import feature modules to trigger @feature registrations
import research.features.ohlcv_features  # noqa: F401
import research.features.cross_sectional_features  # noqa: F401
import research.features.fundamental_features  # noqa: F401
import research.features.regime_features_v1  # noqa: F401
from research.features.feature_registry import registry
from research.features.regime_features_v1 import (
    DEFAULT_REGIME_UNIVERSE,
    compute_regime_features,
)

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


def _date_clause(start_date: str, end_date: str, ts_col: str = "timestamp") -> str:
    parts = []
    if start_date:
        parts.append(f"{ts_col} >= '{start_date}T00:00:00.000000Z'")
    if end_date:
        parts.append(f"{ts_col} <= '{end_date}T23:59:59.999999Z'")
    return " WHERE " + " AND ".join(parts) if parts else ""


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
    where = _date_clause(start_date, end_date)
    ticker_list = ",".join(f"'{t}'" for t in tickers)
    if where:
        where += f" AND symbol IN ({ticker_list})"
    else:
        where = f" WHERE symbol IN ({ticker_list})"

    query = f"SELECT timestamp, symbol, open, high, low, close, volume FROM canonical_equity_ohlcv{where} ORDER BY symbol, timestamp"

    cur = conn.cursor()
    cur.execute(query)
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
    where = _date_clause(start_date, end_date)
    ticker_list = ",".join(f"'{t}'" for t in tickers)
    if where:
        where += f" AND symbol IN ({ticker_list})"
    else:
        where = f" WHERE symbol IN ({ticker_list})"

    fields = [
        "timestamp", "symbol", "pe_ratio", "ps_ratio", "pb_ratio",
        "ev_ebitda", "roe", "gross_margin", "operating_margin",
        "fcf_margin", "debt_to_equity", "eps_growth_yoy",
        "revenue_growth_yoy", "shares_outstanding",
    ]
    field_str = ", ".join(fields)
    query = f"SELECT {field_str} FROM canonical_financial_metrics{where} ORDER BY symbol, timestamp"

    cur = conn.cursor()
    cur.execute(query)
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
        feat_df["timestamp"] = ohlcv_df["timestamp"].values
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
    for i, ts in enumerate(timestamps):
        if pd.isna(ts):
            continue

        columns = {}
        has_any = False
        for col in _FEATURE_COLUMNS:
            if col in features:
                val = features[col].iloc[i] if hasattr(features[col], "iloc") else features[col]
                if isinstance(val, (float, np.floating)):
                    if not np.isnan(val):
                        columns[col] = float(val)
                        has_any = True
                elif val is not None:
                    columns[col] = float(val)
                    has_any = True

        if not has_any:
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
    qdb = QuestDBResource()
    now = datetime.now(timezone.utc)

    # Load feature set definition
    fs = registry.load_feature_set(config.feature_set)
    context.log.info(
        "Feature set '%s': %d features (%d ohlcv, %d cross-sectional, "
        "%d fundamental, %d regime)",
        fs.name, len(fs.all_features),
        len(fs.ohlcv), len(fs.cross_sectional),
        len(fs.fundamental), len(fs.regime),
    )

    # Load universe
    tickers = _load_universe(config.universe)
    context.log.info("Universe '%s': %d tickers", config.universe, len(tickers))

    conn = _pg_conn(qdb)
    conn.autocommit = True

    try:
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
                            all_features[fname] = regime_aligned.values

                written = _write_features(
                    sender, symbol, ohlcv_df_sym["timestamp"],
                    all_features, config.feature_set,
                    config.feature_set_version, now,
                )
                total_written += written

            sender.flush()

        context.log.info(
            "Feature pipeline complete: %d rows written for %d symbols",
            total_written, len(ohlcv_by_symbol),
        )
    finally:
        conn.close()


@job
def feature_pipeline():
    """Dagster job: compute features from canonical data (full recompute)."""
    feature_pipeline_op()
