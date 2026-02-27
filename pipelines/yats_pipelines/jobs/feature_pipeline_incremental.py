"""Dagster incremental feature pipeline — watermark-based.

Reads feature_watermarks per symbol + feature_set, fetches canonical data
from last_computed_date forward (with lookback for rolling windows), computes
only new features, appends to features table via ILP, updates watermarks.

Cross-sectional features use a universe-level watermark and are computed as
full-universe batches per date.

Input: universe, feature_set
Output: rows appended to features table (from watermark forward), watermarks updated
Dependencies: canonical tables, feature_watermarks table
Trigger: yats.features.compute_incremental
"""

import logging
from datetime import datetime, timedelta, timezone

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

# Maximum lookback windows per feature (trading days).
# Used to determine how much extra historical data to fetch so rolling
# computations produce valid values for the new-data dates.
_LOOKBACK_DAYS: dict[str, int] = {
    "ret_1d": 1,
    "ret_5d": 5,
    "ret_21d": 21,
    "rv_21d": 22,      # 21-day rolling + 1 for ret_1d shift
    "rv_63d": 64,      # 63-day rolling + 1 for ret_1d shift
    "dist_20d_high": 20,
    "dist_20d_low": 20,
    "mom_3m": 63,
    "mom_12m_excl_1m": 252,
    "log_mkt_cap": 0,
    "size_rank": 0,
    "value_rank": 0,
    # regime features
    "market_vol_20d": 20,
    "market_trend_20d": 20,
    "dispersion_20d": 20,
    "corr_mean_20d": 20,
}

# Cross-sectional watermark uses this sentinel symbol
_CS_WATERMARK_SYMBOL = "__universe__"
_REGIME_WATERMARK_SYMBOL = "__regime__"


class IncrementalFeaturePipelineConfig(Config):
    """Run config for the incremental feature pipeline."""

    universe: str = "sp500"
    feature_set: str = "core_v1"
    feature_set_version: str = "1.0"
    regime_universe: list[str] = list(DEFAULT_REGIME_UNIVERSE)


# ---------------------------------------------------------------------------
# Helpers (shared with full-recompute pipeline)
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


def _load_universe(name: str) -> list[str]:
    import pathlib
    config_dir = pathlib.Path(__file__).resolve().parents[3] / "configs" / "universes"
    path = config_dir / f"{name}.yml"
    if not path.exists():
        raise FileNotFoundError(f"Universe config not found: {path}")
    with open(path) as f:
        data = yaml.safe_load(f)
    return data["tickers"]


# ---------------------------------------------------------------------------
# Watermark operations
# ---------------------------------------------------------------------------


def _read_watermarks(
    conn, feature_set: str,
) -> dict[str, datetime]:
    """Read per-symbol watermarks for a feature set.

    Returns {symbol: last_computed_date} mapping. Missing symbols have no entry.
    The special symbol __universe__ holds the cross-sectional watermark.
    The special symbol __regime__ holds the regime watermark.
    """
    cur = conn.cursor()
    # QuestDB: use latest by for dedup
    cur.execute(
        "SELECT symbol, last_computed_date "
        "FROM feature_watermarks "
        "WHERE feature_set = %s "
        "LATEST ON updated_at PARTITION BY symbol",
        (feature_set,),
    )
    rows = cur.fetchall()
    cur.close()

    result = {}
    for symbol, last_date in rows:
        if isinstance(last_date, str):
            last_date = pd.Timestamp(last_date)
        result[symbol] = pd.Timestamp(last_date, tz="UTC")
    return result


def _write_watermark(
    sender,
    symbol: str,
    feature_set: str,
    feature_set_version: str,
    last_computed_date: pd.Timestamp,
    now: datetime,
):
    """Write/update watermark for a symbol + feature_set."""
    sender.row(
        "feature_watermarks",
        symbols={
            "symbol": symbol,
            "feature_set": feature_set,
            "feature_set_version": feature_set_version,
        },
        columns={
            "last_computed_date": _ts_nanos(last_computed_date),
        },
        at=_ts_nanos(now),
    )


# ---------------------------------------------------------------------------
# Data loading (with lookback support)
# ---------------------------------------------------------------------------


def _load_ohlcv_from(
    conn, tickers: list[str], start_date: str, ts_col: str = "timestamp",
) -> pd.DataFrame:
    """Load canonical OHLCV data from start_date forward."""
    ticker_list = ",".join(f"'{t}'" for t in tickers)
    where = f" WHERE symbol IN ({ticker_list})"
    if start_date:
        where += f" AND {ts_col} >= '{start_date}T00:00:00.000000Z'"

    query = (
        f"SELECT timestamp, symbol, open, high, low, close, volume "
        f"FROM canonical_equity_ohlcv{where} ORDER BY symbol, timestamp"
    )
    cur = conn.cursor()
    cur.execute(query)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    cur.close()

    if not rows:
        return pd.DataFrame(
            columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"]
        )
    df = pd.DataFrame(rows, columns=columns)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


def _load_financial_metrics_from(
    conn, tickers: list[str], start_date: str,
) -> pd.DataFrame:
    """Load canonical financial metrics from start_date forward."""
    ticker_list = ",".join(f"'{t}'" for t in tickers)
    where = f" WHERE symbol IN ({ticker_list})"
    if start_date:
        where += f" AND timestamp >= '{start_date}T00:00:00.000000Z'"

    fields = [
        "timestamp", "symbol", "pe_ratio", "ps_ratio", "pb_ratio",
        "ev_ebitda", "roe", "gross_margin", "operating_margin",
        "fcf_margin", "debt_to_equity", "eps_growth_yoy",
        "revenue_growth_yoy", "shares_outstanding",
    ]
    field_str = ", ".join(fields)
    query = (
        f"SELECT {field_str} FROM canonical_financial_metrics{where} "
        f"ORDER BY symbol, timestamp"
    )
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


def _compute_lookback_start(
    watermark: pd.Timestamp, feature_names: list[str],
) -> str:
    """Compute the earliest date to fetch, accounting for lookback windows."""
    max_lookback = max(_LOOKBACK_DAYS.get(f, 0) for f in feature_names) if feature_names else 0
    # Convert trading days to calendar days (rough: 1.5x for weekends/holidays)
    calendar_days = int(max_lookback * 1.5) + 5  # buffer
    start = watermark - timedelta(days=calendar_days)
    return start.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Feature writing (from full-recompute pipeline)
# ---------------------------------------------------------------------------

_FEATURE_COLUMNS = [
    "ret_1d", "ret_5d", "ret_21d", "rv_21d", "rv_63d",
    "dist_20d_high", "dist_20d_low",
    "mom_3m", "mom_12m_excl_1m", "log_mkt_cap", "size_rank", "value_rank",
    "pe_ttm", "ps_ttm", "pb", "ev_ebitda", "roe", "gross_margin",
    "operating_margin", "fcf_margin", "debt_equity", "eps_growth_1y",
    "revenue_growth_1y",
    "market_vol_20d", "market_trend_20d", "dispersion_20d", "corr_mean_20d",
]


def _winsorize(series: pd.Series, lower: float = 0.01, upper: float = 0.99) -> pd.Series:
    lo = series.quantile(lower)
    hi = series.quantile(upper)
    return series.clip(lo, hi)


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
# Per-symbol incremental computation
# ---------------------------------------------------------------------------


def _compute_per_symbol_incremental(
    conn,
    tickers: list[str],
    watermarks: dict[str, datetime],
    fs,
    feature_set: str,
    feature_set_version: str,
    qdb: QuestDBResource,
    now: datetime,
    context: OpExecutionContext,
) -> int:
    """Compute per-symbol OHLCV + fundamental features incrementally.

    For each symbol:
    1. Read watermark (last_computed_date)
    2. Fetch canonical data from (watermark - max_lookback)
    3. Compute features on full range
    4. Filter to only dates > watermark
    5. Write new dates via ILP
    6. Update watermark
    """
    per_symbol_features = fs.ohlcv + fs.fundamental
    if not per_symbol_features:
        return 0

    total_written = 0

    with _ilp_sender(qdb) as sender:
        for symbol in tickers:
            wm = watermarks.get(symbol)

            if wm is not None:
                lookback_start = _compute_lookback_start(wm, per_symbol_features)
            else:
                lookback_start = ""  # No watermark = compute all

            # Load data
            ohlcv_df = _load_ohlcv_from(conn, [symbol], lookback_start)
            if ohlcv_df.empty:
                context.log.debug("No OHLCV data for %s from %s", symbol, lookback_start or "start")
                continue

            ohlcv_df = ohlcv_df.sort_values("timestamp").reset_index(drop=True)

            # Compute OHLCV features
            ohlcv_feats: dict[str, pd.Series] = {}
            for fname in fs.ohlcv:
                fn = registry.get(fname)
                result = fn(ohlcv_df)
                ohlcv_feats[fname] = _winsorize(result)

            # Compute fundamental features
            fund_feats: dict[str, pd.Series] = {}
            if fs.fundamental:
                metrics_df = _load_financial_metrics_from(conn, [symbol], lookback_start)
                if not metrics_df.empty:
                    metrics_sorted = metrics_df.sort_values("timestamp")
                    merged = pd.merge_asof(
                        ohlcv_df[["timestamp"]].sort_values("timestamp"),
                        metrics_sorted,
                        on="timestamp",
                        direction="backward",
                    )
                    for fname in fs.fundamental:
                        fn = registry.get(fname)
                        result = fn(merged)
                        fund_feats[fname] = _winsorize(result)
                else:
                    for fname in fs.fundamental:
                        fund_feats[fname] = pd.Series(np.nan, index=ohlcv_df.index)

            # Filter to new dates only (after watermark)
            if wm is not None:
                new_mask = ohlcv_df["timestamp"] > wm
            else:
                new_mask = pd.Series(True, index=ohlcv_df.index)

            new_indices = ohlcv_df.index[new_mask]
            if len(new_indices) == 0:
                context.log.debug("No new data for %s after watermark", symbol)
                continue

            # Slice features to new dates
            new_timestamps = ohlcv_df.loc[new_indices, "timestamp"]
            all_features: dict[str, pd.Series] = {}
            for fname, series in {**ohlcv_feats, **fund_feats}.items():
                all_features[fname] = series.loc[new_indices].reset_index(drop=True)

            new_timestamps = new_timestamps.reset_index(drop=True)

            written = _write_features(
                sender, symbol, new_timestamps,
                all_features, feature_set, feature_set_version, now,
            )
            total_written += written

            # Update watermark to latest date we computed
            if written > 0:
                latest_date = new_timestamps.max()
                _write_watermark(
                    sender, symbol, feature_set, feature_set_version,
                    latest_date, now,
                )

            context.log.debug(
                "%s: %d new rows (watermark: %s → %s)",
                symbol, written,
                wm.isoformat() if wm else "none",
                new_timestamps.max().isoformat() if written > 0 else "unchanged",
            )

        sender.flush()

    return total_written


# ---------------------------------------------------------------------------
# Cross-sectional incremental computation
# ---------------------------------------------------------------------------


def _compute_cross_sectional_incremental(
    conn,
    tickers: list[str],
    watermarks: dict[str, datetime],
    fs,
    feature_set: str,
    feature_set_version: str,
    qdb: QuestDBResource,
    now: datetime,
    context: OpExecutionContext,
) -> int:
    """Compute cross-sectional features incrementally.

    Cross-sectional features require the full universe per date, so they use
    a universe-level watermark (__universe__). New dates are computed as
    full-universe batches.
    """
    if not fs.cross_sectional:
        return 0

    cs_wm = watermarks.get(_CS_WATERMARK_SYMBOL)

    # Lookback for cross-sectional per-symbol computations (mom_3m, mom_12m_excl_1m)
    if cs_wm is not None:
        lookback_start = _compute_lookback_start(cs_wm, fs.cross_sectional)
    else:
        lookback_start = ""

    # Load OHLCV for all tickers
    ohlcv_df = _load_ohlcv_from(conn, tickers, lookback_start)
    if ohlcv_df.empty:
        context.log.warning("No OHLCV data for cross-sectional computation")
        return 0

    # Load metrics for all tickers (needed for log_mkt_cap, value_rank)
    metrics_df = _load_financial_metrics_from(conn, tickers, lookback_start)

    ohlcv_by_symbol = {
        symbol: group.sort_values("timestamp").reset_index(drop=True)
        for symbol, group in ohlcv_df.groupby("symbol")
    }
    metrics_by_symbol = {
        symbol: group.sort_values("timestamp").reset_index(drop=True)
        for symbol, group in metrics_df.groupby("symbol")
    } if not metrics_df.empty else {}

    # Reuse the full-recompute cross-sectional logic from feature_pipeline
    from yats_pipelines.jobs.feature_pipeline import _compute_cross_sectional_features

    cs_features = _compute_cross_sectional_features(
        ohlcv_by_symbol, metrics_by_symbol, fs.cross_sectional,
    )

    # Filter to new dates only
    total_written = 0
    with _ilp_sender(qdb) as sender:
        latest_date = None
        for symbol, feat_df in cs_features.items():
            if "timestamp" not in feat_df.columns:
                continue

            if cs_wm is not None:
                new_mask = feat_df["timestamp"] > cs_wm
            else:
                new_mask = pd.Series(True, index=feat_df.index)

            new_indices = feat_df.index[new_mask]
            if len(new_indices) == 0:
                continue

            new_timestamps = feat_df.loc[new_indices, "timestamp"].reset_index(drop=True)
            all_features: dict[str, pd.Series] = {}
            for fname in fs.cross_sectional:
                if fname in feat_df.columns:
                    all_features[fname] = feat_df.loc[new_indices, fname].reset_index(drop=True)

            written = _write_features(
                sender, symbol, new_timestamps,
                all_features, feature_set, feature_set_version, now,
            )
            total_written += written

            if written > 0:
                sym_latest = new_timestamps.max()
                if latest_date is None or sym_latest > latest_date:
                    latest_date = sym_latest

        # Update universe-level watermark
        if latest_date is not None:
            _write_watermark(
                sender, _CS_WATERMARK_SYMBOL, feature_set, feature_set_version,
                latest_date, now,
            )

        sender.flush()

    return total_written


# ---------------------------------------------------------------------------
# Regime incremental computation
# ---------------------------------------------------------------------------


def _compute_regime_incremental(
    conn,
    tickers: list[str],
    regime_universe: list[str],
    watermarks: dict[str, datetime],
    fs,
    feature_set: str,
    feature_set_version: str,
    qdb: QuestDBResource,
    now: datetime,
    context: OpExecutionContext,
) -> int:
    """Compute regime features incrementally.

    Regime features are broadcast to all symbols, so we use a single
    __regime__ watermark and compute from that point forward.
    """
    if not fs.regime:
        return 0

    regime_wm = watermarks.get(_REGIME_WATERMARK_SYMBOL)

    if regime_wm is not None:
        lookback_start = _compute_lookback_start(regime_wm, fs.regime)
    else:
        lookback_start = ""

    # Load regime universe OHLCV
    ohlcv_df = _load_ohlcv_from(conn, regime_universe, lookback_start)
    if ohlcv_df.empty:
        context.log.warning("No OHLCV data for regime computation")
        return 0

    # Pivot to wide format for regime computation
    pivot = ohlcv_df.pivot_table(
        index="timestamp", columns="symbol", values="close"
    )

    regime_df = compute_regime_features(pivot)
    if regime_df.empty:
        return 0

    # Filter to new dates only
    if regime_wm is not None:
        new_mask = regime_df.index > regime_wm
        regime_new = regime_df.loc[new_mask]
    else:
        regime_new = regime_df

    if regime_new.empty:
        return 0

    # Load ticker OHLCV timestamps to align regime features
    # We need all ticker timestamps that are new
    per_symbol_wms = {s: watermarks.get(s) for s in tickers}

    total_written = 0
    with _ilp_sender(qdb) as sender:
        for symbol in tickers:
            sym_wm = per_symbol_wms.get(symbol)
            # Get this symbol's timestamps from canonical data
            sym_ohlcv = _load_ohlcv_from(conn, [symbol], lookback_start)
            if sym_ohlcv.empty:
                continue

            sym_ohlcv = sym_ohlcv.sort_values("timestamp").reset_index(drop=True)

            # Filter to new timestamps
            if sym_wm is not None:
                new_mask = sym_ohlcv["timestamp"] > sym_wm
            else:
                new_mask = pd.Series(True, index=sym_ohlcv.index)

            new_ts = sym_ohlcv.loc[new_mask, "timestamp"]
            if new_ts.empty:
                continue

            # Align regime features to symbol timestamps
            regime_features: dict[str, pd.Series] = {}
            for fname in fs.regime:
                if fname in regime_new.columns:
                    # Reindex regime to symbol timestamps with forward-fill
                    aligned = regime_df[fname].reindex(new_ts.values, method="ffill")
                    regime_features[fname] = pd.Series(aligned.values)

            new_ts_reset = new_ts.reset_index(drop=True)
            written = _write_features(
                sender, symbol, new_ts_reset,
                regime_features, feature_set, feature_set_version, now,
            )
            total_written += written

        # Update regime watermark
        if total_written > 0:
            latest_regime = regime_new.index.max()
            _write_watermark(
                sender, _REGIME_WATERMARK_SYMBOL, feature_set, feature_set_version,
                pd.Timestamp(latest_regime), now,
            )

        sender.flush()

    return total_written


# ---------------------------------------------------------------------------
# Dagster op + job
# ---------------------------------------------------------------------------


@op
def feature_pipeline_incremental_op(
    context: OpExecutionContext, config: IncrementalFeaturePipelineConfig,
):
    """Incremental feature pipeline with watermark-based tracking.

    1. Read watermarks per symbol + feature_set
    2. Fetch canonical data from watermark forward (with lookback)
    3. Compute features only for new data
    4. Append to features table via ILP
    5. Update watermarks
    """
    qdb = QuestDBResource()
    now = datetime.now(timezone.utc)

    # Load feature set definition
    fs = registry.load_feature_set(config.feature_set)
    context.log.info(
        "Incremental feature pipeline: set '%s' (%d features)",
        fs.name, len(fs.all_features),
    )

    # Load universe
    tickers = _load_universe(config.universe)
    context.log.info("Universe '%s': %d tickers", config.universe, len(tickers))

    conn = _pg_conn(qdb)
    conn.autocommit = True

    try:
        # Read all watermarks for this feature set
        watermarks = _read_watermarks(conn, config.feature_set)
        context.log.info(
            "Read %d watermarks (%d per-symbol, cs=%s, regime=%s)",
            len(watermarks),
            len([k for k in watermarks if k not in (_CS_WATERMARK_SYMBOL, _REGIME_WATERMARK_SYMBOL)]),
            watermarks.get(_CS_WATERMARK_SYMBOL, "none"),
            watermarks.get(_REGIME_WATERMARK_SYMBOL, "none"),
        )

        # 1. Per-symbol features (OHLCV + fundamental)
        context.log.info("Computing per-symbol features incrementally...")
        per_symbol_written = _compute_per_symbol_incremental(
            conn, tickers, watermarks, fs,
            config.feature_set, config.feature_set_version,
            qdb, now, context,
        )
        context.log.info("Per-symbol: %d new rows written", per_symbol_written)

        # 2. Cross-sectional features (full universe per date)
        context.log.info("Computing cross-sectional features incrementally...")
        cs_written = _compute_cross_sectional_incremental(
            conn, tickers, watermarks, fs,
            config.feature_set, config.feature_set_version,
            qdb, now, context,
        )
        context.log.info("Cross-sectional: %d new rows written", cs_written)

        # 3. Regime features
        regime_written = 0
        if fs.regime:
            context.log.info("Computing regime features incrementally...")
            regime_written = _compute_regime_incremental(
                conn, tickers, config.regime_universe, watermarks, fs,
                config.feature_set, config.feature_set_version,
                qdb, now, context,
            )
            context.log.info("Regime: %d new rows written", regime_written)

        total = per_symbol_written + cs_written + regime_written
        context.log.info(
            "Incremental feature pipeline complete: %d total rows "
            "(%d per-symbol, %d cross-sectional, %d regime)",
            total, per_symbol_written, cs_written, regime_written,
        )

    finally:
        conn.close()


@job
def feature_pipeline_incremental():
    """Dagster job: incremental feature computation with watermark tracking."""
    feature_pipeline_incremental_op()
