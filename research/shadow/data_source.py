"""ReplayMarketDataSource — build per-date snapshots from canonical QuestDB data.

Implements PRD Appendix F.1: loads canonical OHLCV (batch-only) and feature
data, aligns dates, and produces ordered snapshot dicts for shadow replay.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Sequence

import yaml

logger = logging.getLogger(__name__)


@dataclass
class Snapshot:
    """A single-date market snapshot for shadow replay (PRD F.1)."""

    as_of: datetime
    symbols: tuple[str, ...]
    panel: dict[str, dict[str, float]]
    regime_features: tuple[float, ...]
    regime_feature_names: tuple[str, ...]
    observation_columns: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "symbols": self.symbols,
            "panel": self.panel,
            "regime_features": self.regime_features,
            "regime_feature_names": self.regime_feature_names,
            "observation_columns": self.observation_columns,
        }


class ReplayMarketDataSource:
    """Build per-date snapshots from canonical QuestDB data for shadow replay.

    PRD Appendix F.1.  Queries canonical_equity_ohlcv (batch mode only) and
    the features table, aligns dates via intersection, and produces ordered
    Snapshot objects — one per trading date.
    """

    def __init__(
        self,
        symbols: Sequence[str],
        start_date: date,
        end_date: date,
        feature_set: str,
        regime_feature_set: str | None = None,
        *,
        pg_host: str = "localhost",
        pg_port: int = 8812,
        pg_user: str = "admin",
        pg_password: str = "quest",
        pg_database: str = "qdb",
        configs_dir: Path | None = None,
    ) -> None:
        self.symbols = tuple(sorted(set(symbols)))
        self.start_date = start_date
        self.end_date = end_date
        self.feature_set = feature_set
        self.regime_feature_set = regime_feature_set

        self._pg_host = pg_host
        self._pg_port = pg_port
        self._pg_user = pg_user
        self._pg_password = pg_password
        self._pg_database = pg_database

        if configs_dir is None:
            configs_dir = Path(__file__).resolve().parents[2] / "configs"
        self._configs_dir = configs_dir

        # Resolve feature columns from YAML config
        fs_path = self._configs_dir / "feature_sets" / f"{feature_set}.yml"
        if fs_path.exists():
            with open(fs_path) as f:
                fs_config = yaml.safe_load(f)
        else:
            fs_config = {"ohlcv": [], "cross_sectional": [], "fundamental": [], "regime": []}

        ohlcv_cols = fs_config.get("ohlcv", [])
        cs_cols = fs_config.get("cross_sectional", [])
        fund_cols = fs_config.get("fundamental", [])
        regime_cols = fs_config.get("regime", [])

        all_feature_cols = ohlcv_cols + cs_cols + fund_cols
        self._observation_columns = tuple(
            ["close"] + [c for c in all_feature_cols if c != "close"]
        )
        self._regime_columns = tuple(regime_cols) if regime_feature_set else ()

    @classmethod
    def from_experiment_spec(
        cls,
        spec: Any,
        *,
        pg_host: str = "localhost",
        pg_port: int = 8812,
        pg_user: str = "admin",
        pg_password: str = "quest",
        pg_database: str = "qdb",
        configs_dir: Path | None = None,
    ) -> ReplayMarketDataSource:
        """Construct from an ExperimentSpec."""
        return cls(
            symbols=spec.symbols,
            start_date=spec.start_date,
            end_date=spec.end_date,
            feature_set=spec.feature_set,
            regime_feature_set=spec.regime_feature_set,
            pg_host=pg_host,
            pg_port=pg_port,
            pg_user=pg_user,
            pg_password=pg_password,
            pg_database=pg_database,
            configs_dir=configs_dir,
        )

    def load_snapshots(self) -> list[Snapshot]:
        """Query QuestDB and build ordered snapshots, one per trading date.

        Steps (PRD F.1):
        1. Query canonical_equity_ohlcv (batch mode only)
        2. Query features table for same range + symbols + feature_set
        3. Align dates (intersection)
        4. Include regime features if regime_feature_set specified
        5. Build ordered snapshots
        """
        import psycopg2

        conn = psycopg2.connect(
            host=self._pg_host,
            port=self._pg_port,
            user=self._pg_user,
            password=self._pg_password,
            database=self._pg_database,
        )
        conn.autocommit = True

        try:
            canonical = self._query_canonical(conn)
            features = self._query_features(conn)
        finally:
            conn.close()

        return self._build_snapshots(canonical, features)

    # ------------------------------------------------------------------
    # Internal query helpers
    # ------------------------------------------------------------------

    def _where_clause(self, extra: str = "") -> str:
        """Common WHERE clause for symbol + date range filtering."""
        symbol_filter = ", ".join(f"'{s}'" for s in self.symbols)
        parts = [f"symbol IN ({symbol_filter})"]
        parts.append(f"timestamp >= '{self.start_date}'")
        parts.append(f"timestamp <= '{self.end_date}'")
        if extra:
            parts.append(extra)
        return " AND ".join(parts)

    def _query_canonical(self, conn: Any) -> dict[str, dict[str, dict[str, float]]]:
        """Query canonical_equity_ohlcv for batch-reconciled rows.

        Returns {date_str: {symbol: {field: value}}} for OHLCV data.
        """
        where = self._where_clause("reconcile_method = 'batch'")
        sql = (
            f"SELECT timestamp, symbol, open, high, low, close, volume, vwap "
            f"FROM canonical_equity_ohlcv "
            f"WHERE {where} "
            f"ORDER BY timestamp, symbol"
        )

        cur = conn.cursor()
        cur.execute(sql)
        col_names = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        cur.close()

        result: dict[str, dict[str, dict[str, float]]] = {}
        for row in rows:
            row_dict = dict(zip(col_names, row))
            ts = row_dict["timestamp"]
            date_key = _to_date_key(ts)
            sym = row_dict["symbol"]

            if date_key not in result:
                result[date_key] = {}
            result[date_key][sym] = {
                "open": _safe_float(row_dict.get("open")),
                "high": _safe_float(row_dict.get("high")),
                "low": _safe_float(row_dict.get("low")),
                "close": _safe_float(row_dict.get("close")),
                "volume": _safe_float(row_dict.get("volume")),
                "vwap": _safe_float(row_dict.get("vwap")),
            }

        logger.info(
            "Queried canonical_equity_ohlcv: %d dates, %d total rows",
            len(result), len(rows),
        )
        return result

    def _query_features(
        self, conn: Any,
    ) -> dict[str, dict[str, dict[str, float]]]:
        """Query features table for per-symbol features and regime features.

        Returns {date_str: {symbol: {feature: value}}}.
        Regime features are stored under a special key '__regime__'.
        """
        where = self._where_clause(f"feature_set = '{self.feature_set}'")
        sql = (
            f"SELECT * FROM features "
            f"WHERE {where} "
            f"ORDER BY timestamp, symbol"
        )

        cur = conn.cursor()
        cur.execute(sql)
        col_names = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        cur.close()

        regime_set = set(self._regime_columns)

        result: dict[str, dict[str, dict[str, float]]] = {}
        for row in rows:
            row_dict = dict(zip(col_names, row))
            ts = row_dict["timestamp"]
            date_key = _to_date_key(ts)
            sym = row_dict.get("symbol", "")

            if date_key not in result:
                result[date_key] = {}

            # Per-symbol observation features
            sym_data: dict[str, float] = {}
            for col in self._observation_columns:
                if col in row_dict:
                    sym_data[col] = _safe_float(row_dict[col])
            result[date_key][sym] = sym_data

            # Regime features (market-wide, same for all symbols — take first)
            if regime_set and "__regime__" not in result[date_key]:
                regime_data: dict[str, float] = {}
                for col in self._regime_columns:
                    if col in row_dict:
                        regime_data[col] = _safe_float(row_dict[col])
                if regime_data:
                    result[date_key]["__regime__"] = regime_data

        logger.info(
            "Queried features: %d dates, %d total rows",
            len(result), len(rows),
        )
        return result

    # ------------------------------------------------------------------
    # Snapshot building
    # ------------------------------------------------------------------

    def _build_snapshots(
        self,
        canonical: dict[str, dict[str, dict[str, float]]],
        features: dict[str, dict[str, dict[str, float]]],
    ) -> list[Snapshot]:
        """Align dates and build ordered snapshots."""
        canonical_dates = set(canonical.keys())
        feature_dates = set(features.keys())
        aligned_dates = sorted(canonical_dates & feature_dates)

        if not aligned_dates:
            logger.warning(
                "No aligned dates between canonical (%d) and features (%d)",
                len(canonical_dates), len(feature_dates),
            )
            return []

        logger.info(
            "Date alignment: canonical=%d, features=%d, aligned=%d",
            len(canonical_dates), len(feature_dates), len(aligned_dates),
        )

        snapshots: list[Snapshot] = []
        for date_key in aligned_dates:
            canon_day = canonical.get(date_key, {})
            feat_day = features.get(date_key, {})

            # Build panel: merge canonical OHLCV with feature data per symbol
            panel: dict[str, dict[str, float]] = {}
            for sym in self.symbols:
                sym_features = dict(feat_day.get(sym, {}))
                # Overlay canonical close (authoritative price)
                sym_canonical = canon_day.get(sym, {})
                if "close" in sym_canonical:
                    sym_features["close"] = sym_canonical["close"]
                panel[sym] = sym_features

            # Regime features
            regime_data = feat_day.get("__regime__", {})
            regime_values = tuple(
                regime_data.get(col, 0.0) for col in self._regime_columns
            )

            snapshots.append(Snapshot(
                as_of=_parse_date(date_key),
                symbols=self.symbols,
                panel=panel,
                regime_features=regime_values,
                regime_feature_names=self._regime_columns,
                observation_columns=self._observation_columns,
            ))

        logger.info("Built %d snapshots", len(snapshots))
        return snapshots


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _to_date_key(ts: Any) -> str:
    """Normalize a timestamp to a date string key (YYYY-MM-DD)."""
    if isinstance(ts, datetime):
        return ts.strftime("%Y-%m-%d")
    if isinstance(ts, date):
        return ts.isoformat()
    return str(ts)[:10]


def _parse_date(date_key: str) -> datetime:
    """Parse a date key string to a datetime."""
    return datetime.fromisoformat(date_key)


def _safe_float(val: Any) -> float:
    """Convert a value to float, defaulting to 0.0 on None/NaN."""
    if val is None:
        return 0.0
    try:
        f = float(val)
        # NaN check
        if f != f:
            return 0.0
        return f
    except (ValueError, TypeError):
        return 0.0
