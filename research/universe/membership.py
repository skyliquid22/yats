"""Point-in-time universe membership loader.

Reads the membership artifact written by scripts/build_pit_universe.py
(default: configs/universes/pit250_membership.parquet) and answers
"who was in the universe as of date X" without lookahead.

Semantics:

* ``symbols_as_of(d)`` returns the members from the latest rebalance date
  <= d. Exactly on a rebalance date, the new membership applies. Before the
  first rebalance date, membership is empty.
* ``all_symbols()`` is the union over all rebalances — every name that was
  ever a member — intended for historical data backfill (delisted names
  included).

Columns: ``rebalance_date`` (datetime), ``symbol`` (str), ``rank``
(1-based int, liquidity rank at that rebalance), ``median_dollar_volume``
(float, the ranking statistic).
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MEMBERSHIP_PATH = (
    _REPO_ROOT / "configs" / "universes" / "pit250_membership.parquet"
)

REQUIRED_COLUMNS = ("rebalance_date", "symbol", "rank", "median_dollar_volume")


def load_membership(path: str | Path | None = None) -> pd.DataFrame:
    """Load and validate the membership table.

    Args:
        path: Parquet artifact path; defaults to
            ``configs/universes/pit250_membership.parquet``.

    Returns:
        DataFrame with the REQUIRED_COLUMNS, ``rebalance_date`` coerced to
        tz-naive datetime64, sorted by (rebalance_date, rank).

    Raises:
        FileNotFoundError: If the artifact is missing (with the
            regeneration command in the message).
        ValueError: If required columns are missing.
    """
    resolved = Path(path) if path is not None else DEFAULT_MEMBERSHIP_PATH
    if not resolved.exists():
        raise FileNotFoundError(
            f"Membership artifact not found: {resolved}. Generate it with: "
            "PYTHONPATH=.:pipelines uv run python scripts/build_pit_universe.py"
        )
    frame = pd.read_parquet(resolved)
    missing = [c for c in REQUIRED_COLUMNS if c not in frame.columns]
    if missing:
        raise ValueError(
            f"Membership artifact {resolved} missing columns: {missing}"
        )
    frame = frame.copy()
    frame["rebalance_date"] = pd.to_datetime(frame["rebalance_date"])
    if getattr(frame["rebalance_date"].dt, "tz", None) is not None:
        frame["rebalance_date"] = frame["rebalance_date"].dt.tz_localize(None)
    return frame.sort_values(["rebalance_date", "rank"]).reset_index(drop=True)


def _coerce_date(value: str | date | datetime | pd.Timestamp) -> pd.Timestamp:
    """Normalize a date-like value to a tz-naive midnight Timestamp."""
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.normalize()


def _resolve(membership: pd.DataFrame | None) -> pd.DataFrame:
    return membership if membership is not None else load_membership()


def symbols_as_of(
    as_of: str | date | datetime | pd.Timestamp,
    membership: pd.DataFrame | None = None,
) -> list[str]:
    """Members from the latest rebalance date <= ``as_of``, in rank order.

    Exactly on a rebalance date the new membership applies; between
    rebalances the prior membership persists; before the first rebalance
    date the result is an empty list.
    """
    frame = _resolve(membership)
    if frame.empty:
        return []
    cutoff = _coerce_date(as_of)
    eligible = frame.loc[frame["rebalance_date"] <= cutoff]
    if eligible.empty:
        return []
    latest = eligible["rebalance_date"].max()
    current = eligible.loc[eligible["rebalance_date"] == latest]
    return current.sort_values("rank")["symbol"].tolist()


def all_symbols(membership: pd.DataFrame | None = None) -> list[str]:
    """Sorted list of every symbol ever a member — for historical backfill.

    Includes names that later delisted; a backfill over this list plus
    ``symbols_as_of`` gating reproduces the point-in-time universe with no
    survivorship bias.
    """
    frame = _resolve(membership)
    return sorted(frame["symbol"].unique().tolist())


def is_member(
    symbol: str,
    as_of: str | date | datetime | pd.Timestamp,
    membership: pd.DataFrame | None = None,
) -> bool:
    """True if ``symbol`` is a member of the universe as of ``as_of``."""
    return symbol in symbols_as_of(as_of, membership=membership)
