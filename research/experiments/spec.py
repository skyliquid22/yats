"""ExperimentSpec — canonical experiment configuration.

Implements the ExperimentSpec dataclass with composable inheritance for
sweep ergonomics. All fields, sub-configs, validation, canonical JSON
serialization, and SHA256 ID derivation per PRD §8.1 and Appendix C.
"""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass, field, fields
from datetime import date
from typing import Any, Mapping


# ---------------------------------------------------------------------------
# Sub-configs
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CostConfig:
    transaction_cost_bp: float
    slippage_bp: float = 0.0

    def __post_init__(self):
        if self.transaction_cost_bp < 0:
            raise ValueError("transaction_cost_bp must be >= 0")
        if self.slippage_bp < 0:
            raise ValueError("slippage_bp must be >= 0")


@dataclass(frozen=True)
class EvaluationSplitConfig:
    train_ratio: float
    test_ratio: float
    test_window_months: int | None = None

    _VALID_WINDOWS = {1, 3, 4, 6, 12}

    def __post_init__(self):
        if self.train_ratio <= 0:
            raise ValueError("train_ratio must be > 0")
        if self.test_ratio <= 0:
            raise ValueError("test_ratio must be > 0")
        if abs(self.train_ratio + self.test_ratio - 1.0) > 1e-9:
            raise ValueError(
                f"train_ratio + test_ratio must equal 1.0, "
                f"got {self.train_ratio} + {self.test_ratio}"
            )
        if (self.test_window_months is not None
                and self.test_window_months not in self._VALID_WINDOWS):
            raise ValueError(
                f"test_window_months must be one of {sorted(self._VALID_WINDOWS)}, "
                f"got {self.test_window_months}"
            )


@dataclass(frozen=True)
class RiskConfig:
    # Global limits
    max_gross_exposure: float = 1.0
    min_cash: float = 0.0
    max_symbol_weight: float = 1.0
    max_daily_turnover: float = 1.0
    max_active_positions: int = 999

    # Kill switches
    daily_loss_limit: float = -1.0
    trailing_drawdown_limit: float = -1.0

    # Net exposure & leverage
    max_net_exposure: float = 1.0
    max_leverage: float = 1.0

    # Concentration
    max_top_n_concentration: float = 0.60
    top_n: int = 5

    # Liquidity
    max_adv_pct: float = 0.05

    # Volatility
    target_vol: float = 0.15
    vol_regime_threshold: float = 0.30
    vol_brake_position_reduction: float = 0.50
    vol_brake_exposure_reduction: float = 0.50

    # Signal constraints
    min_confidence: float = 0.0
    min_holding_period: int = 1

    # Size reduce
    minimum_order_threshold: float = 0.01

    # Operational
    max_broker_errors: int = 5
    data_staleness_threshold: float = 300.0


@dataclass(frozen=True)
class ExecutionSimConfig:
    enabled: bool = False
    slippage_model: str = "flat"
    slippage_bp: float = 5.0
    fill_probability: float = 0.99
    range_shrink: float = 0.5

    def __post_init__(self):
        if self.slippage_model not in ("flat", "volume_scaled"):
            raise ValueError(
                f"slippage_model must be 'flat' or 'volume_scaled', "
                f"got '{self.slippage_model}'"
            )
        if self.slippage_bp < 0:
            raise ValueError("slippage_bp must be >= 0")
        if not 0 <= self.fill_probability <= 1:
            raise ValueError("fill_probability must be in [0, 1]")
        if not 0 < self.range_shrink <= 1:
            raise ValueError("range_shrink must be in (0, 1]")


# ---------------------------------------------------------------------------
# Allowed policies
# ---------------------------------------------------------------------------

_VALID_POLICIES = {"equal_weight", "sma", "ppo", "sac"}


def _is_valid_policy(p: str) -> bool:
    """Check if policy string is valid (includes sac_* variants)."""
    return p in _VALID_POLICIES or p.startswith("sac_")


# ---------------------------------------------------------------------------
# ExperimentSpec
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ExperimentSpec:
    """Canonical experiment configuration — PRD §8.1 / Appendix C.

    All fields are frozen after creation. The experiment_id is derived from
    the canonical JSON serialization (content-addressed, deterministic).
    """

    # Required fields (10)
    experiment_name: str
    symbols: tuple[str, ...]
    start_date: date
    end_date: date
    interval: str
    feature_set: str
    policy: str
    policy_params: Mapping[str, Any]
    cost_config: CostConfig
    seed: int

    # Optional fields (9)
    evaluation_split: EvaluationSplitConfig | None = None
    risk_config: RiskConfig = field(default_factory=RiskConfig)
    execution_sim: ExecutionSimConfig | None = None
    notes: str | None = None
    regime_feature_set: str | None = None
    regime_labeling: str | None = None
    hierarchy_enabled: bool = False
    controller_config: Mapping[str, Any] | None = None
    allocator_by_mode: Mapping[str, Mapping[str, Any]] | None = None

    # Frozen hashes (6, set at creation time)
    regime_thresholds_hash: str = ""
    regime_detector_version: str = ""
    regime_universe: tuple[str, ...] = ()
    feature_set_yaml_hash: str = ""
    risk_overrides: Mapping[str, Any] | None = None
    _base_spec_hash: str | None = None

    def __post_init__(self):
        # --- Required field validation ---
        if not self.experiment_name:
            raise ValueError("experiment_name must be non-empty")

        # Sort and deduplicate symbols
        if not self.symbols:
            raise ValueError("symbols must be non-empty")
        sorted_deduped = tuple(sorted(set(self.symbols)))
        if sorted_deduped != self.symbols:
            object.__setattr__(self, "symbols", sorted_deduped)

        if self.start_date >= self.end_date:
            raise ValueError(
                f"start_date ({self.start_date}) must be before "
                f"end_date ({self.end_date})"
            )

        if self.interval != "daily":
            raise ValueError(f"interval must be 'daily', got '{self.interval}'")

        if not self.feature_set:
            raise ValueError("feature_set must be non-empty")

        if not _is_valid_policy(self.policy):
            raise ValueError(
                f"policy must be one of {sorted(_VALID_POLICIES)} "
                f"(or sac_* variant), got '{self.policy}'"
            )

        # --- Hierarchy validation (PRD C.3) ---
        if self.hierarchy_enabled:
            if self.controller_config is None:
                raise ValueError(
                    "controller_config is required when hierarchy_enabled=True"
                )
            required_controller_keys = {
                "update_frequency", "vol_threshold_high",
                "trend_threshold_high", "dispersion_threshold_high",
                "min_hold_steps",
            }
            missing = required_controller_keys - set(self.controller_config)
            if missing:
                raise ValueError(
                    f"controller_config missing required keys: {sorted(missing)}"
                )
            valid_frequencies = {"weekly", "monthly", "every_k_steps"}
            freq = self.controller_config["update_frequency"]
            if freq not in valid_frequencies:
                raise ValueError(
                    f"controller_config.update_frequency must be one of "
                    f"{sorted(valid_frequencies)}, got '{freq}'"
                )
            if freq == "every_k_steps" and "k_steps" not in self.controller_config:
                raise ValueError(
                    "controller_config must include 'k_steps' when "
                    "update_frequency='every_k_steps'"
                )

            if self.allocator_by_mode is None:
                raise ValueError(
                    "allocator_by_mode is required when hierarchy_enabled=True"
                )
            required_modes = {"risk_on", "neutral", "defensive"}
            missing_modes = required_modes - set(self.allocator_by_mode)
            if missing_modes:
                raise ValueError(
                    f"allocator_by_mode missing required keys: "
                    f"{sorted(missing_modes)}"
                )

        # --- Regime labeling validation ---
        if self.regime_labeling is not None and self.regime_labeling not in ("v1", "v2"):
            raise ValueError(
                f"regime_labeling must be 'v1' or 'v2', got '{self.regime_labeling}'"
            )

    # ------------------------------------------------------------------
    # Canonical JSON + ID derivation (PRD C.4)
    # ------------------------------------------------------------------

    def to_canonical_dict(self) -> dict[str, Any]:
        """Produce a sorted, normalized dict for canonical serialization."""
        d: dict[str, Any] = {}
        for f in fields(self):
            val = getattr(self, f.name)
            d[f.name] = _normalize(val)
        return d

    @property
    def experiment_id(self) -> str:
        """SHA256 hex digest of canonical JSON — deterministic content-address."""
        canonical = json.dumps(
            self.to_canonical_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Composable inheritance (PRD §8.1 Composable Specs)
# ---------------------------------------------------------------------------

def resolve_inheritance(
    spec_dict: dict[str, Any],
    loader: Any = None,
) -> dict[str, Any]:
    """Resolve spec inheritance — produce a fully materialized spec dict.

    If *spec_dict* contains an ``"extends"`` key, the base spec is loaded
    (via *loader*), deep-merged with the overrides, and the ``extends`` /
    ``overrides`` keys are removed. The ``_base_spec_hash`` is set to the
    SHA256 of the base spec's canonical JSON.

    *loader* must be a callable that accepts a base spec name/path and
    returns a dict. If None, ``extends`` is treated as a file path relative
    to ``configs/``.

    Returns a new dict — the input is not mutated.
    """
    if "extends" not in spec_dict:
        return dict(spec_dict)

    base_name = spec_dict["extends"]
    overrides = spec_dict.get("overrides", {})

    if loader is not None:
        base = loader(base_name)
    else:
        base = _load_spec_file(base_name)

    # Recursively resolve if the base itself extends another spec
    base = resolve_inheritance(base, loader=loader)

    # Hash the base BEFORE applying overrides
    base_hash = _dict_sha256(base)

    # Deep merge: overrides win
    merged = _deep_merge(base, overrides)

    # Remove inheritance keys — materialized spec is self-contained
    merged.pop("extends", None)
    merged.pop("overrides", None)

    # Record lineage
    merged["_base_spec_hash"] = base_hash

    return merged


def _load_spec_file(name: str) -> dict[str, Any]:
    """Load a spec JSON file from the configs directory."""
    from pathlib import Path

    configs_dir = Path(__file__).resolve().parents[2] / "configs"
    path = configs_dir / name
    if not path.exists():
        # Try with .json extension
        path = configs_dir / f"{name}.json"
    if not path.exists():
        raise FileNotFoundError(f"Base spec not found: {name} (looked in {configs_dir})")
    with open(path) as f:
        return json.load(f)


def _deep_merge(base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    """Deep merge *overrides* into *base* (overrides win). Returns new dict."""
    result = copy.deepcopy(base)
    for key, val in overrides.items():
        if (
            isinstance(val, dict)
            and key in result
            and isinstance(result[key], dict)
        ):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = copy.deepcopy(val)
    return result


def _dict_sha256(d: dict[str, Any]) -> str:
    """SHA256 hex digest of a dict's canonical JSON."""
    canonical = json.dumps(d, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------

def _normalize(val: Any) -> Any:
    """Recursively normalize a value for canonical JSON."""
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float, str)):
        return val
    if isinstance(val, date):
        return val.isoformat()
    if isinstance(val, tuple):
        return [_normalize(v) for v in val]
    if isinstance(val, (list,)):
        return [_normalize(v) for v in val]
    if isinstance(val, Mapping):
        return {k: _normalize(v) for k, v in sorted(val.items())}
    # Dataclass sub-configs
    if hasattr(val, "__dataclass_fields__"):
        return {
            f.name: _normalize(getattr(val, f.name))
            for f in fields(val)
        }
    return val
