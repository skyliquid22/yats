"""Feature registry — config-driven feature management.

Loads feature set definitions from YAML configs, registers Python feature
implementations via @feature decorator, validates dependencies, and resolves
computation order.

Usage:
    from research.features.feature_registry import registry, feature

    @feature("ret_1d")
    def compute_ret_1d(close: pd.Series) -> pd.Series:
        return np.log(close / close.shift(1))

    fs = registry.load_feature_set("core_v1")
    # fs.ohlcv == ["ret_1d", "ret_5d", ...]
"""

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import yaml


@dataclass
class FeatureSet:
    """A loaded feature set definition from YAML."""

    name: str
    description: str
    ohlcv: list[str] = field(default_factory=list)
    cross_sectional: list[str] = field(default_factory=list)
    fundamental: list[str] = field(default_factory=list)
    regime: list[str] = field(default_factory=list)
    options: list[str] = field(default_factory=list)
    insider: list[str] = field(default_factory=list)
    institutional: list[str] = field(default_factory=list)

    @property
    def all_features(self) -> list[str]:
        return (
            self.ohlcv + self.cross_sectional + self.fundamental
            + self.regime + self.options + self.insider + self.institutional
        )

    def config_hash(self) -> str:
        """Deterministic hash for experiment lineage."""
        payload = json.dumps({
            "name": self.name,
            "ohlcv": self.ohlcv,
            "cross_sectional": self.cross_sectional,
            "fundamental": self.fundamental,
            "regime": self.regime,
            "options": self.options,
            "insider": self.insider,
            "institutional": self.institutional,
        }, sort_keys=True)
        return hashlib.sha256(payload.encode()).hexdigest()[:12]


class FeatureRegistry:
    """Global feature registry — maps feature names to implementations."""

    def __init__(self):
        self._implementations: dict[str, Callable] = {}
        self._lookbacks: dict[str, int] = {}
        self._configs_dir = Path(__file__).resolve().parents[2] / "configs" / "feature_sets"

    def register(self, name: str, fn: Callable, lookback: int = 0) -> None:
        self._implementations[name] = fn
        self._lookbacks[name] = lookback

    def get(self, name: str) -> Callable:
        if name not in self._implementations:
            raise KeyError(f"Feature '{name}' not registered. "
                           f"Available: {sorted(self._implementations.keys())}")
        return self._implementations[name]

    def feature_lookback(self, name: str) -> int:
        """Return the lookback (in bars) for a registered feature, or 0 if unknown."""
        return self._lookbacks.get(name, 0)

    def max_lookback(self, feature_set_name: str) -> int:
        """Return the maximum lookback (in bars) across all features in the given feature set.

        Reads the feature set YAML without requiring feature implementations to be
        registered — falls back to 0 for any feature not in the lookback registry.

        Raises RuntimeError if all features resolve to lookback=0, which indicates
        the feature modules have not been imported (and thus not registered their lookbacks).
        Callers must import the feature modules before calling this method.
        """
        path = self._configs_dir / f"{feature_set_name}.yml"
        if not path.exists():
            raise FileNotFoundError(f"Feature set config not found: {path}")
        with open(path) as f:
            data = yaml.safe_load(f)
        all_names = (
            data.get("ohlcv", [])
            + data.get("cross_sectional", [])
            + data.get("fundamental", [])
            + data.get("regime", [])
            + data.get("options", [])
            + data.get("insider", [])
            + data.get("institutional", [])
        )
        result = max((self._lookbacks.get(n, 0) for n in all_names), default=0)
        if result == 0 and all_names:
            unregistered = [n for n in all_names if n not in self._lookbacks]
            raise RuntimeError(
                f"max_lookback('{feature_set_name}') returned 0: no lookbacks registered "
                f"for any of {len(all_names)} features "
                f"(unregistered: {unregistered[:5]}...). "
                "Import the feature modules before calling max_lookback()."
            )
        return result

    def load_feature_set(self, name: str) -> FeatureSet:
        """Load a feature set from YAML config."""
        path = self._configs_dir / f"{name}.yml"
        if not path.exists():
            raise FileNotFoundError(f"Feature set config not found: {path}")

        with open(path) as f:
            data = yaml.safe_load(f)

        fs = FeatureSet(
            name=data["name"],
            description=data.get("description", ""),
            ohlcv=data.get("ohlcv", []),
            cross_sectional=data.get("cross_sectional", []),
            fundamental=data.get("fundamental", []),
            regime=data.get("regime", []),
            options=data.get("options", []),
            insider=data.get("insider", []),
            institutional=data.get("institutional", []),
        )

        # Validate all features have implementations
        missing = [f for f in fs.all_features if f not in self._implementations]
        if missing:
            raise ValueError(
                f"Feature set '{name}' references unregistered features: {missing}"
            )

        return fs

    @property
    def registered_features(self) -> list[str]:
        return sorted(self._implementations.keys())


# Singleton registry
registry = FeatureRegistry()


def feature(name: str, lookback: int = 0):
    """Decorator to register a feature implementation with its lookback in bars."""
    def decorator(fn: Callable) -> Callable:
        registry.register(name, fn, lookback=lookback)
        return fn
    return decorator
