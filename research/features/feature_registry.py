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

    @property
    def all_features(self) -> list[str]:
        return self.ohlcv + self.cross_sectional + self.fundamental + self.regime

    def config_hash(self) -> str:
        """Deterministic hash for experiment lineage."""
        payload = json.dumps({
            "name": self.name,
            "ohlcv": self.ohlcv,
            "cross_sectional": self.cross_sectional,
            "fundamental": self.fundamental,
            "regime": self.regime,
        }, sort_keys=True)
        return hashlib.sha256(payload.encode()).hexdigest()[:12]


class FeatureRegistry:
    """Global feature registry — maps feature names to implementations."""

    def __init__(self):
        self._implementations: dict[str, Callable] = {}
        self._configs_dir = Path(__file__).resolve().parents[2] / "configs" / "feature_sets"

    def register(self, name: str, fn: Callable) -> None:
        self._implementations[name] = fn

    def get(self, name: str) -> Callable:
        if name not in self._implementations:
            raise KeyError(f"Feature '{name}' not registered. "
                           f"Available: {sorted(self._implementations.keys())}")
        return self._implementations[name]

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


def feature(name: str):
    """Decorator to register a feature implementation."""
    def decorator(fn: Callable) -> Callable:
        registry.register(name, fn)
        return fn
    return decorator
