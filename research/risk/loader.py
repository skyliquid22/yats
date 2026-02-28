"""Risk config loader — production defaults and override merging.

Loads the production risk config from configs/risk.yml (the static contract)
and provides merging logic for risk_overrides in research mode.

PRD §12.2: Risk overrides are ONLY applied when mode=shadow with
execution_mode=sim. Paper and live trading ALWAYS use configs/risk.yml.
"""

from __future__ import annotations

import logging
from dataclasses import fields
from pathlib import Path
from typing import Any, Mapping

import yaml

from research.experiments.spec import RiskConfig

logger = logging.getLogger(__name__)

_RISK_YML = Path(__file__).resolve().parents[2] / "configs" / "risk.yml"

# Fields in RiskConfig that can be overridden
_RISK_CONFIG_FIELDS = {f.name for f in fields(RiskConfig)}


def load_production_risk_config(path: Path | None = None) -> RiskConfig:
    """Load the production risk config from configs/risk.yml.

    This is the static contract — paper and live trading ALWAYS use this.
    """
    p = path or _RISK_YML
    with open(p, encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    # Map YAML keys to RiskConfig fields (only include known fields)
    kwargs = {k: v for k, v in raw.items() if k in _RISK_CONFIG_FIELDS}
    return RiskConfig(**kwargs)


def merge_risk_overrides(
    base: RiskConfig,
    overrides: Mapping[str, Any] | None,
) -> RiskConfig:
    """Merge risk_overrides over a base RiskConfig.

    Only known RiskConfig fields are applied. Unknown keys are logged and
    ignored (they may be for other risk subsystems like volatility braking).

    Returns a new RiskConfig with overrides applied.
    """
    if not overrides:
        return base

    kwargs: dict[str, Any] = {}
    for f in fields(base):
        kwargs[f.name] = getattr(base, f.name)

    applied: list[str] = []
    ignored: list[str] = []

    for key, val in overrides.items():
        if key in _RISK_CONFIG_FIELDS:
            kwargs[key] = val
            applied.append(key)
        else:
            ignored.append(key)

    if applied:
        logger.info(
            "Risk overrides applied: %s",
            ", ".join(f"{k}={overrides[k]}" for k in applied),
        )
    if ignored:
        logger.warning(
            "Risk overrides ignored (unknown fields): %s",
            ", ".join(ignored),
        )

    return RiskConfig(**kwargs)


def effective_risk_config(
    spec_risk_config: RiskConfig,
    risk_overrides: Mapping[str, Any] | None,
    *,
    mode: str,
    execution_mode: str = "none",
    qualification_replay: bool = False,
) -> tuple[RiskConfig, dict[str, Any]]:
    """Determine the effective risk config for a given execution context.

    Returns (risk_config, audit_entry) where audit_entry logs the decision.

    Rules (PRD §12.2):
    - shadow + sim (not qualification): merge overrides over production defaults
    - qualification_replay: ALWAYS use production config (PRD §24.1)
    - paper / live: ALWAYS use production config (configs/risk.yml)
    - training: caller handles this via merge_risk_overrides directly
    """
    production = load_production_risk_config()

    audit: dict[str, Any] = {
        "mode": mode,
        "execution_mode": execution_mode,
        "qualification_replay": qualification_replay,
        "has_overrides": risk_overrides is not None and len(risk_overrides) > 0,
    }

    # Qualification replay ALWAYS uses production config
    if qualification_replay:
        audit["config_source"] = "production"
        audit["reason"] = "qualification_replay forces production config"
        if risk_overrides:
            logger.info(
                "Qualification replay: ignoring risk_overrides, using production config"
            )
        return production, audit

    # Paper and live ALWAYS use production config
    if mode in ("paper", "live"):
        audit["config_source"] = "production"
        audit["reason"] = f"{mode} mode always uses production config"
        if risk_overrides:
            logger.warning(
                "%s mode: ignoring risk_overrides (production config enforced)",
                mode,
            )
        return production, audit

    # Shadow + sim: merge overrides over production defaults
    if mode == "shadow" and execution_mode == "sim" and risk_overrides:
        merged = merge_risk_overrides(production, risk_overrides)
        audit["config_source"] = "production+overrides"
        audit["overrides_applied"] = dict(risk_overrides)
        logger.info(
            "Shadow+sim: applying %d risk overrides over production defaults",
            len(risk_overrides),
        )
        return merged, audit

    # Shadow without sim, or shadow+sim without overrides: use spec config
    # (which defaults to production-equivalent values)
    audit["config_source"] = "spec"
    audit["reason"] = "no override conditions met, using spec risk_config"
    return spec_risk_config, audit
