"""Tests for research.risk.loader — risk config loading and override merging."""

from __future__ import annotations

from typing import Any

import pytest

from research.experiments.spec import RiskConfig
from research.risk.loader import (
    effective_risk_config,
    load_production_risk_config,
    merge_risk_overrides,
)


# ---------------------------------------------------------------------------
# load_production_risk_config
# ---------------------------------------------------------------------------


class TestLoadProductionRiskConfig:
    def test_loads_from_default_path(self):
        config = load_production_risk_config()
        assert isinstance(config, RiskConfig)
        # Verify some known production values from configs/risk.yml
        assert config.daily_loss_limit == -0.05
        assert config.trailing_drawdown_limit == -0.20
        assert config.max_gross_exposure == 1.0
        assert config.max_symbol_weight == 0.20
        assert config.max_active_positions == 50
        assert config.min_cash == 0.02
        assert config.max_daily_turnover == 0.50

    def test_loads_from_custom_path(self, tmp_path):
        custom_yml = tmp_path / "custom_risk.yml"
        custom_yml.write_text(
            "max_gross_exposure: 0.5\ndaily_loss_limit: -0.03\n"
        )
        config = load_production_risk_config(custom_yml)
        assert config.max_gross_exposure == 0.5
        assert config.daily_loss_limit == -0.03


# ---------------------------------------------------------------------------
# merge_risk_overrides
# ---------------------------------------------------------------------------


class TestMergeRiskOverrides:
    def test_no_overrides_returns_base(self):
        base = RiskConfig(max_gross_exposure=0.8)
        result = merge_risk_overrides(base, None)
        assert result is base

    def test_empty_overrides_returns_base(self):
        base = RiskConfig(max_gross_exposure=0.8)
        result = merge_risk_overrides(base, {})
        assert result is base

    def test_applies_known_fields(self):
        base = RiskConfig(max_gross_exposure=1.0, daily_loss_limit=-1.0)
        overrides = {"max_gross_exposure": 0.5, "daily_loss_limit": -0.03}
        result = merge_risk_overrides(base, overrides)
        assert result.max_gross_exposure == 0.5
        assert result.daily_loss_limit == -0.03
        # Unchanged fields stay at base values
        assert result.min_cash == base.min_cash

    def test_ignores_unknown_fields(self):
        base = RiskConfig()
        overrides = {"max_gross_exposure": 0.5, "unknown_field": 42}
        result = merge_risk_overrides(base, overrides)
        assert result.max_gross_exposure == 0.5
        # Should not raise, unknown field silently ignored

    def test_returns_new_instance(self):
        base = RiskConfig()
        overrides = {"max_gross_exposure": 0.5}
        result = merge_risk_overrides(base, overrides)
        assert result is not base


# ---------------------------------------------------------------------------
# effective_risk_config
# ---------------------------------------------------------------------------


class TestEffectiveRiskConfig:
    """Tests for the master routing function."""

    _BASE = RiskConfig(max_gross_exposure=0.8, daily_loss_limit=-0.10)
    _OVERRIDES: dict[str, Any] = {"max_gross_exposure": 0.5, "daily_loss_limit": -0.03}

    def test_shadow_sim_with_overrides_merges(self):
        """shadow + sim + overrides = merge over production."""
        config, audit = effective_risk_config(
            self._BASE, self._OVERRIDES,
            mode="shadow", execution_mode="sim",
        )
        assert audit["config_source"] == "production+overrides"
        assert config.max_gross_exposure == 0.5
        assert config.daily_loss_limit == -0.03

    def test_shadow_sim_without_overrides_uses_spec(self):
        """shadow + sim + no overrides = use spec config."""
        config, audit = effective_risk_config(
            self._BASE, None,
            mode="shadow", execution_mode="sim",
        )
        assert audit["config_source"] == "spec"
        assert config is self._BASE

    def test_shadow_none_mode_uses_spec(self):
        """shadow + execution_mode=none = use spec config."""
        config, audit = effective_risk_config(
            self._BASE, self._OVERRIDES,
            mode="shadow", execution_mode="none",
        )
        assert audit["config_source"] == "spec"
        assert config is self._BASE

    def test_paper_always_production(self):
        """Paper mode always uses production config."""
        config, audit = effective_risk_config(
            self._BASE, self._OVERRIDES,
            mode="paper",
        )
        assert audit["config_source"] == "production"
        # Should be production values, not overrides
        production = load_production_risk_config()
        assert config.daily_loss_limit == production.daily_loss_limit

    def test_live_always_production(self):
        """Live mode always uses production config."""
        config, audit = effective_risk_config(
            self._BASE, self._OVERRIDES,
            mode="live",
        )
        assert audit["config_source"] == "production"

    def test_qualification_replay_forces_production(self):
        """qualification_replay=True always forces production config."""
        config, audit = effective_risk_config(
            self._BASE, self._OVERRIDES,
            mode="shadow", execution_mode="sim",
            qualification_replay=True,
        )
        assert audit["config_source"] == "production"
        assert audit["reason"] == "qualification_replay forces production config"
        production = load_production_risk_config()
        assert config.daily_loss_limit == production.daily_loss_limit

    def test_audit_records_mode_info(self):
        """Audit dict should contain mode and execution_mode."""
        _, audit = effective_risk_config(
            self._BASE, self._OVERRIDES,
            mode="shadow", execution_mode="sim",
        )
        assert audit["mode"] == "shadow"
        assert audit["execution_mode"] == "sim"
        assert audit["has_overrides"] is True

    def test_audit_records_overrides_applied(self):
        """Audit dict should record which overrides were applied."""
        _, audit = effective_risk_config(
            self._BASE, self._OVERRIDES,
            mode="shadow", execution_mode="sim",
        )
        assert "overrides_applied" in audit
        assert audit["overrides_applied"] == self._OVERRIDES
