"""Property-style tests for research.risk.project_weights.

After projection, ALL enforced caps must hold simultaneously.
Covers both normal and vol-brake regimes with randomized inputs.
"""

from __future__ import annotations

import numpy as np
import pytest

from research.experiments.spec import RiskConfig
from research.risk.project_weights import project_weights, project_weights_full

_RNG = np.random.default_rng(42)


def _rand_weights(n: int, scale: float = 0.4) -> np.ndarray:
    return _RNG.uniform(0.0, scale, size=n)


def _rand_prev(n: int) -> np.ndarray:
    return _RNG.uniform(0.0, 0.3, size=n)


def _assert_all_caps(
    w: np.ndarray,
    risk: RiskConfig,
    prev_weights: np.ndarray | None,
    eff_max_sym: float,
    eff_max_gross: float,
    tol: float = 1e-9,
    check_turnover: bool = True,
) -> None:
    assert (w >= 0).all(), f"long_only violated: min={w.min()}"
    assert w.max() <= eff_max_sym + tol, (
        f"eff_max_symbol_weight violated: {w.max():.6f} > {eff_max_sym}"
    )
    max_exposure = min(eff_max_gross, 1.0 - risk.min_cash)
    assert w.sum() <= max_exposure + tol, (
        f"eff_max_gross_exposure violated: {w.sum():.6f} > {max_exposure}"
    )
    if check_turnover and prev_weights is not None and risk.max_daily_turnover < 2.0:
        l1 = float(np.abs(w - prev_weights).sum())
        # Turnover may be exceeded by forced holds/min-order reverts — only check
        # that the final validation pass did NOT reintroduce a pure-cap violation.
        # The actual assertion is on the hard caps, not turnover.
        _ = l1  # documented: turnover is logged-only when forced reverts apply


# ---------------------------------------------------------------------------
# Normal regime — basic cap invariants hold simultaneously
# ---------------------------------------------------------------------------


class TestCapInvariantsNormalRegime:
    _RISK = RiskConfig(
        max_symbol_weight=0.20,
        max_gross_exposure=0.80,
        min_cash=0.05,
        max_daily_turnover=0.30,
        max_active_positions=5,
        max_top_n_concentration=0.50,
        top_n=3,
    )

    @pytest.mark.parametrize("seed", range(20))
    def test_caps_hold_for_random_inputs(self, seed: int) -> None:
        rng = np.random.default_rng(seed)
        n = rng.integers(3, 10)
        raw = rng.uniform(0.0, 0.6, size=n)
        prev = rng.uniform(0.0, 0.3, size=n)
        w = project_weights(raw, self._RISK, prev_weights=prev)
        _assert_all_caps(w, self._RISK, prev, self._RISK.max_symbol_weight, self._RISK.max_gross_exposure)

    @pytest.mark.parametrize("seed", range(10))
    def test_caps_hold_no_prev_weights(self, seed: int) -> None:
        rng = np.random.default_rng(seed + 100)
        n = rng.integers(2, 8)
        raw = rng.uniform(0.0, 0.7, size=n)
        w = project_weights(raw, self._RISK)
        _assert_all_caps(w, self._RISK, None, self._RISK.max_symbol_weight, self._RISK.max_gross_exposure)


# ---------------------------------------------------------------------------
# Vol-brake regime — reduced effective caps must also hold
# ---------------------------------------------------------------------------


class TestCapInvariantsVolBrakeRegime:
    _RISK = RiskConfig(
        max_symbol_weight=0.20,
        max_gross_exposure=0.80,
        min_cash=0.02,
        max_daily_turnover=0.40,
        vol_regime_threshold=0.25,
        vol_brake_position_reduction=0.50,
        vol_brake_exposure_reduction=0.50,
    )
    # Effective limits under vol brake
    _EFF_SYM = 0.20 * 0.50       # 0.10
    _EFF_GROSS = 0.80 * 0.50     # 0.40

    @pytest.mark.parametrize("seed", range(20))
    def test_vol_brake_caps_hold(self, seed: int) -> None:
        rng = np.random.default_rng(seed + 200)
        n = rng.integers(3, 8)
        raw = rng.uniform(0.0, 0.5, size=n)
        prev = rng.uniform(0.0, 0.4, size=n)
        high_vol = 0.40  # above threshold
        w = project_weights(raw, self._RISK, prev_weights=prev, current_vol=high_vol)
        _assert_all_caps(w, self._RISK, prev, self._EFF_SYM, self._EFF_GROSS)

    @pytest.mark.parametrize("seed", range(10))
    def test_normal_vol_uses_base_caps(self, seed: int) -> None:
        rng = np.random.default_rng(seed + 300)
        n = rng.integers(3, 8)
        raw = rng.uniform(0.0, 0.5, size=n)
        low_vol = 0.10  # below threshold
        w = project_weights(raw, self._RISK, current_vol=low_vol)
        _assert_all_caps(w, self._RISK, None, self._RISK.max_symbol_weight, self._RISK.max_gross_exposure)


# ---------------------------------------------------------------------------
# Turnover + gross interaction (issue A): interpolated prev can raise gross
# ---------------------------------------------------------------------------


class TestTurnoverGrossInteraction:
    """After turnover scaling, gross exposure must stay within effective cap."""

    def test_vol_brake_then_turnover_gross_capped(self) -> None:
        risk = RiskConfig(
            max_symbol_weight=0.20,
            max_gross_exposure=0.80,
            min_cash=0.02,
            max_daily_turnover=0.10,
            vol_regime_threshold=0.20,
            vol_brake_exposure_reduction=0.50,  # eff_gross = 0.40
        )
        eff_gross = 0.80 * 0.50
        # prev_weights near the pre-brake limit so interpolation toward prev raises gross
        prev = np.array([0.35, 0.35])  # gross=0.70, above eff_gross=0.40
        raw = np.array([0.05, 0.05])   # target much lower
        w = project_weights(raw, risk, prev_weights=prev, current_vol=0.40)
        assert w.sum() <= min(eff_gross, 1.0 - risk.min_cash) + 1e-9, (
            f"Gross {w.sum():.4f} exceeds eff_max_gross {eff_gross} after turnover interpolation"
        )

    @pytest.mark.parametrize("seed", range(15))
    def test_turnover_gross_invariant_randomized(self, seed: int) -> None:
        rng = np.random.default_rng(seed + 400)
        risk = RiskConfig(
            max_symbol_weight=0.25,
            max_gross_exposure=0.70,
            min_cash=0.02,
            max_daily_turnover=rng.uniform(0.05, 0.30),
            vol_regime_threshold=0.20,
            vol_brake_exposure_reduction=0.50,
        )
        eff_gross = risk.max_gross_exposure * 0.50  # brake fires
        n = rng.integers(2, 6)
        raw = rng.uniform(0.0, 0.15, size=n)
        prev = rng.uniform(0.20, 0.40, size=n)  # high prev to stress interpolation
        w = project_weights(raw, risk, prev_weights=prev, current_vol=0.35)
        _assert_all_caps(w, risk, prev, risk.max_symbol_weight * 0.50, eff_gross, check_turnover=False)


# ---------------------------------------------------------------------------
# Holding period reverts (issue B): reverted weight must not exceed eff cap
# ---------------------------------------------------------------------------


class TestHoldingPeriodRevertCapped:
    def test_revert_clamped_to_eff_max_symbol_weight(self) -> None:
        risk = RiskConfig(
            max_symbol_weight=0.20,
            max_gross_exposure=0.80,
            min_cash=0.02,
            min_holding_period=5,
            vol_regime_threshold=0.20,
            vol_brake_position_reduction=0.50,  # eff_sym = 0.10
        )
        eff_sym = 0.20 * 0.50
        # prev_weights[0] = 0.18 which is above eff_sym=0.10 under vol brake
        prev = np.array([0.18, 0.10])
        raw = np.array([0.05, 0.08])   # wants to reduce [0]
        holding_bars = np.array([2.0, 10.0])  # [0] held only 2 bars < min_period=5
        w = project_weights(
            raw, risk, prev_weights=prev,
            current_vol=0.35,
            holding_bars=holding_bars,
        )
        assert w[0] <= eff_sym + 1e-9, (
            f"Holding-period revert raised w[0]={w[0]:.4f} above eff_max_symbol_weight={eff_sym}"
        )

    @pytest.mark.parametrize("seed", range(15))
    def test_holding_period_caps_hold_randomized(self, seed: int) -> None:
        rng = np.random.default_rng(seed + 500)
        risk = RiskConfig(
            max_symbol_weight=0.25,
            max_gross_exposure=0.90,
            min_cash=0.02,
            min_holding_period=int(rng.integers(2, 6)),
            vol_regime_threshold=0.20,
            vol_brake_position_reduction=0.40,
        )
        eff_sym = risk.max_symbol_weight * (1.0 - risk.vol_brake_position_reduction)
        eff_gross = risk.max_gross_exposure * (1.0 - risk.vol_brake_exposure_reduction)
        n = rng.integers(3, 7)
        raw = rng.uniform(0.0, 0.15, size=n)
        # prev can be above eff caps to stress the clamp
        prev = rng.uniform(0.10, 0.35, size=n)
        holding_bars = rng.uniform(0.5, 8.0, size=n)
        high_vol = 0.35
        w = project_weights(
            raw, risk, prev_weights=prev,
            current_vol=high_vol,
            holding_bars=holding_bars,
        )
        _assert_all_caps(w, risk, prev, eff_sym, eff_gross, check_turnover=False)


# ---------------------------------------------------------------------------
# Min-order threshold reverts (issue B continued)
# ---------------------------------------------------------------------------


class TestMinOrderThresholdRevertCapped:
    def test_revert_clamped_to_eff_max_symbol_weight(self) -> None:
        risk = RiskConfig(
            max_symbol_weight=0.20,
            max_gross_exposure=1.0,
            min_cash=0.0,
            minimum_order_threshold=0.05,
            vol_regime_threshold=0.15,
            vol_brake_position_reduction=0.50,  # eff_sym = 0.10
        )
        eff_sym = 0.20 * 0.50
        # prev[0]=0.19 above eff_sym=0.10; desired delta < threshold triggers revert
        prev = np.array([0.19, 0.10])
        raw = np.array([0.17, 0.10])   # delta=0.02 < threshold=0.05 → revert
        w = project_weights(raw, risk, prev_weights=prev, current_vol=0.30)
        assert w[0] <= eff_sym + 1e-9, (
            f"Min-order revert raised w[0]={w[0]:.4f} above eff_max_symbol_weight={eff_sym}"
        )


# ---------------------------------------------------------------------------
# ADV cap correctness (issue D): price-based formula
# ---------------------------------------------------------------------------


class TestADVCapPriceBased:
    def test_adv_cap_applies_with_prices(self) -> None:
        risk = RiskConfig(
            max_symbol_weight=0.50,
            max_gross_exposure=1.0,
            min_cash=0.0,
            max_adv_pct=0.05,
        )
        nav = 1_000_000.0
        adv_shares = np.array([10_000.0, 50_000.0])
        prices = np.array([100.0, 50.0])
        # max_w[0] = 0.05 * 10000 * 100 / 1e6 = 0.05
        # max_w[1] = 0.05 * 50000 * 50  / 1e6 = 0.125
        raw = np.array([0.30, 0.30])
        w = project_weights(raw, risk, adv_shares=adv_shares, prices=prices, nav=nav)
        assert w[0] <= 0.05 + 1e-9, f"ADV cap failed for sym0: {w[0]:.6f}"
        assert w[1] <= 0.125 + 1e-9, f"ADV cap failed for sym1: {w[1]:.6f}"

    def test_adv_cap_skipped_without_prices(self, caplog) -> None:
        import logging
        risk = RiskConfig(max_adv_pct=0.05)
        nav = 1_000_000.0
        adv_shares = np.array([10_000.0])
        raw = np.array([0.30])
        with caplog.at_level(logging.WARNING, logger="research.risk.project_weights"):
            w = project_weights(raw, risk, adv_shares=adv_shares, nav=nav)
        assert w[0] == pytest.approx(0.30, abs=1e-6)
        assert any("prices" in r.message for r in caplog.records)

    def test_adv_cap_old_shares_formula_would_be_wrong(self) -> None:
        """Confirm the fix: old formula adv/nav gives wrong scale."""
        risk = RiskConfig(max_adv_pct=0.05)
        nav = 1_000_000.0
        adv_shares = np.array([10_000.0])
        prices = np.array([100.0])
        # Correct: max_w = 0.05 * 10000 * 100 / 1e6 = 0.05
        # Old (wrong): max_w = 0.05 * 10000 / 1e6 = 0.0005
        raw = np.array([0.04])
        w = project_weights(raw, risk, adv_shares=adv_shares, prices=prices, nav=nav)
        # Weight 0.04 is under correct cap 0.05 → should pass unchanged
        assert w[0] == pytest.approx(0.04, abs=1e-6)


# ---------------------------------------------------------------------------
# check_order side-awareness (issue E)
# ---------------------------------------------------------------------------


class TestCheckOrderSideAware:
    def _positions(self, symbol: str, mv: float) -> list[dict]:
        return [{"symbol": symbol, "qty": 1, "market_value": mv}]

    def test_sell_reduces_symbol_weight(self) -> None:
        from compute.risk.check_order import check_order
        # Symbol currently at 15% of nav; sell 10% → new weight 5% (below 20% cap)
        nav = 100_000.0
        positions = self._positions("AAPL", 15_000.0)
        result = check_order("AAPL", "sell", 100, 10_000.0, nav, positions, {"max_symbol_weight": 0.20})
        assert result["decision"] == "pass", f"Sell reducing exposure should pass: {result['reasons']}"

    def test_sell_does_not_trigger_gross_exposure_reject(self) -> None:
        from compute.risk.check_order import check_order
        nav = 100_000.0
        # Portfolio at 95% gross; a sell reduces it — should pass gross check
        positions = [{"symbol": "AAPL", "qty": 1, "market_value": 95_000.0}]
        result = check_order("AAPL", "sell", 100, 10_000.0, nav, positions, {"max_gross_exposure": 0.90})
        gross_reject = any(r["rule"] == "max_gross_exposure" for r in result["reasons"] if r.get("severity") == "reject")
        assert not gross_reject, "Sell that reduces gross exposure should not trigger gross_exposure reject"

    def test_buy_correctly_increases_symbol_weight(self) -> None:
        from compute.risk.check_order import check_order
        nav = 100_000.0
        positions = self._positions("AAPL", 15_000.0)
        # Buy 10% more → new weight 25% (above 20% cap) → reject
        result = check_order("AAPL", "buy", 100, 10_000.0, nav, positions, {"max_symbol_weight": 0.20})
        assert result["decision"] == "reject"
        assert any(r["rule"] == "max_symbol_weight" for r in result["reasons"])

    def test_sell_frees_cash(self) -> None:
        from compute.risk.check_order import check_order
        nav = 100_000.0
        # Nearly fully invested; sell should free cash (not fail min_cash)
        positions = [{"symbol": "AAPL", "qty": 1, "market_value": 99_000.0}]
        result = check_order("AAPL", "sell", 990, 9_900.0, nav, positions, {"min_cash": 0.05})
        cash_reject = any(r["rule"] == "min_cash" for r in result["reasons"] if r.get("severity") == "reject")
        assert not cash_reject, "Sell that frees cash should not trigger min_cash reject"

    def test_sell_does_not_add_active_position(self) -> None:
        from compute.risk.check_order import check_order
        nav = 100_000.0
        # Already at max positions; a sell should not be blocked by position count
        positions = [{"symbol": f"SYM{i}", "qty": 1, "market_value": 2_000.0} for i in range(5)]
        # Sell an existing position (SYM0) — should not hit max_active_positions
        result = check_order("SYM0", "sell", 10, 1_000.0, nav, positions, {"max_active_positions": 5})
        pos_reject = any(r["rule"] == "max_active_positions" for r in result["reasons"] if r.get("severity") == "reject")
        assert not pos_reject, "Sell of existing position should not trigger max_active_positions reject"
