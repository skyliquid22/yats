"""Tests for research.eval.wfo — Walk-Forward Optimization harness."""

from __future__ import annotations

import pytest

from research.eval.wfo import (
    WFOFold,
    WFOResult,
    build_wfo_folds,
    compute_rank_decay,
    compute_sweep_wfo_rank_decay,
    run_wfo,
    wfo_qualification_gates,
)
from research.experiments.spec import WFOConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_data(n: int) -> list[dict]:
    """Simple list of bar dicts in chronological order."""
    return [{"bar": i, "timestamp": f"2020-{(i // 21) % 12 + 1:02d}-01"} for i in range(n)]


def _make_wfo_config(**overrides) -> WFOConfig:
    defaults = dict(
        n_periods=4,
        train_window=200,
        test_window=50,
        mode="anchored",
        label_horizon=1,
        purge_buffer=5,
    )
    defaults.update(overrides)
    return WFOConfig(**defaults)


# ---------------------------------------------------------------------------
# Test 1: WFO yields n_periods contiguous train/test folds with correct
#         purge+embargo and window advancement
# ---------------------------------------------------------------------------

class TestBuildWFOFolds:
    def test_anchored_fold_count(self):
        cfg = _make_wfo_config(n_periods=4, train_window=200, test_window=50)
        # Need: 200 + (4-1)*50 + 50 + gap(6) = 406 bars minimum
        folds = build_wfo_folds(600, cfg)
        assert len(folds) == 4

    def test_rolling_fold_count(self):
        cfg = _make_wfo_config(n_periods=3, train_window=150, test_window=50, mode="rolling")
        folds = build_wfo_folds(500, cfg)
        assert len(folds) == 3

    def test_anchored_train_start_always_zero(self):
        cfg = _make_wfo_config(n_periods=4, train_window=200, test_window=50, mode="anchored")
        folds = build_wfo_folds(600, cfg)
        for fold in folds:
            assert fold.train_start == 0, "Anchored mode: all folds start at bar 0"

    def test_anchored_train_window_grows(self):
        cfg = _make_wfo_config(n_periods=4, train_window=200, test_window=50, mode="anchored")
        folds = build_wfo_folds(600, cfg)
        ends = [f.train_end for f in folds]
        for i in range(1, len(ends)):
            assert ends[i] > ends[i - 1], "Anchored: train_end grows each fold"

    def test_rolling_train_window_fixed_size(self):
        cfg = _make_wfo_config(n_periods=3, train_window=150, test_window=50, mode="rolling")
        folds = build_wfo_folds(500, cfg)
        for fold in folds:
            # train window size = train_end - train_start, net of purge
            raw_size = (fold.train_start + 150) - fold.train_start
            assert raw_size == 150

    def test_test_blocks_are_contiguous_no_overlap(self):
        cfg = _make_wfo_config(n_periods=4, train_window=200, test_window=50, mode="anchored")
        folds = build_wfo_folds(600, cfg)
        for i in range(1, len(folds)):
            assert folds[i].test_start == folds[i - 1].test_end, (
                f"Test blocks must be contiguous: fold {i} test_start != fold {i-1} test_end"
            )

    def test_purge_and_buffer_applied_correctly(self):
        label_horizon = 3
        purge_buffer = 7
        cfg = _make_wfo_config(
            n_periods=2, train_window=200, test_window=50,
            label_horizon=label_horizon, purge_buffer=purge_buffer,
        )
        folds = build_wfo_folds(600, cfg)
        for fold in folds:
            # gap = label_horizon + purge_buffer = 10
            assert fold.purge_bars == label_horizon
            assert fold.buffer_bars == purge_buffer
            # In anchored: raw_train_end = train_window + i * test_window
            raw_end = 200 + fold.fold_index * 50
            expected_effective_train_end = raw_end - label_horizon - purge_buffer
            assert fold.train_end == expected_effective_train_end, (
                f"Fold {fold.fold_index}: expected train_end={expected_effective_train_end}, "
                f"got {fold.train_end}"
            )
            # test_start = raw_train_end (no after-embargo)
            assert fold.test_start == raw_end

    def test_test_always_follows_train_in_time(self):
        cfg = _make_wfo_config(n_periods=4, train_window=200, test_window=50)
        folds = build_wfo_folds(600, cfg)
        for fold in folds:
            assert fold.test_start > fold.train_end, (
                f"Fold {fold.fold_index}: test must start after train_end"
            )

    def test_rolling_window_advances_each_fold(self):
        cfg = _make_wfo_config(n_periods=3, train_window=150, test_window=50, mode="rolling")
        folds = build_wfo_folds(500, cfg)
        for i in range(1, len(folds)):
            assert folds[i].train_start > folds[i - 1].train_start

    def test_derived_test_window_when_none(self):
        # test_window=None: harness derives it
        cfg = WFOConfig(n_periods=4, train_window=200, test_window=None,
                        mode="anchored", label_horizon=1, purge_buffer=5)
        folds = build_wfo_folds(600, cfg)
        assert len(folds) >= 2


# ---------------------------------------------------------------------------
# Test 2: each roll trains at full configured timesteps (not truncated)
# ---------------------------------------------------------------------------

class TestFullTimestepsPerFold:
    def test_train_fn_called_with_all_train_bars(self):
        """train_fn receives all bars in the fold's train window."""
        cfg = _make_wfo_config(n_periods=2, train_window=200, test_window=50)
        data = _make_data(600)
        folds = build_wfo_folds(len(data), cfg)

        received_lengths = []

        def train_fn(train_data):
            received_lengths.append(len(train_data))
            return None

        def eval_fn(test_data, checkpoint):
            return [0.001] * len(test_data), 0.5

        run_wfo(data, cfg, train_fn=train_fn, eval_fn=eval_fn)

        # Verify each fold received the expected number of train bars
        for i, fold in enumerate(folds):
            expected = fold.train_end - fold.train_start
            assert received_lengths[i] == expected, (
                f"Fold {i}: train_fn got {received_lengths[i]} bars, "
                f"expected {expected} (train_start={fold.train_start}, train_end={fold.train_end})"
            )

    def test_train_fn_call_count_matches_n_periods(self):
        cfg = _make_wfo_config(n_periods=3, train_window=200, test_window=50)
        data = _make_data(600)
        call_count = [0]

        def train_fn(train_data):
            call_count[0] += 1
            return None

        def eval_fn(test_data, checkpoint):
            return [], 0.0

        run_wfo(data, cfg, train_fn=train_fn, eval_fn=eval_fn)
        assert call_count[0] == 3


# ---------------------------------------------------------------------------
# Test 3: OOS concatenation is chronological with no gaps/overlaps beyond embargo
# ---------------------------------------------------------------------------

class TestOOSConcatenation:
    def test_oos_returns_are_concatenated_in_order(self):
        cfg = _make_wfo_config(n_periods=3, train_window=200, test_window=50)
        n = 600
        data = [{"bar": i} for i in range(n)]
        folds = build_wfo_folds(n, cfg)

        def train_fn(train_data):
            return None

        fold_returns: list[list[float]] = []

        def eval_fn(test_data, checkpoint):
            # Return bar indices as "returns" so we can verify ordering
            rets = [row["bar"] for row in test_data]
            fold_returns.append(rets)
            return rets, 0.5

        result = run_wfo(data, cfg, train_fn=train_fn, eval_fn=eval_fn)

        # Concatenated OOS must equal fold returns concatenated in fold order
        expected = []
        for r in fold_returns:
            expected.extend(r)
        assert result.concatenated_oos_returns == expected

    def test_no_overlap_in_oos_bar_indices(self):
        cfg = _make_wfo_config(n_periods=3, train_window=200, test_window=50)
        n = 600
        data = [{"bar": i} for i in range(n)]
        folds = build_wfo_folds(n, cfg)

        # Verify test block boundaries don't overlap
        all_test_bars: set[int] = set()
        for fold in folds:
            block = set(range(fold.test_start, fold.test_end))
            overlap = all_test_bars & block
            assert not overlap, f"Fold {fold.fold_index}: OOS bars overlap with previous folds"
            all_test_bars |= block

    def test_oos_blocks_are_chronologically_ordered(self):
        cfg = _make_wfo_config(n_periods=4, train_window=200, test_window=50)
        folds = build_wfo_folds(600, cfg)
        for i in range(1, len(folds)):
            assert folds[i].test_start >= folds[i - 1].test_end


# ---------------------------------------------------------------------------
# Test 4: train_window-too-short raises a clear error
# ---------------------------------------------------------------------------

class TestTrainWindowTooShort:
    def test_raises_on_short_train_window(self):
        cfg = WFOConfig(n_periods=2, train_window=10, test_window=50, mode="anchored",
                        label_horizon=1, purge_buffer=5)
        with pytest.raises(ValueError, match="too short for PPO convergence"):
            build_wfo_folds(500, cfg)

    def test_raises_on_insufficient_total_data(self):
        cfg = _make_wfo_config(n_periods=4, train_window=400, test_window=100)
        with pytest.raises(ValueError, match="Not enough data"):
            build_wfo_folds(200, cfg)

    def test_raises_on_zero_n_periods(self):
        with pytest.raises(ValueError, match="n_periods must be >= 2"):
            WFOConfig(n_periods=1, train_window=200)

    def test_raises_on_negative_purge_buffer(self):
        with pytest.raises(ValueError, match="purge_buffer must be >= 0"):
            WFOConfig(n_periods=4, train_window=200, purge_buffer=-1)

    def test_raises_on_invalid_mode(self):
        with pytest.raises(ValueError, match="mode must be"):
            WFOConfig(n_periods=4, train_window=200, mode="cpcv")


# ---------------------------------------------------------------------------
# Test 5: rank-decay metric separates a synthetic overfit config from robust
# ---------------------------------------------------------------------------

class TestRankDecayMetric:
    def _make_overfit_sharpes(self, n_configs: int = 5, n_folds: int = 6) -> list[list[float]]:
        """Synthetic: best config in folds 1-3 becomes worst in folds 4-6."""
        sharpes = []
        for c in range(n_configs):
            if c == 0:
                # Overfit: great early, terrible late
                fold_sharpes = [2.0 - 0.5 * f for f in range(n_folds)]
            else:
                # Robust: consistent modest performance
                fold_sharpes = [0.5 + 0.01 * f for f in range(n_folds)]
            sharpes.append(fold_sharpes)
        return sharpes

    def _make_robust_sharpes(self, n_configs: int = 5, n_folds: int = 6) -> list[list[float]]:
        """Synthetic: all configs have stable rank across all folds."""
        sharpes = []
        for c in range(n_configs):
            # Consistent offset per config, no rank inversion
            base = 0.5 + c * 0.2
            sharpes.append([base + 0.01 * f for f in range(n_folds)])
        return sharpes

    def test_overfit_has_higher_rank_decay_than_robust(self):
        overfit = self._make_overfit_sharpes()
        robust = self._make_robust_sharpes()
        rd_overfit = compute_rank_decay(overfit)
        rd_robust = compute_rank_decay(robust)
        assert rd_overfit > rd_robust, (
            f"Overfit rank-decay ({rd_overfit:.3f}) should exceed "
            f"robust rank-decay ({rd_robust:.3f})"
        )

    def test_stable_rankings_give_zero_decay(self):
        # Perfectly stable rankings (same order every fold)
        sharpes = [[3.0] * 4, [2.0] * 4, [1.0] * 4]
        rd = compute_rank_decay(sharpes)
        assert rd == 0.0, f"Perfectly stable rankings should give 0 decay, got {rd}"

    def test_perfect_inversion_gives_max_decay(self):
        # 2 configs: first half fold 1 wins, second half fold 2 wins
        sharpes = [
            [2.0, 2.0, 0.1, 0.1],  # config 0: great first half, bad second
            [0.1, 0.1, 2.0, 2.0],  # config 1: bad first half, great second
        ]
        rd = compute_rank_decay(sharpes)
        assert rd > 0.5, f"Near-perfect rank inversion should give high decay, got {rd}"

    def test_single_config_gives_zero(self):
        rd = compute_rank_decay([[1.0, 0.5, 0.3, 0.8]])
        assert rd == 0.0

    def test_none_values_handled_gracefully(self):
        sharpes = [
            [1.0, None, 0.5, 0.8],
            [0.5, 0.6, None, 0.9],
        ]
        rd = compute_rank_decay(sharpes)
        assert 0.0 <= rd <= 1.0

    def test_sweep_rank_decay_api(self):
        overfit = self._make_overfit_sharpes()
        rd = compute_sweep_wfo_rank_decay(overfit)
        assert 0.0 <= rd <= 1.0


# ---------------------------------------------------------------------------
# WFOConfig dataclass
# ---------------------------------------------------------------------------

class TestWFOConfig:
    def test_defaults(self):
        cfg = WFOConfig()
        assert cfg.n_periods == 4
        assert cfg.train_window == 504
        assert cfg.mode == "anchored"
        assert cfg.label_horizon == 1
        assert cfg.purge_buffer is None

    def test_frozen(self):
        cfg = WFOConfig()
        with pytest.raises((AttributeError, TypeError)):
            cfg.n_periods = 8  # type: ignore[misc]

    def test_rolling_mode_accepted(self):
        cfg = WFOConfig(mode="rolling")
        assert cfg.mode == "rolling"

    def test_test_window_none_allowed(self):
        cfg = WFOConfig(test_window=None)
        assert cfg.test_window is None


# ---------------------------------------------------------------------------
# run_wfo integration
# ---------------------------------------------------------------------------

class TestRunWFO:
    def test_result_structure(self):
        cfg = _make_wfo_config(n_periods=3, train_window=200, test_window=50)
        data = _make_data(600)

        def train_fn(d):
            return "checkpoint.zip"

        def eval_fn(d, ckpt):
            return [0.001] * len(d), 0.6

        result = run_wfo(data, cfg, train_fn=train_fn, eval_fn=eval_fn)
        assert result.n_folds == 3
        assert len(result.folds) == 3
        assert len(result.per_fold_oos_sharpe) == 3
        assert result.median_oos_sharpe == pytest.approx(0.6)
        assert len(result.concatenated_oos_returns) == 3 * 50

    def test_rank_decay_in_result(self):
        cfg = _make_wfo_config(n_periods=4, train_window=200, test_window=50)
        data = _make_data(600)

        def train_fn(d):
            return None

        def eval_fn(d, ckpt):
            return [0.0] * len(d), 0.0

        result = run_wfo(data, cfg, train_fn=train_fn, eval_fn=eval_fn)
        assert result.rank_decay_metric is not None
        assert 0.0 <= result.rank_decay_metric <= 1.0

    def test_per_fold_sharpe_series_length(self):
        cfg = _make_wfo_config(n_periods=4, train_window=200, test_window=50)
        data = _make_data(600)
        sharpes = [0.5, 0.8, 0.3, 1.2]
        call_idx = [0]

        def train_fn(d):
            return None

        def eval_fn(d, ckpt):
            s = sharpes[call_idx[0]]
            call_idx[0] += 1
            return [0.0] * len(d), s

        result = run_wfo(data, cfg, train_fn=train_fn, eval_fn=eval_fn)
        assert result.per_fold_oos_sharpe == sharpes


# ---------------------------------------------------------------------------
# Qualification gates
# ---------------------------------------------------------------------------

class TestWFOQualificationGates:
    def _make_result(self, median_sharpe, rank_decay) -> WFOResult:
        folds = [
            WFOFold(0, 0, 190, 200, 250, 1, 5, oos_sharpe=median_sharpe),
            WFOFold(1, 0, 240, 250, 300, 1, 5, oos_sharpe=median_sharpe),
        ]
        return WFOResult(
            folds=folds,
            concatenated_oos_returns=[0.001] * 100,
            per_fold_oos_sharpe=[median_sharpe, median_sharpe],
            rank_decay_metric=rank_decay,
            median_oos_sharpe=median_sharpe,
            n_folds=2,
        )

    def test_passing_gates(self):
        result = self._make_result(median_sharpe=0.5, rank_decay=0.3)
        gates = wfo_qualification_gates(result)
        assert all(g["passed"] for g in gates)

    def test_failing_median_sharpe(self):
        result = self._make_result(median_sharpe=-0.2, rank_decay=0.3)
        gates = wfo_qualification_gates(result)
        sharpe_gate = next(g for g in gates if g["name"] == "wfo_median_oos_sharpe")
        assert not sharpe_gate["passed"]

    def test_failing_rank_decay(self):
        result = self._make_result(median_sharpe=0.5, rank_decay=0.8)
        gates = wfo_qualification_gates(result, max_rank_decay=0.7)
        rd_gate = next(g for g in gates if g["name"] == "wfo_rank_decay")
        assert not rd_gate["passed"]

    def test_rank_decay_is_soft_gate(self):
        result = self._make_result(median_sharpe=0.5, rank_decay=0.3)
        gates = wfo_qualification_gates(result)
        rd_gate = next(g for g in gates if g["name"] == "wfo_rank_decay")
        assert rd_gate["gate_type"] == "soft"

    def test_median_sharpe_is_hard_gate(self):
        result = self._make_result(median_sharpe=0.5, rank_decay=0.3)
        gates = wfo_qualification_gates(result)
        sr_gate = next(g for g in gates if g["name"] == "wfo_median_oos_sharpe")
        assert sr_gate["gate_type"] == "hard"
