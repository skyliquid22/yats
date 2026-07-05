# Fixes Summary

## Issue 1: Silent Equal-Weight Fallback in Experiment Evaluation
**File**: `pipelines/yats_pipelines/jobs/experiment_run.py`
**Location**: Lines 232-237 in the `evaluate_experiment` function

### Problem
The code was falling back to equal-weight weights for ALL non-RL policies instead of implementing proper rollout logic for existing non-RL policies like `equal_weight` and `sma`.

### Solution
Implemented proper policy-specific rollout logic:
1. **Equal-weight policy**: Uses the `EqualWeightPolicy` class from `research/policies/equal_weight_policy.py` to generate proper weights
2. **SMA policy**: Uses the `SMAWeightPolicy` class from `research/policies/sma_weight_policy.py` to generate weights based on SMA crossover signals
3. **Unsupported policies**: Now raise a clear `ValueError` with supported policy types listed

### Changes Made
- Replaced the generic equal-weight fallback with policy-specific rollout implementations
- Added imports for `EqualWeightPolicy` and `SMAWeightPolicy` 
- Added proper weight generation logic for each supported policy type
- Added error handling for unsupported policy types

## Issue 2: win_rate Indexing Bug
**File**: `pipelines/yats_pipelines/jobs/experiment_run.py`
**Location**: Line 292 in the `update_experiment_index` function

### Problem
The code was looking for `win_rate` in the `performance` metrics section instead of the `trading` metrics section where it actually resides according to the PRD.

### Solution
Changed the lookup from `perf.get("win_rate")` to `trading.get("win_rate")` to correctly index the win_rate metric.

### Changes Made
- Line 292: `"win_rate": perf.get("win_rate")` → `"win_rate": trading.get("win_rate")`

## Verification
- All existing tests pass (520 tests passed)
- SMA policy now produces different metrics than equal-weight policy as expected
- Unsupported policy types now raise clear error messages
- win_rate is now properly indexed in the experiment index