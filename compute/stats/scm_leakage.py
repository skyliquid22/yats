#!/usr/bin/env python3
"""Structural Causal Model leakage detection.

Detects potential look-ahead bias and data leakage in feature pipelines
by analyzing temporal dependencies between features and target.
"""
import argparse
import json
import sys

import numpy as np


def detect_leakage(
    features: np.ndarray,
    target: np.ndarray,
    feature_names: list[str],
    max_lag: int = 5,
) -> dict:
    """Detect leakage via lead-lag cross-correlation analysis.

    If a feature has higher correlation with future target values than
    with concurrent/past values, it likely contains look-ahead information.
    """
    n_features = features.shape[1]
    results: list[dict] = []

    for i in range(n_features):
        feat = features[:, i]
        # Standardize
        feat_std = (feat - feat.mean()) / (feat.std() + 1e-10)
        tgt_std = (target - target.mean()) / (target.std() + 1e-10)

        correlations: dict[str, float] = {}

        for lag in range(-max_lag, max_lag + 1):
            if lag < 0:
                # Feature leads target (potential leakage if high)
                corr = float(np.corrcoef(feat_std[:lag], tgt_std[-lag:])[0, 1])
                correlations[f"lag_{lag}"] = corr
            elif lag > 0:
                # Feature lags target (normal causal relationship)
                corr = float(np.corrcoef(feat_std[lag:], tgt_std[:-lag])[0, 1])
                correlations[f"lag_{lag}"] = corr
            else:
                corr = float(np.corrcoef(feat_std, tgt_std)[0, 1])
                correlations["lag_0"] = corr

        # Leakage score: max |lead correlation| - max |lag correlation|
        lead_corrs = [abs(v) for k, v in correlations.items() if k.startswith("lag_-") and not np.isnan(v)]
        lag_corrs = [abs(v) for k, v in correlations.items() if k.startswith("lag_") and not k.startswith("lag_-") and k != "lag_0" and not np.isnan(v)]

        max_lead = max(lead_corrs) if lead_corrs else 0.0
        max_lag_corr = max(lag_corrs) if lag_corrs else 0.0
        leakage_score = max_lead - max_lag_corr

        results.append({
            "feature": feature_names[i] if i < len(feature_names) else f"feature_{i}",
            "leakage_score": float(leakage_score),
            "leakage_detected": leakage_score > 0.1,
            "max_lead_correlation": float(max_lead),
            "max_lag_correlation": float(max_lag_corr),
            "concurrent_correlation": float(correlations.get("lag_0", 0.0)),
        })

    # Sort by leakage score descending
    results.sort(key=lambda x: x["leakage_score"], reverse=True)
    leaked = [r for r in results if r["leakage_detected"]]

    return {
        "features_analyzed": n_features,
        "leakage_detected": len(leaked),
        "leaked_features": [r["feature"] for r in leaked],
        "details": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="SCM leakage detection")
    parser.add_argument("--data-file", required=True, help="CSV with features + target column")
    parser.add_argument("--target-column", default="target", help="Name of target column")
    parser.add_argument("--max-lag", type=int, default=5, help="Max lag for cross-correlation")
    args = parser.parse_args()

    try:
        import csv
        with open(args.data_file) as f:
            reader = csv.reader(f)
            header = next(reader)
            data = np.array([list(map(float, row)) for row in reader])
    except Exception as e:
        print(json.dumps({"error": f"Failed to load data: {e}"}))
        sys.exit(1)

    target_idx = header.index(args.target_column) if args.target_column in header else -1
    if target_idx < 0:
        print(json.dumps({"error": f"Target column '{args.target_column}' not found. Columns: {header}"}))
        sys.exit(1)

    feature_names = [h for i, h in enumerate(header) if i != target_idx]
    features = np.delete(data, target_idx, axis=1)
    target = data[:, target_idx]

    result = detect_leakage(features, target, feature_names, max_lag=args.max_lag)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
