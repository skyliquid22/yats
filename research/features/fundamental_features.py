"""Fundamental features — Appendix J.3.

From canonical_fundamentals and canonical_financial_metrics.
Forward-filled to daily frequency with point-in-time semantics.
These are pass-through mappings from canonical fields to feature names.
"""

import pandas as pd

from research.features.feature_registry import feature

# Mapping from feature name to canonical_financial_metrics column.
# Fundamentals are forward-filled before feature computation, so each
# function simply extracts the pre-filled column.

_FUNDAMENTAL_MAP = {
    "pe_ttm": "pe_ratio",
    "ps_ttm": "ps_ratio",
    "pb": "pb_ratio",
    "ev_ebitda": "ev_ebitda",
    "roe": "roe",
    "gross_margin": "gross_margin",
    "operating_margin": "operating_margin",
    "fcf_margin": "fcf_margin",
    "debt_equity": "debt_to_equity",
    "eps_growth_1y": "eps_growth_yoy",
    "revenue_growth_1y": "revenue_growth_yoy",
}


def _make_fundamental_feature(feat_name: str, source_col: str):
    """Create and register a fundamental pass-through feature."""
    @feature(feat_name)
    def compute(df: pd.DataFrame) -> pd.Series:
        return df[source_col]
    compute.__name__ = f"compute_{feat_name}"
    compute.__doc__ = f"Forward-filled {source_col} from canonical_financial_metrics."
    return compute


# Register all fundamental features
for _feat_name, _source_col in _FUNDAMENTAL_MAP.items():
    _make_fundamental_feature(_feat_name, _source_col)
