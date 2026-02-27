import os
from dataclasses import dataclass, field


@dataclass
class FinancialDatasetsResource:
    """financialdatasets.ai resource."""

    api_key: str = field(default_factory=lambda: os.environ.get("FINANCIALDATASETS_API_KEY", ""))
    base_url: str = "https://api.financialdatasets.ai"
