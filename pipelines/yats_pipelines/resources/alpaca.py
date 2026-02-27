import os
from dataclasses import dataclass, field


@dataclass
class AlpacaResource:
    """Alpaca resource — API key from environment."""

    api_key: str = field(default_factory=lambda: os.environ.get("ALPACA_API_KEY", ""))
    api_secret: str = field(default_factory=lambda: os.environ.get("ALPACA_API_SECRET", ""))
    base_url: str = "https://paper-api.alpaca.markets"
