"""Centralized configuration management for onchaindata."""

import os
import json
from typing import Dict, Any, Optional
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class APIs:
    """API-specific settings."""

    etherscan_api_key: Optional[str] = None
    coingecko_api_key: Optional[str] = None

    # Rate limits (requests per second)
    etherscan_rate_limit: float = 5.0
    coingecko_rate_limit: float = 5.0
    defillama_rate_limit: float = 10.0

    def __post_init__(self):
        # Load from environment if not provided
        if self.etherscan_api_key is None:
            self.etherscan_api_key = os.getenv("ETHERSCAN_API_KEY")
        if self.coingecko_api_key is None:
            self.coingecko_api_key = os.getenv("COINGECKO_API_KEY")


@dataclass
class ColumnSchemas:
    """Standardized column schemas for DLT pipelines."""

    # Blockchain-related columns
    LOG_COLUMNS = {
        "topics": {"data_type": "json"},
        "block_number": {"data_type": "bigint"},
        "time_stamp": {"data_type": "bigint"},
        "gas_price": {"data_type": "bigint"},
        "gas_used": {"data_type": "bigint"},
        "log_index": {"data_type": "bigint"},
        "transaction_index": {"data_type": "bigint"},
    }

    TRANSACTION_COLUMNS = {
        "block_number": {"data_type": "bigint"},
        "time_stamp": {"data_type": "timestamp"},
    }

    # Price data columns
    PRICE_COLUMNS = {
        "timestamp": {"data_type": "timestamp", "timezone": False, "precision": 3},
        "price": {"data_type": "decimal"},
    }

    OHLC_COLUMNS = {
        "timestamp": {"data_type": "timestamp", "timezone": False, "precision": 3},
        "open": {"data_type": "decimal"},
        "high": {"data_type": "decimal"},
        "low": {"data_type": "decimal"},
        "close": {"data_type": "decimal"},
    }


class APIUrls:
    """API endpoint URLs."""

    ETHERSCAN = "https://api.etherscan.io/v2/api"
    COINGECKO = "https://api.coingecko.com/api/v3"
    DEFILLAMA_STABLECOINS = "https://stablecoins.llama.fi"
    DEFILLAMA_YIELDS = "https://yields.llama.fi"
    DEFILLAMA_API = "https://api.llama.fi"
    DEFILLAMA_COINS = "https://coins.llama.fi"
