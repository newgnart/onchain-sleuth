"""Centralized configuration management for evm_sleuth."""

import os
import json
from typing import Dict, Any, Optional
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class APISettings:
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
class DatabaseSettings:
    """Database configuration."""
    host: str
    port: int
    database: str
    user: str
    password: str
    
    @classmethod
    def from_env(cls, prefix: str) -> 'DatabaseSettings':
        """Create from environment variables with prefix."""
        return cls(
            host=os.getenv(f"{prefix}_POSTGRES_HOST"),
            port=int(os.getenv(f"{prefix}_POSTGRES_PORT", "5432")),
            database=os.getenv(f"{prefix}_POSTGRES_DB"),
            user=os.getenv(f"{prefix}_POSTGRES_USER"),
            password=os.getenv(f"{prefix}_POSTGRES_PASSWORD")
        )
    
    def get_connection_params(self) -> Dict[str, Any]:
        """Return connection parameters for database clients."""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
        }
    
    def get_connection_url(self) -> str:
        """Return connection URL for database clients."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


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


class Settings:
    """Main settings class."""
    
    def __init__(self):
        self.api = APISettings()
        self.local_db = DatabaseSettings.from_env("LOCAL")
        self.remote_db = DatabaseSettings.from_env("REMOTE")
        self.columns = ColumnSchemas()
        self.api_urls = APIUrls()
        
        # Load token configuration if available
        self._load_token_config()
    
    def _load_token_config(self):
        """Load token configuration from file."""
        try:
            token_config_path = os.path.join(
                os.path.dirname(__file__), "..", "address", "ybs_tokens.json"
            )
            if os.path.exists(token_config_path):
                with open(token_config_path, "r") as f:
                    self.ybs_tokens = json.load(f)
            else:
                self.ybs_tokens = {}
        except Exception:
            self.ybs_tokens = {}


# Global settings instance
settings = Settings()