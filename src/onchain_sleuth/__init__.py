"""EVM Sleuth - Ethereum blockchain data engineering toolkit."""

# from .config import settings, Settings

# from .decoder import EventDecoder, DecodingStrategy
from .source import (
    DeFiLlamaClient,
    DeFiLlamaSource,
    EtherscanClient,
    EtherscanSource,
)

# from .dataloader import PipelineManager, TableConfig

# from .utils.postgres import PostgresDestination, DuckdbDestination

__version__ = "0.0.1"

__all__ = [
    # Configuration
    # "settings",
    # "Settings",
    # Decoder
    # "EventDecoder",
    # "DecodingStrategy",
    # Data source clients
    "DeFiLlamaClient",
    "DeFiLlamaSource",
    "EtherscanClient",
    "EtherscanSource",
    # Data loading
    # "PipelineManager",
    # "TableConfig",
    # Database
    # "PostgresDestination",
    # "DuckdbDestination",
]
