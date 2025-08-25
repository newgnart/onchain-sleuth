"""EVM Sleuth - Ethereum blockchain data engineering toolkit."""

from .config import settings, Settings
from .decoder import EventDecoder, DecodingStrategy
from .datasource import (
    DeFiLlamaClient,
    DeFiLlamaSource,
    EtherscanClient,
    EtherscanSource,
)
from .dataloader import PipelineManager, TableConfig

__version__ = "0.0.1"

__all__ = [
    # Configuration
    "settings",
    "Settings",
    # Decoder
    # "EventDecoder",
    # "DecodingStrategy",
    # Data source clients
    "DeFiLlamaClient",
    "DeFiLlamaSource",
    "EtherscanClient",
    "EtherscanSource",
    # Data loading
    "PipelineManager",
    "TableConfig",
]
