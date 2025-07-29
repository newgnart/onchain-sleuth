"""EVM Sleuth - Ethereum blockchain data analysis toolkit."""

from .factory import APIClientFactory, DLTResourceFactory, PipelineFactory
from .config import settings
from .decoder import EventDecoder, DecodingStrategy
from .datasource import (
    EtherscanClient, EtherscanDLTResource,
    CoinGeckoClient, CoinGeckoDLTResource, 
    DeFiLlamaClient, DeFiLlamaDLTResource
)
from .dataloader import PipelineManager, DataLoaderTemplate

__version__ = "0.0.1"

__all__ = [
    # Factory classes
    "APIClientFactory",
    "DLTResourceFactory",
    "PipelineFactory",
    # Configuration
    "settings",
    # Decoder
    "EventDecoder",
    "DecodingStrategy",
    # Data source clients
    "EtherscanClient",
    "EtherscanDLTResource",
    "CoinGeckoClient", 
    "CoinGeckoDLTResource",
    "DeFiLlamaClient",
    "DeFiLlamaDLTResource",
    # Data loading
    "PipelineManager",
    "DataLoaderTemplate",
]
