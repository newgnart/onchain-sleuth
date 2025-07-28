"""EVM Sleuth - Ethereum blockchain data analysis toolkit."""

from .factory import ClientFactory, DLTResourceFactory, PipelineFactory
from .config import settings
from .decoder import EventDecoder, DecodingStrategy
from .datasource import EtherscanClient, CoinGeckoClient, DeFiLlamaClient
from .dataloader import PipelineManager, DataLoaderTemplate

__version__ = "0.1.0"

__all__ = [
    # Factory classes
    "ClientFactory",
    "DLTResourceFactory", 
    "PipelineFactory",
    
    # Configuration
    "settings",
    
    # Decoder
    "EventDecoder",
    "DecodingStrategy",
    
    # Data source clients
    "EtherscanClient",
    "CoinGeckoClient",
    "DeFiLlamaClient",
    
    # Data loading
    "PipelineManager",
    "DataLoaderTemplate",
]