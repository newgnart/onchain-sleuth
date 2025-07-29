"""EVM Sleuth - Ethereum blockchain data analysis toolkit."""

from .factory import (
    # APIClientFactory,
    # DLTSourceFactory,
    PipelineFactory,
    # SourceRegistry,
)
from .config import settings
from .decoder import EventDecoder, DecodingStrategy
from .datasource import DeFiLlamaClient, DeFiLlamaSource
from .dataloader import PipelineManager, DataLoaderTemplate

__version__ = "0.0.1"

__all__ = [
    # Factory classes
    # "APIClientFactory",
    # "DLTSourceFactory",
    "PipelineFactory",
    # "SourceRegistry",
    # Configuration
    "settings",
    # Decoder
    "EventDecoder",
    "DecodingStrategy",
    # Data source clients
    "DeFiLlamaClient",
    "DeFiLlamaSource",
    # Data loading
    "PipelineManager",
    "DataLoaderTemplate",
]
