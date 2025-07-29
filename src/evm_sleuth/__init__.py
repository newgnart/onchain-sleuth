"""EVM Sleuth - Ethereum blockchain data analysis toolkit."""

from .config import settings
from .decoder import EventDecoder, DecodingStrategy
from .datasource import DeFiLlamaClient, DeFiLlamaSource
from .dataloader import PipelineManager, TableConfig

__version__ = "0.0.1"

__all__ = [
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
    "TableConfig",
]
