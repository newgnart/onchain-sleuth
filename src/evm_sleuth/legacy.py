"""Legacy compatibility layer for existing code."""

import warnings
from typing import Any, Dict, List, Optional

from .factory import ClientFactory, DLTResourceFactory
from .decoder import EventDecoder, DecodingStrategy
from .config.settings import settings


def create_legacy_etherscan_client(chainid: int, api_key: str, calls_per_second: float = 5):
    """Create Etherscan client using legacy interface."""
    warnings.warn(
        "Using legacy interface. Consider using ClientFactory.create_etherscan_client() instead.",
        DeprecationWarning,
        stacklevel=2
    )
    return ClientFactory.create_etherscan_client(chainid, api_key, calls_per_second)


def create_legacy_decoder(abi: List[Dict[str, Any]]) -> EventDecoder:
    """Create EventDecoder using legacy interface."""
    return EventDecoder(abi, DecodingStrategy.BASIC)


def decode_log_entry_with_tuples(
    decoder: EventDecoder,
    address: str,
    topics: List[str], 
    data: str,
    transaction_hash: str,
    block_number: int,
    txn_from: str,
    txn_to: str
):
    """Legacy method for tuple-aware decoding."""
    warnings.warn(
        "decode_log_entry_with_tuples is deprecated. Use decode_with_fallback() instead.",
        DeprecationWarning,
        stacklevel=2
    )
    return decoder.decode_with_fallback(
        address, topics, data, transaction_hash, block_number, txn_from, txn_to
    )


# Legacy configuration compatibility
class LegacyConfig:
    """Backward compatibility for old config access patterns."""
    
    @property
    def ETHERSCAN_API_KEY(self):
        return settings.api.etherscan_api_key
    
    @property
    def COINGECKO_API_KEY(self):
        return settings.api.coingecko_api_key
    
    @property
    def local_pg_config(self):
        return settings.local_db
    
    @property
    def remote_pg_config(self):
        return settings.remote_db
    
    @property
    def BlockExplorerColumns(self):
        return type('BlockExplorerColumns', (), {
            'Log': settings.columns.LOG_COLUMNS,
            'Transaction': settings.columns.TRANSACTION_COLUMNS
        })()
    
    @property
    def API_URL(self):
        return settings.api_urls


# Create legacy config instance for backward compatibility
legacy_config = LegacyConfig()