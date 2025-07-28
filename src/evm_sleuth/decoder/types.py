"""Type definitions for event decoding."""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class EventDefinition:
    """Event definition from ABI."""
    name: str
    signature: str
    topic0: str
    inputs: List[Dict[str, Any]]
    topics: List[Any]  # [signature_hash, ...indexed_params]
    data: List[Dict[str, Any]]  # non-indexed parameters


@dataclass
class DecodedEvent:
    """Decoded event data."""
    address: str
    event_name: str
    parameters: Dict[str, Any]
    topic0: str
    is_unknown: bool = False
    raw_topics: Optional[List[str]] = None
    raw_data: Optional[str] = None