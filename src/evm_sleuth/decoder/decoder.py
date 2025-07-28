"""Core event decoding functionality."""

import json
import os
from typing import Any, Dict, List, Optional
from Crypto.Hash import keccak
from .types import DecodedEvent, EventDefinition
from .utils import HexUtils, TypeUtils
from .strategies import DecodingStrategy, StrategyFactory


class EventDecoder:
    """Event decoder using strategy pattern for different decoding approaches."""
    
    def __init__(
        self, 
        abi: List[Dict[str, Any]], 
        strategy: DecodingStrategy = DecodingStrategy.BASIC
    ):
        self._events_by_topic0 = self._build_event_definitions(abi)
        self._strategy = StrategyFactory.create_strategy(strategy)
        self._current_strategy_type = strategy

    def _build_event_definitions(
        self, abi: List[Dict[str, Any]]
    ) -> Dict[str, EventDefinition]:
        events_by_topic0 = {}
        for item in abi:
            if item.get("type") == "event":
                event_def = self._build_event_definition(item)
                events_by_topic0[event_def.topic0] = event_def
        return events_by_topic0

    def _build_event_definition(self, event_abi: Dict[str, Any]) -> EventDefinition:
        signature = self._get_event_signature(event_abi)
        topic0 = self._get_event_topic0(signature)
        topics = [topic0]
        data_params = []

        for input_item in event_abi.get("inputs", []):
            if input_item.get("indexed", False):
                topics.append(input_item)
            else:
                data_params.append(input_item)

        return EventDefinition(
            name=event_abi["name"],
            signature=signature,
            topic0=topic0,
            inputs=event_abi.get("inputs", []),
            topics=topics,
            data=data_params,
        )

    @staticmethod
    def _get_event_signature(event_abi: Dict[str, Any]) -> str:
        name = event_abi["name"]
        inputs = event_abi.get("inputs", [])

        def get_canonical_type(component: Dict[str, Any]) -> str:
            base_type_str = component["type"]

            # Replace contract types with 'address'
            if base_type_str.startswith("contract "):
                base_type_str = "address"

            if "components" in component and component["type"].startswith("tuple"):
                canonical_components = ",".join(
                    get_canonical_type(c) for c in component["components"]
                )
                base_type = f"({canonical_components})"
                if component["type"].endswith("[]"):
                    return f"{base_type}[]"
                return base_type
            return base_type_str

        param_types = [get_canonical_type(input_item) for input_item in inputs]
        return f"{name}({','.join(param_types)})"

    @staticmethod
    def _get_event_topic0(signature: str) -> str:
        hash_obj = keccak.new(digest_bits=256)
        hash_obj.update(signature.encode("utf-8"))
        return "0x" + hash_obj.hexdigest()

    def decode_log_entry(
        self,
        address: str,
        topics: List[str],
        data: str,
        transaction_hash: Optional[str] = None,
        block_number: Optional[int] = None,
        txn_from: Optional[str] = None,
        txn_to: Optional[str] = None,
    ) -> DecodedEvent:
        """Decode log entry using the configured strategy."""
        if not topics:
            return self._create_unknown_event(address, topics, data)

        topic0 = topics[0]
        event_def = self._events_by_topic0.get(topic0)

        if not event_def:
            return self._create_unknown_event(address, topics, data, topic0)

        # Use the current strategy to decode
        decoded_params = self._strategy.decode(event_def, topics, data)
        event_name = decoded_params.pop("event", "unknown")

        decoded_event = DecodedEvent(
            address=address,
            event_name=event_name,
            parameters=decoded_params,
            topic0=topic0,
            is_unknown=False,
            transaction_hash=transaction_hash,
            block_number=block_number,
            txn_from=txn_from,
            txn_to=txn_to,
        )
        return decoded_event

    def decode_with_fallback(
        self,
        address: str,
        topics: List[str],
        data: str,
        transaction_hash: Optional[str] = None,
        block_number: Optional[int] = None,
        txn_from: Optional[str] = None,
        txn_to: Optional[str] = None,
    ) -> DecodedEvent:
        """Decode log entry with fallback to tuple-aware strategy if basic fails."""
        # Try with current strategy first
        decoded_event = self.decode_log_entry(
            address, topics, data, transaction_hash, block_number, txn_from, txn_to
        )
        
        # If unknown and we're using basic strategy, try tuple-aware
        if (decoded_event.event_name == "unknown" and 
            self._current_strategy_type == DecodingStrategy.BASIC):
            
            # Temporarily switch to tuple-aware strategy
            original_strategy = self._strategy
            self._strategy = StrategyFactory.create_strategy(DecodingStrategy.TUPLE_AWARE)
            
            try:
                decoded_event = self.decode_log_entry(
                    address, topics, data, transaction_hash, block_number, txn_from, txn_to
                )
            finally:
                # Restore original strategy
                self._strategy = original_strategy
        
        return decoded_event


    def set_strategy(self, strategy: DecodingStrategy):
        """Change the decoding strategy."""
        self._strategy = StrategyFactory.create_strategy(strategy)
        self._current_strategy_type = strategy
    
    def get_current_strategy(self) -> DecodingStrategy:
        """Get the current decoding strategy."""
        return self._current_strategy_type
    
    def _create_unknown_event(
        self, address: str, topics: List[str], data: str, topic0: str = ""
    ) -> DecodedEvent:
        return DecodedEvent(
            address=address,
            event_name="unknown",
            parameters={},
            topic0=topic0,
            is_unknown=True,
            raw_topics=topics,
            raw_data=data,
        )
