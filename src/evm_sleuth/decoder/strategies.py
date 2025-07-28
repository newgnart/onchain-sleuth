"""Decoding strategies for event data."""

from enum import Enum
from typing import Any, Dict, List

from evm_sleuth.core.base import BaseDecoder
from .types import EventDefinition
from .utils import HexUtils, TypeUtils


class DecodingStrategy(Enum):
    """Available decoding strategies."""
    BASIC = "basic"
    TUPLE_AWARE = "tuple_aware"


class BasicDecodingStrategy(BaseDecoder):
    """Basic event decoding strategy for simple parameter types."""
    
    def decode(self, event_def: EventDefinition, topics: List[str], data: str) -> Dict[str, Any]:
        """Decode event using basic strategy."""
        decoded = {"event": event_def.name}
        topic_index = 1
        
        # Decode indexed parameters from topics
        for param in event_def.inputs:
            if param.get("indexed", False):
                if topic_index < len(topics):
                    decoded[param["name"]] = self._decode_parameter(
                        param["type"], topics[topic_index]
                    )
                    topic_index += 1
        
        # Decode non-indexed parameters from data
        if not HexUtils.is_empty_hex(data):
            data_params = [p for p in event_def.inputs if not p.get("indexed", False)]
            if data_params:
                decoded_data = self._decode_data_parameters(data_params, data)
                decoded.update(decoded_data)
        
        return decoded
    
    def _decode_data_parameters(self, params: List[Dict[str, Any]], data: str) -> Dict[str, Any]:
        """Decode data parameters using basic approach."""
        if HexUtils.is_empty_hex(data):
            return {}
        
        hex_data = HexUtils.normalize_hex(data)
        decoded = {}
        data_offset = 0
        
        for param in params:
            param_type = param["type"]
            
            # Skip dynamic types in basic strategy
            if param_type == "string" or param_type.startswith("bytes"):
                continue
            
            start = data_offset
            end = start + 64  # Assumes 32-byte slots
            if end <= len(hex_data):
                param_hex = hex_data[start:end]
                decoded[param["name"]] = self._decode_parameter(
                    param_type, "0x" + param_hex
                )
                data_offset = end
        
        return decoded
    
    @staticmethod
    def _decode_parameter(param_type: str, value: str) -> Any:
        """Decode a single parameter based on its type."""
        if HexUtils.is_empty_hex(value):
            return None
        
        hex_value = HexUtils.normalize_hex(value)
        
        if TypeUtils.is_address_type(param_type):
            return HexUtils.extract_address(hex_value)
        elif param_type.startswith("uint"):
            return int(hex_value, 16)
        elif TypeUtils.is_signed_type(param_type):
            bit_size = TypeUtils.get_bit_size(param_type)
            val = int(hex_value, 16)
            if val >= (1 << (bit_size - 1)):
                val -= 1 << bit_size
            return val
        elif TypeUtils.is_bool_type(param_type):
            return int(hex_value, 16) != 0
        elif TypeUtils.is_bytes_type(param_type):
            return "0x" + hex_value
        else:
            return "0x" + hex_value


class TupleAwareDecodingStrategy(BaseDecoder):
    """Tuple-aware event decoding strategy for complex parameter types."""
    
    def decode(self, event_def: EventDefinition, topics: List[str], data: str) -> Dict[str, Any]:
        """Decode event using tuple-aware strategy."""
        decoded = {"event": event_def.name}
        topic_index = 1
        
        # Decode indexed parameters from topics
        for param in event_def.inputs:
            if param.get("indexed", False):
                if topic_index < len(topics):
                    decoded[param["name"]] = BasicDecodingStrategy._decode_parameter(
                        param["type"], topics[topic_index]
                    )
                    topic_index += 1
        
        # Decode non-indexed parameters from data with tuple support
        if not HexUtils.is_empty_hex(data):
            data_params = [p for p in event_def.inputs if not p.get("indexed", False)]
            if data_params:
                decoded_data = self._decode_data_parameters_with_tuples(data_params, data)
                decoded.update(decoded_data)
        
        return decoded
    
    def _decode_data_parameters_with_tuples(
        self, params: List[Dict[str, Any]], data: str
    ) -> Dict[str, Any]:
        """Decode data parameters with tuple support."""
        if HexUtils.is_empty_hex(data):
            return {}
        
        hex_data = HexUtils.normalize_hex(data)
        decoded_params, _ = self._decode_complex_data(params, hex_data, 0)
        return decoded_params
    
    def _decode_complex_data(
        self, params: List[Dict[str, Any]], hex_data: str, data_offset: int
    ) -> tuple[Dict[str, Any], int]:
        """Decode complex data with support for tuples and arrays."""
        decoded = {}
        current_offset = data_offset
        
        for param in params:
            param_type = param["type"]
            param_name = param["name"]
            
            # Skip array types for now (complex to implement)
            if param_type.endswith("[]"):
                current_offset += 64
                continue
            
            if param_type == "tuple":
                # Recursively decode tuple components
                decoded_value, consumed_len = self._decode_complex_data(
                    param["components"], hex_data, current_offset
                )
                decoded[param_name] = decoded_value
                current_offset += consumed_len
            elif param_type == "string" or param_type.startswith("bytes"):
                # Dynamic types are not fully supported, placeholder
                current_offset += 64
                continue
            else:
                # Static types
                start = current_offset
                end = start + 64
                if end <= len(hex_data):
                    param_hex = hex_data[start:end]
                    decoded[param_name] = BasicDecodingStrategy._decode_parameter(
                        param_type, "0x" + param_hex
                    )
                    current_offset = end
                else:
                    break
        
        return decoded, (current_offset - data_offset)


class StrategyFactory:
    """Factory for creating decoding strategies."""
    
    _strategies = {
        DecodingStrategy.BASIC: BasicDecodingStrategy,
        DecodingStrategy.TUPLE_AWARE: TupleAwareDecodingStrategy,
    }
    
    @classmethod
    def create_strategy(cls, strategy_type: DecodingStrategy) -> BaseDecoder:
        """Create a decoding strategy instance."""
        if strategy_type not in cls._strategies:
            raise ValueError(f"Unknown strategy type: {strategy_type}")
        
        return cls._strategies[strategy_type]()