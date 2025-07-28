"""Core event decoding functionality."""

import json
import os
from typing import Any, Dict, List
from Crypto.Hash import keccak
from .types import DecodedEvent, EventDefinition
from .utils import HexUtils, TypeUtils


class EventDecoder:
    def __init__(self, abi: List[Dict[str, Any]]):
        self._events_by_topic0 = self._build_event_definitions(abi)

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
    ) -> DecodedEvent:
        if not topics:
            return self._create_unknown_event(address, topics, data)

        topic0 = topics[0]
        event_def = self._events_by_topic0.get(topic0)

        if not event_def:
            return self._create_unknown_event(address, topics, data, topic0)

        decoded_params = self._decode_log_data(event_def, topics, data)
        event_name = decoded_params.pop("event", "unknown")

        decoded_event = DecodedEvent(
            address=address,
            event_name=event_name,
            parameters=decoded_params,
            topic0=topic0,
            is_unknown=False,
        )
        return decoded_event

    def decode_log_entry_with_tuples(
        self,
        address: str,
        topics: List[str],
        data: str,
    ) -> DecodedEvent:
        if not topics:
            return self._create_unknown_event(address, topics, data)

        topic0 = topics[0]
        event_def = self._events_by_topic0.get(topic0)

        if not event_def:
            return self._create_unknown_event(address, topics, data, topic0)

        decoded_params = self._decode_log_data_with_tuples(event_def, topics, data)
        event_name = decoded_params.pop("event", "unknown")

        decoded_event = DecodedEvent(
            address=address,
            event_name=event_name,
            parameters=decoded_params,
            topic0=topic0,
            is_unknown=False,
        )
        return decoded_event

    def _decode_log_data(
        self, event_def: EventDefinition, topics: List[str], data: str
    ) -> Dict[str, Any]:
        decoded = {"event": event_def.name}
        topic_index = 1

        for param in event_def.inputs:
            if param.get("indexed", False):
                if topic_index < len(topics):
                    decoded[param["name"]] = self._decode_parameter(
                        param["type"], topics[topic_index]
                    )
                    topic_index += 1

        if not HexUtils.is_empty_hex(data):
            data_params = [p for p in event_def.inputs if not p.get("indexed", False)]
            if data_params:
                decoded_data = self._decode_data_parameters(data_params, data)
                decoded.update(decoded_data)

        return decoded

    def _decode_log_data_with_tuples(
        self, event_def: EventDefinition, topics: List[str], data: str
    ) -> Dict[str, Any]:
        decoded = {"event": event_def.name}
        topic_index = 1

        for param in event_def.inputs:
            if param.get("indexed", False):
                if topic_index < len(topics):
                    decoded[param["name"]] = self._decode_parameter(
                        param["type"], topics[topic_index]
                    )
                    topic_index += 1

        if not HexUtils.is_empty_hex(data):
            data_params = [p for p in event_def.inputs if not p.get("indexed", False)]
            if data_params:
                decoded_data = self._decode_data_parameters_with_tuples(
                    data_params, data
                )
                decoded.update(decoded_data)

        return decoded

    def _decode_data_parameters(
        self, params: List[Dict[str, Any]], data: str
    ) -> Dict[str, Any]:
        if HexUtils.is_empty_hex(data):
            return {}

        hex_data = HexUtils.normalize_hex(data)
        decoded = {}
        data_offset = 0

        for param in params:
            param_type = param["type"]
            if param_type == "string" or param_type.startswith("bytes"):
                # Dynamic types (string, bytes) are handled differently
                # This basic decoder assumes fixed-size types for simplicity
                # A more robust implementation would handle dynamic types correctly
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

    def _decode_data_parameters_with_tuples(
        self, params: List[Dict[str, Any]], data: str
    ) -> Dict[str, Any]:
        if HexUtils.is_empty_hex(data):
            return {}
        hex_data = HexUtils.normalize_hex(data)
        decoded_params, _ = self._decode_complex_data(params, hex_data, 0)
        return decoded_params

    def _decode_complex_data(
        self, params: List[Dict[str, Any]], hex_data: str, data_offset: int
    ) -> (Dict[str, Any], int):
        decoded = {}
        current_offset = data_offset

        for param in params:
            param_type = param["type"]
            param_name = param["name"]

            if param_type.endswith("[]"):
                # For now, we skip array types as their handling is complex
                current_offset += 64
                continue

            if param_type == "tuple":
                decoded_value, consumed_len = self._decode_complex_data(
                    param["components"], hex_data, current_offset
                )
                decoded[param_name] = decoded_value
                current_offset += consumed_len
            elif param_type == "string" or param_type.startswith("bytes"):
                # Dynamic types are not fully supported, this is a placeholder
                current_offset += 64
                continue
            else:
                # Static types
                start = current_offset
                end = start + 64
                if end <= len(hex_data):
                    param_hex = hex_data[start:end]
                    decoded[param_name] = self._decode_parameter(
                        param_type, "0x" + param_hex
                    )
                    current_offset = end
                else:
                    break
        return decoded, (current_offset - data_offset)

    @staticmethod
    def _decode_parameter(param_type: str, value: str) -> Any:
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
