"""Utility functions for event decoding."""

from typing import Optional


class HexUtils:
    """Utility class for hex string operations."""

    @staticmethod
    def normalize_hex(value: str) -> str:
        """Normalize hex string by removing 0x prefix if present."""
        return value[2:] if value.startswith("0x") else value

    @staticmethod
    def add_0x_prefix(value: str) -> str:
        """Add 0x prefix to hex string if not present."""
        return value if value.startswith("0x") else f"0x{value}"

    @staticmethod
    def is_empty_hex(value: str) -> bool:
        """Check if hex value is empty or just 0x."""
        return not value or value == "0x"

    @staticmethod
    def extract_address(hex_value: str) -> str:
        """Extract address from hex value (last 40 characters)."""
        return "0x" + hex_value[-40:].lower()


class TypeUtils:
    """Utility class for type-related operations."""

    @staticmethod
    def get_bit_size(param_type: str) -> int:
        """Extract bit size from parameter type."""
        if param_type.startswith(("uint", "int")):
            size_str = param_type[3:] if param_type[3:] else "256"
            return int(size_str)
        return 256

    @staticmethod
    def is_signed_type(param_type: str) -> bool:
        """Check if parameter type is signed."""
        return param_type.startswith("int")

    @staticmethod
    def is_address_type(param_type: str) -> bool:
        """Check if parameter type is address."""
        return param_type == "address"

    @staticmethod
    def is_bool_type(param_type: str) -> bool:
        """Check if parameter type is bool."""
        return param_type == "bool"

    @staticmethod
    def is_bytes_type(param_type: str) -> bool:
        """Check if parameter type is bytes."""
        return param_type.startswith("bytes")
