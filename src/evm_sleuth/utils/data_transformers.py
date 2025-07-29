"""Centralized data transformation utilities."""

import json
import datetime
from typing import Dict, Any, List


class DataTransformer:
    """Centralized data transformation utilities."""
    
    @staticmethod
    def convert_fields_to_json(item: Dict[str, Any], fields: List[str]) -> None:
        """Convert specified fields to JSON strings in place."""
        for field in fields:
            if field in item and item[field]:
                item[field] = json.dumps(item[field])
    
    @staticmethod
    def remove_fields(item: Dict[str, Any], fields: List[str]) -> None:
        """Remove specified fields from item in place."""
        for field in fields:
            item.pop(field, None)
    
    @staticmethod
    def rename_fields(item: Dict[str, Any], field_mappings: Dict[str, str]) -> None:
        """Rename fields according to mapping in place."""
        for old_key, new_key in field_mappings.items():
            if old_key in item:
                item[new_key] = item.pop(old_key)
    
    @staticmethod
    def safe_convert_large_integers(item: Dict[str, Any], fields: List[str] = None) -> None:
        """Convert large integers to strings to avoid 64-bit overflow in place."""
        def convert_nested(obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    obj[k] = convert_nested(v)
            elif isinstance(obj, list):
                for i, v in enumerate(obj):
                    obj[i] = convert_nested(v)
            elif isinstance(obj, int) and (obj > 2**63 - 1 or obj < -2**63):
                return str(obj)
            return obj
        
        if fields is None:
            # If no specific fields provided, check all values recursively
            convert_nested(item)
        else:
            for field in fields:
                if field in item:
                    item[field] = convert_nested(item[field])
    
    @staticmethod
    def process_timestamps(item: Dict[str, Any], timestamp_fields: List[str] = None) -> None:
        """Convert timestamp fields to datetime objects in place."""
        timestamp_fields = timestamp_fields or ["timestamp"]
        for field in timestamp_fields:
            if field in item and item[field] is not None:
                timestamp_value = item.pop(field)
                item["time"] = DataTransformer._convert_timestamp(timestamp_value)
    
    @staticmethod
    def _convert_timestamp(timestamp_value: Any) -> datetime.datetime:
        """Convert various timestamp formats to datetime."""
        if isinstance(timestamp_value, str):
            try:
                # Try parsing as ISO datetime string first (e.g., "2024-02-16T23:01:19.228Z")
                return datetime.datetime.fromisoformat(
                    timestamp_value.replace("Z", "+00:00")
                )
            except ValueError:
                try:
                    # If that fails, try as Unix timestamp string
                    return datetime.datetime.fromtimestamp(
                        int(timestamp_value), tz=datetime.timezone.utc
                    )
                except ValueError:
                    # Keep original value if parsing fails
                    return timestamp_value
        else:
            # Assume it's a numeric Unix timestamp
            return datetime.datetime.fromtimestamp(
                timestamp_value, tz=datetime.timezone.utc
            )
    
    def standardize_item(self, item: Dict[str, Any], transformations: Dict[str, Any] = None) -> Dict[str, Any]:
        """Apply standard transformations to an item."""
        if not transformations:
            return item
        
        # Apply field conversions
        if "json_fields" in transformations:
            self.convert_fields_to_json(item, transformations["json_fields"])
        
        # Remove unwanted fields
        if "remove_fields" in transformations:
            self.remove_fields(item, transformations["remove_fields"])
        
        # Rename fields
        if "field_mappings" in transformations:
            self.rename_fields(item, transformations["field_mappings"])
        
        # Convert timestamps
        if "timestamp_fields" in transformations:
            self.process_timestamps(item, transformations["timestamp_fields"])
        
        # Handle large integers
        if "large_integer_fields" in transformations:
            self.safe_convert_large_integers(item, transformations["large_integer_fields"])
        else:
            # By default, check all fields for large integers
            self.safe_convert_large_integers(item)
        
        return item