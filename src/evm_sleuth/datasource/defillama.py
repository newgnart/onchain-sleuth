import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.sources.helpers.rest_client import paginators
from dlt.common.typing import TDataItems
from typing import Iterable, Literal, Optional
import json
import logging
import datetime


logger = logging.getLogger(__name__)

from stables.config import API_URL


def _create_defillama_source(
    base_url: str, endpoint: str, data_selector: str, params: Optional[dict] = {}
) -> dlt.sources.DltResource:
    """
    Creates a dlt rest_api_source for a given set of API parameters.
    """
    source = rest_api_source(
        {
            "client": {
                "base_url": base_url,
                "paginator": paginators.SinglePagePaginator(),
            },
            "resources": [
                {
                    "name": endpoint,
                    "endpoint": {
                        "path": f"/{endpoint}",
                        "data_selector": data_selector,
                        "params": params,
                    },
                }
            ],
        }
    )
    return source.resources[endpoint]


def _timestamp_to_datetime(
    timestamp: int, timezone: datetime.timezone = datetime.timezone.utc
) -> datetime.datetime:
    """
    Converts a Unix timestamp to a UTC datetime object.
    """
    return datetime.datetime.fromtimestamp(timestamp, tz=timezone)


def _convert_fields_to_json(item: dict, fields: list[str]) -> None:
    """Convert specified fields to JSON strings in place."""
    for field in fields:
        if field in item and item[field]:
            item[field] = json.dumps(item[field])


def _remove_fields(item: dict, fields: list[str]) -> None:
    """Remove specified fields from item in place."""
    for field in fields:
        if field in item:
            item.pop(field)


def _rename_fields(item: dict, field_mappings: dict[str, str]) -> None:
    """Rename fields according to mapping in place."""
    for old_key, new_key in field_mappings.items():
        if old_key in item:
            item[new_key] = item.pop(old_key)


def _process_timestamps(item: dict, timestamp_fields: list[str] = None) -> None:
    """Convert timestamp fields to datetime objects in place."""
    timestamp_fields = timestamp_fields or ["timestamp"]
    for field in timestamp_fields:
        if field in item and item[field] is not None:
            timestamp_value = item.pop(field)
            # Handle different timestamp formats
            if isinstance(timestamp_value, str):
                try:
                    # Try parsing as ISO datetime string first (e.g., "2024-02-16T23:01:19.228Z")
                    item["time"] = datetime.datetime.fromisoformat(
                        timestamp_value.replace("Z", "+00:00")
                    )
                except ValueError:
                    # If that fails, try as Unix timestamp string
                    try:
                        timestamp_value = int(timestamp_value)
                        item["time"] = _timestamp_to_datetime(timestamp_value)
                    except ValueError:
                        # Keep original value if parsing fails
                        item["time"] = timestamp_value
            else:
                # Assume it's a numeric Unix timestamp
                item["time"] = _timestamp_to_datetime(timestamp_value)


def _standardize_item(item: dict, transformations: dict = None) -> dict:
    """Apply standard transformations to an item."""
    if not transformations:
        return item

    # Apply field conversions
    if "json_fields" in transformations:
        _convert_fields_to_json(item, transformations["json_fields"])

    # Remove unwanted fields
    if "remove_fields" in transformations:
        _remove_fields(item, transformations["remove_fields"])

    # Rename fields
    if "field_mappings" in transformations:
        _rename_fields(item, transformations["field_mappings"])

    # Convert timestamps
    if "timestamp_fields" in transformations:
        _process_timestamps(item, transformations["timestamp_fields"])

    return item


@dlt.resource(
    columns={
        "price": {"data_type": "double", "nullable": True},
    }
)
def stables_metadata() -> Iterable[TDataItems]:
    """
    Fetches stablecoin data from DefiLlama and yields data for the 'stables' table.
    """

    def _get_circulating_value(data: dict | None, peg_type: str) -> float | None:
        """Safely extracts circulating value from a dictionary for a given peg type."""
        if isinstance(data, dict):
            return data.get(peg_type)
        return None

    source = _create_defillama_source(
        API_URL.DeFiLlamaStablecoins, "stablecoins", data_selector="peggedAssets"
    )

    for item in source:
        peg_type = item.get("pegType")
        if not peg_type:
            continue

        # Convert nested circulating data to flat values
        circulating_keys = [
            "circulating",
            "circulatingPrevDay",
            "circulatingPrevWeek",
            "circulatingPrevMonth",
        ]
        for key in circulating_keys:
            if key in item:
                item[key] = _get_circulating_value(item[key], peg_type)

        # Apply standardized transformations
        _standardize_item(
            item,
            {
                "json_fields": ["chains"],
                "remove_fields": ["chainCirculating"],
                "field_mappings": {
                    "pegType": "peg_type",
                    "pegMechanism": "peg_mechanism",
                    "priceSource": "price_source",
                },
            },
        )

        yield item


@dlt.resource
def stable_data(
    id: int,
    get_response: Literal[
        "chainBalances", "currentChainBalances"
    ] = "currentChainBalances",
    include_metadata: bool = False,
) -> Iterable[TDataItems]:
    """Get chain circulating data for a specific stablecoin by ID with optional metadata inclusion."""
    source = _create_defillama_source(
        API_URL.DeFiLlamaStablecoins,
        f"stablecoin/{id}",
        data_selector="$",  # Get full response
    )

    def _process_chain_balances(response: dict, metadata: dict) -> Iterable[dict]:
        """Process historical chainBalances data."""
        for chain_name, chain_data in response.get("chainBalances", {}).items():
            for entry in chain_data.get("tokens", []):
                circulating_data = entry.get("circulating", {})
                if not circulating_data:
                    continue

                # Extract circulating value and timestamp
                circulating_value = list(circulating_data.values())[0]
                timestamp = entry.get("date")

                item = {
                    "id": id,
                    "chain": chain_name,
                    "circulating": (
                        int(circulating_value)
                        if circulating_value is not None
                        else None
                    ),
                    "timestamp": timestamp,
                    **metadata,
                }
                _standardize_item(item, {"timestamp_fields": ["timestamp"]})
                yield item

    def _process_current_chain_balances(
        response: dict, metadata: dict
    ) -> Iterable[dict]:
        """Process current chainBalances data."""
        for chain_name, chain_data in response.get("currentChainBalances", {}).items():
            if not isinstance(chain_data, dict) or not chain_data:
                continue

            # Extract the first (and usually only) circulation value
            circulating = list(chain_data.values())[0]

            item = {
                "id": id,
                "chain": chain_name,
                "circulating": int(circulating) if circulating is not None else None,
                "timestamp": int(
                    datetime.datetime.now(tz=datetime.timezone.utc).timestamp()
                ),
                **metadata,
            }
            _standardize_item(item, {"timestamp_fields": ["timestamp"]})
            yield item

    for response in source:
        # Extract metadata once if requested
        metadata = {}
        if include_metadata:
            # Include all metadata fields, excluding the time-series data
            excluded_fields = {"chainBalances", "currentChainBalances"}
            metadata = {k: v for k, v in response.items() if k not in excluded_fields}

            # Convert nested objects/arrays to JSON strings for storage
            json_fields = ["auditLinks", "tokens"]
            _convert_fields_to_json(metadata, json_fields)

        # Process responses based on requested data type
        processor = (
            _process_chain_balances
            if get_response == "chainBalances"
            else _process_current_chain_balances
        )

        yield from processor(response, metadata)


@dlt.resource
def token_price(
    network: str, contract_address: str, params: Optional[dict] = None
) -> Iterable[TDataItems]:
    """Get token price data for a specific network and contract address."""
    default_params = {"span": 1000, "period": "1d"}
    params = params or default_params

    source = _create_defillama_source(
        "https://coins.llama.fi",
        f"chart/{network}:{contract_address}",
        data_selector="coins",
        params=params,
    )

    token_key = f"{network}:{contract_address}"

    for coins_data in source:
        token_info = coins_data.get(token_key, {})
        if not token_info.get("prices"):
            continue

        # Extract token metadata once
        base_metadata = {
            "network": network,
            "contract_address": contract_address,
            "symbol": token_info.get("symbol"),
            "decimals": token_info.get("decimals"),
            "confidence": token_info.get("confidence"),
        }

        # Yield price data for each timestamp
        for price_entry in token_info["prices"]:
            item = {
                **base_metadata,
                **price_entry,  # Contains 'timestamp' and 'price'
            }

            # Apply standardized transformations
            _standardize_item(
                item,
                {"timestamp_fields": ["timestamp"]},
            )
            yield item


@dlt.resource
def protocol_revenue(
    protocol: str,
    data_selector: Literal[
        "totalDataChart", "totalDataChartBreakdown"
    ] = "totalDataChartBreakdown",
    include_metadata: bool = False,
) -> Iterable[TDataItems]:
    """Get protocol revenue data with optional metadata inclusion."""
    source = _create_defillama_source(
        "https://api.llama.fi",
        f"summary/fees/{protocol}",
        data_selector="$",  # Get full response to access metadata if needed
    )

    for response in source:
        # Extract metadata once if requested
        metadata = {}
        if include_metadata:
            # Include all metadata fields, excluding the time-series data
            excluded_fields = {"totalDataChart", "totalDataChartBreakdown"}
            metadata = {k: v for k, v in response.items() if k not in excluded_fields}

            # Convert nested objects/arrays to JSON strings for storage
            json_fields = [
                "chains",
                "audit_links",
                "audits",
                "childProtocols",
                "linkedProtocols",
            ]
            _convert_fields_to_json(metadata, json_fields)

        # Process the time-series data
        time_series_data = response.get(data_selector, [])
        if not time_series_data:
            continue

        for item in time_series_data:
            if not isinstance(item, list) or len(item) != 2:
                continue

            timestamp, data = item[0], item[1]

            if data_selector == "totalDataChart":
                # Simple format: [timestamp, revenue]
                revenue_item = {
                    "timestamp": timestamp,
                    "revenue": data,
                    "protocol": protocol,
                    **metadata,
                }
                _standardize_item(revenue_item, {"timestamp_fields": ["timestamp"]})
                yield revenue_item

            else:  # totalDataChartBreakdown
                # Nested format: [timestamp, {chain: {sub_protocol: revenue}}]
                if not isinstance(data, dict):
                    continue

                # Flatten nested structure and yield each chain/sub_protocol combination
                for chain, chain_data in data.items():
                    if isinstance(chain_data, dict):
                        for sub_protocol, revenue_value in chain_data.items():
                            revenue_item = {
                                "timestamp": timestamp,
                                "chain": chain,
                                "protocol": protocol,
                                "sub_protocol": sub_protocol,
                                "revenue": revenue_value,
                                **metadata,
                            }
                            _standardize_item(
                                revenue_item, {"timestamp_fields": ["timestamp"]}
                            )
                            yield revenue_item


@dlt.resource
def all_yield_pools() -> Iterable[TDataItems]:
    """Get the latest data for all yield pools."""
    source = _create_defillama_source(
        API_URL.DeFiLlamaYields, "pools", data_selector="data"
    )

    for pool in source:
        # Extract token arrays before removing them
        reward_tokens = pool.get("rewardTokens", []) or []
        underlying_tokens = pool.get("underlyingTokens", []) or []

        # Apply standardized transformations
        _standardize_item(
            pool,
            {
                "remove_fields": ["rewardTokens", "underlyingTokens"],
                "field_mappings": {},  # Add any field renames if needed
            },
        )

        # Add processed token arrays as JSON strings
        pool["reward_tokens"] = json.dumps(reward_tokens)
        pool["underlying_tokens"] = json.dumps(underlying_tokens)

        yield pool


@dlt.resource(
    columns={
        "apy_reward": {"data_type": "double", "nullable": True},
        "il7d": {"data_type": "double", "nullable": True},
        "apy_base7d": {"data_type": "double", "nullable": True},
        "apy_base": {"data_type": "double", "nullable": True},
        "apy": {"data_type": "double", "nullable": True},
        "tvl_usd": {"data_type": "double", "nullable": True},
    }
)
def yield_pool(pool_id: str, pool_name: str) -> Iterable[TDataItems]:
    """Get historical data for a yield pool."""
    source = _create_defillama_source(
        API_URL.DeFiLlamaYields, f"chart/{pool_id}", data_selector="data"
    )

    # Common nullable fields that might be missing from the API

    for item in source:
        # Add pool identification
        item["pool_id"] = pool_id
        item["pool_name"] = pool_name

        # Apply standardized transformations
        _standardize_item(
            item,
            {"timestamp_fields": ["timestamp"]},
        )

        yield item
