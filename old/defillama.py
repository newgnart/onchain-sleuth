"""DeFiLlama API client implementation."""

import dlt
import json
import datetime
from typing import Any, Dict, List, Optional, Iterator, Literal
from dlt.common.typing import TDataItems

from evm_sleuth.core.base import BaseAPIClient, BaseDLTResource, APIConfig
from evm_sleuth.config.settings import settings
from evm_sleuth.utils.data_transformers import DataTransformer

from dlt.sources.rest_api import rest_api_source
from dlt.sources.helpers.rest_client import paginators


class DeFiLlamaClient(BaseAPIClient):
    """DeFiLlama API client implementation."""

    def __init__(
        self, calls_per_second: float = 10.0
    ):  # DeFiLlama usually has higher limits
        config = APIConfig(
            base_url=settings.api_urls.DEFILLAMA_API, rate_limit=calls_per_second
        )
        super().__init__(config)

    def _build_request_params(self, **kwargs) -> Dict[str, Any]:
        """Build request parameters (no API key needed for DeFiLlama)."""
        return kwargs

    def _handle_response(self, response) -> Any:
        """Handle DeFiLlama API response."""
        response.raise_for_status()
        return response.json()

    def get_protocol_data(self, protocol: str) -> Dict[str, Any]:
        """Get protocol data from DeFiLlama."""
        endpoint = f"protocol/{protocol}"
        return self.make_request(endpoint)

    def get_stablecoin_data(self, coin_id: int) -> Dict[str, Any]:
        """Get stablecoin data by ID."""
        # Use stablecoins API base URL
        original_base_url = self.config.base_url
        self.config.base_url = settings.api_urls.DEFILLAMA_STABLECOINS

        try:
            endpoint = f"stablecoin/{coin_id}"
            result = self.make_request(endpoint)
            return result
        finally:
            self.config.base_url = original_base_url

    def get_token_price(
        self, network: str, contract_address: str, **params
    ) -> Dict[str, Any]:
        """Get token price data."""
        # Use coins API base URL
        original_base_url = self.config.base_url
        self.config.base_url = settings.api_urls.DEFILLAMA_COINS

        try:
            endpoint = f"chart/{network}:{contract_address}"
            result = self.make_request(endpoint, params)
            return result
        finally:
            self.config.base_url = original_base_url


class DeFiLlamaDLTResource(BaseDLTResource):
    """DLT resource for DeFiLlama data."""

    def __init__(self, client: DeFiLlamaClient):
        super().__init__(client)
        self.data_transformer = DataTransformer()

    def get_resource_name(self) -> str:
        return "defillama"
    
    @dlt.source
    def create_source(self):
        """Create a DLT source with all available resources."""
        return [
            stables_metadata(self.client),
            all_yield_pools(self.client),
        ]

    def create_dlt_source(
        self,
        base_url: str,
        endpoint: str,
        data_selector: str,
        params: Optional[Dict] = None,
    ):
        """Create DLT source for DeFiLlama API."""
        source = rest_api_source(
            {
                "client": {
                    "base_url": base_url,
                    "paginator": paginators.SinglePagePaginator(),
                    "session": self.client._session,
                },
                "resources": [
                    {
                        "name": endpoint,
                        "endpoint": {
                            "path": f"/{endpoint}",
                            "data_selector": data_selector,
                            "params": params or {},
                        },
                    }
                ],
            }
        )
        return source.resources[endpoint]

    def _stables_metadata_impl(self) -> Iterator[TDataItems]:
        """Fetch stablecoin metadata from DeFiLlama."""

        def _get_circulating_value(data: Dict | None, peg_type: str) -> float | None:
            """Safely extract circulating value from a dictionary for a given peg type."""
            if isinstance(data, dict):
                return data.get(peg_type)
            return None

        source = self.create_dlt_source(
            settings.api_urls.DEFILLAMA_STABLECOINS, "stablecoins", "peggedAssets"
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
            self.data_transformer.standardize_item(
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

    def _stable_data_impl(self, id: int, get_response: Literal["chainBalances", "currentChainBalances"] = "currentChainBalances", include_metadata: bool = False) -> Iterator[TDataItems]:
        """Implementation for stable_data resource."""
        # Move existing implementation here
        source = self.create_dlt_source(
            settings.api_urls.DEFILLAMA_STABLECOINS,
            f"stablecoin/{id}",
            "$",  # Get full response
        )

        def _process_chain_balances(response: Dict, metadata: Dict) -> Iterator[Dict]:
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
                    self.data_transformer.standardize_item(
                        item, {"timestamp_fields": ["timestamp"]}
                    )
                    yield item

        def _process_current_chain_balances(
            response: Dict, metadata: Dict
        ) -> Iterator[Dict]:
            """Process current chainBalances data."""
            for chain_name, chain_data in response.get(
                "currentChainBalances", {}
            ).items():
                if not isinstance(chain_data, dict) or not chain_data:
                    continue

                # Extract the first (and usually only) circulation value
                circulating = list(chain_data.values())[0]

                item = {
                    "id": id,
                    "chain": chain_name,
                    "circulating": (
                        int(circulating) if circulating is not None else None
                    ),
                    "timestamp": int(
                        datetime.datetime.now(tz=datetime.timezone.utc).timestamp()
                    ),
                    **metadata,
                }
                self.data_transformer.standardize_item(
                    item, {"timestamp_fields": ["timestamp"]}
                )
                yield item

        for response in source:
            # Extract metadata once if requested
            metadata = {}
            if include_metadata:
                # Include all metadata fields, excluding the time-series data
                excluded_fields = {"chainBalances", "currentChainBalances"}
                metadata = {
                    k: v for k, v in response.items() if k not in excluded_fields
                }

                # Convert nested objects/arrays to JSON strings for storage
                json_fields = ["auditLinks", "tokens"]
                self.data_transformer.convert_fields_to_json(metadata, json_fields)

            # Process responses based on requested data type
            processor = (
                _process_chain_balances
                if get_response == "chainBalances"
                else _process_current_chain_balances
            )

            yield from processor(response, metadata)


    def _token_price_impl(self, network: str, contract_address: str, params: Optional[Dict] = None) -> Iterator[TDataItems]:
        """Implementation for token_price resource."""
        default_params = {"span": 1000, "period": "1d"}
        params = params or default_params

        source = self.create_dlt_source(
            settings.api_urls.DEFILLAMA_COINS,
            f"chart/{network}:{contract_address}",
            "coins",
            params,
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
                self.data_transformer.standardize_item(
                    item,
                    {"timestamp_fields": ["timestamp"]},
                )
                yield item

    def _protocol_revenue_impl(
        self,
        protocol: str,
        data_selector: Literal[
            "totalDataChart", "totalDataChartBreakdown"
        ] = "totalDataChartBreakdown",
        include_metadata: bool = False,
    ) -> Iterator[TDataItems]:
        """Get protocol revenue data with optional metadata inclusion."""
        source = self.create_dlt_source(
            settings.api_urls.DEFILLAMA_API,
            f"summary/fees/{protocol}",
            "$",  # Get full response to access metadata if needed
        )

        for response in source:
            # Extract metadata once if requested
            metadata = {}
            if include_metadata:
                # Include all metadata fields, excluding the time-series data
                excluded_fields = {"totalDataChart", "totalDataChartBreakdown"}
                metadata = {
                    k: v for k, v in response.items() if k not in excluded_fields
                }

                # Convert nested objects/arrays to JSON strings for storage
                json_fields = [
                    "chains",
                    "audit_links",
                    "audits",
                    "childProtocols",
                    "linkedProtocols",
                ]
                self.data_transformer.convert_fields_to_json(metadata, json_fields)

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
                    self.data_transformer.standardize_item(
                        revenue_item, {"timestamp_fields": ["timestamp"]}
                    )
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
                                self.data_transformer.standardize_item(
                                    revenue_item, {"timestamp_fields": ["timestamp"]}
                                )
                                yield revenue_item

    def _all_yield_pools_impl(self) -> Iterator[TDataItems]:
        """Get the latest data for all yield pools."""
        source = self.create_dlt_source(
            settings.api_urls.DEFILLAMA_YIELDS, "pools", "data"
        )

        for pool in source:
            # Extract token arrays before removing them
            reward_tokens = pool.get("rewardTokens", []) or []
            underlying_tokens = pool.get("underlyingTokens", []) or []

            # Apply standardized transformations
            self.data_transformer.standardize_item(
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

    def _yield_pool_impl(self, pool_id: str, pool_name: str) -> Iterator[TDataItems]:
        """Get historical data for a yield pool."""
        source = self.create_dlt_source(
            settings.api_urls.DEFILLAMA_YIELDS, f"chart/{pool_id}", "data"
        )

        for item in source:
            # Add pool identification
            item["pool_id"] = pool_id
            item["pool_name"] = pool_name

            # Apply standardized transformations
            self.data_transformer.standardize_item(
                item,
                {"timestamp_fields": ["timestamp"]},
            )

            yield item


# Standalone DLT resource functions that can be used directly with pipeline.run()

@dlt.resource(
    columns={
        "price": {"data_type": "double", "nullable": True},
    }
)
def stables_metadata(client: DeFiLlamaClient = None) -> Iterator[TDataItems]:
    """Fetch stablecoin metadata from DeFiLlama."""
    if client is None:
        client = DeFiLlamaClient()
    
    resource_helper = DeFiLlamaDLTResource(client)
    yield from resource_helper._stables_metadata_impl()


@dlt.resource
def stable_data(
    client: DeFiLlamaClient,
    id: int,
    get_response: Literal["chainBalances", "currentChainBalances"] = "currentChainBalances",
    include_metadata: bool = False,
) -> Iterator[TDataItems]:
    """Get chain circulating data for a specific stablecoin by ID."""
    resource_helper = DeFiLlamaDLTResource(client)
    yield from resource_helper._stable_data_impl(id, get_response, include_metadata)


@dlt.resource
def token_price(
    client: DeFiLlamaClient,
    network: str,
    contract_address: str,
    params: Optional[Dict] = None,
) -> Iterator[TDataItems]:
    """Get token price data for a specific network and contract address."""
    resource_helper = DeFiLlamaDLTResource(client)
    yield from resource_helper._token_price_impl(network, contract_address, params)


@dlt.resource
def protocol_revenue(
    client: DeFiLlamaClient,
    protocol: str,
    data_selector: Literal["totalDataChart", "totalDataChartBreakdown"] = "totalDataChartBreakdown",
    include_metadata: bool = False,
) -> Iterator[TDataItems]:
    """Get protocol revenue data with optional metadata inclusion."""
    resource_helper = DeFiLlamaDLTResource(client)
    yield from resource_helper._protocol_revenue_impl(protocol, data_selector, include_metadata)


@dlt.resource
def all_yield_pools(client: DeFiLlamaClient = None) -> Iterator[TDataItems]:
    """Get the latest data for all yield pools."""
    if client is None:
        client = DeFiLlamaClient()
    
    resource_helper = DeFiLlamaDLTResource(client)
    yield from resource_helper._all_yield_pools_impl()


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
def yield_pool(client: DeFiLlamaClient, pool_id: str, pool_name: str) -> Iterator[TDataItems]:
    """Get historical data for a yield pool."""
    resource_helper = DeFiLlamaDLTResource(client)
    yield from resource_helper._yield_pool_impl(pool_id, pool_name)
