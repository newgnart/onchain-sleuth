"""DeFiLlama API client implementation."""

import dlt
import json
import datetime
from typing import Any, Dict, List, Optional, Iterator, Literal
from dlt.common.typing import TDataItems

from data_sleuth.core.base import BaseAPIClient, BaseSource, APIConfig
from data_sleuth.config.settings import settings
from data_sleuth.utils.data_transformers import DataTransformer


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

    def get_stablecoins_metadata(self) -> Dict[str, Any]:
        """Fetch stablecoins metadata from DeFiLlama API."""
        original_base_url = self.config.base_url
        self.config.base_url = settings.api_urls.DEFILLAMA_STABLECOINS

        try:
            endpoint = "stablecoins"
            result = self.make_request(endpoint)
            return result
        finally:
            self.config.base_url = original_base_url

    def get_stablecoin_data(self, coin_id: int) -> Dict[str, Any]:
        """Get stablecoin data by ID."""
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
        original_base_url = self.config.base_url
        self.config.base_url = settings.api_urls.DEFILLAMA_COINS

        try:
            endpoint = f"chart/{network}:{contract_address}"
            result = self.make_request(endpoint, params)
            return result
        finally:
            self.config.base_url = original_base_url

    def get_all_yield_pools(self) -> Dict[str, Any]:
        """Get all yield pools data."""
        original_base_url = self.config.base_url
        self.config.base_url = settings.api_urls.DEFILLAMA_YIELDS

        try:
            endpoint = "pools"
            result = self.make_request(endpoint)
            return result
        finally:
            self.config.base_url = original_base_url

    def get_yield_pool(self, pool_id: str) -> Dict[str, Any]:
        """Get historical data for a yield pool."""
        original_base_url = self.config.base_url
        self.config.base_url = settings.api_urls.DEFILLAMA_YIELDS

        try:
            endpoint = f"chart/{pool_id}"
            result = self.make_request(endpoint)
            return result
        finally:
            self.config.base_url = original_base_url

    def get_protocol_revenue(self, protocol: str) -> Dict[str, Any]:
        """Get protocol revenue/fees data."""
        endpoint = f"summary/fees/{protocol}"
        return self.make_request(endpoint)


class DeFiLlamaSource(BaseSource):
    """Creating DLT source for DeFiLlama data."""

    def __init__(self, client: DeFiLlamaClient):
        super().__init__(client)
        self.data_transformer = DataTransformer()

    def get_available_sources(self) -> List[str]:
        """Return list of available source names."""
        return [
            "stablecoins_metadata",
            "protocol_data",
            "stablecoin_data",
            "token_price",
            "yield_pools",
            "yield_pool_chart",
            "protocol_revenue",
        ]

    def stablecoins_metadata(self):
        """DLT resource for fetching stablecoins metadata with flattened circulating data."""

        def _get_circulating_value(data: Dict | None, peg_type: str) -> float | None:
            """Safely extract circulating value from a dictionary for a given peg type."""
            if isinstance(data, dict):
                return data.get(peg_type)
            return None

        def _fetch():
            data = self.client.get_stablecoins_metadata()

            # Process each stablecoin
            if "peggedAssets" in data:
                for item in data["peggedAssets"]:
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

        return dlt.resource(_fetch)

    def protocol_data(self, protocol: str):
        """DLT resource for fetching protocol data."""

        def _fetch():
            data = self.client.get_protocol_data(protocol)

            # Apply transformations if needed
            self.data_transformer.standardize_item(
                data,
                {
                    "json_fields": ["chains", "audit_links", "audits"],
                    "field_mappings": {
                        "defillamaId": "defillama_id",
                        "parentProtocol": "parent_protocol",
                    },
                },
            )
            yield data

        return dlt.resource(_fetch)

    def stablecoin_circulating(
        self,
        coin_id: int,
        get_response: Literal[
            "chainBalances", "currentChainBalances"
        ] = "currentChainBalances",
        include_metadata: bool = False,
    ):
        """DLT resource for fetching individual stablecoin data."""

        def _fetch():
            response = self.client.get_stablecoin_data(coin_id)

            # Extract metadata if requested
            metadata = {}
            if include_metadata:
                excluded_fields = {"chainBalances", "currentChainBalances"}
                metadata = {
                    k: v for k, v in response.items() if k not in excluded_fields
                }
                # Convert nested objects to JSON strings
                self.data_transformer.convert_fields_to_json(
                    metadata, ["auditLinks", "tokens"]
                )

            # Process chain balances based on requested type
            if get_response == "chainBalances":
                # Historical data
                for chain_name, chain_data in response.get("chainBalances", {}).items():
                    for entry in chain_data.get("tokens", []):
                        circulating_data = entry.get("circulating", {})
                        if not circulating_data:
                            continue

                        circulating_value = list(circulating_data.values())[0]
                        timestamp = entry.get("date")

                        item = {
                            "id": coin_id,
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
            else:
                # Current balances
                for chain_name, chain_data in response.get(
                    "currentChainBalances", {}
                ).items():
                    if not isinstance(chain_data, dict) or not chain_data:
                        continue

                    circulating = list(chain_data.values())[0]

                    item = {
                        "id": coin_id,
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

        return dlt.resource(_fetch)

    def token_price(
        self, network: str, contract_address: str, params: Optional[Dict] = None
    ):
        """DLT resource for fetching token price data."""

        def _fetch():
            default_params = {"span": 1000, "period": "1d"}
            request_params = params or default_params

            data = self.client.get_token_price(
                network, contract_address, **request_params
            )
            token_key = f"{network}:{contract_address}"

            coins_data = data.get("coins", {})
            token_info = coins_data.get(token_key, {})

            if not token_info.get("prices"):
                return

            # Extract token metadata
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

                self.data_transformer.standardize_item(
                    item, {"timestamp_fields": ["timestamp"]}
                )
                yield item

        return dlt.resource(_fetch)

    def all_yield_pools(self):
        """DLT resource for fetching all yield pools data."""

        def _fetch():
            data = self.client.get_all_yield_pools()

            for pool in data.get("data", []):
                # Extract token arrays before removing them
                reward_tokens = pool.get("rewardTokens", []) or []
                underlying_tokens = pool.get("underlyingTokens", []) or []

                # Apply transformations
                self.data_transformer.standardize_item(
                    pool, {"remove_fields": ["rewardTokens", "underlyingTokens"]}
                )

                # Add processed token arrays as JSON strings
                pool["reward_tokens"] = json.dumps(reward_tokens)
                pool["underlying_tokens"] = json.dumps(underlying_tokens)

                yield pool

        return dlt.resource(_fetch)

    def yield_pool(self, pool_id: str, pool_name: str):
        """DLT resource for fetching historical yield pool data."""

        def _fetch():
            data = self.client.get_yield_pool(pool_id)

            for item in data.get("data", []):
                # Add pool identification
                item["pool_id"] = pool_id
                item["pool_name"] = pool_name

                self.data_transformer.standardize_item(
                    item, {"timestamp_fields": ["timestamp"]}
                )
                yield item

        return dlt.resource(_fetch)

    def protocol_revenue(
        self,
        protocol: str,
        data_selector: Literal[
            "totalDataChart", "totalDataChartBreakdown"
        ] = "totalDataChartBreakdown",
        include_metadata: bool = False,
    ):
        """DLT resource for fetching protocol revenue data."""

        def _fetch():
            response = self.client.get_protocol_revenue(protocol)

            # Extract metadata if requested
            metadata = {}
            if include_metadata:
                excluded_fields = {"totalDataChart", "totalDataChartBreakdown"}
                metadata = {
                    k: v for k, v in response.items() if k not in excluded_fields
                }

                # Convert nested objects to JSON strings
                json_fields = [
                    "chains",
                    "audit_links",
                    "audits",
                    "childProtocols",
                    "linkedProtocols",
                ]
                self.data_transformer.convert_fields_to_json(metadata, json_fields)

            # Process time-series data
            time_series_data = response.get(data_selector, [])
            if not time_series_data:
                return

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
                else:
                    # Nested format: [timestamp, {chain: {sub_protocol: revenue}}]
                    if not isinstance(data, dict):
                        continue

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

        return dlt.resource(_fetch)
