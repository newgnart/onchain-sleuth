"""DeFiLlama API client implementation."""

import dlt
from typing import Any, Dict, List

from evm_sleuth.core.base import BaseAPIClient, BaseSource, APIConfig
from evm_sleuth.config.settings import settings
from evm_sleuth.utils.data_transformers import DataTransformer


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

    def stablecoins_metadata(self) -> Dict[str, Any]:
        """Fetch stablecoins metadata from DeFiLlama API."""
        return self.make_request(
            f"{settings.api_urls.DEFILLAMA_STABLECOINS}/stablecoins"
        )


class DeFiLlamaSource(BaseSource):
    """Creating DLT source for DeFiLlama data."""

    def __init__(self, client: DeFiLlamaClient):
        super().__init__(client)
        self.data_transformer = DataTransformer()

    def get_available_resources(self) -> List[str]:
        """Return list of available resource names."""
        return ["stablecoins_metadata"]

    def stablecoins_metadata(self):
        """DLT resource for fetching stablecoins metadata, excluding chainCirculating field."""

        def _fetch_stablecoins():
            data = self.client.stablecoins_metadata()

            # Process each stablecoin and exclude chainCirculating field
            if "peggedAssets" in data:
                for asset in data["peggedAssets"]:
                    # Use DataTransformer to remove unwanted fields
                    self.data_transformer.remove_fields(asset, ["chainCirculating"])
                    yield asset

        return dlt.resource(_fetch_stablecoins, name="stablecoins_metadata")
