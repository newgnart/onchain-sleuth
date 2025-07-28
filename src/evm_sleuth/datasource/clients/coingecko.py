"""CoinGecko API client implementation."""

import dlt
from datetime import datetime
from typing import Any, Dict, List, Optional, Iterator

from evm_sleuth.core.base import BaseAPIClient, BaseDLTResource, APIConfig
from evm_sleuth.config.settings import settings

from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources


class CoinGeckoClient(BaseAPIClient):
    """CoinGecko API client implementation."""
    
    def __init__(self, api_key: Optional[str] = None, calls_per_second: float = 5.0):
        config = APIConfig(
            base_url=settings.api_urls.COINGECKO,
            api_key=api_key or settings.api.coingecko_api_key,
            rate_limit=calls_per_second
        )
        super().__init__(config)
    
    def _build_request_params(self, **kwargs) -> Dict[str, Any]:
        """Build request parameters with optional API key."""
        params = kwargs.copy()
        if self.config.api_key:
            params["x_cg_demo_api_key"] = self.config.api_key
        return params
    
    def _handle_response(self, response) -> Any:
        """Handle CoinGecko API response."""
        response.raise_for_status()
        return response.json()
    
    def get_price_data(
        self, 
        coin_id: str, 
        vs_currency: str = "usd", 
        days: int = 30
    ) -> Dict[str, Any]:
        """Get price data for a specific coin."""
        endpoint = f"coins/{coin_id}/market_chart"
        params = {"vs_currency": vs_currency, "days": days}
        return self.make_request(endpoint, params)
    
    def get_ohlc_data(
        self, 
        coin_id: str, 
        vs_currency: str = "usd", 
        days: int = 30
    ) -> Dict[str, Any]:
        """Get OHLC data for a specific coin."""
        endpoint = f"coins/{coin_id}/ohlc"
        params = {"vs_currency": vs_currency, "days": days}
        return self.make_request(endpoint, params)


class CoinGeckoDLTResource(BaseDLTResource):
    """DLT resource for CoinGecko data."""
    
    def get_resource_name(self) -> str:
        return "coingecko"
    
    def create_dlt_source(self, **kwargs):
        """Create DLT source for CoinGecko API."""
        # Implementation would use rest_api_resources
        pass
    
    @dlt.source()
    def price_data(
        self,
        coin_id: str,
        vs_currency: str = "usd",
        days: Optional[int] = 30,
    ):
        """Resource for CoinGecko price data."""
        
        def map_market_chart(data):
            return {
                "timestamp": datetime.fromtimestamp(int(data[0]) / 1000),
                "price": float(data[1]),
            }
        
        def map_ohlc(data):
            return {
                "timestamp": datetime.fromtimestamp(int(data[0]) / 1000),
                "open": float(data[1]),
                "high": float(data[2]),
                "low": float(data[3]),
                "close": float(data[4]),
            }
        
        config: RESTAPIConfig = {
            "client": {
                "base_url": f"{self.client.config.base_url}/coins/{coin_id}/",
                "headers": {
                    "accept": "application/json",
                },
                "session": self.client._session,
            },
            "resource_defaults": {
                "primary_key": "timestamp",
                "endpoint": {
                    "params": {
                        "vs_currency": vs_currency,
                        "days": days,
                    },
                },
            },
            "resources": [
                {
                    "name": "market_chart",
                    "endpoint": {
                        "path": "market_chart",
                        "data_selector": "prices",
                    },
                    "processing_steps": [
                        {
                            "map": map_market_chart,
                        }
                    ],
                },
                {
                    "name": "ohlc",
                    "endpoint": {
                        "path": "ohlc",
                    },
                    "processing_steps": [
                        {
                            "map": map_ohlc,
                        }
                    ],
                },
            ],
        }
        
        yield from rest_api_resources(config)