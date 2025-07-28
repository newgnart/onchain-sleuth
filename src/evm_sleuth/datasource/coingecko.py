from typing import Optional
from datetime import datetime
import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
from stables.config import *


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


@dlt.source()
def coingecko_prices(
    coin_id: str,
    vs_currency: str = "usd",
    days: Optional[int] = 30,
):
    """Resource for CoinGecko price data only, volume, market cap is available but not used"""

    config: RESTAPIConfig = {
        "client": {
            "base_url": f"{COINGECKO_API_BASE_URL}/coins/{coin_id}/",
            "headers": {
                "accept": "application/json",
            },
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
                    # "data_selector": "prices",
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
