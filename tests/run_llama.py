"""
Test stablecoins_metadata functionality with DuckDB integration.
"""

import pytest
import dlt
import duckdb
import logging
from evm_sleuth import (
    DeFiLlamaClient,
    DeFiLlamaSource,
    settings,
    PipelineManager,
    TableConfig,
)


def main():
    client = DeFiLlamaClient()
    source = DeFiLlamaSource(client=client)
    pipeline_manager = PipelineManager()
    postgres_connection_url = settings.postgres.get_connection_url()

    # Define stablecoin IDs to fetch
    stablecoin_ids = [1, 146, 2, 4, 5]

    # Define yield pools to fetch (pool_id, pool_name)
    yield_pools = [
        ("13392973-be6e-4b2f-bce9-4f7dd53d1c3a", "sdai"),
        # ("747c1d2a-c668-4682-b9f9-296708a3dd90", "aave_v3_usdc"),
        # ("bbf25f9e-ad33-47bd-af7c-e5c20c40fda3", "compound_v3_usdc"),
        # ("d4b3c522-6127-4b89-bedf-83641cdcd2eb", "aave_v3_weth"),
        # ("01bfd47c-b3a3-4deb-9c52-6e5a3ba70bce", "morpho_aave_usdc"),
    ]

    tokens = [("ethereum", "0x57e114B691Db790C35207b2e685D4A43181e6061")]

    tables = {
        "stablecoins_metadata": TableConfig(
            source=source.stablecoins_metadata(),
            write_disposition="replace",
        ),
        "all_yield_pools": TableConfig(
            source=source.all_yield_pools(), write_disposition="replace"
        ),
        **{
            "stablecoins_circulating": TableConfig(
                source=source.stablecoin_circulating(coin_id),
                write_disposition="append",
                # primary_key=["id", "chain", "timestamp"],
            )
            for coin_id in stablecoin_ids
        },
        **{
            "token_price": TableConfig(
                source=source.token_price(network, address),
                write_disposition="append",
                # primary_key=["pool_id", "timestamp"],
            )
            for network, address in tokens
        },
        **{
            "yield_pools": TableConfig(
                source=source.yield_pool(pool_id, pool_name),
                write_disposition="append",
                # primary_key=["pool_id", "timestamp"],
            )
            for pool_id, pool_name in yield_pools
        },
        "protocol_revenue": TableConfig(
            source=source.protocol_revenue("ethena"),
            write_disposition="append",
            # primary_key=["protocol", "timestamp", "chain", "sub_protocol"],
        ),
    }
    pipeline_manager.run(
        sources=tables,
        pipeline_name="llama",
        dataset_name="llama",  # schema in postgres
        destination=dlt.destinations.postgres(postgres_connection_url),
    )


if __name__ == "__main__":
    main()
