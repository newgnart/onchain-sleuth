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
    pipeline = PipelineManager()
    postgres_connection_url = settings.postgres.get_connection_url()

    tables = {
        "stablecoins_metadata": TableConfig(
            source=source.stablecoins_metadata(),
            write_disposition="replace",
        ),
        "all_yield_pools": TableConfig(
            source=source.yield_pools(), write_disposition="append"
        ),
        "stable_data": TableConfig(
            source=source.stablecoin_data(1),
            write_disposition="merge",
            primary_key=["time"],
        ),
    }
    pipeline.run(
        sources=tables,
        pipeline_name="llama",
        dataset_name="llama",  # schema in postgres
        destination=dlt.destinations.postgres(postgres_connection_url),
    )


if __name__ == "__main__":
    main()
