"""
Test stablecoins_metadata functionality with DuckDB integration.
"""

import pytest
import dlt
import duckdb
import logging
from evm_sleuth import (
    DeFiLlamaClient,
    PipelineFactory,
    DeFiLlamaSource,
    # PipelineManager,
)


def main():
    client = DeFiLlamaClient()
    # data = client.stablecoins_metadata()

    pipeline = PipelineFactory.create_dlt_pipeline(
        name="test_stablecoins_pytest",
        destination="duckdb",
        dataset_name="test_stablecoins",
    )

    source = DeFiLlamaSource(client=client)
    resource = source.stablecoins_metadata()

    # Load data
    pipeline = PipelineFactory.create_dlt_pipeline(
        name="test_stablecoins_pytest",
        destination="duckdb",
        dataset_name="test_stablecoins",
    )
    pipeline.run(resource)


if __name__ == "__main__":
    main()
