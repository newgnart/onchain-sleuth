"""
Helper functions for loading data from Etherscan.
"""

import argparse
import json
import logging
import sys
import time
from functools import partial
import re

from typing import Optional, Any, List

import dlt
from evm_sleuth import settings, EtherscanClient, PipelineManager, EtherscanSource
from evm_sleuth.utils.postgres import PostgresClient

# Configure logging
logger = logging.getLogger(__name__)


def load_chunks(
    dataset_name: str,  # schema
    table_name: str,
    contract_address: str,
    etherscan_client: EtherscanClient,
    postgres_client: PostgresClient,
    source_factory: Any,  # Callable that creates source (logs or txns or)
    from_block: Optional[int] = None,
    to_block: Optional[int] = None,
    block_chunk_size: int = 50_000,
    write_disposition: str = "append",
    primary_key: Optional[List[str]] = None,
):
    pipeline_manager = PipelineManager()
    contract_address = contract_address.lower()
    chainid = etherscan_client.chainid
    block_column_name = "block_number"
    if source_factory.__name__ == "logs":
        address_column_name = "address"
    elif source_factory.__name__ == "transactions":
        address_column_name = '"to"'
    else:
        raise ValueError(f"Unknown source factory: {source_factory}")

    # Get end block from Etherscan client
    if to_block is None:
        to_block = etherscan_client.get_latest_block()
    # Determine start block using PostgresClient
    if from_block is None:
        max_loaded_block = postgres_client.get_max_loaded_block(
            table_schema=dataset_name,
            table_name=table_name,
            chainid=chainid,
            address=contract_address,
            address_column_name=address_column_name,
            block_column_name=block_column_name,
        )
        contract_creation_block = etherscan_client.get_contract_creation_block_number(
            contract_address
        )
        if max_loaded_block > contract_creation_block:
            from_block = max_loaded_block
            logger.info(
                f"🚧🚧🚧 {contract_address}, {source_factory.__name__}, chain {chainid}, continue from {from_block} to {to_block}"
            )
        else:
            from_block = contract_creation_block
            logger.info(
                f"🚧🚧🚧 {contract_address}, {source_factory.__name__}, chain {chainid}, start from creation block {contract_creation_block} to {to_block}"
            )

    # Process in chunks
    end_block = to_block  # Save the original end block
    for chunk_start in range(from_block, end_block, block_chunk_size):
        chunk_end = min(chunk_start + block_chunk_size - 1, end_block)
        try:
            # Get row count before loading
            query = f"SELECT COUNT(*) FROM {dataset_name}.{table_name} WHERE chainid = {chainid} AND {address_column_name} = '{contract_address}'"
            result = postgres_client.fetch_one(query)
            n_before = result[0] if result else 0

            # Create source with current block range
            source = source_factory(
                address=contract_address, from_block=chunk_start, to_block=chunk_end
            )

            # Run pipeline with resource
            postgres_connection_url = settings.postgres.get_connection_url()
            pipeline_manager.run(
                sources={table_name: source},
                pipeline_name=f"{dataset_name}-{table_name}-{chainid}-{contract_address}",
                dataset_name=dataset_name,  # schema
                destination=dlt.destinations.postgres(postgres_connection_url),
                write_disposition=write_disposition,
                primary_key=primary_key,
            )

            # Get row count after loading
            query = f"SELECT COUNT(*) FROM {dataset_name}.{table_name} WHERE chainid = {chainid} AND {address_column_name} = '{contract_address}'"
            result = postgres_client.fetch_one(query)
            n_after = result[0] if result else 0
            n_loaded = n_after - n_before

            logger.info(
                f"Loaded {n_loaded} {source_factory.__name__}, {chunk_start} to {chunk_end}"
            )

        except Exception as e:

            logger.error(
                f"Failed to load for block range {chunk_start}-{chunk_end} with error {e}"
            )
    logger.info(
        f"✅✅✅ {contract_address}, {source_factory.__name__}, chain {chainid}, {from_block} to {to_block}"
    )
