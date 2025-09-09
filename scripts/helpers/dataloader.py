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
import random
import os

from typing import Optional, Any, List

from onchain_sleuth import EtherscanClient, PipelineManager, EtherscanSource
from onchain_sleuth.utils.chain import get_chainid
from onchain_sleuth.utils.database import PostgresClient

# Configure logging
logger = logging.getLogger(__name__)


def backfill_in_chunks_from_etherscan_to_postgres(
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
    """Backfill blockchain data from Etherscan to PostgreSQL in manageable chunks.

    This function efficiently loads historical data, in configurable block ranges:
    - logs, of a specific contract address
    - transactions,to a specific contract address

    It handles resumption from the last loaded block in the database if it exists

    Args:
        dataset_name: PostgreSQL schema name where data will be stored
        table_name: Table name within the schema to store the data
        contract_address: Ethereum contract address to fetch data for (case-insensitive)
        etherscan_client: Configured Etherscan API client for data retrieval
        postgres_client: PostgreSQL database client for data storage
        source_factory: Callable that creates data source (logs or transactions)
        from_block: Starting block number (auto-detected if None)
        to_block: Ending block number (uses latest block if None)
        block_chunk_size: Number of blocks to process per chunk (default: 50,000)
        write_disposition: How to handle existing data ('append', 'replace', etc.)
        primary_key: List of column names that form the primary key

    Returns:
        None

    Raises:
        ValueError: If source_factory is not recognized (not 'logs' or 'transactions')

    Example:
        >>> backfill_in_chunks_from_etherscan_to_postgres(
        ...     dataset_name="ethereum",
        ...     table_name="usdc_transfers",
        ...     contract_address="0xA0b86a33E6441b8c4C8C0E4A0e8b8b8b8b8b8b8b",
        ...     etherscan_client=client,
        ...     postgres_client=pg_client,
        ...     source_factory=logs
        ... )
    """

    pipeline_manager = PipelineManager()
    contract_address = contract_address.lower()
    chainid = etherscan_client.chainid
    block_column_name = "block_number"

    if source_factory.__name__ == "logs":
        address_column_name = "address"  # get all logs for an address
    elif source_factory.__name__ == "transactions":
        address_column_name = '"to"'  # get all transactions to an address
    else:
        raise ValueError(f"Unknown source factory: {source_factory}")

    # Get end block from Etherscan client
    if to_block is None:
        to_block = etherscan_client.get_latest_block()
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
    error_block_ranges = []
    for chunk_start in range(from_block, end_block, block_chunk_size):
        chunk_end = min(chunk_start + block_chunk_size - 1, end_block)
        try:
            # Create source with current block range
            source = source_factory(
                address=contract_address, from_block=chunk_start, to_block=chunk_end
            )

            # Run pipeline with resource
            pipeline_manager.run(
                sources={table_name: source},
                pipeline_name=f"{dataset_name}-{table_name}-{chainid}-{contract_address}",
                dataset_name=dataset_name,  # schema
                destination=postgres_client.get_dlt_destination(),
                write_disposition=write_disposition,
                primary_key=primary_key,
            )

            # Get row count after loading
            query = f"SELECT COUNT(*) FROM {dataset_name}.{table_name} WHERE chainid = {chainid} AND {address_column_name} = '{contract_address}'"
            result = postgres_client.fetch_one(query)
            n_loaded = result[0] if result and result[0] is not None else 0

            # Only log progress 5% of the time to avoid excessive logging
            # This provides periodic status updates while keeping the log file manageable
            if random.random() < 0.1:
                logger.debug(
                    f"Loaded {n_loaded} {source_factory.__name__}, up to {chunk_end}"
                )

        except Exception as e:
            logger.error(
                f"Failed to load {source_factory.__name__} {chunk_start} to {chunk_end} with error {e}"
            )
            error_block_ranges.append([chunk_start, chunk_end])

    if error_block_ranges:
        error_file = f"logs/load_{source_factory.__name__}_error.json"
        if not os.path.exists(error_file):
            with open(error_file, "w") as f:
                json.dump({}, f, indent=4)
        with open(error_file, "a") as f:
            json.dump(
                {f"{contract_address}-{chainid}": error_block_ranges},
                f,
                indent=4,
                ensure_ascii=False,
            )

    logger.info(
        f"✅✅✅ {contract_address}, {source_factory.__name__}, chain {chainid}, {from_block} to {to_block}"
    )


def backfill_from_etherscan_to_postgres(
    address, chain, postgres_client: PostgresClient
):
    chainid = get_chainid(chain)
    etherscan_client = EtherscanClient(chainid=chainid)
    source = EtherscanSource(client=etherscan_client)

    backfill_in_chunks_from_etherscan_to_postgres(
        dataset_name="etherscan_raw",  # schema
        table_name="txns",
        contract_address=address,
        etherscan_client=etherscan_client,
        postgres_client=postgres_client,
        source_factory=source.transactions,
    )

    backfill_in_chunks_from_etherscan_to_postgres(
        dataset_name="etherscan_raw",  # schema
        table_name="logs",
        contract_address=address,
        etherscan_client=etherscan_client,
        postgres_client=postgres_client,
        source_factory=source.logs,
    )
