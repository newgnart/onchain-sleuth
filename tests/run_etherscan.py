"""
Test script for Etherscan log loading with PostgreSQL integration using modern EVM Sleuth architecture.
"""

import logging
import time
from functools import partial

from typing import Optional, Any

import dlt
from evm_sleuth import settings, EtherscanClient, PipelineManager, EtherscanSource
from evm_sleuth.utils.postgres import PostgresClient
from evm_sleuth.utils.logging import setup_logging

# Configure logging
setup_logging()
logger = logging.getLogger(__name__)


def load_chunks(
    dataset_name: str,  # schema
    table_name: str,
    contract_address: str,
    chainid: int,
    etherscan_client: EtherscanClient,
    postgres_client: PostgresClient,
    source_factory: Any,  # Callable that creates source (logs or txns or)
    from_block: Optional[int] = None,
    to_block: Optional[int] = None,
    block_chunk_size: int = 50_000,
):
    pipeline_manager = PipelineManager()
    contract_address = contract_address.lower()
    # Determine start block using PostgresClient
    if from_block is None:
        max_loaded_block = postgres_client.get_max_loaded_block(
            table_schema=dataset_name,
            table_name=table_name,
            chainid=chainid,
            address=contract_address,
            column_name="block_number",
        )
        contract_creation_block = int(
            etherscan_client.get_contract_creation_info(contract_address)["blockNumber"]
        )
        from_block = max(max_loaded_block, contract_creation_block)

    # Get end block from Etherscan client
    if to_block is None:
        to_block = etherscan_client.get_latest_block()

    logger.info(
        f"Loading from block {from_block} to {to_block}, with block_chunk_size {block_chunk_size}"
    )
    # Process in chunks
    end_block = to_block  # Save the original end block
    for chunk_start in range(from_block, end_block, block_chunk_size):
        chunk_end = min(chunk_start + block_chunk_size - 1, end_block)
        logger.info(f"Loading from block {chunk_start} to {chunk_end}")

        max_retries = 2
        retries = max_retries
        while retries > 0:
            try:
                # Get row count before loading
                n_before = postgres_client.get_table_row_count(dataset_name, table_name)

                # Create source with current block range
                source = source_factory(
                    address=contract_address, from_block=chunk_start, to_block=chunk_end
                )

                # Run pipeline with resource
                postgres_connection_url = settings.postgres.get_connection_url()
                pipeline_manager.run(
                    sources={table_name: source},
                    pipeline_name="etherscan_raw",
                    dataset_name=dataset_name,  # schema
                    destination=dlt.destinations.postgres(postgres_connection_url),
                    write_disposition="append",
                )

                # Get row count after loading
                n_after = postgres_client.get_table_row_count(dataset_name, table_name)
                n_loaded = n_after - n_before

                # Log results and check for potential API limits
                if n_loaded >= 1000:
                    logger.warning(
                        f"Loaded {n_loaded} from {chunk_start} to {chunk_end}, "
                        "smaller batch size may be needed."
                    )
                else:
                    logger.info(f"Loaded {n_loaded} from {chunk_start} to {chunk_end}")

                break  # Success, move to next chunk

            except Exception as e:
                retries -= 1
                logger.error(
                    f"Error loading: {e}. Retrying... ({retries} retries left)"
                )
                if retries > 0:
                    time.sleep(3)  # Wait before retry
                else:
                    logger.error(
                        f"Failed to load for block range {chunk_start}-{chunk_end} "
                        f"after {max_retries} retries."
                    )
    logger.info(f"Completed loading for {contract_address}")


if __name__ == "__main__":
    etherscan_client = EtherscanClient(chainid=1)
    source = EtherscanSource(client=etherscan_client)
    postgres_client = PostgresClient(settings.postgres)

    # # Load a small chunk of data first
    load_chunks(
        dataset_name="etherscan_raw",
        table_name="logs",
        contract_address="0x323c03c48660fE31186fa82c289b0766d331Ce21",
        chainid=1,
        etherscan_client=etherscan_client,
        postgres_client=postgres_client,
        source_factory=source.logs,
        block_chunk_size=100000,
    )
