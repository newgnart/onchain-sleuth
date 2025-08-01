"""
CLI script for Etherscan log loading with PostgreSQL integration using modern EVM Sleuth architecture.
"""

import argparse
import json
import logging
import sys
import time
from functools import partial

from typing import Optional, Any

import dlt
from evm_sleuth import settings, EtherscanClient, PipelineManager, EtherscanSource
from evm_sleuth.utils.postgres import PostgresClient
from evm_sleuth.utils.logging import setup_logging

# Configure logging
setup_logging("logs/crvusd.log")
logger = logging.getLogger(__name__)


def get_source_factory(source: EtherscanSource, table_name: str):
    """Get the appropriate source factory based on table_name."""
    source_mapping = {
        "logs": source.logs,
        "transactions": source.transactions,
        # Add more mappings as needed
    }

    if table_name not in source_mapping:
        raise ValueError(
            f"Unsupported table_name: {table_name}. Supported values: {list(source_mapping.keys())}"
        )

    return source_mapping[table_name]


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
):
    pipeline_manager = PipelineManager()
    contract_address = contract_address.lower()
    chainid = etherscan_client.chainid
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
                    pipeline_name="crvusd",
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
                        "smaller block_chunk_size may be needed."
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


def crvusd():
    chainid = 1
    etherscan_client = EtherscanClient(chainid=chainid)
    source = EtherscanSource(client=etherscan_client)
    postgres_client = PostgresClient(settings.postgres)
    with open("resource/crvusd.json", "r") as f:
        crvusd_data = json.load(f)

    markets = crvusd_data["markets"]
    contract_type = "monetary_policy"
    for market_name, market_data in markets.items():
        contract_address = market_data[contract_type].lower()
        logger.info(
            f"Loading markets-{market_name}-{contract_type}, {contract_address}"
        )

        load_chunks(
            dataset_name="etherscan_raw",
            table_name="logs",
            contract_address=contract_address,
            etherscan_client=etherscan_client,
            postgres_client=postgres_client,
            source_factory=source.logs,
            block_chunk_size=100_000,
        )


def cli():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Load Etherscan data for a contract address",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_etherscan.py 0x323c03c48660fE31186fa82c289b0766d331Ce21 1 logs
  python run_etherscan.py 0x323c03c48660fE31186fa82c289b0766d331Ce21 1 logs --block-chunk-size 100000
  python run_etherscan.py 0x323c03c48660fE31186fa82c289b0766d331Ce21 1 transactions
        """,
    )

    parser.add_argument("contract_address", help="Contract address to load data for")

    parser.add_argument("chainid", type=int, help="Chain ID for the blockchain network")

    parser.add_argument(
        "table_name",
        choices=["logs", "transactions"],
        help="Table name to load (determines source factory)",
    )

    parser.add_argument(
        "--block-chunk-size",
        type=int,
        default=50_000,
        help="Block chunk size for processing (default: 50000)",
    )

    parser.add_argument(
        "--dataset-name",
        default="etherscan_raw",
        help="Dataset name/schema (default: etherscan_raw)",
    )

    args = parser.parse_args()

    try:
        # Initialize clients
        etherscan_client = EtherscanClient(chainid=args.chainid)
        source = EtherscanSource(client=etherscan_client)
        postgres_client = PostgresClient(settings.postgres)

        # Get the appropriate source factory based on table_name
        source_factory = get_source_factory(source, args.table_name)

        logger.info(
            f"Starting data load for contract {args.contract_address} on chain {args.chainid}"
        )
        logger.info(f"Table: {args.table_name}, Chunk size: {args.block_chunk_size}")

        # Load data
        load_chunks(
            dataset_name=args.dataset_name,
            table_name=args.table_name,
            contract_address=args.contract_address,
            etherscan_client=etherscan_client,
            postgres_client=postgres_client,
            source_factory=source_factory,
            block_chunk_size=args.block_chunk_size,
        )

        logger.info("Data loading completed successfully")

    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # main()
    crvusd()
