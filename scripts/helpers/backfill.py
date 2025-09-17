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

from onchain_sleuth import EtherscanClient
from onchain_sleuth.extractor.historical import HistoricalDataExtractor
from onchain_sleuth.dataloader.batch_loader import BatchLoader
from onchain_sleuth.utils.chain import get_chainid
from onchain_sleuth.utils.database import PostgresClient

# Configure logging
logger = logging.getLogger(__name__)


def backfill_in_chunks_from_etherscan_to_parquet(
    contract_address: str,
    etherscan_client: EtherscanClient,
    from_block: Optional[int] = None,
    to_block: Optional[int] = None,
    block_chunk_size: int = 50_000,
    data_dir: str = "data",
    logs_subdir: str = "etherscan_raw",
    extract_logs: bool = True,
    extract_transactions: bool = False,
):
    """Backfill blockchain data from Etherscan to protocol-grouped Parquet files in chunks.

    This function efficiently extracts historical data and saves to Parquet files:
    - logs of a specific contract address
    - transactions to a specific contract address (optional)

    Args:
        contract_address: Ethereum contract address to fetch data for (case-insensitive)
        etherscan_client: Configured Etherscan API client for data retrieval
        from_block: Starting block number (uses contract creation block if None)
        to_block: Ending block number (uses latest block if None)
        block_chunk_size: Number of blocks to process per chunk (default: 50,000)
        data_dir: Directory to save Parquet files (default: "data")
        extract_logs: Whether to extract event logs (default: True)
        extract_transactions: Whether to extract transactions (default: False)

    Returns:
        None

    Example:
        >>> backfill_in_chunks_from_etherscan_to_parquet(
        ...     contract_address="0xA0b86a33E6441b8c4C8C0E4A0e8b8b8b8b8b8b8b",
        ...     etherscan_client=client,
        ... )
    """

    extractor = HistoricalDataExtractor(etherscan_client, output_dir=data_dir, logs_subdir=logs_subdir)
    contract_address = contract_address.lower()
    chainid = etherscan_client.chainid

    # Get end block from Etherscan client
    if to_block is None:
        to_block = etherscan_client.get_latest_block()

    if from_block is None:
        from_block = etherscan_client.get_contract_creation_block_number(
            contract_address
        )
        logger.info(
            f"🚧🚧🚧 {contract_address}, chain {chainid}, starting from creation block {from_block}"
        )

    # Process in chunks
    end_block = to_block
    error_block_ranges = []
    total_logs_extracted = 0

    for chunk_start in range(from_block, end_block, block_chunk_size):
        chunk_end = min(chunk_start + block_chunk_size - 1, end_block)

        try:
            chunk_contracts = [contract_address]

            if extract_logs:
                # Extract logs for this chunk
                result_paths = extractor.extract_to_parquet(
                    contracts=chunk_contracts,
                    from_block=chunk_start,
                    to_block=chunk_end,
                    offset=1000,
                )

                # Count extracted logs (approximate)
                if result_paths:
                    import polars as pl

                    for protocol, path in result_paths.items():
                        if path and os.path.exists(path):
                            # Use scan_parquet for memory efficiency
                            chunk_logs = (
                                pl.scan_parquet(path)
                                .filter(
                                    (pl.col("contract_address") == contract_address)
                                    & (pl.col("blockNumber") >= chunk_start)
                                    & (pl.col("blockNumber") <= chunk_end)
                                )
                                .select(pl.len())
                                .collect()
                                .item()
                            )
                            total_logs_extracted += chunk_logs

                            # Only log progress 10% of the time to avoid excessive logging
                            if random.random() < 0.1:
                                logger.debug(
                                    f"Extracted {chunk_logs} logs from blocks {chunk_start}-{chunk_end}"
                                )

            # Note: Transaction extraction would be similar but using different source
            if extract_transactions:
                logger.warning(
                    "Transaction extraction to Parquet not yet implemented in chunked mode"
                )

        except Exception as e:
            logger.error(
                f"Failed to extract data for blocks {chunk_start} to {chunk_end} with error {e}"
            )
            error_block_ranges.append([chunk_start, chunk_end])

    # Log errors if any
    if error_block_ranges:
        error_file = f"logs/extract_error.json"
        os.makedirs("logs", exist_ok=True)
        error_data = {f"{contract_address}-{chainid}": error_block_ranges}

        if os.path.exists(error_file):
            with open(error_file, "r") as f:
                existing_errors = json.load(f)
            existing_errors.update(error_data)
            error_data = existing_errors

        with open(error_file, "w") as f:
            json.dump(error_data, f, indent=4, ensure_ascii=False)

    logger.info(
        f"✅✅✅ {contract_address}, chain {chainid}, extracted {total_logs_extracted} logs from blocks {from_block} to {to_block}"
    )


def backfill_from_etherscan_to_parquet(
    address: str, chain: str, data_dir: str = "data", logs_subdir: str = "etherscan_raw"
) -> str:
    """Extract historical data for a contract and save to Parquet files.

    Args:
        address: Contract address to extract data for
        chain: Chain name (e.g. "ethereum", "polygon")
        data_dir: Directory to save Parquet files

    Returns:
        Path to the protocol-specific Parquet file
    """
    chainid = get_chainid(chain)
    etherscan_client = EtherscanClient(chainid=chainid)

    # Extract to Parquet files
    backfill_in_chunks_from_etherscan_to_parquet(
        contract_address=address,
        etherscan_client=etherscan_client,
        data_dir=data_dir,
        logs_subdir=logs_subdir,
        extract_logs=True,
        extract_transactions=False,  # Can be enabled if needed
    )

    # Return the expected protocol directory path
    from onchain_sleuth.config.protocol_registry import ProtocolRegistry

    registry = ProtocolRegistry()
    protocol = registry.get_protocol(address)
    return f"{data_dir}/{logs_subdir}/protocol={protocol}/logs.parquet"


def load_parquet_to_postgres(
    protocol: str,
    postgres_client: PostgresClient,
    data_dir: str = "data",
    logs_subdir: str = "etherscan_raw",
    dataset_name: str = "etherscan_raw",
    write_disposition: str = "append",
) -> Any:
    """Load protocol data from Parquet files to PostgreSQL.

    Args:
        protocol: Protocol name (e.g. "curve", "uniswap-v3")
        postgres_client: PostgreSQL client
        data_dir: Directory containing Parquet files
        dataset_name: Target dataset/schema name
        write_disposition: How to handle existing data

    Returns:
        DLT pipeline run result
    """
    batch_loader = BatchLoader(data_dir=data_dir, logs_subdir=logs_subdir)
    destination = postgres_client.get_dlt_destination()

    return batch_loader.load_protocol_data(
        protocol=protocol,
        destination=destination,
        dataset_name=dataset_name,
        table_name=f"{protocol}_logs",
        write_disposition=write_disposition,
        primary_key=[
            "blockNumber",
            "logIndex",
            "transactionHash",
        ],  # Common primary key for logs
    )


def backfill_from_etherscan_to_postgres(
    address: str, chain: str, postgres_client: PostgresClient, data_dir: str = "data"
):
    """Complete workflow: Extract to Parquet, then load to PostgreSQL.

    Args:
        address: Contract address to process
        chain: Chain name
        postgres_client: PostgreSQL client
        data_dir: Directory for Parquet files
    """
    # Step 1: Extract to Parquet
    parquet_path = backfill_from_etherscan_to_parquet(
        address=address, chain=chain, data_dir=data_dir
    )

    # Step 2: Load to PostgreSQL
    from onchain_sleuth.config.protocol_registry import ProtocolRegistry

    registry = ProtocolRegistry()
    protocol = registry.get_protocol(address)

    result = load_parquet_to_postgres(
        protocol=protocol,
        postgres_client=postgres_client,
        data_dir=data_dir,
        dataset_name="etherscan_raw",
    )

    logger.info(f"✅ Complete workflow finished for {address} (protocol: {protocol})")
    return result
