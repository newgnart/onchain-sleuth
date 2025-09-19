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

from typing import Optional, Any, List, Dict
from collections import defaultdict
import polars as pl

from onchain_sleuth import EtherscanClient
from onchain_sleuth.extractor.historical import HistoricalDataExtractor
from onchain_sleuth.dataloader.batch_loader import BatchLoader
from onchain_sleuth.utils.chain import get_chainid
from onchain_sleuth.utils.database import PostgresClient

# Configure logging
logger = logging.getLogger(__name__)


def _get_resume_block(output_path: str, address: str) -> Optional[int]:
    """Get the maximum block number from existing parquet file to resume from.

    Args:
        output_path: Path to the parquet file
        address: Contract address to filter by

    Returns:
        Next block number to resume from (max_block + 1), or None if no existing data
    """
    if not output_path or not os.path.exists(output_path):
        return None

    try:
        # Determine address column name based on file content
        # Logs use 'contract_address', transactions use 'address'
        schema = pl.scan_parquet(output_path).collect_schema()

        if "contract_address" in schema:
            # This is a logs file
            address_col = "contract_address"
        elif "address" in schema:
            # This is a transactions file
            address_col = "address"
        else:
            logger.warning(f"No appropriate address column found in {output_path}")
            return None

        # Use scan_parquet for memory efficiency
        max_block = (
            pl.scan_parquet(output_path)
            .filter(pl.col(address_col) == address.lower())
            .select(pl.col("blockNumber").max())
            .collect()
            .item()
        )
        return max_block + 1 if max_block is not None else None
    except Exception as e:
        logger.warning(f"Could not read existing file {output_path}: {e}")
        return None


def _backfill_in_chunks_from_etherscan_to_parquet(
    contract_address: str,
    etherscan_client: EtherscanClient,
    from_block: Optional[int] = None,
    to_block: Optional[int] = None,
    block_chunk_size: int = 50_000,
    logs_output_path: str = None,
    transactions_output_path: str = None,
    extract_logs: bool = True,
    extract_transactions: bool = True,
    resume: bool = True,
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
        logs_output_path: Path for logs parquet file output
        transactions_output_path: Path for transactions parquet file output
        extract_logs: Whether to extract event logs (default: True)
        extract_transactions: Whether to extract transactions (default: False)
        resume: Whether to resume from existing data (default: True)

    Returns:
        None

    Example:
        >>> backfill_in_chunks_from_etherscan_to_parquet(
        ...     contract_address="0xA0b86a33E6441b8c4C8C0E4A0e8b8b8b8b8b8b8b",
        ...     etherscan_client=client,
        ... )
    """

    extractor = HistoricalDataExtractor(etherscan_client)
    contract_address = contract_address.lower()
    chainid = etherscan_client.chainid
    chain = etherscan_client.chain

    # Get end block from Etherscan client
    if to_block is None:
        to_block = etherscan_client.get_latest_block()

    # Determine starting blocks with independent resume logic for each table
    if from_block is None:
        from_block = etherscan_client.get_contract_creation_block_number(
            contract_address
        )
        logger.info(
            f"🚧 Starting from creation block {from_block} for {contract_address}"
        )

    # Handle resume logic independently for each table
    logs_from_block = from_block
    txns_from_block = from_block

    if resume:
        # Check logs resume independently
        if extract_logs and logs_output_path:
            resume_from_logs = _get_resume_block(logs_output_path, contract_address)
            if resume_from_logs and resume_from_logs > logs_from_block:
                logs_from_block = resume_from_logs
                logger.info(
                    f"📈 Resuming logs from block {logs_from_block} for {contract_address}"
                )

        # Check transactions resume independently
        if extract_transactions and transactions_output_path:
            resume_from_txns = _get_resume_block(transactions_output_path, contract_address)
            if resume_from_txns and resume_from_txns > txns_from_block:
                txns_from_block = resume_from_txns
                logger.info(
                    f"📈 Resuming transactions from block {txns_from_block} for {contract_address}"
                )

    # Skip if both tables are already up to date
    if (not extract_logs or logs_from_block > to_block) and (not extract_transactions or txns_from_block > to_block):
        logger.info(
            f"✅ {contract_address} already up to date"
        )
        return

    # Process each table independently with their own block ranges
    end_block = to_block
    error_block_ranges = []
    total_logs_extracted = 0
    total_transactions_extracted = 0

    # Process logs chunks
    if extract_logs and logs_from_block <= end_block:
        for chunk_start in range(logs_from_block, end_block + 1, block_chunk_size):
            chunk_end = min(chunk_start + block_chunk_size - 1, end_block)

            try:
                result_path = extractor.extract_to_parquet(
                    address=contract_address,
                    chain=chain,
                    table="logs",
                    from_block=chunk_start,
                    to_block=chunk_end,
                    offset=1000,
                    output_path=logs_output_path,
                )

                # Count extracted logs (approximate)
                if result_path and os.path.exists(result_path):
                    chunk_logs = (
                        pl.scan_parquet(result_path)
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

            except Exception as e:
                logger.error(
                    f"Failed to extract logs for blocks {chunk_start} to {chunk_end} with error {e}"
                )
                error_block_ranges.append(["logs", chunk_start, chunk_end])

    # Process transactions chunks
    if extract_transactions and txns_from_block <= end_block:
        for chunk_start in range(txns_from_block, end_block + 1, block_chunk_size):
            chunk_end = min(chunk_start + block_chunk_size - 1, end_block)

            try:
                result_path = extractor.extract_to_parquet(
                    address=contract_address,
                    chain=chain,
                    table="transactions",
                    from_block=chunk_start,
                    to_block=chunk_end,
                    offset=1000,
                    output_path=transactions_output_path,
                )

                # Count extracted transactions (approximate)
                if result_path and os.path.exists(result_path):
                    chunk_transactions = (
                        pl.scan_parquet(result_path)
                        .filter(
                            (pl.col("address") == contract_address)
                            & (pl.col("blockNumber") >= chunk_start)
                            & (pl.col("blockNumber") <= chunk_end)
                        )
                        .select(pl.len())
                        .collect()
                        .item()
                    )
                    total_transactions_extracted += chunk_transactions

                    # Only log progress 10% of the time to avoid excessive logging
                    if random.random() < 0.1:
                        logger.debug(
                            f"Extracted {chunk_transactions} transactions from blocks {chunk_start}-{chunk_end}"
                        )

            except Exception as e:
                logger.error(
                    f"Failed to extract transactions for blocks {chunk_start} to {chunk_end} with error {e}"
                )
                error_block_ranges.append(["transactions", chunk_start, chunk_end])

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

    # Build summary message with independent block ranges
    summary_parts = []
    if extract_logs:
        summary_parts.append(f"{total_logs_extracted} logs from blocks {logs_from_block} to {to_block}")
    if extract_transactions:
        summary_parts.append(f"{total_transactions_extracted} transactions from blocks {txns_from_block} to {to_block}")

    logger.info(
        f"✅✅✅ {contract_address}, chain {chainid}, extracted {' and '.join(summary_parts)}"
    )


def backfill_from_etherscan_to_parquet(
    address: str,
    chain: str,
    logs_output_path: str = None,
    transactions_output_path: str = None,
    resume: bool = True,
    data_dir: str = "data/etherscan_raw",
    extract_logs: bool = True,
    extract_transactions: bool = True,
) -> str:
    """Extract historical data for a contract and save to Parquet files.

    Args:
        address: Contract address to extract data for
        chain: Chain name (e.g. "ethereum", "polygon")
        logs_output_path: Path for logs parquet file output
        transactions_output_path: Path for transactions parquet file output
        resume: Whether to resume from existing data (default: True)
        data_dir: Base directory for parquet files (default: "data/etherscan_raw")
        extract_logs: Whether to extract event logs (default: True)
        extract_transactions: Whether to extract transactions (default: True)

    Returns:
        None
    """
    chainid = get_chainid(chain)
    etherscan_client = EtherscanClient(chainid=chainid)

    # Generate default paths if not provided (matching HistoricalDataExtractor format)
    if extract_logs and logs_output_path is None:
        logs_output_path = f"{data_dir}/{chain}_{address.lower()}/logs.parquet"

    if extract_transactions and transactions_output_path is None:
        transactions_output_path = f"{data_dir}/{chain}_{address.lower()}/transactions.parquet"

    # Extract to Parquet files
    _backfill_in_chunks_from_etherscan_to_parquet(
        contract_address=address,
        etherscan_client=etherscan_client,
        extract_logs=extract_logs,
        extract_transactions=extract_transactions,
        logs_output_path=logs_output_path,
        transactions_output_path=transactions_output_path,
        resume=resume,
    )

    # Return the expected file path using new directory structure
    # return f"{data_dir}/{logs_subdir}/chain={chain}/table=logs/address={address}/logs.parquet"


def load_parquet_to_postgres(
    address: str,
    chain: str,
    postgres_client: PostgresClient,
    data_dir: str = "data",
    logs_subdir: str = "etherscan_raw",
    dataset_name: str = "etherscan_raw",
    write_disposition: str = "append",
    protocol: Optional[str] = None,
) -> Any:
    """Load address data from Parquet files to PostgreSQL.

    Args:
        address: Contract address
        chain: Chain name
        postgres_client: PostgreSQL client
        data_dir: Directory containing Parquet files
        dataset_name: Target dataset/schema name
        write_disposition: How to handle existing data
        protocol: Optional protocol name for table naming (auto-detected if None)

    Returns:
        DLT pipeline run result
    """
    # Auto-detect protocol if not provided
    # if protocol is None:
    #     from onchain_sleuth.config.protocol_registry import ProtocolRegistry

    #     registry = ProtocolRegistry()
    #     protocol = registry.get_protocol(address)

    batch_loader = BatchLoader(data_dir=data_dir, logs_subdir=logs_subdir)
    destination = postgres_client.get_dlt_destination()

    # Load from new directory structure: chain=ethereum/table=logs/address=0x123.../
    parquet_path = f"{data_dir}/{logs_subdir}/chain={chain}/table=logs/address={address}/logs.parquet"

    return batch_loader.load_single_file(
        file_path=parquet_path,
        destination=destination,
        dataset_name=dataset_name,
        table_name=f"{protocol}_logs",
        write_disposition=write_disposition,
        primary_key=[
            "block_number",
            "log_index",
            "transaction_hash",
        ],  # Common primary key for logs
    )


def backfill_from_etherscan_to_postgres(
    address: str,
    chain: str,
    postgres_client: PostgresClient,
    data_dir: str = "data",
    resume: bool = True,
):
    """Complete workflow: Extract to Parquet, then load to PostgreSQL.

    Args:
        address: Contract address to process
        chain: Chain name
        postgres_client: PostgreSQL client
        data_dir: Directory for Parquet files
        resume: Whether to resume from existing data (default: True)
    """
    # Step 1: Extract to Parquet
    backfill_from_etherscan_to_parquet(address=address, chain=chain, resume=resume)

    # Step 2: Load to PostgreSQL
    from onchain_sleuth.config.protocol_registry import ProtocolRegistry

    registry = ProtocolRegistry()
    protocol = registry.get_protocol(address)

    result = load_parquet_to_postgres(
        address=address,
        chain=chain,
        postgres_client=postgres_client,
        data_dir=data_dir,
        dataset_name="etherscan_raw",
        protocol=protocol,
    )

    logger.info(f"✅ Complete workflow finished for {address} (protocol: {protocol})")
    return result
