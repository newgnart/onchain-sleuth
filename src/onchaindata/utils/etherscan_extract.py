"""
Helper functions for loading data from Etherscan.
"""

import logging
import random
import os
import csv
from datetime import datetime
from pathlib import Path

from typing import Optional, Any, List, Dict, Literal, Tuple
import polars as pl
import pandas as pd

from ..extractor.etherscan import EtherscanClient
from ..extractor.etherscan import EtherscanExtractor
from ..utils.chain import get_chainid
from ..utils.database_client import PostgresClient

# Configure logging
logger = logging.getLogger(__name__)


def _log_error_to_csv(
    contract_address: str,
    chainid: int,
    table_name: str,
    from_block: int,
    to_block: int,
    block_chunk_size: int,
):
    """Immediately log an error to CSV file."""
    error_file = f"logs/extract_error_{table_name}.csv"
    os.makedirs("logs", exist_ok=True)

    # CSV headers
    csv_headers = [
        "timestamp",
        "contract_address",
        "chainid",
        "from_block",
        "to_block",
        "block_chunk_size",
    ]

    # Check if file exists to determine if we need to write headers
    file_exists = os.path.exists(error_file)

    # Append error to CSV file immediately
    with open(error_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # Write headers if this is a new file
        if not file_exists:
            writer.writerow(csv_headers)

        timestamp = datetime.now().isoformat()

        writer.writerow(
            [
                timestamp,
                contract_address,
                chainid,
                from_block,
                to_block,
                block_chunk_size,
            ]
        )

    logger.warning(
        f"💥 Error {contract_address} - {chainid} - {table_name} - {from_block}-{to_block}"
    )


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
            logger.error(f"No appropriate address column found in {output_path}")
            return None

        # Use scan_parquet for memory efficiency
        max_block = (
            pl.scan_parquet(output_path)
            .filter(pl.col(address_col) == address.lower())
            .select(pl.col("blockNumber").max())
            .collect()
            .item()
        )
        return max_block if max_block is not None else None
    except Exception as e:
        logger.warning(f"Could not read existing file {output_path}: {e}")
        return None


def _etherscan_to_parquet_in_chunks(
    contract_address: str,
    etherscan_client: EtherscanClient,
    data_dir: str = os.getenv("PARQUET_DATA_DIR"),
    from_block: Optional[int] = None,
    to_block: Optional[int] = None,
    block_chunk_size: int = 50_000,
    table: Literal["logs", "transactions"] = "logs",
) -> Path:
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
        data_dir: Path for parquet file output
        table: Whether to extract event logs or transactions (default: "logs")
    Returns:
        Path to the parquet file
    """

    extractor = EtherscanExtractor(etherscan_client)
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

    output_path = f"{data_dir}/{chain}_{contract_address.lower()}/{table}.parquet"
    if table == "logs":
        address_col = "contract_address"
    elif table == "transactions":
        address_col = "address"

    total_extracted = (
        pl.scan_parquet(output_path).select(pl.len()).collect().item()
        if os.path.exists(output_path)
        else 0
    )
    end_block = to_block

    for chunk_start in range(from_block, end_block + 1, block_chunk_size):
        chunk_end = min(chunk_start + block_chunk_size - 1, end_block)

        try:
            result_path = extractor.to_parquet(
                address=contract_address,
                chain=chain,
                table=table,
                from_block=chunk_start,
                to_block=chunk_end,
                offset=1000,
                output_path=output_path,
            )

            chunk_extracted = (
                pl.scan_parquet(result_path)
                .filter(
                    (pl.col(address_col) == contract_address)
                    & (pl.col("blockNumber") >= chunk_start)
                    & (pl.col("blockNumber") <= chunk_end)
                )
                .select(pl.len())
                .collect()
                .item()
            )
            total_extracted += chunk_extracted

        except Exception as e:
            logger.error(
                f"Failed to extract logs for blocks {chunk_start} to {chunk_end} with error {e}"
            )
            # Immediately log error to CSV
            _log_error_to_csv(
                contract_address=contract_address,
                chainid=chainid,
                table_name=table,
                from_block=chunk_start,
                to_block=chunk_end,
                block_chunk_size=block_chunk_size,
            )

    summary_parts = []
    summary_parts.append(f"{total_extracted} {table} - {chunk_start}-{chunk_end}")

    logger.info(f"✅ {contract_address} - {chainid} - {summary_parts}")


def etherscan_to_parquet(
    address: str,
    chain: str,
    resume: bool = True,
    data_dir: str = os.getenv("PARQUET_DATA_DIR"),
    table: Literal["logs", "transactions"] = "logs",
    block_chunk_size: int = 50_000,
) -> Tuple[str, str]:
    """Extract historical data for a contract and save to Parquet files.

    Args:
        address: Contract address to extract data for
        chain: Chain name (e.g. "ethereum", "polygon")
        logs_output_path: Path for logs parquet file output
        transactions_output_path: Path for transactions parquet file output
        resume: Whether to resume from existing data (default: True)
        data_dir: Base directory for parquet files (default: "data/etherscan_raw")
        table: Whether to extract event logs or transactions (default: "logs")
        block_chunk_size: Number of blocks to process per chunk (default: 50,000)
    Returns:
        Tuple[str, str]
    """
    chainid = get_chainid(chain)
    etherscan_client = EtherscanClient(chainid=chainid)

    output_path = f"{data_dir}/{chain}_{address.lower()}/{table}.parquet"

    # Extract to Parquet files
    _etherscan_to_parquet_in_chunks(
        contract_address=address,
        etherscan_client=etherscan_client,
        data_dir=data_dir,
        block_chunk_size=block_chunk_size,
        table=table,
    )
    return output_path


def find_error_file(table_name: str) -> str:
    """Find the CSV error file for given address and chainid."""
    error_file = f"logs/extract_error_{table_name}.csv"
    if not os.path.exists(error_file):
        raise FileNotFoundError(f"No error file found for {table_name}")
    return error_file


def retry_failed_blocks(table_name: Literal["logs", "transactions"]) -> Tuple[str, str]:
    """Retry failed block ranges with smaller chunk size."""
    error_file = find_error_file(table_name)

    df = pd.read_csv(error_file)
    resolved_error_file = error_file.replace(".csv", "_resolved.csv")
    df.to_csv(resolved_error_file, index=False)
    os.remove(error_file)
    resume = False

    for _, row in df.iterrows():
        chainid = row.chainid
        etherscan_client = EtherscanClient(chainid=chainid)
        address = row.contract_address

        from_block = row.from_block
        to_block = row.to_block
        block_chunk_size = max(int(row.block_chunk_size / 10), 1000)

        logs_output_path, transactions_output_path = _etherscan_to_parquet_in_chunks(
            contract_address=address,
            etherscan_client=etherscan_client,
            from_block=from_block,
            to_block=to_block,
            block_chunk_size=block_chunk_size,
            resume=resume,
            table=table_name,
        )
    return logs_output_path, transactions_output_path
