"""
Helper functions for loading data from Etherscan.
"""

import logging
import random
import os
import csv
from datetime import datetime

from typing import Optional, Any, List, Dict, Literal
import polars as pl
import pandas as pd

from onchain_sleuth import EtherscanClient
from onchain_sleuth.extractor.etherscan import EtherscanExtractor
from onchain_sleuth.utils.chain import get_chainid
from onchain_sleuth.utils.database import PostgresClient

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
        return max_block + 1 if max_block is not None else None
    except Exception as e:
        logger.warning(f"Could not read existing file {output_path}: {e}")
        return None


def _etherscan_to_parquet_in_chunks(
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

    # Handle resume logic independently for each table
    logs_from_block = from_block
    txns_from_block = from_block

    if resume:
        # Check logs resume independently
        if extract_logs and logs_output_path:
            resume_from_logs = _get_resume_block(logs_output_path, contract_address)
            if resume_from_logs and resume_from_logs > logs_from_block:
                logs_from_block = resume_from_logs
                logger.debug(
                    f"{contract_address} - {chainid} - logs - {logs_from_block}, fetching data"
                )

        # Check transactions resume independently
        if extract_transactions and transactions_output_path:
            resume_from_txns = _get_resume_block(
                transactions_output_path, contract_address
            )
            if resume_from_txns and resume_from_txns > txns_from_block:
                txns_from_block = resume_from_txns
                logger.debug(
                    f"{contract_address} - {chainid} - transactions - {txns_from_block}, fetching data"
                )

    # Skip if both tables are already up to date
    if (not extract_logs or logs_from_block > to_block) and (
        not extract_transactions or txns_from_block > to_block
    ):
        logger.info(f"✅ {contract_address} - {chainid} - already up to date")
        return

    # Process each table independently with their own block ranges
    end_block = to_block
    total_logs_extracted = 0
    total_transactions_extracted = 0

    # Process logs chunks
    if extract_logs and logs_from_block <= end_block:
        for chunk_start in range(logs_from_block, end_block + 1, block_chunk_size):
            chunk_end = min(chunk_start + block_chunk_size - 1, end_block)

            try:
                result_path = extractor.to_parquet(
                    address=contract_address,
                    chain=chain,
                    table="logs",
                    from_block=chunk_start,
                    to_block=chunk_end,
                    offset=1000,
                    output_path=logs_output_path,
                )

                # Check if extraction failed and log to CSV
                if result_path is None:
                    _log_error_to_csv(
                        contract_address=contract_address,
                        chainid=chainid,
                        table_name="logs",
                        from_block=chunk_start,
                        to_block=chunk_end,
                        block_chunk_size=block_chunk_size,
                    )
                    continue

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

            except Exception as e:
                logger.error(
                    f"Failed to extract logs for blocks {chunk_start} to {chunk_end} with error {e}"
                )
                # Immediately log error to CSV
                _log_error_to_csv(
                    contract_address=contract_address,
                    chainid=chainid,
                    table_name="logs",
                    from_block=chunk_start,
                    to_block=chunk_end,
                    block_chunk_size=block_chunk_size,
                )

    # Process transactions chunks
    if extract_transactions and txns_from_block <= end_block:
        for chunk_start in range(txns_from_block, end_block + 1, block_chunk_size):
            chunk_end = min(chunk_start + block_chunk_size - 1, end_block)

            try:
                result_path = extractor.to_parquet(
                    address=contract_address,
                    chain=chain,
                    table="transactions",
                    from_block=chunk_start,
                    to_block=chunk_end,
                    offset=1000,
                    output_path=transactions_output_path,
                )

                # Check if extraction failed and log to CSV
                if result_path is None:
                    _log_error_to_csv(
                        contract_address=contract_address,
                        chainid=chainid,
                        table_name="transactions",
                        from_block=chunk_start,
                        to_block=chunk_end,
                        block_chunk_size=block_chunk_size,
                    )
                    continue

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

            except Exception as e:
                logger.error(
                    f"Failed to extract transactions for blocks {chunk_start} to {chunk_end} with error {e}"
                )
                # Immediately log error to CSV
                _log_error_to_csv(
                    contract_address=contract_address,
                    chainid=chainid,
                    table_name="transactions",
                    from_block=chunk_start,
                    to_block=chunk_end,
                    block_chunk_size=block_chunk_size,
                )

    # Build summary message with independent block ranges
    summary_parts = []
    if extract_logs:
        summary_parts.append(
            f"{total_logs_extracted} logs - {logs_from_block}-{to_block}"
        )
    if extract_transactions:
        summary_parts.append(
            f"{total_transactions_extracted} transactions - {txns_from_block}-{to_block}"
        )

    logger.info(f"✅ {contract_address} - {chainid} - {summary_parts}")


def etherscan_to_parquet(
    address: str,
    chain: str,
    logs_output_path: str = None,
    transactions_output_path: str = None,
    resume: bool = True,
    data_dir: str = "data/etherscan_raw",
    extract_logs: bool = True,
    extract_transactions: bool = True,
    block_chunk_size: int = 50_000,
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
        block_chunk_size: Number of blocks to process per chunk (default: 50,000)
    Returns:
        None
    """
    chainid = get_chainid(chain)
    etherscan_client = EtherscanClient(chainid=chainid)

    # Generate default paths if not provided (matching HistoricalDataExtractor format)
    if extract_logs and logs_output_path is None:
        logs_output_path = f"{data_dir}/{chain}_{address.lower()}/logs.parquet"

    if extract_transactions and transactions_output_path is None:
        transactions_output_path = (
            f"{data_dir}/{chain}_{address.lower()}/transactions.parquet"
        )

    # Extract to Parquet files
    _etherscan_to_parquet_in_chunks(
        contract_address=address,
        etherscan_client=etherscan_client,
        extract_logs=extract_logs,
        extract_transactions=extract_transactions,
        logs_output_path=logs_output_path,
        transactions_output_path=transactions_output_path,
        resume=resume,
        block_chunk_size=block_chunk_size,
    )

    # Return the expected file path using new directory structure
    # return f"{data_dir}/{logs_subdir}/chain={chain}/table=logs/address={address}/logs.parquet"


def load_parquet_to_postgres(
    parquet_file_path: str,
    table_name: str,
    postgres_client: PostgresClient,
    dataset_name: str = "etherscan_raw",
    write_disposition: str = "append",
    primary_key: Optional[List[str]] = None,
) -> Any:
    """Load data from a Parquet file to PostgreSQL using DLT.

    Args:
        parquet_file_path: Full path to the parquet file
        table_name: Target table name in PostgreSQL
        postgres_client: PostgreSQL client
        dataset_name: Target dataset/schema name
        write_disposition: How to handle existing data ("append", "replace", "merge")
        primary_key: Optional primary key columns for the table

    Returns:
        DLT pipeline run result
    """
    import dlt
    from pathlib import Path

    parquet_path = Path(parquet_file_path)

    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {parquet_file_path}")

    # Set default primary keys based on file type (using snake_case column names)
    if primary_key is None:
        if "logs" in parquet_file_path.lower():
            primary_key = [
                "transaction_hash",
                "log_index",
            ]
        elif "transactions" in parquet_file_path.lower():
            primary_key = ["hash", "transaction_index"]
        else:
            primary_key = []

    logger.debug(f"Loading {parquet_file_path} to table {table_name}")

    try:
        # Use scan_parquet for memory efficiency
        lazy_df = pl.scan_parquet(parquet_path).unique()

        # Transform column names from camelCase to snake_case and handle NULL values
        df = lazy_df.collect()

        # Column name mapping for logs
        if "logs" in parquet_file_path.lower():
            # Handle NULL logIndex values by filtering them out or setting to 0
            if "logIndex" in df.columns:
                df = df.filter(pl.col("logIndex").is_not_null())
                df = df.rename({"logIndex": "log_index"})


            # Rename other camelCase columns to snake_case
            column_mapping = {
                "blockNumber": "block_number",
                "blockHash": "block_hash",
                "timeStamp": "time_stamp",
                "gasPrice": "gas_price",
                "gasUsed": "gas_used",
                "transactionHash": "transaction_hash",
                "transactionIndex": "transaction_index"
            }

            for old_name, new_name in column_mapping.items():
                if old_name in df.columns:
                    df = df.rename({old_name: new_name})

        # Column name mapping for transactions
        elif "transactions" in parquet_file_path.lower():
            column_mapping = {
                "blockNumber": "block_number",
                "blockHash": "block_hash",
                "timeStamp": "time_stamp",
                "transactionIndex": "transaction_index"
            }

            for old_name, new_name in column_mapping.items():
                if old_name in df.columns:
                    df = df.rename({old_name: new_name})

        # Get row count efficiently
        row_count = len(df)
        logger.debug(f"Loaded {row_count} rows from Parquet file")

        # Convert to records for DLT
        records = df.to_dicts()

        # Convert numpy arrays in topics to Python lists for JSON serialization
        if "logs" in parquet_file_path.lower():
            for record in records:
                if "topics" in record and record["topics"] is not None:
                    if hasattr(record["topics"], 'tolist'):
                        record["topics"] = record["topics"].tolist()

        # Get destination from postgres client
        destination = postgres_client.get_dlt_destination()

        # Create and run pipeline
        pipeline = dlt.pipeline(
            pipeline_name="backfill_to_postgres",
            destination=destination,
            dataset_name=dataset_name,
        )

        # Define column hints for logs table to properly handle topics array
        columns = None
        if "logs" in parquet_file_path.lower() and "topics" in df.columns:
            columns = {
                "topics": {"data_type": "json", "nullable": True}
            }

        run_kwargs = {
            "table_name": table_name,
            "write_disposition": write_disposition,
        }
        if primary_key:
            run_kwargs["primary_key"] = primary_key
        if columns:
            run_kwargs["columns"] = columns

        result = pipeline.run(records, **run_kwargs)
        logger.debug(
            f"✅ Successfully loaded {len(records)} rows to table '{table_name}'"
        )

        return result

    except Exception as e:
        logger.error(
            f"❌ Failed to load {parquet_file_path} to table '{table_name}': {e}"
        )
        raise


def etherscan_to_postgres(
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

    logger.debug(f"✅ Complete workflow finished for {address} (protocol: {protocol})")
    return result


def find_error_file(table_name: str) -> str:
    """Find the CSV error file for given address and chainid."""
    error_file = f"logs/extract_error_{table_name}.csv"
    if not os.path.exists(error_file):
        raise FileNotFoundError(f"No error file found for {table_name}")
    return error_file


def retry_failed_blocks(
    table_name: Literal["logs", "transactions"],
    data_dir: str = "data/etherscan_raw",
) -> bool:
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

        if table_name == "logs":
            extract_logs = True
            extract_transactions = False
            logs_output_path = (
                f"{data_dir}/{etherscan_client.chain}_{address}/logs.parquet"
            )
            transactions_output_path = None
        elif table_name == "transactions":
            extract_logs = False
            extract_transactions = True
            transactions_output_path = (
                f"{data_dir}/{etherscan_client.chain}_{address}/transactions.parquet"
            )
            logs_output_path = None

        from_block = row.from_block
        to_block = row.to_block
        block_chunk_size = max(int(row.block_chunk_size / 10), 1000)

        _etherscan_to_parquet_in_chunks(
            contract_address=address,
            etherscan_client=etherscan_client,
            extract_logs=extract_logs,
            extract_transactions=extract_transactions,
            from_block=from_block,
            to_block=to_block,
            block_chunk_size=block_chunk_size,
            resume=resume,
            logs_output_path=logs_output_path,
            transactions_output_path=transactions_output_path,
        )
