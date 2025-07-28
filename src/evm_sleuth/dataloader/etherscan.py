import time, logging
from stables.utils.postgres import get_rows_count, get_loaded_block, PostgresConfig
from stables.data.source.etherscan import etherscan_logs, get_latest_block

logger = logging.getLogger(__name__)


def logs(
    pipeline,
    pg_config: PostgresConfig,
    table_schema: str,
    table_name: str,
    chainid: int,
    contract_address: str,
    start_block=None,
    end_block=None,
    block_chunk_size=100000,
):
    """
    Load blockchain event logs for a specific contract address into PostgreSQL using DLT pipeline.

    This function performs incremental loading of blockchain logs by:
    1. Determining the starting block (either from last loaded block or specified)
    2. Fetching logs in configurable chunks to manage API rate limits
    3. Loading data via DLT pipeline with retry logic for robustness
    4. Tracking progress and warning about potential API limit hits

    Args:
        pipeline (dlt.Pipeline): Configured DLT pipeline instance for data loading
        table_schema (str): PostgreSQL schema name for the target table
        table_name (str): Target table name in PostgreSQL database

        chainid (int): Blockchain network ID (1 for Ethereum mainnet)
        contract_address (str): Ethereum contract address to fetch logs for (lowercase)

        config (PostgresConfig): Database configuration object with connection parameters

        start_block (int, optional): Starting block number. If None, continues from last loaded block
        end_block (int, optional): Ending block number. If None, uses latest blockchain block
        block_chunk_size (int, optional): Number of blocks to process per batch. Defaults to 100000

    Note:
        - Uses exponential backoff and retry logic for API failures
        - Warns when exactly 1000 logs are loaded (potential API limit)
        - Includes 0.2 second delay between batches to respect rate limits
        - Automatically determines incremental loading start point
    """
    if start_block is None:
        start_block = get_loaded_block(
            pg_config,
            table_schema,
            table_name,
            chainid,
            contract_address,
            column_name="block_number",
        )

    if end_block is None:
        end_block = get_latest_block(chainid=chainid)

    for from_block in range(start_block, end_block, block_chunk_size):
        to_block = str(min(from_block + block_chunk_size - 1, end_block))
        logger.info(f"Loading logs from block {from_block} to {to_block}")

        max_retries = 2
        retries = max_retries
        while retries > 0:
            try:
                n_before = get_rows_count(pg_config, table_schema, table_name)
                pipeline.run(
                    etherscan_logs(
                        chainid=chainid,
                        address=contract_address,
                        fromBlock=from_block,
                        toBlock=to_block,
                    ),
                    table_name=table_name,
                    write_disposition="append",
                )
                n_after = get_rows_count(pg_config, table_schema, table_name)

                n = n_after - n_before

                if n >= 1000:
                    logger.warning(
                        f"Loaded {n} logs from {from_block} to {to_block}, smaller batch size may be needed."
                    )
                else:
                    logger.info(f"Loaded {n} logs from {from_block} to {to_block}")
                break  # Succeeded
            except Exception as e:
                retries -= 1
                logger.error(
                    f"Error loading logs: {e}. Retrying... ({retries} retries left)"
                )
                if retries > 0:
                    time.sleep(3)
                else:
                    logger.error(
                        f"Failed to load logs for block range {from_block}-{to_block} after {max_retries} retries."
                    )
        time.sleep(0.2)
