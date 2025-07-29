from typing import Optional, Any

import psycopg2
import pandas as pd
from contextlib import contextmanager
from sqlalchemy import create_engine

from evm_sleuth.config.settings import DatabaseSettings

import logging

logger = logging.getLogger(__name__)


@contextmanager
def get_postgres_connection(db_config: DatabaseSettings):
    """
    Context manager for PostgreSQL database connections.

    Args:
        db_config: DatabaseSettings instance.

    Yields:
        psycopg2.connection: Database connection

    Example:
        with get_postgres_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM table")
            result = cursor.fetchone()
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_config.get_connection_params())
        yield conn
    finally:
        if conn:
            conn.close()


def get_sqlalchemy_engine(db_config: DatabaseSettings):
    """
    Create a SQLAlchemy engine for pandas operations.

    Args:
        db_config: DatabaseSettings instance

    Returns:
        sqlalchemy.engine.Engine: SQLAlchemy engine
    """
    params = db_config.get_connection_params()
    connection_string = f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}"
    return create_engine(connection_string)


def _fetch_one(
    db_config: DatabaseSettings,
    query: str,
    params: Optional[tuple] = None,
) -> Any:
    """
    Execute a query and return the result.

    Args:
        db_config: DatabaseSettings instance
        query: SQL query string
        params: Query parameters (optional)

    Returns:
        Query result (fetchone())
    """
    with get_postgres_connection(db_config) as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        result = cursor.fetchone()
        cursor.close()
        return result


def get_rows_count(
    pg_config: DatabaseSettings,
    table_schema: str,
    table_name: str,
) -> int:
    """
    Get the row count for a specific table.

    Args:
        db_config: DatabaseSettings instance
        table_schema: Schema name
        table_name: Table name


    Returns:
        Number of rows in the table, or 0 if table doesn't exist
    """
    try:
        # Check if table exists first
        check_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s
        )
        """
        table_exists = _fetch_one(pg_config, check_query, (table_schema, table_name))

        if not table_exists or not table_exists[0]:
            return 0

        # If table exists, get row count
        query = f"SELECT COUNT(*) FROM {table_schema}.{table_name}"
        result = _fetch_one(pg_config, query)
        return result[0] if result else 0
    except Exception as e:
        logger.warning(f"Error getting row count for {table_schema}.{table_name}: {e}")
        return 0


def get_loaded_block(
    pg_config: DatabaseSettings,
    table_schema: str,
    table_name: str,
    chainid: int,
    address: str,
    column_name: str = "block_number",
) -> int:
    """
    Get the last loaded block number for a specific address from PostgreSQL.

    This function determines the starting point for incremental data loading by finding
    the highest block number already processed for a given contract address. If no data
    exists for the address or the table doesn't exist, it falls back to the contract
    creation block number to ensure complete data coverage.

    Args:
        db_config: DatabaseSettings instance with database connection parameters
        table_schema: Schema name containing the target table
        table_name: Name of the table to query for loaded block numbers
        chainid: Blockchain chain ID (e.g., 1 for Ethereum mainnet)
        address: Contract address to filter records by
        column_name: Name of the column containing block numbers. Defaults to "block_number"

    Returns:
        int: The next block number to start loading from (last loaded block + 1),
             or the contract creation block number if no data exists.

    Note:
        - If the table doesn't exist or contains no data for the address,
          returns the contract creation block number
        - Used primarily for incremental data loading to avoid reprocessing existing data
    """
    try:
        query = f"""
        SELECT MAX(CAST({column_name} AS INTEGER)) 
        FROM {table_schema}.{table_name} 
        WHERE address = %s
        """
        result = _fetch_one(pg_config, query, (address,))

        if result and result[0] is not None:
            return result[0]
        else:
            # No data found, start from contract creation block
            from evm_sleuth.datasource.etherscan import EtherscanClient
            client = EtherscanClient(chainid)
            creation_info = client.get_contract_creation_info([address])
            return int(creation_info["blockNumber"])

    except Exception as e:
        logger.warning(
            f"No result found querying loaded blocks: {e}. Starting from contract creation block."
        )
        # Fall back to contract creation block on any error
        from evm_sleuth.datasource.etherscan import EtherscanClient
        client = EtherscanClient(chainid)
        creation_info = client.get_contract_creation_info([address])
        return int(creation_info["blockNumber"])
