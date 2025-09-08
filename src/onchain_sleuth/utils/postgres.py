from typing import Optional, Any, Dict, Union
from contextlib import contextmanager
from abc import ABC, abstractmethod
import os
import logging

import psycopg2
import duckdb
from sqlalchemy import create_engine
import dlt

from onchain_sleuth.config.settings import Postgres

logger = logging.getLogger(__name__)


class Destination(ABC):
    """Abstract base class for database destinations."""

    @abstractmethod
    def fetch_one(self, query: str, params: Optional[tuple] = None) -> Any:
        """Execute a query and return the first result."""
        pass

    @abstractmethod
    def fetch_all(self, query: str, params: Optional[tuple] = None) -> list:
        """Execute a query and return all results."""
        pass

    @abstractmethod
    def execute(self, query: str, params: Optional[tuple] = None) -> None:
        """Execute a query without returning results (INSERT, UPDATE, DELETE)."""
        pass

    @abstractmethod
    def table_exists(self, table_schema: str, table_name: str) -> bool:
        """Check if a table exists in the database."""
        pass

    @abstractmethod
    def get_table_row_count(self, table_schema: str, table_name: str) -> int:
        """Get the row count for a specific table."""
        pass

    @abstractmethod
    def get_max_loaded_block(
        self,
        table_schema: str,
        table_name: str,
        chainid: int,
        address: str,
        address_column_name: str = "address",
        block_column_name: str = "block_number",
    ) -> int:
        """Get the maximum loaded block for a specific address and chain."""
        pass

    @abstractmethod
    def get_connection_params(self) -> Dict[str, Any]:
        """Return connection parameters for database clients."""
        pass

    @abstractmethod
    def get_connection_url(self) -> str:
        """Return connection URL for database clients."""
        pass

    @abstractmethod
    def get_dlt_destination(self) -> Any:
        """Return DLT destination for pipeline operations."""
        pass


class PostgresDestination(Destination):
    """Object-oriented PostgreSQL client for database operations."""

    def __init__(self, setting: Postgres):
        """
        Initialize PostgresDestination with database configuration.

        Args:
            setting: PostgresSettings instance with connection parameters
        """
        self.setting = setting
        self._engine = None

    def get_connection_params(self) -> Dict[str, Any]:
        """Return connection parameters for database clients."""
        return {
            "host": self.setting.host,
            "port": self.setting.port,
            "database": self.setting.database,
            "user": self.setting.user,
            "password": self.setting.password,
        }

    def get_connection_url(self) -> str:
        """Return connection URL for database clients."""
        return f"postgresql://{self.setting.user}:{self.setting.password}@{self.setting.host}:{self.setting.port}/{self.setting.database}"

    def get_dlt_destination(self) -> Any:
        """Return DLT destination for pipeline operations."""
        return dlt.destinations.postgres(self.get_connection_url())

    @contextmanager
    def get_connection(self):
        """
        Context manager for PostgreSQL database connections.

        Yields:
            psycopg2.connection: Database connection

        Example:
            with destination.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM table")
                result = cursor.fetchone()
        """
        conn = None
        try:
            conn = psycopg2.connect(**self.get_connection_params())
            yield conn
        finally:
            if conn:
                conn.close()

    @property
    def sqlalchemy_engine(self):
        """
        Get SQLAlchemy engine for pandas operations (cached).

        Returns:
            sqlalchemy.engine.Engine: SQLAlchemy engine
        """
        if self._engine is None:
            params = self.get_connection_params()
            connection_string = f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}"
            self._engine = create_engine(connection_string)
        return self._engine

    def fetch_one(self, query: str, params: Optional[tuple] = None) -> Any:
        """
        Execute a query and return the first result.

        Args:
            query: SQL query string
            params: Query parameters (optional)

        Returns:
            Query result (fetchone())
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                result = cursor.fetchone()
                cursor.close()
                return result
        except Exception as e:
            logger.warning(f"Failed to query {query} with error {e}, returning None")
            return None

    def fetch_all(self, query: str, params: Optional[tuple] = None) -> list:
        """
        Execute a query and return all results.

        Args:
            query: SQL query string
            params: Query parameters (optional)

        Returns:
            List of query results (fetchall())
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            result = cursor.fetchall()
            cursor.close()
            return result

    def execute(self, query: str, params: Optional[tuple] = None) -> None:
        """
        Execute a query without returning results (INSERT, UPDATE, DELETE).

        Args:
            query: SQL query string
            params: Query parameters (optional)
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            cursor.close()

    def table_exists(self, table_schema: str, table_name: str) -> bool:
        """
        Check if a table exists in the database.

        Args:
            table_schema: Schema name
            table_name: Table name

        Returns:
            True if table exists, False otherwise
        """
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s
        )
        """
        result = self.fetch_one(query, (table_schema, table_name))
        return result[0] if result else False

    def get_table_row_count(self, table_schema: str, table_name: str) -> int:
        """
        Get the row count for a specific table.

        Args:
            table_schema: Schema name
            table_name: Table name

        Returns:
            Number of rows in the table, or 0 if table doesn't exist
        """
        try:
            if not self.table_exists(table_schema, table_name):
                return 0

            query = f"SELECT COUNT(*) FROM {table_schema}.{table_name}"
            result = self.fetch_one(query)
            return result[0] if result else 0
        except Exception as e:
            logger.warning(
                f"Error getting row count for {table_schema}.{table_name}: {e}"
            )
            return 0

    def get_max_loaded_block(
        self,
        table_schema: str,
        table_name: str,
        chainid: int,
        address: str,
        address_column_name: str = "address",
        block_column_name: str = "block_number",
    ) -> int:
        address = address.lower()
        try:
            query = f"""
            SELECT MAX({block_column_name}) 
            FROM {table_schema}.{table_name} 
            WHERE {address_column_name} = %s
            AND chainid = %s
            """
            result = self.fetch_one(query, (address, chainid))

            if result and result[0] is not None:
                return int(result[0])
            else:
                return 0

        except Exception as e:
            logger.warning(f"No result found querying loaded blocks: {e}")
            return 0


class DuckdbDestination(Destination):
    """Object-oriented DuckDB client for database operations."""

    def __init__(self, database_path: str = "data/duckdb/data.duckdb"):
        """
        Initialize DuckdbDestination with database configuration.

        Args:
            database_path: Path to DuckDB database file (default: in-memory)
        """
        _dir = os.path.dirname(database_path)
        if not os.path.exists(_dir):
            os.makedirs(_dir)
        # if not os.path.exists(database_path):
        self.database_path = database_path
        self._connection = None

    def get_connection_params(self) -> Dict[str, Any]:
        """Return connection parameters for database clients."""
        return {
            "database": self.database_path,
        }

    def get_connection_url(self) -> str:
        """Return connection URL for database clients."""
        return f"duckdb://{self.database_path}"

    def get_dlt_destination(self) -> Any:
        """Return DLT destination for pipeline operations."""
        return dlt.destinations.duckdb(self.database_path)

    @contextmanager
    def get_connection(self):
        """
        Context manager for DuckDB database connections.

        Yields:
            duckdb.DuckDBPyConnection: Database connection

        Example:
            with destination.get_connection() as conn:
                result = conn.execute("SELECT COUNT(*) FROM table").fetchone()
        """
        conn = None
        try:
            conn = duckdb.connect(self.database_path)
            yield conn
        finally:
            if conn:
                conn.close()

    def fetch_one(self, query: str, params: Optional[tuple] = None) -> Any:
        """
        Execute a query and return the first result.

        Args:
            query: SQL query string
            params: Query parameters (optional)

        Returns:
            Query result (fetchone())
        """
        try:
            with self.get_connection() as conn:
                if params:
                    result = conn.execute(query, params).fetchone()
                else:
                    result = conn.execute(query).fetchone()
                return result
        except Exception as e:
            logger.warning(f"Failed to query {query} with error {e}, returning None")
            return None

    def fetch_all(self, query: str, params: Optional[tuple] = None) -> list:
        """
        Execute a query and return all results.

        Args:
            query: SQL query string
            params: Query parameters (optional)

        Returns:
            List of query results (fetchall())
        """
        with self.get_connection() as conn:
            if params:
                result = conn.execute(query, params).fetchall()
            else:
                result = conn.execute(query).fetchall()
            return result

    def execute(self, query: str, params: Optional[tuple] = None) -> None:
        """
        Execute a query without returning results (INSERT, UPDATE, DELETE).

        Args:
            query: SQL query string
            params: Query parameters (optional)
        """
        with self.get_connection() as conn:
            if params:
                conn.execute(query, params)
            else:
                conn.execute(query)

    def table_exists(self, table_schema: str, table_name: str) -> bool:
        """
        Check if a table exists in the database.

        Args:
            table_schema: Schema name (DuckDB uses 'main' as default schema)
            table_name: Table name

        Returns:
            True if table exists, False otherwise
        """
        query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = ? AND table_name = ?
        )
        """
        result = self.fetch_one(query, (table_schema, table_name))
        return result[0] if result else False

    def get_table_row_count(self, table_schema: str, table_name: str) -> int:
        """
        Get the row count for a specific table.

        Args:
            table_schema: Schema name
            table_name: Table name

        Returns:
            Number of rows in the table, or 0 if table doesn't exist
        """
        try:
            if not self.table_exists(table_schema, table_name):
                return 0

            query = f"SELECT COUNT(*) FROM {table_schema}.{table_name}"
            result = self.fetch_one(query)
            return result[0] if result else 0
        except Exception as e:
            logger.warning(
                f"Error getting row count for {table_schema}.{table_name}: {e}"
            )
            return 0

    def get_max_loaded_block(
        self,
        table_schema: str,
        table_name: str,
        chainid: int,
        address: str,
        address_column_name: str = "address",
        block_column_name: str = "block_number",
    ) -> int:
        """
        Get the maximum loaded block for a specific address and chain.

        Args:
            table_schema: Schema name
            table_name: Table name
            chainid: Chain ID
            address: Contract address
            address_column_name: Name of the address column
            block_column_name: Name of the block number column

        Returns:
            Maximum block number, or 0 if not found
        """
        address = address.lower()
        try:
            query = f"""
            SELECT MAX({block_column_name}) 
            FROM {table_schema}.{table_name} 
            WHERE {address_column_name} = ?
            AND chainid = ?
            """
            result = self.fetch_one(query, (address, chainid))

            if result and result[0] is not None:
                return int(result[0])
            else:
                return 0

        except Exception as e:
            logger.warning(f"No result found querying loaded blocks: {e}")
            return 0
