from typing import Optional, Any
from contextlib import contextmanager
import logging

import psycopg2
from sqlalchemy import create_engine

from evm_sleuth.config.settings import PostgresSettings

logger = logging.getLogger(__name__)


class PostgresClient:
    """Object-oriented PostgreSQL client for database operations."""

    def __init__(self, setting: PostgresSettings):
        """
        Initialize PostgresClient with database configuration.

        Args:
            setting: PostgresSettings instance with connection parameters
        """
        self.setting = setting
        self._engine = None

    @contextmanager
    def get_connection(self):
        """
        Context manager for PostgreSQL database connections.

        Yields:
            psycopg2.connection: Database connection

        Example:
            with client.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM table")
                result = cursor.fetchone()
        """
        conn = None
        try:
            conn = psycopg2.connect(**self.setting.get_connection_params())
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
            params = self.setting.get_connection_params()
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
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            result = cursor.fetchone()
            cursor.close()
            return result

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
