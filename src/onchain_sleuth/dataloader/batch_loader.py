"""Batch loading of historical data from Parquet files to PostgreSQL."""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass

import polars as pl
import dlt

from onchain_sleuth.core.exceptions import PipelineError


@dataclass
class TableConfig:
    """Configuration for a single table in a batch load operation."""

    write_disposition: str = "append"
    primary_key: Optional[List[str]] = None


class BatchLoader:
    """Loads historical data from Parquet files to PostgreSQL using DLT."""

    def __init__(self, data_dir: str = "data", logs_subdir: str = "etherscan_raw"):
        self.data_dir = Path(data_dir)
        self.logs_subdir = logs_subdir
        self.logger = logging.getLogger(self.__class__.__name__)

    def load_protocol_data(
        self,
        protocol: str,
        destination: Any,
        dataset_name: str = "historical_data",
        table_name: Optional[str] = None,
        write_disposition: str = "append",
        primary_key: Optional[List[str]] = None,
    ) -> Any:
        """
        Load data for a specific protocol from Parquet to PostgreSQL.

        Args:
            protocol: Protocol name (e.g., "curve", "uniswap-v3")
            destination: DLT destination configuration
            dataset_name: Target dataset name in PostgreSQL
            table_name: Target table name (defaults to "{protocol}_logs")
            write_disposition: How to handle existing data ("append", "replace", "merge")
            primary_key: Primary key columns for merge operations

        Returns:
            DLT pipeline run result
        """
        parquet_path = (
            self.data_dir / self.logs_subdir / f"protocol={protocol}" / "logs.parquet"
        )

        if not parquet_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

        # Set default table name
        if table_name is None:
            table_name = f"{protocol}_logs"

        self.logger.info(
            f"Loading protocol '{protocol}' data from {parquet_path} to table '{table_name}'"
        )

        try:
            # Use scan_parquet for memory efficiency, then collect only when needed
            lazy_df = pl.scan_parquet(parquet_path)

            # Get row count efficiently
            row_count = lazy_df.select(pl.len()).collect().item()
            self.logger.info(f"Loaded {row_count} rows from Parquet file")

            # Convert to records for DLT (materialize the data)
            records = lazy_df.collect().to_dicts()

            # Create and run pipeline
            pipeline = dlt.pipeline(
                pipeline_name=f"batch_load_{protocol}",
                destination=destination,
                dataset_name=dataset_name,
            )

            run_kwargs = {
                "table_name": table_name,
                "write_disposition": write_disposition,
            }
            if primary_key:
                run_kwargs["primary_key"] = primary_key

            result = pipeline.run(records, **run_kwargs)
            self.logger.info(
                f"Successfully loaded {len(records)} rows to table '{table_name}'"
            )

            return result

        except Exception as e:
            error_msg = f"Failed to load protocol '{protocol}' data: {e}"
            self.logger.error(error_msg)
            raise PipelineError(error_msg) from e

    def load_multiple_protocols(
        self,
        protocols: List[str],
        destination: Any,
        dataset_name: str = "historical_data",
        table_configs: Optional[Dict[str, TableConfig]] = None,
    ) -> Dict[str, Any]:
        """
        Load data for multiple protocols.

        Args:
            protocols: List of protocol names to load
            destination: DLT destination configuration
            dataset_name: Target dataset name
            table_configs: Optional per-protocol table configurations

        Returns:
            Dict mapping protocol names to their load results
        """
        results = {}
        table_configs = table_configs or {}

        for protocol in protocols:
            try:
                config = table_configs.get(protocol, TableConfig())
                table_name = f"{protocol}_logs"

                result = self.load_protocol_data(
                    protocol=protocol,
                    destination=destination,
                    dataset_name=dataset_name,
                    table_name=table_name,
                    write_disposition=config.write_disposition,
                    primary_key=config.primary_key,
                )
                results[protocol] = result

            except Exception as e:
                self.logger.error(f"Failed to load protocol '{protocol}': {e}")
                results[protocol] = {"error": str(e)}

        return results

    def load_all_available_protocols(
        self, destination: Any, dataset_name: str = "historical_data"
    ) -> Dict[str, Any]:
        """
        Load all available protocol data found in the data directory.

        Args:
            destination: DLT destination configuration
            dataset_name: Target dataset name

        Returns:
            Dict mapping protocol names to their load results
        """
        # Find all protocol directories
        logs_dir = self.data_dir / self.logs_subdir
        if not logs_dir.exists():
            raise FileNotFoundError(f"Logs directory not found: {logs_dir}")

        protocols = []
        for protocol_dir in logs_dir.iterdir():
            if protocol_dir.is_dir() and protocol_dir.name.startswith("protocol="):
                protocol_name = protocol_dir.name.replace("protocol=", "")
                parquet_file = protocol_dir / "logs.parquet"
                if parquet_file.exists():
                    protocols.append(protocol_name)

        if not protocols:
            self.logger.warning(f"No protocol data found in {logs_dir}")
            return {}

        self.logger.info(f"Found {len(protocols)} protocols to load: {protocols}")

        return self.load_multiple_protocols(protocols, destination, dataset_name)

    def get_protocol_stats(self, protocol: str) -> Dict[str, Any]:
        """Get statistics about a protocol's Parquet data."""
        parquet_path = (
            self.data_dir / self.logs_subdir / f"protocol={protocol}" / "logs.parquet"
        )

        if not parquet_path.exists():
            return {"exists": False, "path": str(parquet_path)}

        try:
            # Use scan_parquet for memory efficiency
            lazy_df = pl.scan_parquet(parquet_path)

            # Get basic stats efficiently
            row_count = lazy_df.select(pl.len()).collect().item()
            columns = lazy_df.collect_schema().names()

            stats = {
                "exists": True,
                "path": str(parquet_path),
                "row_count": row_count,
                "columns": columns,
                "file_size_mb": round(parquet_path.stat().st_size / (1024 * 1024), 2),
            }

            # Add date range if timestamp column exists
            timestamp_cols = [
                col
                for col in columns
                if "timestamp" in col.lower() or "time" in col.lower()
            ]
            if timestamp_cols and row_count > 0:
                timestamp_col = timestamp_cols[0]
                date_stats = (
                    lazy_df.select([
                        pl.col(timestamp_col).min().alias("min"),
                        pl.col(timestamp_col).max().alias("max")
                    ])
                    .collect()
                    .to_dicts()[0]
                )
                stats["date_range"] = date_stats

            # Add contract count if available
            if "contract_address" in columns:
                unique_contracts = (
                    lazy_df.select(pl.col("contract_address").n_unique())
                    .collect()
                    .item()
                )
                stats["unique_contracts"] = unique_contracts

            return stats

        except Exception as e:
            return {"exists": True, "path": str(parquet_path), "error": str(e)}

    def list_available_protocols(self) -> List[str]:
        """List all protocols that have Parquet data available."""
        logs_dir = self.data_dir / self.logs_subdir
        if not logs_dir.exists():
            return []

        protocols = []
        for protocol_dir in logs_dir.iterdir():
            if protocol_dir.is_dir() and protocol_dir.name.startswith("protocol="):
                protocol_name = protocol_dir.name.replace("protocol=", "")
                parquet_file = protocol_dir / "logs.parquet"
                if parquet_file.exists():
                    protocols.append(protocol_name)

        return sorted(protocols)
