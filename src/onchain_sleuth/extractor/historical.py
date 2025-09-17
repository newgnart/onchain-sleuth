"""Historical data extraction to Parquet files."""

import os
import logging
from pathlib import Path
from typing import Dict, List, Optional
from collections import defaultdict

import polars as pl

from onchain_sleuth.datasource.etherscan import EtherscanClient, EtherscanSource
from onchain_sleuth.config.protocol_registry import ProtocolRegistry
from onchain_sleuth.core.exceptions import APIError


class HistoricalDataExtractor:
    """Extracts historical blockchain data and saves to protocol-grouped Parquet files."""

    def __init__(self, client: EtherscanClient, output_dir: str = "data"):
        self.client = client
        self.output_dir = output_dir
        self.protocol_registry = ProtocolRegistry()
        self.logger = logging.getLogger(self.__class__.__name__)

    def extract_to_parquet(
        self,
        contracts: List[str],
        from_block: int = 0,
        to_block: str = "latest",
        offset: int = 1000
    ) -> Dict[str, str]:
        """
        Extract logs for multiple contracts and save to protocol-grouped Parquet files.

        Args:
            contracts: List of contract addresses
            from_block: Starting block number
            to_block: Ending block number or "latest"
            offset: Number of logs per API call

        Returns:
            Dict mapping protocol names to output file paths
        """
        # Group contracts by protocol
        protocol_groups = self._group_by_protocol(contracts)

        output_paths = {}
        for protocol, addresses in protocol_groups.items():
            self.logger.info(f"Extracting data for protocol '{protocol}' ({len(addresses)} contracts)")
            output_path = self._extract_protocol_data(
                protocol, addresses, from_block, to_block, offset
            )
            output_paths[protocol] = output_path

        return output_paths

    def _group_by_protocol(self, contracts: List[str]) -> Dict[str, List[str]]:
        """Group contract addresses by protocol."""
        protocol_groups = defaultdict(list)

        for contract in contracts:
            protocol = self.protocol_registry.get_protocol(contract)
            protocol_groups[protocol].append(contract)

        return dict(protocol_groups)

    def _extract_protocol_data(
        self,
        protocol: str,
        addresses: List[str],
        from_block: int,
        to_block: str,
        offset: int
    ) -> str:
        """Extract logs for all contracts in a protocol and save to Parquet."""
        all_logs = []
        source = EtherscanSource(self.client)

        for address in addresses:
            self.logger.info(f"Fetching logs for {address} (protocol: {protocol})")

            try:
                logs_resource = source.logs(
                    address=address,
                    from_block=from_block,
                    to_block=to_block,
                    offset=offset
                )

                # Collect logs and add metadata
                for log in logs_resource:
                    log['contract_address'] = address
                    log['protocol'] = protocol
                    all_logs.append(log)

            except APIError as e:
                self.logger.error(f"Failed to fetch logs for {address}: {e}")
                continue
            except Exception as e:
                self.logger.error(f"Unexpected error fetching logs for {address}: {e}")
                continue

        if not all_logs:
            self.logger.warning(f"No logs extracted for protocol '{protocol}'")
            return None

        # Convert to Polars DataFrame and save
        return self._save_to_parquet(protocol, all_logs)

    def _save_to_parquet(self, protocol: str, logs_data: List[Dict]) -> str:
        """Save logs data to protocol-specific Parquet file."""
        # Create output directory
        output_dir = Path(self.output_dir) / "etherscan" / "logs" / f"protocol={protocol}"
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = output_dir / "logs.parquet"

        try:
            # Create Polars DataFrame
            df = pl.DataFrame(logs_data)

            # Ensure consistent schema - convert common fields to proper types
            if len(df) > 0:
                schema_conversions = {}

                # Convert timestamp and numeric fields
                if 'timeStamp' in df.columns:
                    schema_conversions['timeStamp'] = pl.Int64
                if 'blockNumber' in df.columns:
                    schema_conversions['blockNumber'] = pl.Int64
                if 'logIndex' in df.columns:
                    schema_conversions['logIndex'] = pl.Int64
                if 'transactionIndex' in df.columns:
                    schema_conversions['transactionIndex'] = pl.Int64
                if 'gasPrice' in df.columns:
                    schema_conversions['gasPrice'] = pl.Int64
                if 'gasUsed' in df.columns:
                    schema_conversions['gasUsed'] = pl.Int64

                # Apply conversions if any
                if schema_conversions:
                    df = df.with_columns([
                        pl.col(col).cast(dtype, strict=False)
                        for col, dtype in schema_conversions.items()
                        if col in df.columns
                    ])

            # Save to Parquet
            df.write_parquet(output_path)

            self.logger.info(f"Saved {len(df)} logs for protocol '{protocol}' to {output_path}")
            return str(output_path)

        except Exception as e:
            self.logger.error(f"Failed to save data for protocol '{protocol}': {e}")
            raise

    def get_protocol_stats(self, contracts: List[str]) -> Dict[str, int]:
        """Get statistics on how many contracts belong to each protocol."""
        protocol_groups = self._group_by_protocol(contracts)
        return {protocol: len(addresses) for protocol, addresses in protocol_groups.items()}

    def extract_single_protocol(
        self,
        protocol: str,
        from_block: int = 0,
        to_block: str = "latest",
        offset: int = 1000
    ) -> Optional[str]:
        """Extract data for all known contracts of a specific protocol."""
        # Get all contracts for this protocol
        known_contracts = self.protocol_registry.get_known_contracts()
        protocol_contracts = [
            addr for addr, proto in known_contracts.items()
            if proto == protocol
        ]

        if not protocol_contracts:
            self.logger.warning(f"No known contracts found for protocol '{protocol}'")
            return None

        self.logger.info(f"Found {len(protocol_contracts)} contracts for protocol '{protocol}'")

        return self._extract_protocol_data(
            protocol, protocol_contracts, from_block, to_block, offset
        )