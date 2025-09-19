"""Historical data extraction to Parquet files."""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Literal, Any

import polars as pl

from onchain_sleuth.datasource.etherscan import EtherscanClient, EtherscanSource
from onchain_sleuth.core.exceptions import APIError


class HistoricalDataExtractor:
    """Extracts historical blockchain data and saves to Parquet files.

    Example:
        extractor = HistoricalDataExtractor(etherscan_client)

        # Extract logs for single address
        path = extractor.extract_to_parquet("0x123...", "ethereum", "logs")

    """

    def __init__(
        self,
        client: EtherscanClient,
        save_dir: str = "data/etherscan_raw",
    ):
        self.client = client
        self.save_dir = save_dir
        self.logger = logging.getLogger(self.__class__.__name__)

    def extract_to_parquet(
        self,
        address: str,
        chain: str = "ethereum",
        table: Literal["logs", "transactions"] = "logs",
        from_block: int = 0,
        to_block: str = "latest",
        offset: int = 1000,
        output_path: Optional[str] = None,
    ) -> Optional[str]:
        """
        Core building block function to extract blockchain data to Parquet files.

        Args:
            address: Contract address to extract data for
            chain: Blockchain network (default: "ethereum")
            table: Type of data to extract ("logs" or "transactions")
            from_block: Starting block number
            to_block: Ending block number or "latest"
            offset: Number of records per API call

        Returns:
            Path to the created Parquet file, or None if no data extracted
        """
        self.logger.info(
            f"Extracting {table} for address {address} on {chain} from block {from_block} to {to_block}"
        )

        source = EtherscanSource(self.client)
        data = []

        try:
            if table == "logs":
                resource = source.logs(
                    address=address,
                    from_block=from_block,
                    to_block=to_block,
                    offset=offset,
                )

                for record in resource:
                    # Convert hex strings to integers for numeric fields
                    record = self._process_hex_fields(record)
                    record["contract_address"] = address
                    record["chain"] = chain
                    data.append(record)

            elif table == "transactions":
                resource = source.transactions(
                    address=address,
                    from_block=from_block,
                    to_block=to_block,
                    offset=offset,
                )

                for record in resource:
                    # Convert hex strings to integers for numeric fields
                    record = self._process_hex_fields(record)
                    record["address"] = address
                    record["chain"] = chain
                    data.append(record)

            if not data:
                self.logger.warning(f"No {table} extracted for address {address}")
                return None

            return self._save_to_parquet(address, chain, table, data, output_path)

        except APIError as e:
            self.logger.error(f"Failed to fetch {table} for {address}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error fetching {table} for {address}: {e}")
            return None

    def _process_hex_fields(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert numeric string fields to integers (handles both hex and decimal formats)."""
        numeric_fields = {
            "blockNumber",
            "timeStamp",
            "logIndex",
            "transactionIndex",
            "gasPrice",
            "gasUsed",
            "nonce",
            "value",
            "gas",
            "cumulativeGasUsed",
            "confirmations",
        }

        for field in numeric_fields:
            if field in record and isinstance(record[field], str):
                str_value = record[field].strip()
                if str_value and str_value != "0x":
                    try:
                        # Auto-detect format based on prefix
                        if str_value.startswith("0x"):
                            # Hex format (logs API)
                            record[field] = int(str_value, 16)
                        else:
                            # Decimal format (transactions API)
                            record[field] = int(str_value, 10)
                    except ValueError:
                        self.logger.warning(
                            f"Could not convert {field} value '{str_value}' to int"
                        )
                        record[field] = None
                else:
                    record[field] = None

        return record

    def _save_to_parquet(
        self,
        address: str,
        chain: str,
        table: str,
        data: List[Dict[str, Any]],
        output_path: Optional[str] = None,
    ) -> str:
        """Save data to Parquet file organized by chain/table/address."""
        # Create output directory structure: chain=ethereum/table=logs/address=0x123...
        if output_path is None:
            output_dir = Path(self.save_dir) / f"{chain}_{address}"
            output_dir.mkdir(parents=True, exist_ok=True)

            output_path = output_dir / f"{table}.parquet"
        else:
            # Ensure output_path is a Path object
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            # Create Polars DataFrame
            df = pl.DataFrame(data)

            # Save to Parquet (append if file exists)
            if output_path.exists():
                # Use scan_parquet for memory efficiency, then concatenate and collect
                existing_lazy = pl.scan_parquet(output_path)

                # Ensure column order matches between existing and new data
                existing_columns = existing_lazy.collect_schema().names()
                new_lazy = pl.LazyFrame(df).select(existing_columns)

                combined_lazy = pl.concat([existing_lazy, new_lazy])

                # Get count before materializing for logging
                existing_count = existing_lazy.select(pl.len()).collect().item()
                new_count = len(df)
                total_count = existing_count + new_count

                # Materialize and write the combined data
                combined_lazy.collect().write_parquet(output_path)
                self.logger.info(
                    f"Appended {new_count} {table} to existing {existing_count} records (total now: {total_count})"
                )
            else:
                # Write new file
                df.write_parquet(output_path)
                self.logger.info(f"Created new file with {len(df)} {table}")

            return str(output_path)

        except Exception as e:
            self.logger.error(f"Failed to save {table} data for address {address}: {e}")
            raise
