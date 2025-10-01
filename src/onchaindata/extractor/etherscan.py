"""Historical data extraction to Parquet files."""

import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Literal, Any

import polars as pl

from .exceptions import APIError
from .base import BaseAPIClient, BaseSource


class EtherscanClient(BaseAPIClient):
    """Etherscan API client implementation."""

    @classmethod
    def _load_chainid_mapping(cls) -> Dict[str, int]:
        """Load chain name to chainid mapping from resource file."""
        # Get the path to the chainid.json file relative to this module
        current_file = Path(__file__)
        chainid_path = (
            current_file.parent.parent.parent.parent / "resource" / "chainid.json"
        )

        try:
            with chainid_path.open("r") as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(
                f"Chain ID mapping file not found at {chainid_path}"
            )
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in chain ID mapping file: {e}")

    def __init__(
        self,
        chainid: Optional[int] = None,
        chain: Optional[str] = None,
        api_key: Optional[str] = None,
        calls_per_second: float = 5.0,
    ):
        # Validate that exactly one of chainid or chain is provided
        if chainid is not None and chain is not None:
            raise ValueError(
                "Cannot specify both 'chainid' and 'chain' parameters. Use only one."
            )
        if chainid is None and chain is None:
            raise ValueError("Must specify either 'chainid' or 'chain' parameter.")

        # Resolve chainid from chain name if needed
        chainid_mapping = self._load_chainid_mapping()
        if chain is not None:
            if chain not in chainid_mapping:
                available_chains = ", ".join(sorted(chainid_mapping.keys()))
                raise ValueError(
                    f"Unknown chain '{chain}'. Available chains: {available_chains}"
                )
            chainid = chainid_mapping[chain]

        self.chainid = chainid
        chain_name_mapping = {v: k for k, v in chainid_mapping.items()}
        self.chain = chain_name_mapping.get(chainid, "unknown")

        # Create APIs instance to load environment variables
        apis = APIs()
        config = APIConfig(
            base_url=APIUrls.ETHERSCAN,
            api_key=api_key or apis.etherscan_api_key,
            rate_limit=calls_per_second,
        )
        super().__init__(config)

    def _build_request_params(self, **kwargs) -> Dict[str, Any]:
        """Build request parameters with chain ID and API key."""
        return {"chainid": self.chainid, "apikey": self.config.api_key, **kwargs}

    def _handle_response(self, response) -> Any:
        """Handle Etherscan API response."""
        response.raise_for_status()
        data = response.json()

        if data.get("status") == "0":
            message = data.get("message", "Etherscan API error")
            if "rate limit" in message.lower():
                raise APIError(f"Rate limit exceeded: {message}")
            raise APIError(f"API error: {message}")

        return data["result"]

    def get_latest_block(
        self, timestamp: Optional[int] = None, closest: str = "before"
    ) -> int:
        """Get the latest block number or block closest to timestamp."""
        if timestamp is None:
            timestamp = int(datetime.now().timestamp())

        pass  # Getting latest block

        params = {
            "module": "block",
            "action": "getblocknobytime",
            "timestamp": timestamp,
            "closest": closest,
        }
        result = self.make_request("", params)

        latest_block = int(result)
        pass  # Latest block retrieved
        return latest_block

    def get_contract_abi(
        self, address: str, save: bool = True, save_dir: str = "data/abi"
    ) -> Dict[str, Any]:
        """Get contract ABI and optionally save to file."""
        # Get contract metadata to check for proxy
        try:
            contract_metadata = self.get_contract_metadata(address)
        except Exception as e:
            self.logger.warning(f"Could not get metadata for {address}: {e}")
            contract_metadata = {}

        # Fetch main contract ABI
        params = {
            "module": "contract",
            "action": "getabi",
            "address": address,
        }
        result = self.make_request("", params)
        abi = json.loads(result)

        # Check if it's a proxy and fetch implementation ABI
        implementation_abi = None
        implementation_address = None
        if contract_metadata.get("Proxy"):
            implementation_address = contract_metadata.get("Implementation")
            if implementation_address:
                pass  # Contract is a proxy, fetching implementation ABI
                try:
                    impl_params = {
                        "module": "contract",
                        "action": "getabi",
                        "address": implementation_address,
                    }
                    impl_result = self.make_request("", impl_params)
                    implementation_abi = json.loads(impl_result)
                except Exception as e:
                    self.logger.warning(
                        f"Could not fetch implementation ABI for {implementation_address}: {e}"
                    )

        if save:
            self._save_abi(
                address, abi, implementation_address, implementation_abi, save_dir
            )

        return abi, implementation_abi

    def get_contract_metadata(self, address: str) -> Dict[str, Any]:
        """Get contract metadata including proxy status."""
        pass  # Fetching metadata for contract

        params = {
            "module": "contract",
            "action": "getsourcecode",
            "address": address,
        }
        result = self.make_request("", params)

        source_data = result[0] if isinstance(result, list) else result
        if not source_data:
            raise ValueError(f"No source code found for contract {address}")

        return {
            "ContractName": source_data.get("ContractName"),
            "Proxy": source_data.get("Proxy") == "1",
            "Implementation": source_data.get("Implementation"),
        }

    def get_contract_creation_block_number(self, address: str) -> int:
        """Get contract creation block number for given address."""
        return int(self.get_contract_creation_info(address)["blockNumber"])

    def get_transaction_receipt(
        self, txhash: str, save: bool = True, save_dir: str = "data/receipts"
    ) -> Dict[str, Any]:
        """Get transaction receipt for given transaction hash."""
        # Ensure txhash has 0x prefix
        if not txhash.startswith("0x"):
            txhash = "0x" + txhash

        pass  # Getting transaction receipt

        params = {
            "module": "proxy",
            "action": "eth_getTransactionReceipt",
            "txhash": txhash,
        }

        result = self.make_request("", params)

        if result is None:
            raise APIError(f"Transaction receipt not found for {txhash}")

        if save:
            self._save_receipt(txhash, result, save_dir)

        return result

    def get_contract_creation_info(
        self, contract_addresses: List[str]
    ) -> Dict[str, Any]:
        """Get contract creation information for one or more addresses."""
        if isinstance(contract_addresses, str):
            contract_addresses = [contract_addresses]

        pass  # Getting creation info for contracts

        params = {
            "module": "contract",
            "action": "getcontractcreation",
            "contractaddresses": ",".join(contract_addresses),
        }
        result = self.make_request("", params)

        if len(contract_addresses) == 1:
            return result[0] if isinstance(result, list) else result
        return result

    def _save_abi(
        self,
        address: str,
        abi: Dict[str, Any],
        implementation_address: Optional[str],
        implementation_abi: Optional[Dict[str, Any]],
        save_dir: str,
    ):
        """Save ABI(s) to file."""
        os.makedirs(save_dir, exist_ok=True)
        # create a csv file with the following columns: address, implementation_address
        csv_path = os.path.join(save_dir, "implementation.csv")

        # Check if file exists to determine whether to write headers
        if not os.path.exists(csv_path):
            # Create new file with headers
            with open(csv_path, "w") as f:
                f.write("address,implementation_address\n")

        with open(csv_path, "a") as f:
            f.write(f"{address},{implementation_address}\n")
        df = pd.read_csv(csv_path)
        df = df.drop_duplicates()
        df.to_csv(csv_path, index=False)

        # Save main ABI
        main_path = os.path.join(save_dir, f"{address}.json")
        with open(main_path, "w") as f:
            json.dump(abi, f, indent=2)
        pass  # ABI saved

        # Save implementation ABI if available
        if implementation_abi:
            impl_path = os.path.join(save_dir, f"{implementation_address}.json")
            with open(impl_path, "w") as f:
                json.dump(implementation_abi, f, indent=2)
            pass  # Implementation ABI saved

    def _save_receipt(self, txhash: str, receipt: Dict[str, Any], save_dir: str):
        """Save transaction receipt to file."""
        os.makedirs(save_dir, exist_ok=True)

        receipt_path = os.path.join(save_dir, f"{txhash}.json")
        with open(receipt_path, "w") as f:
            json.dump(receipt, f, indent=2)
        pass  # Receipt saved


class EtherscanSource(BaseSource):
    """Creating DLT source for Etherscan data."""

    def __init__(self, client: EtherscanClient):
        super().__init__(client)

    def get_available_sources(self) -> List[str]:
        """Return list of available source names."""
        return ["logs", "transactions"]

    def create_dlt_source(self, **kwargs):
        """Create DLT source for Etherscan API."""
        session = self.client._session
        return rest_api_source(
            {
                "client": {
                    "base_url": self.client.config.base_url,
                    "paginator": paginators.PageNumberPaginator(
                        base_page=1, total_path=None, page_param="page"
                    ),
                    "session": session,
                },
                "resources": [
                    {
                        "name": "",  # Etherscan result is not nested
                        "endpoint": {"params": kwargs},
                    },
                ],
            }
        )

    def logs(
        self,
        address: str,
        from_block: int = 0,
        to_block: str = "latest",
        offset: int = 1000,
    ):
        """Get event logs for a given address."""

        def _fetch():
            params = {
                "module": "logs",
                "action": "getLogs",
                "address": address,
                "fromBlock": from_block,
                "toBlock": to_block,
                "offset": offset,
                "chainid": self.client.chainid,
                "apikey": self.client.config.api_key,
            }

            pass  # Fetching logs for address

            source = self.create_dlt_source(**params)
            for item in source:
                item["chainid"] = self.client.chainid
                yield item

        return dlt.resource(
            _fetch,
            columns={
                "topics": {"data_type": "json"},
                "time_stamp": {"data_type": "bigint"},
                "block_number": {"data_type": "bigint"},
                "log_index": {"data_type": "bigint"},
                "transaction_index": {"data_type": "bigint"},
                "gas_price": {"data_type": "bigint"},
                "gas_used": {"data_type": "bigint"},
            },
        )

    def transactions(
        self,
        address: str,
        from_block: int = 0,
        to_block: str = "latest",
        offset: int = 1000,
        sort: str = "asc",
    ):
        """Get transactions for a given address."""

        def _fetch():
            params = {
                "module": "account",
                "action": "txlist",
                "address": address,
                "startblock": from_block,
                "endblock": to_block,
                "offset": offset,
                "sort": sort,
                "chainid": self.client.chainid,
                "apikey": self.client.config.api_key,
            }

            pass  # Fetching transactions for address

            source = self.create_dlt_source(**params)
            for item in source:
                item["chainid"] = self.client.chainid
                yield item

        return dlt.resource(
            _fetch,
            columns={
                "time_stamp": {"data_type": "bigint"},
            },
        )


class EtherscanExtractor:
    """Extracts historical blockchain data and saves to Parquet files.

    Example:
        extractor = EtherscanExtractor(etherscan_client)

        # Extract logs for single address
        path = extractor.to_parquet("0x123...", "ethereum", "logs")

    """

    def __init__(
        self,
        client: EtherscanClient,
        save_dir: str = os.getenv("PARQUET_DATA_DIR"),
    ):
        self.client = client
        self.save_dir = save_dir
        self.logger = logging.getLogger(self.__class__.__name__)

    def to_parquet(
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
        self.logger.debug(
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

            if not len(data) == 0:
                self.logger.debug(f"No {table} extracted for address {address}")
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
            new_lazy = pl.LazyFrame(data)

            # Save to Parquet (append if file exists)
            if output_path.exists():
                # Use scan_parquet for memory efficiency, then concatenate and collect
                existing_lazy = pl.scan_parquet(output_path)

                # Ensure column order matches between existing and new data
                existing_columns = existing_lazy.collect_schema().names()
                new_lazy = new_lazy.select(existing_columns)

                combined_lazy = pl.concat([existing_lazy, new_lazy]).unique()
                # only keep unique records, sometime dup happens especially running retry_failed_blocks

                # Get count
                existing_count = existing_lazy.select(pl.len()).collect().item()
                combined_count = combined_lazy.select(pl.len()).collect().item()

                if existing_count != combined_count:
                    # Materialize and write the combined data
                    combined_lazy.collect().write_parquet(output_path)
                    self.logger.debug(
                        f"{output_path}: Existing count: {existing_count}, added: {combined_count - existing_count}"
                    )
                else:
                    self.logger.debug(f"{output_path}: No new records to append")

            else:
                # Write new file
                new_lazy.collect().write_parquet(output_path)
                self.logger.debug(f"{output_path}: Created new file")

            return str(output_path)

        except Exception as e:
            self.logger.error(f"Failed to save {table} data for address {address}: {e}")
            raise
