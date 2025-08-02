"""Etherscan API client implementation."""

import os
import json
import dlt
from datetime import datetime
from typing import Any, Dict, List, Optional, Iterator

from evm_sleuth.core.base import BaseAPIClient, BaseSource, APIConfig
from evm_sleuth.core.exceptions import APIError
from evm_sleuth.config.settings import settings

from dlt.sources.rest_api import rest_api_source
from dlt.sources.helpers.rest_client import paginators


class EtherscanClient(BaseAPIClient):
    """Etherscan API client implementation."""

    def __init__(
        self, chainid: int, api_key: Optional[str] = None, calls_per_second: float = 5.0
    ):
        self.chainid = chainid

        config = APIConfig(
            base_url=settings.api_urls.ETHERSCAN,
            api_key=api_key or settings.api.etherscan_api_key,
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
        pass  # Getting ABI for contract

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
        abi_json = json.loads(result)

        # Check if it's a proxy and fetch implementation ABI
        implementation_abi = None
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
            self._save_abi(address, abi_json, implementation_abi, save_dir)

        return abi_json, implementation_abi

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
        abi_json: Dict[str, Any],
        implementation_abi: Optional[Dict[str, Any]],
        save_dir: str,
    ):
        """Save ABI(s) to file."""
        os.makedirs(save_dir, exist_ok=True)

        # Save main ABI
        main_path = os.path.join(save_dir, f"{address}.json")
        with open(main_path, "w") as f:
            json.dump(abi_json, f, indent=2)
        pass  # ABI saved

        # Save implementation ABI if available
        if implementation_abi:
            impl_path = os.path.join(save_dir, f"{address}-implementation.json")
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
                "block_number": {"data_type": "bigint"},
                "time_stamp": {"data_type": "bigint"},
                "gas_price": {"data_type": "bigint"},
                "gas_used": {"data_type": "bigint"},
                "log_index": {"data_type": "bigint"},
                "transaction_index": {"data_type": "bigint"},
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

        return dlt.resource(_fetch)
