import os
import requests
from datetime import datetime
import json
import time
import logging
import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.sources.helpers.rest_client import paginators


# Set up logging
logger = logging.getLogger(__name__)


class RateLimitedSession(requests.Session):
    """Simple rate-limited session for Etherscan API"""

    def __init__(self, calls_per_second=5):
        super().__init__()
        self.calls_per_second = calls_per_second
        self.last_request_time = 0
        self.min_interval = 1.0 / calls_per_second
        self.request_count = 0

    def request(self, method, url, **kwargs):
        # Rate limiting
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_interval:
            sleep_time = self.min_interval - time_since_last
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.3f}s")
            time.sleep(sleep_time)

        self.last_request_time = time.time()
        self.request_count += 1

        # Log API call
        logger.info(f"API Call #{self.request_count}: {method} {url}")

        response = super().request(method, url, **kwargs)

        # Log response status
        logger.info(
            f"Response #{self.request_count}: {response.status_code} - {response.reason}"
        )

        return response


class Etherscan:
    """
    A client for the Etherscan API.
    """

    def __init__(self, chainid: int, api_key: str, calls_per_second: int = 5):
        """
        Initializes the Etherscan client.

        Args:
            chainid: The chain ID to interact with.
            api_key: Your Etherscan API key.
            calls_per_second: The maximum number of API calls per second.
        """
        self.chainid = chainid
        self.api_key = api_key
        self.base_url = "https://api.etherscan.io/v2/api"
        self._session = RateLimitedSession(calls_per_second=calls_per_second)

    def _v2_call(self, params: dict):
        """
        Helper to make a call to the Etherscan 'v2' API.
        It uses a shared, rate-limited session and handles common error checking.
        """
        all_params = {"chainid": self.chainid, "apikey": self.api_key, **params}

        response = self._session.get(self.base_url, params=all_params)
        response.raise_for_status()
        data = response.json()

        # Check for API errors
        if data.get("status") == "0":
            message = data.get("message", "Etherscan API error")
            logger.error(f"API error: {message}")
            if "rate limit" in message.lower():
                raise Exception(f"Etherscan rate limit exceeded: {message}")
            raise Exception(f"Etherscan API error: {message}")

        return data["result"]

    def get_latest_block(self, timestamp: int = None, closest="before"):
        """Gets the latest block number, or the block number closest to a timestamp."""
        if timestamp is None:
            timestamp = int(datetime.now().timestamp())

        logger.info(f"Getting latest block for chain {self.chainid}")

        params = {
            "module": "block",
            "action": "getblocknobytime",
            "timestamp": timestamp,
            "closest": closest,
        }
        result = self._v2_call(params)

        latest_block = int(result)
        logger.info(f"Latest block: {latest_block}")
        return latest_block

    def fetch_source_code(self, address: str) -> None:
        """
        Fetch source code for a contract address and store it in self.sourcecode.

        Args:
            address: The contract address to fetch source code for.
        """
        if not hasattr(self, "sourcecode"):
            self.sourcecode = {}

        logger.info(
            f"Fetching source code for contract {address} on chain {self.chainid}"
        )

        params = {
            "module": "contract",
            "action": "getsourcecode",
            "address": address,
        }
        result = self._v2_call(params)

        # Store the result in sourcecode dict
        self.sourcecode[address] = result[0] if isinstance(result, list) else result

    def get_contract_metadata(self, address: str) -> dict:
        """
        Fetch contract metadata from Etherscan,
        including contract name, proxy status, and implementation address.

        Args:
            address: The contract address.

        Returns:
            dict: Contract metadata including name, proxy status, and implementation address.

        Raises:
            ValueError: If no source code is found for the contract.
        """
        self.fetch_source_code(address)
        all_data = self.sourcecode.get(address)
        if all_data is None:
            raise ValueError(f"No source code found for contract {address}")
        metadata = {
            "ContractName": all_data.get("ContractName"),
            "Proxy": all_data.get("Proxy") == "1",
            "Implementation": all_data.get("Implementation"),
            # "CompilerVersion": all_data.get("CompilerVersion"),
            # "Library": all_data.get("Library"),
        }
        return metadata

    def get_contract_abi(self, address, save=True, save_dir: str = "data/abi"):
        """
        Gets the ABI for a given contract address.
        If the contract is a proxy, also fetches and saves the implementation ABI.

        Args:
            address: The contract address.
            save: Whether to save the ABI to file.
            save_dir: Directory to save ABI files.

        Returns:
            dict: The main contract ABI. If it's a proxy, also returns implementation ABI.
        """
        logger.info(f"Getting ABI for contract {address} on chain {self.chainid}")

        # Get contract metadata to check for proxy
        try:
            contract_metadata = self.get_contract_metadata(address)
        except Exception as e:
            logger.warning(f"Could not get metadata for {address}: {e}")
            contract_metadata = {}

        # Fetch main contract ABI
        params = {
            "module": "contract",
            "action": "getabi",
            "address": address,
        }
        result = self._v2_call(params)

        # Parse the ABI string to get proper JSON
        abi_json = json.loads(result)
        # Check if it's a proxy and fetch implementation ABI
        implementation_abi = None
        if contract_metadata.get("Proxy"):
            implementation_address = contract_metadata.get("Implementation")
            if implementation_address:
                logger.info(
                    f"Contract {address} is a proxy. Fetching implementation ABI for {implementation_address}"
                )
                try:
                    impl_params = {
                        "module": "contract",
                        "action": "getabi",
                        "address": implementation_address,
                    }
                    impl_result = self._v2_call(impl_params)
                    implementation_abi = json.loads(impl_result)
                except Exception as e:
                    logger.warning(
                        f"Could not fetch implementation ABI for {implementation_address}: {e}"
                    )
        if save:
            os.makedirs(save_dir, exist_ok=True)
            with open(os.path.join(save_dir, f"{address}.json"), "w") as f:
                json.dump(abi_json, f, indent=2)
            logger.info(f"ABI saved to {os.path.join(save_dir, f'{address}.json')}")
            if implementation_abi:
                impl_path = os.path.join(save_dir, f"{address}-implementation.json")
                with open(impl_path, "w") as f:
                    json.dump(implementation_abi, f, indent=2)
                logger.info(f"Implementation ABI saved to {impl_path}")

        return abi_json, implementation_abi

    def get_contract_creation_txn(self, contract_addresses):
        """
        Gets contract creation block numbers for one or more contract addresses.

        Args:
            contract_addresses: A single contract address string or a list of contract address strings.

        Returns:
            A tuple containing:
            - A dictionary mapping contract addresses (lowercase) to their creation block numbers.
            - The raw API response.
        """
        # Handle single address case
        if isinstance(contract_addresses, str):
            contract_addresses = [contract_addresses]

        logger.info(
            f"Getting creation block numbers for {len(contract_addresses)} contracts on chain {self.chainid}"
        )

        params = {
            "module": "contract",
            "action": "getcontractcreation",
            "contractaddresses": ",".join(contract_addresses),
        }
        result = self._v2_call(params)
        if len(result) == 1:
            return result[0]
        return result

    def get_transaction_receipt(
        self, txhash, save=True, save_dir: str = "data/receipts"
    ):
        """
        Gets the transaction receipt for a given transaction hash.

        Args:
            txhash: The transaction hash (with or without 0x prefix).

        Returns:
            dict: The transaction receipt containing gas used, logs, status, etc.

        Raises:
            Exception: If the API returns an error or transaction not found.
        """
        # Ensure txhash has 0x prefix
        if not txhash.startswith("0x"):
            txhash = "0x" + txhash

        logger.info(f"Getting transaction receipt for {txhash} on chain {self.chainid}")

        params = {
            "module": "proxy",
            "action": "eth_getTransactionReceipt",
            "txhash": txhash,
        }

        result = self._v2_call(params)

        if result is None:
            raise Exception(f"Transaction receipt not found for {txhash}")

        logger.info(f"Retrieved transaction receipt for {txhash}")
        if save:
            os.makedirs(save_dir, exist_ok=True)
            with open(os.path.join(save_dir, f"{txhash}.json"), "w") as f:
                json.dump(result, f, indent=2)
            logger.info(
                f"Transaction receipt saved to {os.path.join(save_dir, f'{txhash}.json')}"
            )

        return result

    def get_contract_transactions(
        self,
        address: str,
        startblock: int = 0,
        endblock: str = "latest",
        offset: int = 1000,
        sort: str = "asc",
        save: bool = True,
        save_dir: str = "data/transactions",
    ):
        """
        Gets all transactions for a given contract address.

        Args:
            address: The contract address to get transactions for.
            startblock: The starting block number (default: 0).
            endblock: The ending block number (default: "latest").
            offset: The number of transactions to return (default: 1000).
            sort: Sort order - "asc" or "desc" (default: "asc").
            save: Whether to save the transactions to file.
            save_dir: Directory to save transaction files.

        Returns:
            list: List of transaction objects for the contract.

        Raises:
            Exception: If the API returns an error.
        """
        logger.info(
            f"Getting transactions for contract {address} on chain {self.chainid}"
        )

        params = {
            "module": "account",
            "action": "txlist",
            "address": address,
            "startblock": startblock,
            "endblock": endblock,
            "offset": offset,
            "sort": sort,
        }

        result = self._v2_call(params)

        if save:
            os.makedirs(save_dir, exist_ok=True)
            filename = f"{address}_transactions.json"
            filepath = os.path.join(save_dir, filename)
            with open(filepath, "w") as f:
                json.dump(result, f, indent=2)
            logger.info(f"Transactions saved to {filepath}")

        logger.info(f"Retrieved {len(result)} transactions for contract {address}")
        return result


class EtherscanDLTResource:
    """
    A client for the Etherscan DLT Resource API.
    """

    def __init__(self, chainid: int, api_key: str, calls_per_second: int = 5):
        self.chainid = chainid
        self.api_key = api_key
        self.base_url = "https://api.etherscan.io/v2/api"

    def _create_etherscan_source(self, params: dict):
        """
        Creates a dlt rest_api_source for a given set of Etherscan API parameters.
        It includes a rate-limited session for the client.
        """
        session = RateLimitedSession(calls_per_second=5)
        return rest_api_source(
            {
                "client": {
                    "base_url": self.base_url,
                    "paginator": paginators.PageNumberPaginator(
                        base_page=1, total_path=None, page_param="page"
                    ),
                    "session": session,
                },
                "resources": [
                    {
                        "name": "",  # Etherscan result is not nested
                        "endpoint": {"params": params},
                    },
                ],
            }
        )

    @dlt.resource()
    def transactions(
        self,
        address: str,
        module: str = "account",
        action: str = "txlist",
        startblock: int = 0,
        endblock: str = "latest",
        offset: int = 1000,
        sort: str = "asc",
    ):
        """dlt resource to get transactions for a given address."""
        params = {
            "chainid": self.chainid,
            "module": module,
            "action": action,
            "address": address,
            "startblock": startblock,
            "endblock": endblock,
            "offset": offset,
            "sort": sort,
            "apikey": self.api_key,
        }
        logger.info(
            f"Fetching transactions for address {address} from block {startblock}"
        )
        return self._create_etherscan_source(params)

    @dlt.resource()
    def logs(
        self,
        address: str,
        module: str = "logs",
        action: str = "getLogs",
        fromBlock: int = 0,
        toBlock: str = "latest",
        offset: int = 1000,
    ):
        """dlt resource to get event logs for a given address."""
        params = {
            "chainid": self.chainid,
            "module": module,
            "action": action,
            "address": address,
            "fromBlock": fromBlock,
            "toBlock": toBlock,
            "offset": offset,
            "apikey": self.api_key,
        }
        logger.info(
            f"Fetching logs for address {address} from block {fromBlock} to {toBlock}"
        )

        trasource = self._create_etherscan_source(params)
        for item in source:
            item["chainid"] = self.chainid
            yield item
