"""
Helper functions for loading data from Etherscan.
"""

import argparse
import json
import logging
import sys
import time
from functools import partial
import re

from typing import Optional, Any, List

import dlt
from evm_sleuth import settings, EtherscanClient, PipelineManager, EtherscanSource
from evm_sleuth.utils.postgres import PostgresClient

# Configure logging
logger = logging.getLogger(__name__)


def load_chunks(
    dataset_name: str,  # schema
    table_name: str,
    contract_address: str,
    etherscan_client: EtherscanClient,
    postgres_client: PostgresClient,
    source_factory: Any,  # Callable that creates source (logs or txns or)
    from_block: Optional[int] = None,
    to_block: Optional[int] = None,
    block_chunk_size: int = 50_000,
    write_disposition: str = "append",
    primary_key: Optional[List[str]] = None,
):
    pipeline_manager = PipelineManager()
    contract_address = contract_address.lower()
    chainid = etherscan_client.chainid
    # Determine start block using PostgresClient
    if from_block is None:
        max_loaded_block = postgres_client.get_max_loaded_block(
            table_schema=dataset_name,
            table_name=table_name,
            chainid=chainid,
            address=contract_address,
            column_name="block_number",
        )
        contract_creation_block = int(
            etherscan_client.get_contract_creation_info(contract_address)["blockNumber"]
        )
        from_block = max(max_loaded_block, contract_creation_block)

    # Get end block from Etherscan client
    if to_block is None:
        to_block = etherscan_client.get_latest_block()

    logger.info(
        f"🚧🚧🚧 {contract_address}, chain {chainid}, from block {from_block} to {to_block} 🚧🚧🚧"
    )
    # Process in chunks
    end_block = to_block  # Save the original end block
    for chunk_start in range(from_block, end_block, block_chunk_size):
        chunk_end = min(chunk_start + block_chunk_size - 1, end_block)
        pass  # Loading chunk

        max_retries = 2
        retries = max_retries
        while retries > 0:
            try:
                # Get row count before loading
                n_before = postgres_client.get_table_row_count(dataset_name, table_name)

                # Create source with current block range
                source = source_factory(
                    address=contract_address, from_block=chunk_start, to_block=chunk_end
                )

                # Run pipeline with resource
                postgres_connection_url = settings.postgres.get_connection_url()
                pipeline_manager.run(
                    sources={table_name: source},
                    pipeline_name=f"{dataset_name}-{table_name}-{chainid}-{contract_address}",
                    dataset_name=dataset_name,  # schema
                    destination=dlt.destinations.postgres(postgres_connection_url),
                    write_disposition=write_disposition,
                    primary_key=primary_key,
                )

                # Get row count after loading
                n_after = postgres_client.get_table_row_count(dataset_name, table_name)
                n_loaded = n_after - n_before

                # Log results and check for potential API limits
                if n_loaded >= 1000:
                    logger.warning(
                        f"Load {n_loaded} {source_factory.__name__} (>1000) from {chunk_start} to {chunk_end}"
                    )
                else:
                    logger.info(
                        f"Loaded {n_loaded} {source_factory.__name__} from {chunk_start} to {chunk_end}"
                    )
                    pass  # Chunk loaded successfully

                break  # Success, move to next chunk

            except Exception as e:
                retries -= 1
                logger.error(
                    f"Error loading: {e}. Retrying... ({retries} retries left)"
                )
                if retries > 0:
                    time.sleep(3)  # Wait before retry
                else:
                    logger.error(
                        f"Failed to load for block range {chunk_start}-{chunk_end} "
                        f"after {max_retries} retries."
                    )
    logger.info(
        f"✅✅✅ {contract_address}, chain {chainid}, from block {from_block} to {to_block} ✅✅✅"
    )


def _to_snake(name):
    """Convert camelCase to snake_case and handle spaces."""
    # First convert camelCase to snake_case
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)

    # Convert to lowercase
    name = name.lower()

    # Replace spaces with underscores and clean up multiple underscores
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"_+", "_", name)

    # Remove leading/trailing underscores
    name = name.strip("_")

    return name


def _recursive_snakify(obj):
    """Recursively convert all string values and keys in a nested structure to lowercase and camelCase to snake_case."""
    if isinstance(obj, dict):
        return {_to_snake(key): _recursive_snakify(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [_recursive_snakify(item) for item in obj]
    elif isinstance(obj, str):
        return obj.lower()
    else:
        return obj


def rewrite_json_snakecase(input_file: str, output_file: str = None):
    """
    Read a JSON file, convert all string values to lowercase recursively,
    and write the result back to a file.

    Args:
        input_file: Path to the input JSON file
        output_file: Path to the output JSON file. If None, overwrites the input file
    """
    with open(input_file, "r") as f:
        data = json.load(f)

    # Convert all string values to lowercase recursively
    snakecase_data = _recursive_snakify(data)

    # Determine output file path
    if output_file is None:
        output_file = input_file

    # Write the lowercase data back to file
    with open(output_file, "w") as f:
        json.dump(snakecase_data, f, indent=2)

    pass  # Successfully converted to snakecase


def get_all_addresses(data: dict) -> dict[str, str]:
    """Extract all address strings from the JSON data recursively with flattened keys."""
    address_map = {}

    def _check_address(obj):  # TODO: verify this is correct
        if isinstance(obj, str):
            if (
                obj.startswith("0x")
                and len(obj) == 42
                and all(c in "0123456789abcdefABCDEF" for c in obj[2:])
            ):
                return True
        return False

    def _extract_addresses(obj, path=""):
        """Recursively extract all string values that look like addresses with their paths."""
        if isinstance(obj, dict):
            for key, value in obj.items():
                current_path = f"{path}.{key}" if path else key
                _extract_addresses(value, current_path)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                current_path = f"{path}[{i}]" if path else f"[{i}]"
                _extract_addresses(item, current_path)
        elif isinstance(obj, str):
            # Check if string looks like an Ethereum address (0x followed by 40 hex chars)
            if _check_address(obj):
                address_map[path] = obj.lower()

    _extract_addresses(data)
    return address_map


def get_chainid(chain: str, chainid_data: Optional[dict] = None) -> int:
    """Get the chainid for a given chain name."""
    if chainid_data is None:
        with open("resource/chainid.json", "r") as f:
            chainid_data = json.load(f)
            pass  # Loaded chainid.json
    try:
        chainid = chainid_data[chain]
        return chainid
    except KeyError:
        raise ValueError(f"Chain {chain} not found in chainid.json")


def get_contract_creation_info(
    contract_address: str, etherscan_client: EtherscanClient
) -> dict:
    """Get the contract creation info for a given contract address."""
    return etherscan_client.get_contract_creation_info(contract_address)
