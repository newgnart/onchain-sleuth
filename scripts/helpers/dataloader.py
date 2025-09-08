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
import random
import os

from typing import Optional, Any, List

import dlt
from onchain_sleuth import EtherscanClient, PipelineManager, EtherscanSource
from onchain_sleuth.utils.postgres import Destination

# Configure logging
logger = logging.getLogger(__name__)


def load_chunks(
    dataset_name: str,  # schema
    table_name: str,
    contract_address: str,
    etherscan_client: EtherscanClient,
    destination: Destination,
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
    block_column_name = "block_number"
    # if source_factory.__name__ == "logs":
    #     address_column_name = "address"
    # elif source_factory.__name__ == "transactions":
    #     address_column_name = '"to"'
    # else:
    #     raise ValueError(f"Unknown source factory: {source_factory}")

    # Get end block from Etherscan client
    if to_block is None:
        to_block = etherscan_client.get_latest_block()
    # Determine start block using PostgresDestination
    if from_block is None:
        max_loaded_block = destination.get_max_loaded_block(
            table_schema=dataset_name,
            table_name=table_name,
            chainid=chainid,
            address=contract_address,
            # address_column_name=address_column_name,
            block_column_name=block_column_name,
        )
        contract_creation_block = etherscan_client.get_contract_creation_block_number(
            contract_address
        )
        if max_loaded_block > contract_creation_block:
            from_block = max_loaded_block
            logger.info(
                f"🚧🚧🚧 {contract_address}, {source_factory.__name__}, chain {chainid}, continue from {from_block} to {to_block}"
            )
        else:
            from_block = contract_creation_block
            logger.info(
                f"🚧🚧🚧 {contract_address}, {source_factory.__name__}, chain {chainid}, start from creation block {contract_creation_block} to {to_block}"
            )

    # Process in chunks
    end_block = to_block  # Save the original end block
    error_block_ranges = []
    for chunk_start in range(from_block, end_block, block_chunk_size):
        chunk_end = min(chunk_start + block_chunk_size - 1, end_block)
        try:
            # Create source with current block range
            source = source_factory(
                address=contract_address, from_block=chunk_start, to_block=chunk_end
            )

            # Run pipeline with resource
            pipeline_manager.run(
                sources={table_name: source},
                pipeline_name=f"{dataset_name}-{table_name}-{chainid}-{contract_address}",
                dataset_name=dataset_name,  # schema
                destination=destination.get_dlt_destination(),
                write_disposition=write_disposition,
                primary_key=primary_key,
            )

            # Get row count after loading
            query = f"SELECT COUNT(*) FROM {dataset_name}.{table_name} WHERE chainid = {chainid} AND {address_column_name} = '{contract_address}'"
            result = destination.fetch_one(query)
            n_loaded = result[0] if result and result[0] is not None else 0

            # Only log progress 5% of the time to avoid excessive logging
            # This provides periodic status updates while keeping the log file manageable
            if random.random() < 0.05:
                logger.info(
                    f"Loaded {n_loaded} {source_factory.__name__}, up to {chunk_end}"
                )

        except Exception as e:
            logger.error(
                f"Failed to load {source_factory.__name__} {chunk_start} to {chunk_end} with error {e}"
            )
            error_block_ranges.append([chunk_start, chunk_end])

    if error_block_ranges:
        error_file = f"logs/load_{source_factory.__name__}_error.json"
        if not os.path.exists(error_file):
            with open(error_file, "w") as f:
                json.dump({}, f, indent=4)
        with open(error_file, "a") as f:
            json.dump(
                {f"{contract_address}-{chainid}": error_block_ranges},
                f,
                indent=4,
                ensure_ascii=False,
            )

    logger.info(
        f"✅✅✅ {contract_address}, {source_factory.__name__}, chain {chainid}, {from_block} to {to_block}"
    )


# def _to_snake(name):
#     """Convert camelCase to snake_case and handle spaces."""
#     # First convert camelCase to snake_case
#     name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
#     name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)

#     # Convert to lowercase
#     name = name.lower()

#     # Replace spaces with underscores and clean up multiple underscores
#     name = re.sub(r"\s+", "_", name)
#     name = re.sub(r"_+", "_", name)

#     # Remove leading/trailing underscores
#     name = name.strip("_")

#     return name


# def _recursive_snakify(obj):
#     """Recursively convert all string values and keys in a nested structure to lowercase and camelCase to snake_case."""
#     if isinstance(obj, dict):
#         return {_to_snake(key): _recursive_snakify(value) for key, value in obj.items()}
#     elif isinstance(obj, list):
#         return [_recursive_snakify(item) for item in obj]
#     elif isinstance(obj, str):
#         return obj.lower()
#     else:
#         return obj


# def rewrite_json_snakecase(input_file: str, output_file: str = None):
#     """
#     Read a JSON file, convert all string values to lowercase recursively,
#     and write the result back to a file.

#     Args:
#         input_file: Path to the input JSON file
#         output_file: Path to the output JSON file. If None, overwrites the input file
#     """
#     with open(input_file, "r") as f:
#         data = json.load(f)

#     # Convert all string values to lowercase recursively
#     snakecase_data = _recursive_snakify(data)

#     # Determine output file path
#     if output_file is None:
#         output_file = input_file

#     # Write the lowercase data back to file
#     with open(output_file, "w") as f:
#         json.dump(snakecase_data, f, indent=2)

#     pass  # Successfully converted to snakecase


# def get_all_addresses(data: dict) -> dict[str, str]:
#     """Extract all address strings from the JSON data recursively with flattened keys."""
#     address_map = {}

#     def _check_address(obj):  # TODO: verify this is correct
#         if isinstance(obj, str):
#             if (
#                 obj.startswith("0x")
#                 and len(obj) == 42
#                 and all(c in "0123456789abcdefABCDEF" for c in obj[2:])
#             ):
#                 return True
#         return False

#     def _extract_addresses(obj, path=""):
#         """Recursively extract all string values that look like addresses with their paths."""
#         if isinstance(obj, dict):
#             for key, value in obj.items():
#                 current_path = f"{path}.{key}" if path else key
#                 _extract_addresses(value, current_path)
#         elif isinstance(obj, list):
#             for i, item in enumerate(obj):
#                 current_path = f"{path}[{i}]" if path else f"[{i}]"
#                 _extract_addresses(item, current_path)
#         elif isinstance(obj, str):
#             # Check if string looks like an Ethereum address (0x followed by 40 hex chars)
#             if _check_address(obj):
#                 address_map[path] = obj.lower()

#     _extract_addresses(data)
#     return address_map


# def get_chainid(chain: str, chainid_data: Optional[dict] = None) -> int:
#     """Get the chainid for a given chain name."""
#     if chainid_data is None:
#         with open("resource/chainid.json", "r") as f:
#             chainid_data = json.load(f)
#             pass  # Loaded chainid.json
#     try:
#         chainid = chainid_data[chain]
#         return chainid
#     except KeyError:
#         raise ValueError(f"Chain {chain} not found in chainid.json")
