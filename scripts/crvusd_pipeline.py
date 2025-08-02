"""
CLI script for Etherscan log loading with PostgreSQL integration using modern EVM Sleuth architecture.
"""

import json

import dlt
from evm_sleuth import settings, EtherscanClient, EtherscanSource
from evm_sleuth.utils.postgres import PostgresClient
from evm_sleuth.utils.logging import setup_logging
from helper import rewrite_json_snakecase, load_chunks, get_all_addresses, get_chainid

# Configure logging
setup_logging("logs/crvusd.log")


def snakify():
    rewrite_json_snakecase("resource/chainid.json", "resource/chainid.json")
    rewrite_json_snakecase("resource/crvusd.json", "resource/crvusd.json")


def main():
    # get all addresses
    chain = "ethereum"
    with open("resource/crvusd.json", "r") as f:
        crvusd_data = json.load(f)

    address_map = get_all_addresses(crvusd_data[chain])  # {path: address}
    all_addresses = [
        address
        for contract_name, address in address_map.items()
        if "collateral_token"
        not in contract_name  # exclude the collateral_token contracts
    ]
    chainid = get_chainid(chain)

    # load data for all addresses
    etherscan_client = EtherscanClient(chainid=chainid)
    source = EtherscanSource(client=etherscan_client)
    postgres_client = PostgresClient(settings.postgres)
    for address in all_addresses:
        if address == "0xf939e0a03fb07f59a73314e73794be0e57ac1b4e":
            continue
        load_chunks(
            dataset_name="etherscan_raw",
            table_name="logs",
            contract_address=address,
            etherscan_client=etherscan_client,
            postgres_client=postgres_client,
            source_factory=source.logs,
            block_chunk_size=100_000,
            write_disposition="append",
            # primary_key=["block_number", "transaction_index", "log_index"],
        )


def adhoc():
    # get all addresses
    address = "0xf939e0a03fb07f59a73314e73794be0e57ac1b4e"
    # get chainid
    chainid = get_chainid("ethereum")

    etherscan_client = EtherscanClient(chainid=chainid)
    source = EtherscanSource(client=etherscan_client)
    postgres_client = PostgresClient(settings.postgres)
    load_chunks(
        dataset_name="etherscan_raw",
        table_name="logs",
        contract_address=address,
        etherscan_client=etherscan_client,
        postgres_client=postgres_client,
        source_factory=source.logs,
        block_chunk_size=5_000,
        # write_disposition="append",
        # primary_key=["block_number", "transaction_index", "log_index"],
    )


if __name__ == "__main__":
    # adhoc()\
    main()
