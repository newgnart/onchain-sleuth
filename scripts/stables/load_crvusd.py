"""
CLI script for Etherscan log loading with PostgreSQL integration using modern EVM Sleuth architecture.
"""

import json
import sys
import os

from evm_sleuth import settings, EtherscanClient, EtherscanSource
from evm_sleuth.utils.postgres import PostgresClient
from evm_sleuth.utils.logging import setup_logging

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from helpers import load_chunks, get_all_addresses, get_chainid, rewrite_json_snakecase

# Configure logging
setup_logging("logs/load_crvusd.log")


def snakify():
    rewrite_json_snakecase("resource/chainid.json", "resource/chainid.json")
    rewrite_json_snakecase("resource/crvusd.json", "resource/crvusd.json")


def main():
    # get all addresses
    chain = "ethereum"
    with open("resource/address/crvusd.json", "r") as f:
        crvusd_data = json.load(f)

    address_map = get_all_addresses(crvusd_data[chain])  # {path: address}
    all_addresses = [
        address
        for contract_name, address in address_map.items()
        if "collateral_token"
        not in contract_name  # exclude all the collateral_token contracts
    ]
    chainid = get_chainid(chain)

    # load data for all addresses
    etherscan_client = EtherscanClient(chainid=chainid)
    source = EtherscanSource(client=etherscan_client)
    postgres_client = PostgresClient(settings.postgres)
    for address in all_addresses:
        if address == "0xf939e0a03fb07f59a73314e73794be0e57ac1b4e":
            block_chunk_size = 10_000
        else:
            block_chunk_size = 100_000
        load_chunks(
            dataset_name="etherscan_raw",
            table_name="logs",
            contract_address=address,
            etherscan_client=etherscan_client,
            postgres_client=postgres_client,
            source_factory=source.logs,
            from_block=17907952,
            to_block=17917951,
            block_chunk_size=block_chunk_size,
        )
        # load_chunks(
        #     dataset_name="etherscan_raw",
        #     table_name="transactions",
        #     contract_address=address,
        #     etherscan_client=etherscan_client,
        #     postgres_client=postgres_client,
        #     source_factory=source.transactions,
        #     block_chunk_size=block_chunk_size,
        # )


def adhoc():

    # get all addresses
    chain = "ethereum"
    chainid = get_chainid(chain)

    # load data for all addresses
    etherscan_client = EtherscanClient(chainid=chainid)
    source = EtherscanSource(client=etherscan_client)
    postgres_client = PostgresClient(settings.postgres)
    address = "0xf939e0a03fb07f59a73314e73794be0e57ac1b4e"

    load_chunks(
        dataset_name="etherscan_raw",
        table_name="logs",
        contract_address=address,
        etherscan_client=etherscan_client,
        postgres_client=postgres_client,
        source_factory=source.logs,
        from_block=22207515,
        to_block=22237514,
        block_chunk_size=1000,
    )


if __name__ == "__main__":
    adhoc()
    # main()


# """ architecture
# -- curve.
# -- staging.decoded_logs
# -- staging.evt_erc20_transfers
# -- staging.evt_erc20_approve
# -- staging.evt_erc20_approve

# -- staging.decoded_transactions
# -- staging.txn_erc20_transfers


# -- crvusd.erc20_transfers
# -- crvusd.erc20_mint

# """
