import logging
import json

from onchain_sleuth.utils.logging import setup_logging
from onchain_sleuth.utils import get_events_list
from onchain_sleuth import EtherscanClient, EtherscanSource
from onchain_sleuth.utils.postgres import (
    PostgresDestination,
    Postgres,
    Destination,
)
from onchain_sleuth.utils.postgres import DuckdbDestination
from onchain_sleuth.utils.chain import get_chainid
from scripts.helpers.dataloader import load_chunks

logger = logging.getLogger(__name__)
setup_logging(log_filename=None, level="INFO")


def backfill_raw_data_from_etherscan(address, chain, destination: Destination):
    chainid = get_chainid(chain)
    etherscan_client = EtherscanClient(chainid=chainid)
    source = EtherscanSource(client=etherscan_client)

    load_chunks(
        dataset_name="etherscan_raw",  # schema
        table_name="txns",
        contract_address=address,
        etherscan_client=etherscan_client,
        destination=destination,
        source_factory=source.transactions,
    )

    load_chunks(
        dataset_name="etherscan_raw",  # schema
        table_name="logs",
        contract_address=address,
        etherscan_client=etherscan_client,
        destination=destination,
        source_factory=source.logs,
    )


def load_and_save_abi(address, chain="ethereum"):
    chainid = get_chainid(chain)
    etherscan_client = EtherscanClient(chainid=chainid)
    etherscan_client.get_contract_abi(address)
    # destination = PostgresDestination(PostgresSettings.from_env())
    # duckdb_destination = DuckdbDestination(database_path="data/duckdb/data.duckdb")


if __name__ == "__main__":
    # destination = PostgresDestination(Postgres.from_env())
    destination = DuckdbDestination(
        database_path="/home/gnart/dev/data-sleuth/data/duckdb/open.duckdb"
    )
    with open("scripts/open/contract_addresses.json", "r") as f:
        contract_addresses = json.load(f)
    for address in contract_addresses.values():
        backfill_raw_data_from_etherscan(
            address=address, chain="ethereum", destination=destination
        )
        # load_and_save_abi(address=address, chain="ethereum")
        # get_events_list(address=address)
