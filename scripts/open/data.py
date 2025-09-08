import logging
import json

from onchain_sleuth.utils.logging import setup_logging
from onchain_sleuth.utils import events_list
from onchain_sleuth import (
    EtherscanClient,
    EtherscanSource,
    PostgresClient,
    settings,
)
from scripts.helpers.dataloader import load_chunks, get_chainid

logger = logging.getLogger(__name__)
setup_logging(log_filename=None, level="INFO")


def load_raw_data_from_etherscan_to_postgres(address, chain="ethereum"):
    chainid = get_chainid(chain)
    etherscan_client = EtherscanClient(chainid=chainid)
    source = EtherscanSource(client=etherscan_client)
    postgres_client = PostgresClient(settings.postgres)

    load_chunks(
        dataset_name="etherscan_raw",  # schema
        table_name="txns",
        contract_address=address,
        etherscan_client=etherscan_client,
        postgres_client=postgres_client,
        source_factory=source.transactions,
    )

    load_chunks(
        dataset_name="etherscan_raw",  # schema
        table_name="logs",
        contract_address=address,
        etherscan_client=etherscan_client,
        postgres_client=postgres_client,
        source_factory=source.logs,
    )


def load_and_save_abi(address, chain="ethereum"):
    chainid = get_chainid(chain)
    etherscan_client = EtherscanClient(chainid=chainid)
    etherscan_client.get_contract_abi(address)


if __name__ == "__main__":
    with open("scripts/open/contract_addresses.json", "r") as f:
        contract_addresses = json.load(f)
    for address in contract_addresses.values():
        # load_raw_data_from_etherscan_to_postgres(address=address, chain="ethereum")
        # load_and_save_abi(address=address, chain="ethereum")
        events_list(address=address)
