import logging
import json

from onchain_sleuth import EtherscanClient
from onchain_sleuth.utils.logging import setup_logging
from onchain_sleuth.utils.database import PostgresClient

from scripts.helpers.dataloader import backfill_from_etherscan_to_postgres

logger = logging.getLogger(__name__)
setup_logging(log_filename=None, level="INFO")


if __name__ == "__main__":
    postgres_client = PostgresClient.from_env()
    with open("scripts/open/contract_addresses.json", "r") as f:
        contract_addresses = json.load(f)

    for value in contract_addresses.values():
        etherscan_client = EtherscanClient(chain=value["chain"])
        etherscan_client.get_contract_abi(value["address"])
        backfill_from_etherscan_to_postgres(
            address=value["address"],
            chain=value["chain"],
            postgres_client=postgres_client,
        )
