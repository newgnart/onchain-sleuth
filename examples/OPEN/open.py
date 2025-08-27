import logging


from evm_sleuth.utils.logging import setup_logging
from evm_sleuth.utils import events_list
from evm_sleuth import (
    EtherscanClient,
    EtherscanSource,
    PostgresClient,
    settings,
)
from ..helpers.dataloader import load_chunks, get_chainid

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
    address = "0x323c03c48660fE31186fa82c289b0766d331Ce21"
    chain = "ethereum"

    load_and_save_abi(address=address, chain=chain)
    events_list(address=address)
