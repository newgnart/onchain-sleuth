import logging
import json


from onchain_sleuth import EtherscanClient
from onchain_sleuth.utils.database import PostgresClient
from onchain_sleuth.utils.logging import setup_logging


from scripts.helpers.backfill import (
    etherscan_to_parquet,
    load_parquet_to_postgres,
    retry_failed_blocks,
)

logger = logging.getLogger(__name__)
setup_logging(log_filename="stablecoins_backfill.log", level="INFO")


if __name__ == "__main__":
    # get contract addresses
    with open("scripts/stablecoins/contracts.json", "r") as f:
        contracts = json.load(f)
    # # contract_addresses = {
    # #     "dai": {
    # #         "address": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
    # #         "chain": "ethereum",
    # #         "protocol": "sky",
    # #     }
    # # }

    # # # Extract
    # for name, detail in contracts.items():
    #     logger.info(
    #         f"📥 Extracting data for {name} ({detail['protocol']}): {detail['address']}"
    #     )
    #     try:
    #         # Get ABI first
    #         etherscan_client = EtherscanClient(chain=detail["chain"])
    #         etherscan_client.get_contract_abi(detail["address"])

    #         # Extract to Parquet
    #         parquet_path = etherscan_to_parquet(
    #             address=detail["address"],
    #             chain=detail["chain"],
    #             block_chunk_size=1000,
    #         )
    #         logger.info(f"✅ Extracted to: {parquet_path}")

    #     except Exception as e:
    #         logger.error(f"❌ Failed to extract {name}: {e}")
    # retry_failed_blocks(table_name="logs")
    # retry_failed_blocks(table_name="transactions")

    postgres_client = PostgresClient.from_env()

    for name, detail in contracts.items():
        logger.info(
            f"📥 Loading data for {name} ({detail['protocol']}): {detail['address']}"
        )
        for table_name in ["logs", "transactions"]:
            parquet_file_path = f"/home/gnart/dev/onchain-sleuth/data/etherscan_raw/ethereum_{detail['address'].lower()}/{table_name}.parquet"
            load_parquet_to_postgres(
                parquet_file_path=parquet_file_path,
                postgres_client=postgres_client,
                table_name=table_name,
            )
