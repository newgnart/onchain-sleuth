import json
import logging
from onchaindata.utils.etherscan_extract import etherscan_to_parquet

logging.basicConfig(level=logging.DEBUG)

if __name__ == "__main__":
    contracts = json.load(open("scripts/extraction/contracts.json"))

    for name, details in contracts.items():
        etherscan_to_parquet(
            address=details["address"],
            chain=details["chain"],
            table="logs",
        )
        etherscan_to_parquet(
            address=details["address"],
            chain=details["chain"],
            table="transactions",
        )
