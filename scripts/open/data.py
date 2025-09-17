import logging
import json
import argparse

from onchain_sleuth import EtherscanClient
from onchain_sleuth.utils.logging import setup_logging
from onchain_sleuth.utils.database import PostgresClient
from onchain_sleuth.dataloader.batch_loader import BatchLoader

from scripts.helpers.backfill import (
    backfill_from_etherscan_to_parquet,
    load_parquet_to_postgres,
    backfill_from_etherscan_to_postgres
)

logger = logging.getLogger(__name__)
setup_logging(log_filename=None, level="INFO")


def extract_only_workflow(contract_addresses: dict, data_dir: str = "data"):
    """Extract historical data to Parquet files only."""
    logger.info("🔄 Starting EXTRACTION-ONLY workflow")

    for name, config in contract_addresses.items():
        logger.info(f"📥 Extracting data for {name} ({config['protocol']}): {config['address']}")

        try:
            # Get ABI first
            etherscan_client = EtherscanClient(chain=config["chain"])
            etherscan_client.get_contract_abi(config["address"])

            # Extract to Parquet
            parquet_path = backfill_from_etherscan_to_parquet(
                address=config["address"],
                chain=config["chain"],
                data_dir=data_dir
            )
            logger.info(f"✅ Extracted to: {parquet_path}")

        except Exception as e:
            logger.error(f"❌ Failed to extract {name}: {e}")


def load_only_workflow(data_dir: str = "data"):
    """Load existing Parquet files to PostgreSQL."""
    logger.info("🔄 Starting LOAD-ONLY workflow")

    postgres_client = PostgresClient.from_env()
    batch_loader = BatchLoader(data_dir=data_dir)

    # Get available protocols
    available_protocols = batch_loader.list_available_protocols()
    logger.info(f"📂 Found protocols: {available_protocols}")

    for protocol in available_protocols:
        try:
            logger.info(f"📤 Loading {protocol} to PostgreSQL...")

            result = load_parquet_to_postgres(
                protocol=protocol,
                postgres_client=postgres_client,
                data_dir=data_dir
            )
            logger.info(f"✅ Loaded {protocol} successfully")

        except Exception as e:
            logger.error(f"❌ Failed to load {protocol}: {e}")


def full_workflow(contract_addresses: dict, data_dir: str = "data"):
    """Complete workflow: Extract to Parquet then load to PostgreSQL."""
    logger.info("🔄 Starting FULL workflow (Extract + Load)")

    postgres_client = PostgresClient.from_env()

    for name, config in contract_addresses.items():
        logger.info(f"🔄 Processing {name} ({config['protocol']}): {config['address']}")

        try:
            # Get ABI first
            etherscan_client = EtherscanClient(chain=config["chain"])
            etherscan_client.get_contract_abi(config["address"])

            # Run complete workflow
            backfill_from_etherscan_to_postgres(
                address=config["address"],
                chain=config["chain"],
                postgres_client=postgres_client,
                data_dir=data_dir
            )

        except Exception as e:
            logger.error(f"❌ Failed to process {name}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process blockchain contract data")
    parser.add_argument("--mode", choices=["extract", "load", "full"], default="full",
                       help="Processing mode: extract (API->Parquet), load (Parquet->PostgreSQL), or full (both)")
    parser.add_argument("--data-dir", default="data", help="Data directory for Parquet files")

    args = parser.parse_args()

    # Load contract addresses
    with open("scripts/open/contract_addresses.json", "r") as f:
        contract_addresses = json.load(f)

    logger.info(f"📋 Loaded {len(contract_addresses)} contracts")

    if args.mode == "extract":
        extract_only_workflow(contract_addresses, args.data_dir)
    elif args.mode == "load":
        load_only_workflow(args.data_dir)
    elif args.mode == "full":
        full_workflow(contract_addresses, args.data_dir)

    logger.info("🎉 Workflow completed!")
