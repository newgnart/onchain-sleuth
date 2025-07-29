#!/usr/bin/env python3
"""
Example usage of the evm_sleuth
"""


from evm_sleuth import APIClientFactory
from evm_sleuth.utils.logging import setup_logging
import logging

logger = logging.getLogger(__name__)


def defillama_api_client():
    """Test DeFiLlama client by pulling data and saving to CSV files."""
    import pandas as pd
    import os

    # Create DeFiLlama client using factory by name
    defillama_client = APIClientFactory.create_client("defillama")
    logger.info(f"Created DeFiLlama client: {type(defillama_client).__name__}")

    # Create output directory
    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Test 1: Get protocol data (using Uniswap as example)
        logger.info("Fetching Uniswap protocol data...")
        protocol_data = defillama_client.get_protocol_data("uniswap")

        # Convert protocol data to DataFrame and save
        protocol_df = pd.json_normalize(protocol_data)
        protocol_csv_path = os.path.join(output_dir, "uniswap_protocol_data.csv")
        protocol_df.to_csv(protocol_csv_path, index=False)
        logger.info(f"Saved protocol data to {protocol_csv_path}")

        # Test 2: Get stablecoin data (using USDT ID as example)
        logger.info("Fetching USDT stablecoin data...")
        stablecoin_data = defillama_client.get_stablecoin_data(
            1
        )  # USDT typically has ID 1

        # Convert stablecoin data to DataFrame and save
        stablecoin_df = pd.json_normalize(stablecoin_data)
        stablecoin_csv_path = os.path.join(output_dir, "usdt_stablecoin_data.csv")
        stablecoin_df.to_csv(stablecoin_csv_path, index=False)
        logger.info(f"Saved stablecoin data to {stablecoin_csv_path}")

        # Test 3: Get token price data (using USDC on Ethereum as example)
        logger.info("Fetching USDC token price data...")
        token_price_data = defillama_client.get_token_price(
            network="ethereum",
            contract_address="0xA0b86a33E6441C8C",  # USDC contract address
        )

        # Extract price data if available
        if "coins" in token_price_data:
            coins_data = token_price_data["coins"]
            if coins_data:
                # Get the first coin's price data
                coin_key = list(coins_data.keys())[0]
                coin_data = coins_data[coin_key]

                if "prices" in coin_data:
                    prices_df = pd.DataFrame(
                        coin_data["prices"], columns=["timestamp", "price"]
                    )
                    prices_df["symbol"] = coin_data.get("symbol", "UNKNOWN")
                    prices_df["network"] = "ethereum"

                    price_csv_path = os.path.join(output_dir, "usdc_price_data.csv")
                    prices_df.to_csv(price_csv_path, index=False)
                    logger.info(f"Saved token price data to {price_csv_path}")

        logger.info(
            f"Successfully tested DeFiLlama client. Data saved to {output_dir}/ directory"
        )
        logger.info(f"Files created: {os.listdir(output_dir)}")

    except Exception as e:
        logger.error(f"Error testing DeFiLlama client: {e}")
        raise


def defillama_loader():
    """Load DeFiLlama data using the new registry-based architecture."""
    import dlt
    from evm_sleuth.config.settings import settings

    # Create DeFiLlama client directly
    defillama_client = APIClientFactory.create_client("defillama")
    logger.info(f"Created DeFiLlama client: {type(defillama_client).__name__}")

    # Create DLT pipeline directly
    pipeline = dlt.pipeline(
        pipeline_name="defillama",
        destination=dlt.destinations.postgres(
            f"postgresql://{settings.local_db.user}:{settings.local_db.password}@{settings.local_db.host}:{settings.local_db.port}/{settings.local_db.database}"
        ),
        dataset_name="llama",
    )

    # 1. Load stables metadata using standalone DLT resource function
    logger.info("Loading stables metadata...")
    
    from evm_sleuth.datasource.defillama import stables_metadata
    
    # Run the pipeline with the standalone resource function
    result = pipeline.run(stables_metadata(defillama_client))

    logger.info(f"Pipeline completed successfully: {result}")


def main():
    """Main execution function."""
    setup_logging()

    # api client
    # defillama_api_client()
    defillama_loader()


if __name__ == "__main__":
    main()
