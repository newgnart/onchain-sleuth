#!/usr/bin/env python3
"""
Example usage of the refactored evm_sleuth architecture.
"""

import logging
from dotenv import load_dotenv

from evm_sleuth import (
    ClientFactory,
    DLTResourceFactory,
    PipelineFactory,
    settings
)
from evm_sleuth.dataloader import DataLoaderTemplate

logger = logging.getLogger(__name__)
load_dotenv()


def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )


def load_logs(table_schema: str, table_name: str, contract_address: str, chainid: int):
    """Load blockchain logs using the new architecture."""

    # Create pipeline manager and Etherscan resource using factories
    pipeline_manager = PipelineFactory.create_pipeline_manager(use_local_db=True)
    etherscan_resource = DLTResourceFactory.create_etherscan_resource(chainid=chainid)

    # Create data loader template for standardized loading
    loader = DataLoaderTemplate(pipeline_manager)

    # Load logs using the template with incremental strategy
    result = loader.load_incremental_data(
        resource_func=etherscan_resource.logs,
        pipeline_name="ethena_etherscan",
        dataset_name=table_schema,
        table_name=table_name,
        resource_args=(contract_address,),
        resource_kwargs={
            "from_block": 0,
            "to_block": "latest",
            "offset": 1000
        },
        primary_key=["transaction_hash", "log_index"]
    )

    logger.info(f"Completed loading logs for {contract_address}")
    return result


def load_defillama_data():
    """Load DeFiLlama data using the new architecture."""

    # Create DeFiLlama resource and pipeline manager
    defillama_resource = DLTResourceFactory.create_defillama_resource()
    pipeline_manager = PipelineFactory.create_pipeline_manager(use_local_db=True)
    loader = DataLoaderTemplate(pipeline_manager)

    # 1. Load stables metadata
    logger.info("Loading stables metadata...")
    loader.load_data(
        resource_func=defillama_resource.stables_metadata,
        pipeline_name="defillama",
        dataset_name="llama",
        table_name="stables_metadata",
        write_disposition="replace"
    )

    # 2. Load stable circulating data
    logger.info("Loading stable circulating data...")

    # Historical data (first time)
    loader.load_incremental_data(
        resource_func=defillama_resource.stable_data,
        pipeline_name="defillama",
        dataset_name="llama",
        table_name="circulating",
        resource_args=(146,),  # Stablecoin ID
        resource_kwargs={
            "get_response": "chainBalances",
            "include_metadata": True
        },
        primary_key=["time", "id", "chain"]
    )

    # Current data (ongoing updates)
    loader.load_incremental_data(
        resource_func=defillama_resource.stable_data,
        pipeline_name="defillama",
        dataset_name="llama",
        table_name="circulating",
        resource_args=(221,),  # Another stablecoin ID
        resource_kwargs={
            "get_response": "currentChainBalances",
            "include_metadata": False
        },
        primary_key=["time", "id", "chain"]
    )

    # 3. Load token price data
    logger.info("Loading token price data...")
    network = "ethereum"
    contract_address = "0x57e114B691Db790C35207b2e685D4A43181e6061"

    # First time load with default parameters
    loader.load_incremental_data(
        resource_func=defillama_resource.token_price,
        pipeline_name="defillama",
        dataset_name="llama",
        table_name="token_price",
        resource_args=(network, contract_address),
        resource_kwargs={
            "params": {"span": 1000, "period": "1d"}
        },
        primary_key=["time", "network", "contract_address"]
    )

    # Subsequent loads with shorter timespan
    loader.load_incremental_data(
        resource_func=defillama_resource.token_price,
        pipeline_name="defillama",
        dataset_name="llama",
        table_name="token_price",
        resource_args=(network, contract_address),
        resource_kwargs={
            "params": {"span": 10, "period": "1d"}
        },
        primary_key=["time", "network", "contract_address"]
    )

    # 4. Load protocol revenue data
    logger.info("Loading protocol revenue data...")
    loader.load_incremental_data(
        resource_func=defillama_resource.protocol_revenue,
        pipeline_name="defillama",
        dataset_name="llama",
        table_name="protocol_revenue",
        resource_args=("ethena",),
        resource_kwargs={
            "data_selector": "totalDataChartBreakdown",
            "include_metadata": False
        },
        primary_key=["time", "chain", "protocol", "sub_protocol"]
    )

    # 5. Load all yield pools
    logger.info("Loading all yield pools...")
    loader.load_data(
        resource_func=defillama_resource.all_yield_pools,
        pipeline_name="defillama",
        dataset_name="llama",
        table_name="all_yield_pools",
        write_disposition="replace"
    )

    # 6. Load specific yield pool data  
    logger.info("Loading specific yield pool data...")
    loader.load_incremental_data(
        resource_func=defillama_resource.yield_pool,
        pipeline_name="defillama",
        dataset_name="llama",
        table_name="yield_pools",
        resource_args=("13392973-be6e-4b2f-bce9-4f7dd53d1c3a", "sdai"),
        primary_key=["time", "pool_id"]
    )


def main():
    """Main execution function."""
    setup_logging()

    logger.info("Starting data loading with new architecture...")

    # Load DeFiLlama data
    load_defillama_data()

    # Load blockchain logs
    load_logs(
        table_schema="open_raw",
        table_name="open_contract_logs",
        chainid=1,
        contract_address="0x323c03c48660fE31186fa82c289b0766d331Ce21".lower()
    )

    logger.info("Data loading completed successfully!")


if __name__ == "__main__":
    main()