import logging
from dataclasses import dataclass, field
from typing import Optional, List, Callable
import dlt
from stables.config import PostgresConfig

from stables.data.source.defillama import (
    token_price,
    protocol_revenue,
    stable_data,
    all_yield_pools,
    yield_pool,
    stables_metadata,
)

logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    pipeline_name: str = "defillama"
    dataset_name: str = "llama"


@dataclass
class LoadConfig:
    """Configuration for a DLT load operation."""

    resource_func: Callable
    resource_args: tuple = field(default_factory=tuple)
    resource_kwargs: dict = field(default_factory=dict)
    table_name: str = ""
    write_disposition: str = "replace"
    primary_key: Optional[List[str]] = None
    pipeline_config: Optional[PipelineConfig] = None


def _create_pipeline(pg_config: PostgresConfig, config: PipelineConfig) -> dlt.Pipeline:
    """Create a DLT pipeline with PostgreSQL destination."""
    try:
        destination = dlt.destinations.postgres(
            f"postgresql://{pg_config.user}:{pg_config.password}@{pg_config.host}:{pg_config.port}/{pg_config.database}"
        )

        # Create pipeline with schema settings for nullable columns
        pipeline = dlt.pipeline(
            pipeline_name=config.pipeline_name,
            destination=destination,
            dataset_name=config.dataset_name,
        )

        return pipeline
    except Exception as e:
        logger.error(f"Failed to create pipeline: {e}")
        raise


def _run_load_pipeline(pg_config: PostgresConfig, load_config: LoadConfig) -> None:
    """Generic function to run a DLT load pipeline."""
    try:
        # Use provided pipeline config or create default
        pipeline_config = load_config.pipeline_config or PipelineConfig()

        # Create pipeline with nullable columns by default
        pipeline = _create_pipeline(pg_config, pipeline_config)

        # Create resource with provided arguments
        resource = load_config.resource_func(
            *load_config.resource_args, **load_config.resource_kwargs
        )

        # Prepare run kwargs
        run_kwargs = {
            "table_name": load_config.table_name,
            "write_disposition": load_config.write_disposition,
        }

        # Add primary key if specified
        if load_config.primary_key:
            run_kwargs["primary_key"] = load_config.primary_key

        # Run pipeline
        pipeline.run(resource, **run_kwargs)

        logger.info(f"Successfully loaded data to {load_config.table_name}")

    except Exception as e:
        logger.error(f"Failed to load data to {load_config.table_name}: {e}")
        raise


def create_default_pipeline_config(
    pipeline_name: str = "defillama", dataset_name: str = "llama"
) -> PipelineConfig:
    """Create a default pipeline configuration."""
    return PipelineConfig(pipeline_name=pipeline_name, dataset_name=dataset_name)


def load_stables_metadata(
    pg_config: PostgresConfig,
    pipeline_name: str = "defillama",
    dataset_name: str = "llama",
    table_name: str = "stables_metadata",
):
    """Load stablecoins metadata from DeFiLlama."""
    load_config = LoadConfig(
        resource_func=stables_metadata,
        table_name=table_name,
        write_disposition="replace",
        pipeline_config=create_default_pipeline_config(pipeline_name, dataset_name),
    )
    _run_load_pipeline(pg_config, load_config)


def load_stable_circulating(
    id: int,
    pg_config: PostgresConfig,
    pipeline_name: str = "defillama",
    dataset_name: str = "llama",
    table_name: str = "circulating",
    get_response: str = "currentChainBalances",
):
    """Load stablecoin circulating supply data by coin ID."""
    load_config = LoadConfig(
        resource_func=stable_data,
        resource_args=(id,),
        resource_kwargs={"get_response": get_response},
        table_name=table_name,
        write_disposition="merge",
        primary_key=["time", "id", "chain"],
        pipeline_config=create_default_pipeline_config(pipeline_name, dataset_name),
    )
    _run_load_pipeline(pg_config, load_config)


def load_stable_data(
    id: int,
    pg_config: PostgresConfig,
    pipeline_name: str = "defillama",
    dataset_name: str = "llama",
    table_name: str = "circulating",
    include_metadata: bool = True,
):
    """Load stablecoin circulating supply data by coin ID."""
    load_config = LoadConfig(
        resource_func=stable_data,
        resource_args=(id,),
        resource_kwargs={"include_metadata": include_metadata},
        table_name=table_name,
        write_disposition="merge",
        primary_key=["time", "id", "chain"],
        pipeline_config=create_default_pipeline_config(pipeline_name, dataset_name),
    )
    _run_load_pipeline(pg_config, load_config)


def load_token_price(
    network: str,
    token_address: str,
    pg_config: PostgresConfig,
    pipeline_name: str = "defillama",
    dataset_name: str = "llama",
    table_name: str = "token_price",
    params=None,
):
    """Load token price data for a specific network and contract address."""
    load_config = LoadConfig(
        resource_func=token_price,
        resource_args=(network, token_address, params),
        table_name=table_name,
        write_disposition="merge",
        primary_key=["time", "network", "contract_address"],
        pipeline_config=create_default_pipeline_config(pipeline_name, dataset_name),
    )
    _run_load_pipeline(pg_config, load_config)


def load_protocol_revenue(
    protocol: str,
    pg_config: PostgresConfig,
    pipeline_name: str = "defillama",
    dataset_name: str = "llama",
    table_name: str = "protocol_revenue",
    data_selector: str = "totalDataChartBreakdown",
    include_metadata: bool = False,
):
    """Load protocol revenue data from DeFiLlama."""
    load_config = LoadConfig(
        resource_func=protocol_revenue,
        resource_args=(protocol,),
        resource_kwargs={
            "data_selector": data_selector,
            "include_metadata": include_metadata,
        },
        table_name=table_name,
        write_disposition="merge",
        primary_key=["time", "chain", "protocol", "sub_protocol"],
        pipeline_config=create_default_pipeline_config(pipeline_name, dataset_name),
    )
    _run_load_pipeline(pg_config, load_config)


def load_all_yield_pools(
    pg_config: PostgresConfig,
    pipeline_name: str = "defillama",
    dataset_name: str = "llama",
    table_name: str = "all_yield_pools",
):
    """Load all yield pools data from DeFiLlama."""
    load_config = LoadConfig(
        resource_func=all_yield_pools,
        table_name=table_name,
        write_disposition="replace",
        pipeline_config=create_default_pipeline_config(pipeline_name, dataset_name),
    )
    _run_load_pipeline(pg_config, load_config)


def load_yield_pool(
    pool_id: str,
    pool_name: str,
    pg_config: PostgresConfig,
    pipeline_name: str = "defillama",
    dataset_name: str = "llama",
    table_name: str = "yield_pools",
):
    """Load historical yield pool data for a specific pool."""
    load_config = LoadConfig(
        resource_func=yield_pool,
        resource_args=(pool_id, pool_name),
        table_name=table_name,
        write_disposition="merge",
        primary_key=["time", "pool_id"],
        pipeline_config=create_default_pipeline_config(pipeline_name, dataset_name),
    )
    _run_load_pipeline(pg_config, load_config)
