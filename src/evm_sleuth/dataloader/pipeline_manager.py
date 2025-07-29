"""Unified pipeline management for DLT operations."""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Callable
import dlt

from evm_sleuth.config.settings import DatabaseSettings
from evm_sleuth.core.exceptions import PipelineError


@dataclass
class PipelineConfig:
    """Unified pipeline configuration."""

    pipeline_name: str
    dataset_name: str
    destination_type: str = "postgres"

    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.pipeline_name:
            raise ValueError("Pipeline name cannot be empty")
        if not self.dataset_name:
            raise ValueError("Dataset name cannot be empty")


@dataclass
class SourceConfig:
    """Source-specific configuration."""

    source_func: Callable
    source_args: tuple = field(default_factory=tuple)
    source_kwargs: Dict[str, Any] = field(default_factory=dict)
    table_name: str = ""
    write_disposition: str = "replace"
    primary_key: Optional[List[str]] = None

    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.table_name:
            raise ValueError("Table name cannot be empty")
        if not callable(self.source_func):
            raise ValueError("Source function must be callable")


class PipelineManager:
    """Centralized pipeline management for DLT operations."""

    def __init__(self, database_config: DatabaseSettings):
        self.database_config = database_config
        self.logger = logging.getLogger(self.__class__.__name__)

    def create_pipeline(self, config: PipelineConfig) -> dlt.Pipeline:
        """Create a DLT pipeline with standardized configuration."""
        try:
            if config.destination_type == "postgres":
                destination = dlt.destinations.postgres(
                    self.database_config.get_connection_url()
                )
            else:
                raise ValueError(
                    f"Unsupported destination type: {config.destination_type}"
                )

            pipeline = dlt.pipeline(
                pipeline_name=config.pipeline_name,
                destination=destination,
                dataset_name=config.dataset_name,
            )

            self.logger.info(
                f"Created pipeline '{config.pipeline_name}' for dataset '{config.dataset_name}'"
            )
            return pipeline

        except Exception as e:
            raise PipelineError(
                f"Failed to create pipeline '{config.pipeline_name}': {e}"
            ) from e

    def run_pipeline(
        self, pipeline_config: PipelineConfig, source_config: SourceConfig
    ) -> Any:
        """Run a pipeline with given configuration."""
        try:
            pipeline = self.create_pipeline(pipeline_config)

            # Create source with provided arguments
            source = source_config.source_func(
                *source_config.source_args, **source_config.source_kwargs
            )

            # Prepare run arguments
            run_kwargs = {
                "table_name": source_config.table_name,
                "write_disposition": source_config.write_disposition,
            }

            if source_config.primary_key:
                run_kwargs["primary_key"] = source_config.primary_key

            # Execute pipeline
            self.logger.info(
                f"Running pipeline '{pipeline_config.pipeline_name}' "
                f"for table '{source_config.table_name}'"
            )

            result = pipeline.run(source, **run_kwargs)

            self.logger.info(
                f"Successfully completed pipeline run for table '{source_config.table_name}'"
            )

            return result

        except Exception as e:
            error_msg = (
                f"Pipeline run failed for table '{source_config.table_name}' "
                f"in pipeline '{pipeline_config.pipeline_name}': {e}"
            )
            self.logger.error(error_msg)
            raise PipelineError(error_msg) from e

    def run_multiple_sources(
        self, pipeline_config: PipelineConfig, source_configs: List[SourceConfig]
    ) -> Dict[str, Any]:
        """Run multiple sources in the same pipeline."""
        if not source_configs:
            raise ValueError("At least one source configuration is required")

        results = {}
        pipeline = self.create_pipeline(pipeline_config)

        for source_config in source_configs:
            try:
                # Create source
                source = source_config.source_func(
                    *source_config.source_args, **source_config.source_kwargs
                )

                # Prepare run arguments
                run_kwargs = {
                    "table_name": source_config.table_name,
                    "write_disposition": source_config.write_disposition,
                }

                if source_config.primary_key:
                    run_kwargs["primary_key"] = source_config.primary_key

                # Execute pipeline for this source
                self.logger.info(
                    f"Running source for table '{source_config.table_name}'"
                )
                result = pipeline.run(source, **run_kwargs)
                results[source_config.table_name] = result

            except Exception as e:
                error_msg = (
                    f"Failed to run source for table '{source_config.table_name}': {e}"
                )
                self.logger.error(error_msg)
                results[source_config.table_name] = {"error": str(e)}

        return results


class DataLoaderTemplate:
    """Template class for creating standardized data loaders."""

    def __init__(self, pipeline_manager: PipelineManager):
        self.pipeline_manager = pipeline_manager
        self.logger = logging.getLogger(self.__class__.__name__)

    def load_data(
        self,
        source_func: Callable,
        pipeline_name: str,
        dataset_name: str,
        table_name: str,
        source_args: tuple = (),
        source_kwargs: Optional[Dict[str, Any]] = None,
        write_disposition: str = "replace",
        primary_key: Optional[List[str]] = None,
    ) -> Any:
        """Template method for loading data with standardized configuration."""

        pipeline_config = PipelineConfig(
            pipeline_name=pipeline_name, dataset_name=dataset_name
        )

        source_config = SourceConfig(
            source_func=source_func,
            source_args=source_args,
            source_kwargs=source_kwargs or {},
            table_name=table_name,
            write_disposition=write_disposition,
            primary_key=primary_key,
        )

        self.logger.info(f"Loading data to table '{table_name}' using template")
        return self.pipeline_manager.run_pipeline(pipeline_config, source_config)

    def load_incremental_data(
        self,
        source_func: Callable,
        pipeline_name: str,
        dataset_name: str,
        table_name: str,
        primary_key: List[str],
        source_args: tuple = (),
        source_kwargs: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Load data with incremental merge strategy."""
        return self.load_data(
            source_func=source_func,
            pipeline_name=pipeline_name,
            dataset_name=dataset_name,
            table_name=table_name,
            source_args=source_args,
            source_kwargs=source_kwargs,
            write_disposition="merge",
            primary_key=primary_key,
        )

    def load_append_data(
        self,
        source_func: Callable,
        pipeline_name: str,
        dataset_name: str,
        table_name: str,
        source_args: tuple = (),
        source_kwargs: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Load data with append strategy."""
        return self.load_data(
            source_func=source_func,
            pipeline_name=pipeline_name,
            dataset_name=dataset_name,
            table_name=table_name,
            source_args=source_args,
            source_kwargs=source_kwargs,
            write_disposition="append",
        )
