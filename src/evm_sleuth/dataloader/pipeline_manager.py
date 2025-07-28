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
class ResourceConfig:
    """Resource-specific configuration."""
    resource_func: Callable
    resource_args: tuple = field(default_factory=tuple)
    resource_kwargs: Dict[str, Any] = field(default_factory=dict)
    table_name: str = ""
    write_disposition: str = "replace"
    primary_key: Optional[List[str]] = None
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.table_name:
            raise ValueError("Table name cannot be empty")
        if not callable(self.resource_func):
            raise ValueError("Resource function must be callable")


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
                raise ValueError(f"Unsupported destination type: {config.destination_type}")
            
            pipeline = dlt.pipeline(
                pipeline_name=config.pipeline_name,
                destination=destination,
                dataset_name=config.dataset_name
            )
            
            self.logger.info(f"Created pipeline '{config.pipeline_name}' for dataset '{config.dataset_name}'")
            return pipeline
            
        except Exception as e:
            raise PipelineError(f"Failed to create pipeline '{config.pipeline_name}': {e}") from e
    
    def run_pipeline(
        self, 
        pipeline_config: PipelineConfig, 
        resource_config: ResourceConfig
    ) -> Any:
        """Run a pipeline with given configuration."""
        try:
            pipeline = self.create_pipeline(pipeline_config)
            
            # Create resource with provided arguments
            resource = resource_config.resource_func(
                *resource_config.resource_args, 
                **resource_config.resource_kwargs
            )
            
            # Prepare run arguments
            run_kwargs = {
                "table_name": resource_config.table_name,
                "write_disposition": resource_config.write_disposition
            }
            
            if resource_config.primary_key:
                run_kwargs["primary_key"] = resource_config.primary_key
            
            # Execute pipeline
            self.logger.info(
                f"Running pipeline '{pipeline_config.pipeline_name}' "
                f"for table '{resource_config.table_name}'"
            )
            
            result = pipeline.run(resource, **run_kwargs)
            
            self.logger.info(
                f"Successfully completed pipeline run for table '{resource_config.table_name}'"
            )
            
            return result
            
        except Exception as e:
            error_msg = (
                f"Pipeline run failed for table '{resource_config.table_name}' "
                f"in pipeline '{pipeline_config.pipeline_name}': {e}"
            )
            self.logger.error(error_msg)
            raise PipelineError(error_msg) from e
    
    def run_multiple_resources(
        self,
        pipeline_config: PipelineConfig,
        resource_configs: List[ResourceConfig]
    ) -> Dict[str, Any]:
        """Run multiple resources in the same pipeline."""
        if not resource_configs:
            raise ValueError("At least one resource configuration is required")
        
        results = {}
        pipeline = self.create_pipeline(pipeline_config)
        
        for resource_config in resource_configs:
            try:
                # Create resource
                resource = resource_config.resource_func(
                    *resource_config.resource_args,
                    **resource_config.resource_kwargs
                )
                
                # Prepare run arguments
                run_kwargs = {
                    "table_name": resource_config.table_name,
                    "write_disposition": resource_config.write_disposition
                }
                
                if resource_config.primary_key:
                    run_kwargs["primary_key"] = resource_config.primary_key
                
                # Execute pipeline for this resource
                self.logger.info(f"Running resource for table '{resource_config.table_name}'")
                result = pipeline.run(resource, **run_kwargs)
                results[resource_config.table_name] = result
                
            except Exception as e:
                error_msg = f"Failed to run resource for table '{resource_config.table_name}': {e}"
                self.logger.error(error_msg)
                results[resource_config.table_name] = {"error": str(e)}
        
        return results


class DataLoaderTemplate:
    """Template class for creating standardized data loaders."""
    
    def __init__(self, pipeline_manager: PipelineManager):
        self.pipeline_manager = pipeline_manager
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def load_data(
        self, 
        resource_func: Callable,
        pipeline_name: str,
        dataset_name: str,
        table_name: str,
        resource_args: tuple = (),
        resource_kwargs: Optional[Dict[str, Any]] = None,
        write_disposition: str = "replace",
        primary_key: Optional[List[str]] = None
    ) -> Any:
        """Template method for loading data with standardized configuration."""
        
        pipeline_config = PipelineConfig(
            pipeline_name=pipeline_name,
            dataset_name=dataset_name
        )
        
        resource_config = ResourceConfig(
            resource_func=resource_func,
            resource_args=resource_args,
            resource_kwargs=resource_kwargs or {},
            table_name=table_name,
            write_disposition=write_disposition,
            primary_key=primary_key
        )
        
        self.logger.info(f"Loading data to table '{table_name}' using template")
        return self.pipeline_manager.run_pipeline(pipeline_config, resource_config)
    
    def load_incremental_data(
        self,
        resource_func: Callable,
        pipeline_name: str,
        dataset_name: str,
        table_name: str,
        primary_key: List[str],
        resource_args: tuple = (),
        resource_kwargs: Optional[Dict[str, Any]] = None
    ) -> Any:
        """Load data with incremental merge strategy."""
        return self.load_data(
            resource_func=resource_func,
            pipeline_name=pipeline_name,
            dataset_name=dataset_name,
            table_name=table_name,
            resource_args=resource_args,
            resource_kwargs=resource_kwargs,
            write_disposition="merge",
            primary_key=primary_key
        )
    
    def load_append_data(
        self,
        resource_func: Callable,
        pipeline_name: str,
        dataset_name: str,
        table_name: str,
        resource_args: tuple = (),
        resource_kwargs: Optional[Dict[str, Any]] = None
    ) -> Any:
        """Load data with append strategy."""
        return self.load_data(
            resource_func=resource_func,
            pipeline_name=pipeline_name,
            dataset_name=dataset_name,
            table_name=table_name,
            resource_args=resource_args,
            resource_kwargs=resource_kwargs,
            write_disposition="append"
        )