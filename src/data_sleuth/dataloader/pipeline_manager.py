"""Pipeline management for DLT operations."""

import logging
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
import dlt

from data_sleuth.core.exceptions import PipelineError


@dataclass
class TableConfig:
    """Configuration for a single table in a pipeline."""

    source: Any
    write_disposition: str = "append"
    primary_key: Optional[List[str]] = None


class PipelineManager:
    """Manages DLT pipeline creation and execution."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(
        self,
        sources: Union[Any, Dict[str, Union[Any, TableConfig]]],
        pipeline_name: str,
        dataset_name: str,
        destination: Any,
        write_disposition: str = "append",
        primary_key: Optional[List[str]] = None,
    ) -> Union[Any, Dict[str, Any]]:
        """Create and run pipeline with sources.

        Args:
            sources: Single source, dict mapping table names to sources, or dict mapping table names to TableConfig
            pipeline_name: Name of the pipeline
            dataset_name: Name of the dataset
            destination: DLT destination
            write_disposition: Default write disposition
            primary_key: Default primary key

        Returns:
            Single result for single source, dict of results for named sources
        """
        try:
            # Create pipeline
            pipeline = dlt.pipeline(
                pipeline_name=pipeline_name,
                destination=destination,
                dataset_name=dataset_name,
            )

            # Execute pipeline
            if isinstance(sources, dict):
                return self._run_named_sources(
                    pipeline, sources, write_disposition, primary_key
                )
            else:
                return self._run_single_source(
                    pipeline, sources, write_disposition, primary_key
                )

        except Exception as e:
            error_msg = f"Pipeline '{pipeline_name}' failed: {e}"
            self.logger.error(error_msg)
            raise PipelineError(error_msg) from e

    def _run_single_source(
        self,
        pipeline: dlt.Pipeline,
        source: Any,
        write_disposition: str,
        primary_key: Optional[List[str]],
    ) -> Any:
        """Run pipeline with single source."""
        run_kwargs = {"write_disposition": write_disposition}
        if primary_key:
            run_kwargs["primary_key"] = primary_key

        result = pipeline.run(source, **run_kwargs)
        return result

    def _run_named_sources(
        self,
        pipeline: dlt.Pipeline,
        sources: Dict[str, Union[Any, TableConfig]],
        write_disposition: str,
        primary_key: Optional[List[str]],
    ) -> Dict[str, Any]:
        """Run pipeline with named sources."""
        results = {}

        for table_name, source_config in sources.items():
            try:
                # Handle both TableConfig and raw source objects
                if isinstance(source_config, TableConfig):
                    source = source_config.source
                    table_write_disposition = source_config.write_disposition
                    table_primary_key = source_config.primary_key
                else:
                    source = source_config
                    table_write_disposition = write_disposition
                    table_primary_key = primary_key

                run_kwargs = {
                    "table_name": table_name,
                    "write_disposition": table_write_disposition,
                }
                if table_primary_key:
                    run_kwargs["primary_key"] = table_primary_key

                result = pipeline.run(source, **run_kwargs)
                results[table_name] = result

            except Exception as e:
                self.logger.error(f"Failed to run source for table '{table_name}': {e}")
                results[table_name] = {"error": str(e)}

        return results
