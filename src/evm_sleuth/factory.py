"""Factory classes for creating clients and pipeline components."""

import importlib
import os
from typing import Union, Optional, Dict, Type, Any, Callable, Literal, TypeVar, Generic

# Removed hardcoded imports to avoid circular import
# Clients will be auto-registered via metaclass
from .dataloader.pipeline_manager import PipelineManager
from .config.settings import settings
from .core.base import BaseAPIClient, BaseDLTSource, BaseSource

T = TypeVar("T")


# class BaseFactory(Generic[T]):
#     """Base factory class for registry pattern."""

#     _registry: Dict[str, Dict[str, Any]] = {}

#     @classmethod
#     def register(
#         cls,
#         name: str,
#         item_class: Type[T],
#         factory_func: Callable[..., T],
#     ) -> None:
#         """Register a new item type."""
#         cls._registry[name] = {"class": item_class, "factory": factory_func}

#     @classmethod
#     def create(cls, name: str, **kwargs) -> T:
#         """Create item by name."""
#         cls._load_plugins()
#         if name not in cls._registry:
#             raise ValueError(
#                 f"Unknown {cls.__name__.lower().replace('factory', '')} type: {name}. Available: {list(cls._registry.keys())}"
#             )

#         return cls._registry[name]["factory"](**kwargs)

#     @classmethod
#     def get_available(cls) -> list[str]:
#         """Get list of available item names."""
#         cls._load_plugins()
#         return list(cls._registry.keys())

#     @classmethod
#     def _load_plugins(cls) -> None:
#         """Load plugins from settings or package discovery."""
#         # Trigger auto-registration of pending classes
#         from .core.base import AutoRegisterMeta

#         AutoRegisterMeta.register_pending_classes()


# class APIClientFactory(BaseFactory[BaseAPIClient]):
#     """Factory for creating API clients."""

#     _registry: Dict[str, Dict[str, Any]] = {}

#     @classmethod
#     def register_client(
#         cls,
#         name: str,
#         client_class: Type[BaseAPIClient],
#         factory_func: Callable[..., BaseAPIClient],
#     ) -> None:
#         """Register a new client type."""
#         cls.register(name, client_class, factory_func)

#     @classmethod
#     def create_client(cls, name: str, **kwargs) -> BaseAPIClient:
#         """Create client by name."""
#         return cls.create(name, **kwargs)

#     @classmethod
#     def get_available_clients(cls) -> list[str]:
#         """Get list of available client names."""
#         return cls.get_available()

#     @classmethod
#     def _load_plugins(cls) -> None:
#         """Load client plugins from settings or package discovery."""
#         super()._load_plugins()

#         # Load from settings if configured
#         if hasattr(settings, "plugins") and hasattr(settings.plugins, "client_modules"):
#             for module_path in settings.plugins.client_modules:
#                 try:
#                     importlib.import_module(module_path)
#                 except ImportError:
#                     pass  # Skip missing plugins


# class DLTSourceFactory(BaseFactory[BaseDLTSource]):
#     """Factory for creating DLT sources."""

#     _registry: Dict[str, Dict[str, Any]] = {}

#     @classmethod
#     def register_source(
#         cls,
#         name: str,
#         source_class: Type[BaseDLTSource],
#         factory_func: Callable[..., BaseDLTSource],
#     ) -> None:
#         """Register a new DLT source type."""
#         cls.register(name, source_class, factory_func)

#     @classmethod
#     def create_source(cls, name: str, **kwargs) -> BaseDLTSource:
#         """Create DLT source by name."""
#         return cls.create(name, **kwargs)

#     @classmethod
#     def get_available_sources(cls) -> list[str]:
#         """Get list of available source names."""
#         return cls.get_available()

#     @classmethod
#     def _load_plugins(cls) -> None:
#         """Load source plugins from settings or package discovery."""
#         super()._load_plugins()

#         # Load from settings if configured
#         if hasattr(settings, "plugins") and hasattr(settings.plugins, "source_modules"):
#             for module_path in settings.plugins.source_modules:
#                 try:
#                     importlib.import_module(module_path)
#                 except ImportError:
#                     pass  # Skip missing plugins


class PipelineFactory:
    """Factory for creating pipeline components."""

    @staticmethod
    def create_pipeline_manager(use_local_db: bool = True) -> PipelineManager:
        """Create pipeline manager with appropriate database config."""
        db_config = settings.local_db if use_local_db else settings.remote_db
        return PipelineManager(db_config)

    @staticmethod
    def create_dlt_pipeline(
        name: str,
        destination: Literal["postgres", "duckdb"] = "duckdb",
        dataset_name: Optional[str] = None,
    ):
        """Create DLT pipeline with specified destination."""
        import dlt

        # Ensure data directory exists for DuckDB
        if destination == "duckdb":
            os.makedirs(os.path.dirname(settings.duckdb.database_path), exist_ok=True)
            dest = dlt.destinations.duckdb(settings.duckdb.database_path)
        elif destination == "postgres":
            connection_url = settings.local_db.get_connection_url()
            dest = dlt.destinations.postgres(connection_url)
        else:
            raise ValueError(f"Unsupported destination: {destination}")

        return dlt.pipeline(
            pipeline_name=name, destination=dest, dataset_name=dataset_name or name
        )

    @staticmethod
    def create_etherscan_pipeline(
        chainid: int, use_local_db: bool = True, api_key: Optional[str] = None
    ) -> tuple[PipelineManager, BaseDLTSource]:
        """Create pipeline manager and Etherscan source together."""
        pipeline_manager = PipelineFactory.create_pipeline_manager(use_local_db)
        etherscan_source = DLTSourceFactory.create_source(
            "etherscan", chainid=chainid, api_key=api_key
        )
        return pipeline_manager, etherscan_source
