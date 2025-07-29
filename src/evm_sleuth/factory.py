"""Factory classes for creating clients and pipeline components."""

import importlib
from typing import Union, Optional, Dict, Type, Any, Callable

# Removed hardcoded imports to avoid circular import
# Clients will be auto-registered via metaclass
from .dataloader.pipeline_manager import PipelineManager
from .config.settings import settings
from .core.base import BaseAPIClient, BaseDLTResource


class APIClientFactory:
    """Factory for creating API clients."""

    _client_registry: Dict[str, Dict[str, Any]] = {}

    @classmethod
    def register_client(
        cls,
        name: str,
        client_class: Type[BaseAPIClient],
        factory_func: Callable[..., BaseAPIClient],
    ) -> None:
        """Register a new client type."""
        cls._client_registry[name] = {"class": client_class, "factory": factory_func}

    @classmethod
    def create_client(cls, name: str, **kwargs) -> BaseAPIClient:
        """Create client by name."""
        cls._load_plugins()
        if name not in cls._client_registry:
            raise ValueError(
                f"Unknown client type: {name}. Available: {list(cls._client_registry.keys())}"
            )

        return cls._client_registry[name]["factory"](**kwargs)

    @classmethod
    def get_available_clients(cls) -> list[str]:
        """Get list of available client names."""
        cls._load_plugins()
        return list(cls._client_registry.keys())

    @classmethod
    def _load_plugins(cls) -> None:
        """Load client plugins from settings or package discovery."""
        # Trigger auto-registration of pending classes
        from .core.base import AutoRegisterMeta

        AutoRegisterMeta.register_pending_classes()

        # Load from settings if configured
        if hasattr(settings, "plugins") and hasattr(settings.plugins, "client_modules"):
            for module_path in settings.plugins.client_modules:
                try:
                    importlib.import_module(module_path)
                except ImportError:
                    pass  # Skip missing plugins


class DLTResourceFactory:
    """Factory for creating DLT resources."""

    _resource_registry: Dict[str, Dict[str, Any]] = {}

    @classmethod
    def register_resource(
        cls,
        name: str,
        resource_class: Type[BaseDLTResource],
        factory_func: Callable[..., BaseDLTResource],
    ) -> None:
        """Register a new DLT resource type."""
        cls._resource_registry[name] = {
            "class": resource_class,
            "factory": factory_func,
        }

    @classmethod
    def create_resource(cls, name: str, **kwargs) -> BaseDLTResource:
        """Create DLT resource by name."""
        cls._load_plugins()
        if name not in cls._resource_registry:
            raise ValueError(
                f"Unknown resource type: {name}. Available: {list(cls._resource_registry.keys())}"
            )

        return cls._resource_registry[name]["factory"](**kwargs)

    @classmethod
    def get_available_resources(cls) -> list[str]:  
        """Get list of available resource names."""
        cls._load_plugins()
        return list(cls._resource_registry.keys())

    @classmethod
    def _load_plugins(cls) -> None:
        """Load resource plugins from settings or package discovery."""
        # Trigger auto-registration of pending classes
        from .core.base import AutoRegisterMeta

        AutoRegisterMeta.register_pending_classes()

        # Load from settings if configured
        if hasattr(settings, "plugins") and hasattr(settings.plugins, "resource_modules"):
            for module_path in settings.plugins.resource_modules:
                try:
                    importlib.import_module(module_path)
                except ImportError:
                    pass  # Skip missing plugins


class PipelineFactory:
    """Factory for creating pipeline components."""

    @staticmethod
    def create_pipeline_manager(use_local_db: bool = True) -> PipelineManager:
        """Create pipeline manager with appropriate database config."""
        db_config = settings.local_db if use_local_db else settings.remote_db
        return PipelineManager(db_config)

    @staticmethod
    def create_etherscan_pipeline(
        chainid: int, use_local_db: bool = True, api_key: Optional[str] = None
    ) -> tuple[PipelineManager, BaseDLTResource]:
        """Create pipeline manager and Etherscan resource together."""
        pipeline_manager = PipelineFactory.create_pipeline_manager(use_local_db)
        etherscan_resource = DLTResourceFactory.create_resource(
            "etherscan", chainid=chainid, api_key=api_key
        )
        return pipeline_manager, etherscan_resource
