"""Abstract base classes for onchaindata package."""

import time
import logging
import requests
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type, Callable

from .rate_limiter import RateLimitedSession, RateLimitStrategy
from .exceptions import APIError


@dataclass
class APIConfig:
    """Configuration for API clients."""

    base_url: str
    api_key: Optional[str] = None
    rate_limit: float = 5.0  # requests per second
    timeout: int = 30
    retry_attempts: int = 3
    retry_delay_base: float = 1.0  # base delay for exponential backoff


class AutoRegisterMeta(type(ABC)):
    """Metaclass that automatically registers API clients and DLT sources."""

    def __new__(cls, name: str, bases: tuple, namespace: dict):
        # Create the class
        new_class = super().__new__(cls, name, bases, namespace)

        # Only register concrete classes (not abstract base classes)
        if not getattr(new_class, "__abstractmethods__", None):
            cls._register_class(new_class, name, bases)

        return new_class

    @staticmethod
    def _register_class(new_class: Type, name: str, bases: tuple):
        """Register the class with appropriate factory."""
        # Store registration info for lazy registration
        if not hasattr(AutoRegisterMeta, "_pending_registrations"):
            AutoRegisterMeta._pending_registrations = []

        # Check if this is an API client or DLT source
        is_api_client = any(
            getattr(base, "__name__", "") == "BaseAPIClient" for base in bases
        )
        is_dlt_source = any(
            getattr(base, "__name__", "") == "BaseDLTSource" for base in bases
        )

        if is_api_client or is_dlt_source:
            AutoRegisterMeta._pending_registrations.append(
                {
                    "class": new_class,
                    "name": name,
                    "type": "client" if is_api_client else "source",
                }
            )

    @staticmethod
    def _register_api_client(client_class: Type, name: str, factory_class):
        """Register API client with factory."""
        # Extract client name from class name (e.g., EtherscanClient -> etherscan)
        client_name = (
            name.lower().replace("client", "")
            if name.endswith("Client")
            else name.lower()
        )

        # Create a generic factory function
        def factory_func(**kwargs):
            return client_class(**kwargs)

        factory_class.register_client(client_name, client_class, factory_func)

    @staticmethod
    def _register_dlt_source(
        source_class: Type, name: str, client_factory, source_factory
    ):
        """Register DLT source with factory."""
        # Extract source name from class name (e.g., EtherscanDLTSource -> etherscan)
        source_name = (
            name.lower().replace("dltsource", "")
            if name.endswith("DLTSource")
            else name.lower()
        )

        # Create a factory function that creates client and source
        def factory_func(**kwargs):
            client = client_factory.create_client(source_name, **kwargs)
            return source_class(client)

        source_factory.register_source(source_name, source_class, factory_func)


class BaseAPIClient(ABC, metaclass=AutoRegisterMeta):
    """Abstract base class for all API clients."""

    def __init__(
        self,
        config: APIConfig,
        rate_limit_strategy: RateLimitStrategy = RateLimitStrategy.FIXED_INTERVAL,
    ):
        self.config = config
        self.rate_limit_strategy = rate_limit_strategy
        self.logger = logging.getLogger(self.__class__.__name__)
        self._session = self._create_session()

    def _create_session(self) -> RateLimitedSession:
        """Create configured session with rate limiting."""
        return RateLimitedSession(
            calls_per_second=self.config.rate_limit,
            strategy=self.rate_limit_strategy,
            logger=self.logger,
        )

    @abstractmethod
    def _build_request_params(self, **kwargs) -> Dict[str, Any]:
        """Build request parameters specific to the API."""
        pass

    @abstractmethod
    def _handle_response(self, response: requests.Response) -> Any:
        """Handle API response and extract data."""
        pass

    def make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Any:
        """Generic request method with error handling and retry logic."""
        # Handle full URLs (when endpoint starts with http)
        if endpoint.startswith(("http://", "https://")):
            url = endpoint
        else:
            url = (
                f"{self.config.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
                if endpoint
                else self.config.base_url
            )
        request_params = self._build_request_params(**(params or {}))

        last_exception = None
        for attempt in range(self.config.retry_attempts):
            try:
                response = self._session.get(
                    url, params=request_params, timeout=self.config.timeout
                )
                return self._handle_response(response)
            except Exception as e:
                last_exception = e
                if attempt < self.config.retry_attempts - 1:
                    delay = self.config.retry_delay_base * (2**attempt)
                    self.logger.warning(
                        f"Request failed (attempt {attempt + 1}): {e}. Retrying in {delay}s..."
                    )
                    time.sleep(delay)
                else:
                    self.logger.error(
                        f"Request failed after {self.config.retry_attempts} attempts: {e}"
                    )

        raise APIError(
            f"Request failed after {self.config.retry_attempts} attempts"
        ) from last_exception


class BaseDLTSource(ABC, metaclass=AutoRegisterMeta):
    """Abstract base class for DLT sources."""

    def __init__(self, client: BaseAPIClient):
        self.client = client
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def get_source_name(self) -> str:
        """Return the DLT source name."""
        pass

    @abstractmethod
    def create_dlt_source(self, **kwargs) -> Any:
        """Create DLT source configuration."""
        pass


class BaseSource(ABC):
    """Abstract base class for DLT source factories."""

    def __init__(self, client: BaseAPIClient):
        self.client = client
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def get_available_sources(self) -> List[str]:
        """Return list of available source names."""
        pass


class BaseDecoder(ABC):
    """Abstract base class for decoders."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def decode(self, data: Any, **kwargs) -> Any:
        """Decode data according to specific strategy."""
        pass
