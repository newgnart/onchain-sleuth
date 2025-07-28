"""Factory classes for creating clients and pipeline components."""

from typing import Union, Optional

from .datasource.clients.etherscan import EtherscanClient, EtherscanDLTResource
from .datasource.clients.coingecko import CoinGeckoClient, CoinGeckoDLTResource
from .datasource.clients.defillama import DeFiLlamaClient, DeFiLlamaDLTResource
from .dataloader.pipeline_manager import PipelineManager
from .config.settings import settings


class ClientFactory:
    """Factory for creating API clients."""
    
    @staticmethod
    def create_etherscan_client(
        chainid: int, 
        api_key: Optional[str] = None,
        calls_per_second: Optional[float] = None
    ) -> EtherscanClient:
        """Create Etherscan client with default configuration."""
        api_key = api_key or settings.api.etherscan_api_key
        calls_per_second = calls_per_second or settings.api.etherscan_rate_limit
        
        return EtherscanClient(
            chainid=chainid,
            api_key=api_key,
            calls_per_second=calls_per_second
        )
    
    @staticmethod
    def create_coingecko_client(
        api_key: Optional[str] = None,
        calls_per_second: Optional[float] = None
    ) -> CoinGeckoClient:
        """Create CoinGecko client with default configuration."""
        api_key = api_key or settings.api.coingecko_api_key
        calls_per_second = calls_per_second or settings.api.coingecko_rate_limit
        
        return CoinGeckoClient(
            api_key=api_key,
            calls_per_second=calls_per_second
        )
    
    @staticmethod
    def create_defillama_client(
        calls_per_second: Optional[float] = None
    ) -> DeFiLlamaClient:
        """Create DeFiLlama client with default configuration."""
        calls_per_second = calls_per_second or settings.api.defillama_rate_limit
        
        return DeFiLlamaClient(calls_per_second=calls_per_second)


class DLTResourceFactory:
    """Factory for creating DLT resources."""
    
    @staticmethod
    def create_etherscan_resource(
        chainid: int,
        api_key: Optional[str] = None,
        calls_per_second: Optional[float] = None
    ) -> EtherscanDLTResource:
        """Create Etherscan DLT resource."""
        client = ClientFactory.create_etherscan_client(
            chainid=chainid,
            api_key=api_key,
            calls_per_second=calls_per_second
        )
        return EtherscanDLTResource(client)
    
    @staticmethod
    def create_coingecko_resource(
        api_key: Optional[str] = None,
        calls_per_second: Optional[float] = None
    ) -> CoinGeckoDLTResource:
        """Create CoinGecko DLT resource."""
        client = ClientFactory.create_coingecko_client(
            api_key=api_key,
            calls_per_second=calls_per_second
        )
        return CoinGeckoDLTResource(client)
    
    @staticmethod
    def create_defillama_resource(
        calls_per_second: Optional[float] = None
    ) -> DeFiLlamaDLTResource:
        """Create DeFiLlama DLT resource."""
        client = ClientFactory.create_defillama_client(calls_per_second=calls_per_second)
        return DeFiLlamaDLTResource(client)


class PipelineFactory:
    """Factory for creating pipeline components."""
    
    @staticmethod
    def create_pipeline_manager(use_local_db: bool = True) -> PipelineManager:
        """Create pipeline manager with appropriate database config."""
        db_config = settings.local_db if use_local_db else settings.remote_db
        return PipelineManager(db_config)
    
    @staticmethod
    def create_etherscan_pipeline(
        chainid: int,
        use_local_db: bool = True,
        api_key: Optional[str] = None
    ) -> tuple[PipelineManager, EtherscanDLTResource]:
        """Create pipeline manager and Etherscan resource together."""
        pipeline_manager = PipelineFactory.create_pipeline_manager(use_local_db)
        etherscan_resource = DLTResourceFactory.create_etherscan_resource(
            chainid=chainid,
            api_key=api_key
        )
        return pipeline_manager, etherscan_resource