"""Protocol registry for mapping contracts to protocols."""

import json
import logging
from pathlib import Path
from typing import Dict


class ProtocolRegistry:
    """Maps contract addresses to protocol names."""

    def __init__(self, registry_path: str = None):
        self.logger = logging.getLogger(self.__class__.__name__)
        if registry_path is None:
            registry_path = self._get_default_registry_path()
        self.registry = self._load_registry(registry_path)

    def _get_default_registry_path(self) -> str:
        """Get default path to protocol mapping file."""
        current_file = Path(__file__)
        registry_path = (
            current_file.parent.parent.parent.parent / "resource" / "protocol_mapping.json"
        )
        return str(registry_path)

    def _load_registry(self, registry_path: str) -> Dict[str, str]:
        """Load protocol mapping from JSON file."""
        try:
            with open(registry_path, 'r') as f:
                registry = json.load(f)
            self.logger.info(f"Loaded {len(registry)} contract-to-protocol mappings")
            return {addr.lower(): protocol for addr, protocol in registry.items()}
        except FileNotFoundError:
            self.logger.warning(f"Protocol registry file not found at {registry_path}")
            return {}
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in protocol registry file: {e}")
            return {}

    def get_protocol(self, contract_address: str) -> str:
        """Get protocol name for a contract address."""
        return self.registry.get(contract_address.lower(), "misc")

    def get_known_contracts(self) -> Dict[str, str]:
        """Get all known contract addresses and their protocols."""
        return self.registry.copy()

    def add_contract(self, contract_address: str, protocol: str):
        """Add a new contract-to-protocol mapping."""
        self.registry[contract_address.lower()] = protocol
        self.logger.info(f"Added mapping: {contract_address} -> {protocol}")