import json
from typing import Optional


def get_chainid(chain: str, chainid_data: Optional[dict] = None) -> int:
    """Get the chainid for a given chain name."""
    if chainid_data is None:
        with open("resource/chainid.json", "r") as f:
            chainid_data = json.load(f)
            pass  # Loaded chainid.json
    try:
        chainid = chainid_data[chain]
        return chainid
    except KeyError:
        raise ValueError(f"Chain {chain} not found in chainid.json")
