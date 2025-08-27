import os, json, logging

import pandas as pd
from web3 import Web3

logger = logging.getLogger(__name__)


def events_list(address, save_dir="data/events", abi_dir="data/abi"):
    """
    Get all events and its signature for a contract using web3 library, totally off-chain decoding process.
    """
    if not os.path.exists(save_dir):
        os.makedirs(save_dir, exist_ok=True)

    w3 = Web3()

    # Load the main contract ABI
    with open(f"{abi_dir}/{address}.json", "r") as f:
        main_abi = json.load(f)

    # Check if this contract has an implementation (proxy pattern)
    combined_abi = main_abi.copy()  # Start with main ABI

    try:
        implementation_df = pd.read_csv(f"{abi_dir}/implementation.csv")
        mask = implementation_df["address"] == address

        if mask.any():
            implementation_address = implementation_df.loc[
                mask, "implementation_address"
            ].iloc[0]
        else:
            logger.info(f"No matching implementation address found for {address}")
            implementation_address = None

        if implementation_address:
            implementation_abi_path = f"{abi_dir}/{implementation_address}.json"
            if os.path.exists(implementation_abi_path):
                with open(implementation_abi_path, "r") as f:
                    implementation_abi = json.load(f)

                # This ensures proxy events are available while adding implementation events
                combined_abi = main_abi + implementation_abi
                logger.info(
                    f"Combined proxy ABI ({len(main_abi)} entries) with implementation ABI ({len(implementation_abi)} entries)"
                )
            else:
                logger.info(f"Implementation ABI file not found for {address}")
        else:
            logger.info(f"No implementation address found for this {address}")
    except FileNotFoundError:
        logger.info("Implementation CSV file not found, using main ABI only")
    except Exception as e:
        logger.info(f"Error loading implementation ABI: {e}")

    contract = w3.eth.contract(address=address, abi=combined_abi)

    all_events = contract.all_events()

    # Collect event data into a list
    event_data = []
    for event in all_events:
        event_data.append(
            {
                "name": event.name,
                "topic": event.topic,
                "signature": event.signature,
                "address": address,  # put all in address, not implementation address
                "abi": event.abi,
            }
        )
    df = pd.DataFrame(event_data)
    df.to_csv(
        f"{save_dir}/{address}.csv", index=False
    )  # put all in address, not implementation address
