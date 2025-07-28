#!/usr/bin/env python3
"""
Example script demonstrating how to use the Etherscan class to get transaction receipts.
"""

import os
import sys
from dotenv import load_dotenv
from evm_sleuth.blockexplorer.etherscan import Etherscan
from evm_sleuth.decoder import EventDecoder
import json
import csv


load_dotenv()


def main():
    """
    Example usage of the Etherscan class to get transaction receipts.
    """
    api_key = os.getenv("ETHERSCAN_API_KEY")
    blockexplorer = Etherscan(chainid=1, api_key=api_key)

    # execution transaction
    tx_hash = "0x42e484c3a4c3ea296c2d95365bc8b6e515d0a3d5764b06410e612a4604552ec3"
    receipt = blockexplorer.get_transaction_receipt(tx_hash, save=True)

    # get all address that has logs in the receipt
    contract_addresses = set([log["address"] for log in receipt["logs"]])

    # get all contracts abi
    for contract_address in contract_addresses:
        blockexplorer.get_contract_abi(contract_address)

    # decode all event
    decoded_events = []
    for log in receipt["logs"]:
        contract_address = log["address"]
        if contract_address == "0x323c03c48660fe31186fa82c289b0766d331ce21":
            contract_address = "0x089357A774151Ffdd24269204Cb789e298E31f09"
        try:
            with open(f"data/abi/{contract_address}.json") as f:
                abi = json.load(f)
            # check if there is an implementation contract
            try:
                with open(f"data/abi/{contract_address}-implementation.json") as f:
                    abi.extend(json.load(f))
            except FileNotFoundError:
                pass

            decoder = EventDecoder(abi)
            decoded_event = decoder.decode_log_entry(
                contract_address, log["topics"], log["data"]
            )
            if decoded_event.event_name == "unknown":
                decoded_event = decoder.decode_log_entry_with_tuples(
                    contract_address, log["topics"], log["data"]
                )
            decoded_events.append(decoded_event)
        except FileNotFoundError:
            print(f"ABI not found for {contract_address}")
        if contract_address == "0x323c03c48660fe31186fa82c289b0766d331ce21":
            print(decoded_event)

    # save all decoded events
    with open("data/event/decoded_events.csv", "w", newline="") as f:
        if decoded_events:
            writer = csv.DictWriter(f, fieldnames=decoded_events[0].__dict__.keys())
            writer.writeheader()
            for decoded_event in decoded_events:
                writer.writerow(decoded_event.__dict__)
            print("Saved decoded events to data/event/decoded_events.csv")
        else:
            print("No events to save.")


def get_transactions(contract_address: str):
    api_key = os.getenv("ETHERSCAN_API_KEY")
    blockexplorer = Etherscan(chainid=1, api_key=api_key)
    transactions = blockexplorer.get_contract_transactions(contract_address)
    return transactions


if __name__ == "__main__":
    # main()
    get_transactions("0x089357A774151Ffdd24269204Cb789e298E31f09")
    # im()
