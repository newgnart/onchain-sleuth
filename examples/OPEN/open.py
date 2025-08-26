import sys
import os
import json


from ..helpers.dataloader import load_chunks, get_chainid
from evm_sleuth import (
    EtherscanClient,
    EtherscanSource,
    PostgresClient,
    settings,
)
from web3 import AsyncWeb3, WebSocketProvider, Web3
from eth_abi.abi import decode


def load_data():
    chain = "ethereum"
    chainid = get_chainid(chain)

    # load data for all addresses
    etherscan_client = EtherscanClient(chainid=chainid)
    source = EtherscanSource(client=etherscan_client)
    postgres_client = PostgresClient(settings.postgres)
    address = "0x323c03c48660fE31186fa82c289b0766d331Ce21".lower()

    load_chunks(
        dataset_name="etherscan_raw",  # schema
        table_name="txns",
        contract_address=address,
        etherscan_client=etherscan_client,
        postgres_client=postgres_client,
        source_factory=source.transactions,
        # from_block=22207515,
        # to_block=22237514,
        # block_chunk_size=1000,
    )

    load_chunks(
        dataset_name="etherscan_raw",  # schema
        table_name="logs",
        contract_address=address,
        etherscan_client=etherscan_client,
        postgres_client=postgres_client,
        source_factory=source.logs,
    )


def abi(address):
    chain = "ethereum"
    chainid = get_chainid(chain)
    etherscan_client = EtherscanClient(chainid=chainid)
    abi, implementation_abi = etherscan_client.get_contract_abi(address)
    return abi, implementation_abi


def decode():
    # transfer_event_topic = keccak.new(text="Transfer(address,address,uint256)")
    w3 = Web3()
    topic0 = w3.keccak(text="Transfer(address,address,uint256)")

    with open(
        "data/abi/0x323c03c48660fE31186fa82c289b0766d331Ce21.json".lower(), "r"
    ) as f:
        abi = json.load(f)

    contract = w3.eth.contract(
        address="0x323c03c48660fE31186fa82c289b0766d331Ce21", abi=abi
    )
    # print(contract.functions.transfer(
    #         "0x323c03c48660fE31186fa82c289b0766d331Ce21",
    #         "0x323c03c48660fE31186fa82c289b0766d331Ce21",
    #     ).encode_abi()
    # )

    # print(dir(contract))
    # print(contract.all_functions())
    # print(contract.all_events())
    # t = contract.get_event_by_topic(
    #     "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    # )
    # print(t)

    with open(
        "data/abi/0xba9642b0690e083fc11def8eac49fc05aaa5d725.json".lower(), "r"
    ) as f:
        abi = json.load(f)

    contract = w3.eth.contract(
        address="0xBA9642b0690E083fc11def8Eac49FC05aAA5d725", abi=abi
    )
    # print(contract.functions.transfer(
    #         "0x323c03c48660fE31186fa82c289b0766d331Ce21",
    #         "0x323c03c48660fE31186fa82c289b0766d331Ce21",
    #     ).encode_abi()
    # )

    # print(dir(contract))
    # print(contract.all_functions())
    all_events = contract.all_events()
    # print(all_events)
    for event in all_events:
        print(event.name)
        # print(event.abi)
        print(event.topic)
        # print(event.signature)
    # t = contract.get_event_by_topic(
    #     "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    # )
    # print(t)
    # print(dir(contract))


if __name__ == "__main__":
    # main()
    # decode()
    # abi(address="0x323c03c48660fE31186fa82c289b0766d331Ce21".lower())
    decode()
