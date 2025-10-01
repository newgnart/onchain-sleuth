import json, argparse
from onchaindata.utils.etherscan_extract import etherscan_to_parquet


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--logs",
        action="store_true",
        help="Extract logs",
    )
    parser.add_argument(
        "--transactions",
        action="store_true",
        help="Extract transactions",
    )
    parser.add_argument(
        "--from-block",
        type=int,
        help="Start block number",
        default=None,
    )
    parser.add_argument(
        "--to-block",
        type=int,
        help="End block number",
        default=None,
    )
    args = parser.parse_args()

    contracts = json.load(open("scripts/extraction/contracts.json"))

    for name, details in contracts.items():
        if args.logs:
            etherscan_to_parquet(
                address=details["address"],
                chain=details["chain"],
                table="logs",
                from_block=args.from_block,
                to_block=args.to_block,
            )
        if args.transactions:
            etherscan_to_parquet(
                address=details["address"],
                chain=details["chain"],
                table="transactions",
                from_block=args.from_block,
                to_block=args.to_block,
            )


if __name__ == "__main__":
    main()
