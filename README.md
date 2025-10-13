# Onchain data
This repo is archived and moved to https://github.com/newgnart/fa-dae2-gnart
A data engineering toolkit for extracting, transforming, and loading EVM blockchains data.

## Usage
### Setup python environment
```bash
uv sync
cp .env.example .env # Then fill the .env file
```

### Most basic usage: Extract data from Etherscan to local parquet files
Fill the `scripts/extraction/contracts.json` file with the contracts you want to extract data for.
```bash
uv run scripts/extraction/runner.py --logs --transactions --from-block 18000000 --to-block 19000000 # or just --logs or --transactions, if --from-block defaulted to the contract creation block, --to-block defaulted to the latest block
```

