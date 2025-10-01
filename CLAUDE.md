# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

### Environment Setup
- `uv sync` - Install dependencies using UV package manager
- `cp .env.example .env` - Create environment file from template
- `docker-compose up -d` - Start PostgreSQL container

### Data Pipeline Commands
- `python scripts/stables/load_crvusd.py` - Run specific data loading script
- `./scripts/helpers/run_dbt.sh [subdirectory] [dbt_commands]` - Run dbt transformations
  - Examples: `./scripts/helpers/run_dbt.sh staging run`, `./scripts/helpers/run_dbt.sh run` (defaults to staging)
  - Available commands: run, test, compile, build, docs, seed, snapshot, source, freshness, debug

### Testing
- `uv run pytest` - Run test suite (uses pytest framework)

## High-Level Architecture

**onchain-sleuth** is an Ethereum blockchain data engineering toolkit following a modular ELT (Extract, Load, Transform) pipeline architecture with these key layers:

### Core Components
- **BaseAPIClient**: Abstract base class for all API clients with automatic rate limiting, retry logic, and error handling
- **AutoRegisterMeta**: Metaclass that automatically registers API clients and DLT sources with factory classes
- **RateLimitedSession**: HTTP session wrapper with configurable rate limiting strategies
- **PipelineManager**: Orchestrates DLT (Data Load Tool) pipelines for data extraction and loading

### Data Sources Layer
- **EtherscanClient**: Extracts blockchain data (logs, transactions, ABIs) from Etherscan API
- **DeFiLlamaClient**: Fetches DeFi protocol data, yield pools, and revenue metrics
- **CoinGeckoClient**: Retrieves token price data and market information
- All clients extend `BaseAPIClient` and are auto-registered via metaclass

### Data Processing
- **Event Decoders**: Multiple strategies for decoding smart contract events and transaction data
- **Data Transformers**: Utilities for data cleaning, type casting, and formatting
- **Pipeline Orchestration**: Uses `dlt` (Data Load Tool) for pipeline management with PostgreSQL as destination

### dbt Transformation Layer
- **Staging Models** (`dbt_subprojects/staging/`): Raw data cleaning and initial transformations
  - `decoded_logs.sql`: Event log parsing and indexing
  - `erc20_transfer.sql`: ERC20 token transfer events
  - `curve/pool.sql`: Curve protocol-specific models
- **Shared Macros** (`dbt_subprojects/macros/`): Reusable SQL utilities
- Default materialization: `table` (configured in `dbt_project.yml`)

### Configuration Management
- **Centralized Settings** (`src/onchaindata/config/settings.py`):
  - API keys and rate limits
  - Standardized column schemas for different data types
  - API endpoint URLs
- **Environment Variables**: Loaded via python-dotenv
- **Protocol Registry**: Manages protocol-specific configurations

### Key Design Patterns
- **Factory Pattern**: Dynamic client and source creation via `APIClientFactory` and `DLTSourceFactory`
- **Strategy Pattern**: Multiple decoding strategies for different data types
- **Template Method**: Base classes define workflow, subclasses implement specifics
- **Auto-Registration**: Metaclass automatically registers clients and sources on import

### Data Flow
1. **Extract**: API clients fetch data from various blockchain and DeFi sources
2. **Load**: DLT pipelines load raw data into PostgreSQL staging tables
3. **Transform**: dbt models process staging data into analytics-ready tables
4. **Rate Limiting**: All API calls are automatically rate-limited per source configuration

The project uses UV for Python package management, PostgreSQL 15+ for data storage, and follows modern data engineering practices with clear separation between extraction, loading, and transformation phases.