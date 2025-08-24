# EVM-Sleuth
An Ethereum blockchain data engineering toolkit designed for extracting, transforming, and loading EVM blockchain data to PostgreSQL from various sources into analytics-ready formats.

The toolkit focuses on:

- Multi-source data ingestion from blockchain APIs (Etherscan, DeFiLlama, CoinGecko, etc.)
- Rate-limited API client management with robust error handling
- Pipeline orchestration using modern data tooling (dlt, dbt)
- PostgreSQL-based data warehousing with staging transformations

```
evm-sleuth/
├── src/evm_sleuth/          # Core Python package
│   ├── config/              # Settings and configuration
│   ├── core/                # Base classes and utilities
│   ├── datasource/          # API client implementations
│   ├── decoder/             # Event decoding logic
│   ├── dataloader/          # Pipeline management
│   └── utils/               # Helper functions
├── dbt_subprojects/         # dbt transformation projects
├── scripts/                 # Data loading and utility scripts
├── resource/                # ABI files and configuration
└── tests/                   # Test suite
```
## Architecture

The project follows a modular, layered architecture designed for scalability and maintainability:

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
├─────────────────────────────────────────────────────────────┤
│  Scripts & CLI Tools                                       │
│  • decoder.py - decoding utilities                   │
│  • load_crvusd.py - data loading script               │
│  • run_dbt.sh - dbt pipeline execution                     │
├─────────────────────────────────────────────────────────────┤
│                    Core Components                          │
├─────────────────────────────────────────────────────────────┤
│  • Decoder - Smart contract event/transaction parsing             │
│  • PipelineManager - DLT pipeline orchestration            │
│  • Rate Limiter - API call management                      │
├─────────────────────────────────────────────────────────────┤
│                   Data Sources                             │
├─────────────────────────────────────────────────────────────┤
│  • EtherscanClient - Blockchain data extraction            │
│  • DeFiLlamaClient - DeFi protocol data                   │
│  • BaseAPIClient - Extensible API framework               │
├─────────────────────────────────────────────────────────────┤
│                  Infrastructure                             │
├─────────────────────────────────────────────────────────────┤
│  • PostgreSQL - Data warehouse                             │
│  • dlt - Data loading & transformation                     │
│  • dbt - Data modeling & testing                           │
└─────────────────────────────────────────────────────────────┘
```

**Key Design Patterns:**
- **Strategy Pattern** - Multiple decoding strategies for different types of data
- **Factory Pattern** - Dynamic strategy creation based on requirements
- **Base Class Inheritance** - Consistent API client interfaces
- **Configuration Management** - Centralized settings with environment variable support

## Data Sources

**Blockchain APIs:**
- **Etherscan** - Contract logs, transactions, ABI retrieval
- **DeFiLlama** - Coin metadata, yield pools, protocol revenue, ...
- **CoinGecko** - Token price data and market information
- ... (more to come)

**Data Types Extraction:**
- Smart contract event logs with indexed and non-indexed parameters
- Transaction metadata (gas usage, timestamps, block numbers)
- Contract ABIs for decoding
- DeFi protocol metrics and historical data

**Rate Limiting & Error Handling:**
- Configurable rate limits per API endpoint, backoff for failed requests, error logging and retry mechanisms

## Data Pipeline

The data pipeline follows a modern ELT (Extract, Load, Transform) approach:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Extract   │───▶│    Load     │───▶│ Transform  │───▶│   Analyze   │
│             │    │             │    │             │    │             │
│ • API Calls │    │ • dlt       │    │ • dbt       │    │ • PostgreSQL│
│ • Rate      │    │ • PostgreSQL│    │ • Staging   │    │ • Analytics │
│   Limiting  │    │ • Raw Data  │    │ • Models    │    │ • Dashboards│
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

**Data Flow:**
1. **Raw Data Ingestion** - Contract logs, transactions, metadata, ...
2. **Event/Transaction Decoding** - ABI-based parsing with multiple strategies
3. **Staging Transformations** - Data cleaning, type casting
4. **Analytics Tables** - Business-ready models

## Data Transformation

**dbt Models Structure:**
```
staging/
├── decoded_logs.sql          # Event log parsing and indexing
├── schema.yml                # Model documentation and tests
└── sources.yml               # Source table definitions
```

**Key Transformations:**
- **Event Decoding** - Convert hex topics to readable event names
- **Data Type Casting** - Timestamp conversion, numeric formatting
- **Indexing** - Optimized database indexes for common query patterns
- **Data Validation** - Schema tests and data quality checks


## Getting Started

**Prerequisites:**
- Python 3.11+
- PostgreSQL 15+
- Docker & Docker Compose

**Quick Start:**
```bash
# Clone repository
git clone https://github.com/newgnart/evm-sleuth.git
cd evm-sleuth

# Install dependencies
uv sync

# Start PostgreSQL
docker-compose up -d

# Set environment variables
cp .env.example .env

# Run data pipeline, for example:
python scripts/stables/load_crvusd.py

# Execute dbt transformations
cd dbt_subprojects/staging
dbt run
```


## License

[License information to be added]

