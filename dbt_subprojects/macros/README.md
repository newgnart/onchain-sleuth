# Shared DBT Macros for Contract Analysis

This directory contains reusable macros for processing blockchain contract data across different dbt subprojects.

## Available Macros

### Hex Utilities (`hex_utils.sql`)
- `clean_hex_field(field_name)` - Cleans hex fields, returns null for empty/invalid values
- `extract_hex_value(field_name)` - Extracts hex value without 0x prefix, returns '0' for empty
- `hex_to_address(field_name)` - Converts hex field to proper address format (0x + 40 chars)

### Contract Logs (`contract_logs.sql`)
- `process_contract_logs(source_schema, source_table)` - Standardizes raw contract logs

### ERC20 Operations (`erc20_transfers.sql`)
- `extract_erc20_transfers(logs_ref, contract_name='')` - Extracts ERC20 Transfer events

### Contract Events (`contract_events.sql`)
- `filter_by_event_signature(logs_ref, event_signature, event_name='')` - Filters logs by event signature
- `filter_by_contract_address(logs_ref, contract_address, contract_name='')` - Filters logs by contract
- `extract_contract_deployment(logs_ref)` - Extracts contract deployment events

## Usage Examples

### Processing Raw Logs
```sql
{{ process_contract_logs('my_schema', 'raw_logs') }}
```

### Extracting ERC20 Transfers
```sql
{{ extract_erc20_transfers(ref('processed_logs'), 'USDC') }}
```

### Filtering by Event
```sql
{{ filter_by_event_signature(ref('logs'), '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef', 'Transfer') }}
```

### Chaining Macros
```sql
select * from (
  {{ extract_erc20_transfers(
      filter_by_contract_address(ref('logs'), '0xa0b86a33e6b52cab2a5a1e6d1a6c5434c55ae53b', 'USDE'),
      'USDE'
    ) }}
)
```

## Setting Up in New dbt Projects

Add to your `dbt_project.yml`:
```yaml
macro-paths: ["../macros"]
```

## Common Event Signatures
- ERC20 Transfer: `0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef`
- ERC20 Approval: `0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925`
- Contract Deployment: topic0 = null, topic1 = deployer