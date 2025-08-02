# Marts

This dbt subproject contains mart tables that transform staged data into business-ready models.

## Models

### transfers
- **Description**: ERC20 token transfer events decoded from raw logs
- **Source**: `staging.decoded_logs`
- **Filters**: 
  - `topic0 = {{ get_event_topic0('transfer') }}` (ERC20 Transfer event)
  - Excludes mint/burn events (addresses with all zeros)
- **Decoded Fields**:
  - `from_address`: Address sending tokens (decoded from topic1)
  - `to_address`: Address receiving tokens (decoded from topic2)  
  - `amount`: Token amount transferred (decoded from data field)

### approvals
- **Description**: ERC20 token approval events decoded from raw logs
- **Source**: `staging.decoded_logs`
- **Filters**: 
  - `topic0 = {{ get_event_topic0('approval') }}` (ERC20 Approval event)
- **Decoded Fields**:
  - `owner`: Address that owns the tokens (decoded from topic1)
  - `spender`: Address approved to spend tokens (decoded from topic2)
  - `amount`: Token amount approved (decoded from data field)

## Macros

### get_event_topic0(event_name)
Returns the topic0 signature for common Ethereum events:
- `transfer`: ERC20 Transfer event
- `approval`: ERC20 Approval event
- `mint`: Mint event
- `burn`: Burn event
- `swap`: Swap event
- `sync`: Sync event

## Configuration

- **Schema**: `marts`
- **Materialization**: Table with indexes on key columns
- **Dependencies**: Requires `staging.decoded_logs` to be built first

## Usage

```bash
# Run the marts models
cd dbt_subprojects/marts
dbt run

# Test the models
dbt test
``` 