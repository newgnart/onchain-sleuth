# dbt Macros for Ethereum Data Processing

This directory contains reusable macros for processing Ethereum blockchain data in dbt models.

## Available Macros

### `from_uint256_to_address(topic_column)`
Converts a uint256 formatted address (32-byte padded) to proper Ethereum address format (20 bytes).

**Usage:**
```sql
{{ from_uint256_to_address('topic1') }} as from_address
```

**What it does:**
- Extracts the last 40 characters from a 32-byte padded topic field
- Adds '0x' prefix to create a valid Ethereum address
- Useful for converting indexed event parameters stored in topics

**Example:**
- Input: `0x00000000000000000000000066f66192f91eaa3d48c95ab6bdaf9ade26db06f1`
- Output: `0x66f66192f91eaa3d48c95ab6bdaf9ade26db06f1`

### `hex_to_bigint(hex_column)`
Converts hex data to bigint for smaller values that fit in bigint range.

### `extract_hex_data(hex_column)`
Extracts hex data without the 0x prefix.

### `is_zero_address(address_column)`
Checks if an address is the zero address (0x0000...).

## Usage in Models

```sql
select 
    {{ from_uint256_to_address('topic1') }} as from_address,
    {{ from_uint256_to_address('topic2') }} as to_address
from {{ ref('decoded_logs') }}
```

## Benefits

1. **Reusability**: Use the same macro across multiple models
2. **Maintainability**: Update address formatting logic in one place
3. **Readability**: Clear intent in the SQL code
4. **Consistency**: Ensures all address formatting follows the same pattern
