{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['token_address', 'block_number'], 'type': 'btree'},
            {'columns': ['from_address'], 'type': 'btree'},
            {'columns': ['to_address'], 'type': 'btree'},
            {'columns': ['transaction_hash'], 'type': 'btree'},
            {'columns': ['datetime'], 'type': 'btree'}
        ]
    )
}}

select 
    chainid,
    address as token_address,
    -- Decode the data column for Transfer(address,address,uint256)
    -- topic1 contains the from_address (indexed parameter) - use macro for proper address format
    {{ from_uint256_to_address('topic1') }} as from_address,
    -- topic2 contains the to_address (indexed parameter) - use macro for proper address format
    {{ from_uint256_to_address('topic2') }} as to_address,
    -- data contains only the amount (non-indexed parameter)
    -- Note: Amount kept as hex string due to uint256 values exceeding PostgreSQL bigint range
    -- For conversion to decimal, consider using external tools or custom functions
    data as amount_hex,
    data,
    block_number,
    datetime,
    transaction_hash,
    log_index
from {{ ref('decoded_logs') }}
where topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'