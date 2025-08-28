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
    {{ uint256_to_address('topic1') }} as from_address,
    {{ uint256_to_address('topic2') }} as to_address,
    {{ uint256_to_numeric('data') }} as amount,
--    {{ 'data'}} as amount_hex,
    block_number,
    datetime,
    transaction_hash,
    log_index
from {{ ref('decoded_logs') }}
where topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'