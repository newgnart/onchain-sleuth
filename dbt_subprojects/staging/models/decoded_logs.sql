{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['address', 'block_number'], 'type': 'btree'},
            {'columns': ['topic0'], 'type': 'btree'},
            {'columns': ['transaction_hash'], 'type': 'btree'},
            {'columns': ['datetime'], 'type': 'btree'}
        ]
    )
}}

select 
    chainid,
    address,
    topics::json->>0 as topic0,
    case when json_array_length(topics::json) >= 2 then topics::json->>1 end as topic1,
    case when json_array_length(topics::json) >= 3 then topics::json->>2 end as topic2,
    case when json_array_length(topics::json) >= 4 then topics::json->>3 end as topic3,
    data,
    block_number,
    to_timestamp(time_stamp) as datetime,
    transaction_hash,
    log_index
from {{ source('etherscan_raw', 'logs') }}
where topics::json->>0 is not null