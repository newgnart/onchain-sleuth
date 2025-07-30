{% macro process_contract_logs(source_schema, source_table) %}
select distinct
    topics::json->>0 as topic0,
    case when json_array_length(topics::json) >= 2 then topics::json->>1 end as topic1,
    case when json_array_length(topics::json) >= 3 then topics::json->>2 end as topic2,
    case when json_array_length(topics::json) >= 4 then topics::json->>3 end as topic3,
    chainid,
    address as contract_address,
    {{ clean_hex_field('data') }} as data,
    block_number,
    block_hash,
    to_timestamp(time_stamp) as block_timestamp,
    gas_price,
    gas_used,
    log_index,
    transaction_hash,
    transaction_index
from {{ source(source_schema, source_table) }}
{% endmacro %}