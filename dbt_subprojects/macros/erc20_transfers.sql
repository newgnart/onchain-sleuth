{% macro extract_erc20_transfers(logs_ref, contract_name='', decimals=18) %}
select 
    chainid,
    contract_address as token_address,
    {{ hex_to_address('topic1') }} as from_address,
    {{ hex_to_address('topic2') }} as to_address,
    {{ extract_hex_value('data') }} as amount_hex,
    (
        {{ hex_to_numeric(extract_hex_value('data')) }} / power(10, {{ decimals }})
    ) as amount,
    block_number,
    block_hash,
    block_timestamp,
    gas_price,
    gas_used,
    log_index,
    transaction_hash,
    transaction_index
    {% if contract_name %}
    , '{{ contract_name }}' as contract_name
    {% endif %}
from {{ logs_ref }}
where topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'  -- Transfer event signature
    and topic1 is not null
    and topic2 is not null
order by block_number, log_index
{% endmacro %}