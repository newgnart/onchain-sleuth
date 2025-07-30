{% macro decode_event_logs(logs_source) %}
    with event_signatures as (
        {{ get_event_signatures() }}
    ),
    
    contract_metadata as (
        {{ get_contract_metadata() }}
    ),
    
    base_logs as (
        select 
            chainid,
            address as contract_address,
            topics::json->>0 as topic0,
            case when json_array_length(topics::json) >= 2 then topics::json->>1 end as topic1,
            case when json_array_length(topics::json) >= 3 then topics::json->>2 end as topic2,
            case when json_array_length(topics::json) >= 4 then topics::json->>3 end as topic3,
            {{ clean_hex_field('data') }} as data,
            block_number,
            block_hash,
            to_timestamp(time_stamp) as block_timestamp,
            gas_price,
            gas_used,
            log_index,
            transaction_hash,
            transaction_index
        from {{ logs_source }}
        where topics::json->>0 is not null
    ),
    
    decoded_events as (
        select 
            l.*,
            coalesce(es.event_name, 'Unknown') as event_name,
            coalesce(es.event_category, 'Unknown') as event_category,
            es.event_signature,
            coalesce(es.event_abi, 'Unknown') as event_abi,
            coalesce(cm.project, 'unknown') as project,
            coalesce(cm.contract_name, 'Unknown') as contract_name,
            coalesce(cm.description, 'Unknown contract') as contract_description,
            
            -- Enhanced parameter decoding based on event type and category
            case 
                when es.event_name = 'Transfer' and es.event_category = 'ERC20' then 
                    json_build_object(
                        'from_address', {{ hex_to_address('l.topic1') }},
                        'to_address', {{ hex_to_address('l.topic2') }},
                        'amount_hex', {{ extract_hex_value('l.data') }},
                        'amount_numeric', {{ hex_to_numeric(extract_hex_value('l.data')) }}
                    )
                when es.event_name = 'Approval' and es.event_category = 'ERC20' then 
                    json_build_object(
                        'owner', {{ hex_to_address('l.topic1') }},
                        'spender', {{ hex_to_address('l.topic2') }},
                        'amount_hex', {{ extract_hex_value('l.data') }},
                        'amount_numeric', {{ hex_to_numeric(extract_hex_value('l.data')) }}
                    )
                when es.event_name = 'Deposit' and es.event_category = 'ERC4626' then 
                    json_build_object(
                        'caller', {{ hex_to_address('l.topic1') }},
                        'owner', {{ hex_to_address('l.topic2') }},
                        'assets_hex', {{ extract_hex_value("substring(l.data, 3, 64)") }},
                        'assets_numeric', {{ hex_to_numeric(extract_hex_value("substring(l.data, 3, 64)")) }},
                        'shares_hex', {{ extract_hex_value("substring(l.data, 67, 64)") }},
                        'shares_numeric', {{ hex_to_numeric(extract_hex_value("substring(l.data, 67, 64)")) }}
                    )
                when es.event_name in ('Mint', 'Redeem') and es.event_category in ('EthenaV1', 'EthenaV2') then 
                    json_build_object(
                        'caller', {{ hex_to_address('l.topic1') }},
                        'benefactor', {{ hex_to_address('l.topic2') }},
                        'beneficiary', {{ hex_to_address('l.topic3') }},
                        'collateral_asset', {{ hex_to_address("substring(l.data, 3, 64)") }},
                        'collateral_amount', {{ hex_to_numeric("substring(l.data, 67, 64)") }},
                        'usde_amount', {{ hex_to_numeric("substring(l.data, 131, 64)") }}
                    )
                when es.event_name = 'OwnershipTransferred' then
                    json_build_object(
                        'previous_owner', {{ hex_to_address('l.topic1') }},
                        'new_owner', {{ hex_to_address('l.topic2') }}
                    )
                when es.event_name in ('Paused', 'Unpaused') then
                    json_build_object(
                        'account', {{ hex_to_address('l.topic1') }}
                    )
                when es.event_name in ('RoleGranted', 'RoleRevoked') then
                    json_build_object(
                        'role', l.topic1,
                        'account', {{ hex_to_address('l.topic2') }},
                        'sender', {{ hex_to_address('l.topic3') }}
                    )
                else 
                    json_build_object(
                        'topic1', l.topic1,
                        'topic2', l.topic2,
                        'topic3', l.topic3,
                        'data_hex', l.data,
                        'raw_decoding', true
                    )
            end as decoded_params
            
        from base_logs l
        left join event_signatures es on l.topic0 = es.event_signature
        left join contract_metadata cm on lower(l.contract_address) = lower(cm.contract_address)
    )
    
    select * from decoded_events
{% endmacro %}