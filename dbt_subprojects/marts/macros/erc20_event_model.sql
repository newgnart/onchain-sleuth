{% macro erc20_event_model(event_name, fields_map) %}
  {# 
    Generic macro for ERC20 events
    
    Args:
      event_name (str): Name of the event (e.g., 'transfer', 'approval')
      fields_map (dict): Mapping of output field names to their topic positions
                        Format: {'field_name': {'topic': 'topic1|topic2|topic3|data', 'type': 'address|uint256'}}
    
    Example usage:
    {{ erc20_event_model('transfer', {
        'from_address': {'topic': 'topic1', 'type': 'address'},
        'to_address': {'topic': 'topic2', 'type': 'address'},
        'amount': {'topic': 'data', 'type': 'uint256'}
    }) }}
  #}
  
  with decoded_{{ event_name }} as (
    select 
      chainid,
      address as token_address,
      
      {%- for field_name, field_config in fields_map.items() %}
      {%- if field_config.type == 'address' and field_config.topic in ['topic1', 'topic2', 'topic3'] %}
      {{ decode_topic_address(field_config.topic) }} as {{ field_name }},
      {%- elif field_config.type == 'uint256' and field_config.topic == 'data' %}
      {{ decode_uint256('data') }} as {{ field_name }},
      {%- elif field_config.type == 'uint256' and field_config.topic in ['topic1', 'topic2', 'topic3'] %}
      {{ decode_hex_to_decimal(field_config.topic) }} as {{ field_name }},
      {%- else %}
      {{ field_config.topic }} as {{ field_name }}, -- Raw field for unsupported type/topic combinations
      {%- endif %}
      {%- endfor %}
      
      block_number,
      datetime,
      transaction_hash,
      log_index
    from {{ source('staging', 'decoded_logs') }}
    where topic0 = {{ get_event_topic0(event_name) }}
  )
  
  select * from decoded_{{ event_name }}

{% endmacro %}