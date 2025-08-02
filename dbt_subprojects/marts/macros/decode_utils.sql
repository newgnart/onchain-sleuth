{% macro decode_address(hex_value, start_pos=3, target_length=40) %}
  '0x' || lpad(substring({{ hex_value }} from {{ start_pos }}), {{ target_length }}, '0')
{% endmacro %}

{% macro decode_uint256(hex_value, start_pos=3) %}
  CASE 
    WHEN length(substring({{ hex_value }} from {{ start_pos }})) <= 16 
    THEN ('x' || substring({{ hex_value }} from {{ start_pos }}))::bit(64)::bigint
    ELSE ('x' || right(substring({{ hex_value }} from {{ start_pos }}), 16))::bit(64)::bigint
  END
{% endmacro %}

{% macro decode_bytes32_to_address(bytes32_value) %}
  '0x' || lpad(substring({{ bytes32_value }} from 27), 40, '0')
{% endmacro %}

{% macro decode_topic_address(topic_field) %}
  {{ decode_bytes32_to_address(topic_field) }}
{% endmacro %}

{% macro decode_hex_to_decimal(hex_value, bit_size=256) %}
  ('x' || substring({{ hex_value }} from 3))::bit({{ bit_size }})::bigint
{% endmacro %}