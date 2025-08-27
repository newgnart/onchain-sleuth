-- Macro to convert uint256 formatted addresses to proper Ethereum address format
-- This macro extracts the last 40 characters (20 bytes) from a 32-byte padded topic field
-- to get the actual Ethereum address
-- Usage: {{ from_uint256_to_address('topic1') }}
{% macro from_uint256_to_address(topic_column) %}
    '0x' || substring({{ topic_column }} from 27)
{% endmacro %}

-- Macro to convert hex data to bigint (for smaller values that fit in bigint range)
-- Usage: {{ hex_to_bigint('data') }}
{% macro hex_to_bigint(hex_column) %}
    ('x' || substring({{ hex_column }} from 3))::bit(256)::bigint
{% endmacro %}

-- Macro to extract hex data without 0x prefix
-- Usage: {{ extract_hex_data('data') }}
{% macro extract_hex_data(hex_column) %}
    substring({{ hex_column }} from 3)
{% endmacro %}

-- Macro to check if address is zero address (0x0000...)
-- Usage: {{ is_zero_address('address_column') }}
{% macro is_zero_address(address_column) %}
    {{ address_column }} = '0x0000000000000000000000000000000000000000'
{% endmacro %}