-- from uint256 to address
-- Usage: {{ uint256_to_address('topic1') }}


{% macro uint256_to_address(hex_string) %}
    '0x' || substring({{ hex_string }} from 27)
{% endmacro %}


{% macro uint256_to_numeric(hex_string) %}
    substring({{ hex_string }} from 3)
{% endmacro %}