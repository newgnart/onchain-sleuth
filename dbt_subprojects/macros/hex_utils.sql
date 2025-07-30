{% macro clean_hex_field(field_name) %}
    case 
        when {{ field_name }} is null or {{ field_name }} = '' or {{ field_name }} = '0x' 
        then null
        else {{ field_name }}
    end
{% endmacro %}

{% macro extract_hex_value(field_name) %}
    case 
        when {{ field_name }} is null or {{ field_name }} = '' or {{ field_name }} = '0x' 
        then '0'
        else ltrim({{ field_name }}, '0x')
    end
{% endmacro %}

{% macro hex_to_address(field_name) %}
    case 
        when {{ field_name }} is null or length({{ field_name }}) < 42
        then null
        else '0x' || right({{ field_name }}, 40)
    end
{% endmacro %}

{% macro hex_to_numeric(hex_value) %}
    case 
        when {{ hex_value }} is null or {{ hex_value }} = '' or {{ hex_value }} = '0'
        then 0::numeric
        else (
            select sum(
                case 
                    when substr({{ hex_value }}, i, 1) between '0' and '9' 
                    then substr({{ hex_value }}, i, 1)::int
                    when upper(substr({{ hex_value }}, i, 1)) = 'A' then 10
                    when upper(substr({{ hex_value }}, i, 1)) = 'B' then 11
                    when upper(substr({{ hex_value }}, i, 1)) = 'C' then 12
                    when upper(substr({{ hex_value }}, i, 1)) = 'D' then 13
                    when upper(substr({{ hex_value }}, i, 1)) = 'E' then 14
                    when upper(substr({{ hex_value }}, i, 1)) = 'F' then 15
                    else 0
                end * power(16, length({{ hex_value }}) - i)
            )::numeric
            from generate_series(1, length({{ hex_value }})) as i
        )
    end
{% endmacro %}