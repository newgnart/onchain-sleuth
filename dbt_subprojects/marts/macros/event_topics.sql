{% macro get_event_topic0(event_name) %}
    {% set event_topics = {
        'transfer': '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
        'approval': '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925'
    } %}
    
    {% if event_name in event_topics %}
        '{{ event_topics[event_name] }}'
    {% else %}
        {{ exceptions.raise_compiler_error("Unknown event name: " ~ event_name ~ ". Available events: " ~ event_topics.keys() | list | join(", ")) }}
    {% endif %}
{% endmacro %} 