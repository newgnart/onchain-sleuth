{{ config(materialized='table') }}

{{ erc20_event_model('transfer', {
    'from_address': {'topic': 'topic1', 'type': 'address'},
    'to_address': {'topic': 'topic2', 'type': 'address'},
    'amount': {'topic': 'data', 'type': 'uint256'}
}) }} 