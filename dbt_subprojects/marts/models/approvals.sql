{{ config(materialized='table') }}

{{ erc20_event_model('approval', {
    'owner': {'topic': 'topic1', 'type': 'address'},
    'spender': {'topic': 'topic2', 'type': 'address'},
    'amount': {'topic': 'data', 'type': 'uint256'}
}) }} 