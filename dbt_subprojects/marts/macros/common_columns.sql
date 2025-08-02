{% macro erc20_base_columns() %}
  - name: chainid
    description: "Chain ID (1 for Ethereum mainnet)"
    tests:
      - not_null
  - name: token_address
    description: "Address of the ERC20 token contract"
    tests:
      - not_null
  - name: block_number
    description: "Block number where event occurred"
    tests:
      - not_null
  - name: datetime
    description: "Timestamp of the event"
    tests:
      - not_null
  - name: transaction_hash
    description: "Hash of the transaction containing the event"
    tests:
      - not_null
  - name: log_index
    description: "Index of the log within the transaction"
    tests:
      - not_null
{% endmacro %}