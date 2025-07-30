{% macro get_contract_metadata() %}
    select * from (
        values 
            -- Ethena Protocol Contracts
            ('0x4c9edd5852cd905f086c759e8383e09bff1e68b3', 'ethena', 'USDe', 'ERC20 token contract for USDe'),
            ('0x323c03c48660fe31186fa82c289b0766d331ce21', 'ethena', 'EthenaMintRedeemV1', 'Ethena mint/redeem contract version 1'),
            ('0x57e4d5d1be2f24995b14a7b5e3f5cb71cb786b88', 'ethena', 'EthenaMintRedeemV2', 'Ethena mint/redeem contract version 2'),
            
            -- Open Protocol Contracts  
            ('0x58b6a8a3302369daec383334672404ee733ab239', 'open', 'OPEN', 'OPEN token contract'),
            
            -- Other known contracts can be added here
            ('0xa0b86a33e6b6c8e4f5b55e4e1b2b4c9d5c8e4f5b5', 'unknown', 'Generic', 'Unknown contract')
    ) as t(contract_address, project, contract_name, description)
{% endmacro %}

{% macro get_event_signatures() %}
    select * from (
        values 
            -- Standard ERC20 Events
            ('0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef', 'Transfer', 'ERC20', 'Transfer(address,address,uint256)'),
            ('0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925', 'Approval', 'ERC20', 'Approval(address,address,uint256)'),
            
            -- Standard ERC4626 Events
            ('0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c', 'Deposit', 'ERC4626', 'Deposit(address,address,uint256,uint256)'),
            ('0xfbde797d201c681b91056529119e0b02407c7bb96a4a2c75c01fc9667232c8db', 'Withdraw', 'ERC4626', 'Withdraw(address,address,address,uint256,uint256)'),
            
            -- Ethena V1 Custom Events
            ('0xf114ca9eb82947af39f957fa726280fd3d5d81c3d7635a4aeb5c302962856eba', 'Mint', 'EthenaV1', 'Mint(address,address,address,address,uint256,uint256)'),
            ('0x18fd144d7dbcbaa6f00fd47a84adc7dc3cc64a326ffa2dc7691a25e3837dba03', 'Redeem', 'EthenaV1', 'Redeem(address,address,address,address,uint256,uint256)'),
            
            -- Ethena V2 Custom Events  
            ('0x29ee92e51cda311463f5c9ef98c54824a4bebe45e689c37da35edc774585d437', 'Mint', 'EthenaV2', 'Mint(address,address,address,address,uint256,uint256)'),
            ('0x0ea36c5b7b274f8fe58654fe884bb9307dec1899e0312f40ae10d9b3d100cc0c', 'Redeem', 'EthenaV2', 'Redeem(address,address,address,address,uint256,uint256)'),
            
            -- Common DeFi Events
            ('0x2f00e3cdd69a77be7ed215ec7b2a36784dd158f921fca79ac29deffa353fe6ee', 'Mint', 'Custom', 'Custom mint event'),
            ('0x222838db2794d11532d940e8dec38ec41d52c2a5db0f34b6e7e9b8b83b8b9f80', 'Redeem', 'Custom', 'Custom redeem event'),
            
            -- Ownership Events
            ('0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0', 'OwnershipTransferred', 'Ownable', 'OwnershipTransferred(address,address)'),
            
            -- Pause Events
            ('0x62e78cea01bee320cd4e420270b5ea74000d11b0c9f74754ebdbfc544b05a258', 'Paused', 'Pausable', 'Paused(address)'),
            ('0x5db9ee0a495bf2e6ff9c91a7834c1ba4fdd244a5e8aa4e537bd38aeae4b073aa', 'Unpaused', 'Pausable', 'Unpaused(address)'),
            
            -- Role-based Access Control
            ('0x2f8788117e7eff1d82e926ec794901d17c78024a50270940304540a733656f0d', 'RoleGranted', 'AccessControl', 'RoleGranted(bytes32,address,address)'),
            ('0xf6391f5c32d9c69d2a47ea670b442974b53935d1edc7fd64eb21e047a839171b', 'RoleRevoked', 'AccessControl', 'RoleRevoked(bytes32,address,address)')
    ) as t(event_signature, event_name, event_category, event_abi)
{% endmacro %}