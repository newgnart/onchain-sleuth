# Event Decoder Architecture

This module provides a modular, object-oriented approach to decoding Ethereum event logs. The architecture follows DRY principles and separation of concerns.

## Architecture Overview

### Core Components

1. **EventSignatureGenerator** - Handles event signature generation and hashing
2. **EventDefinitionBuilder** - Builds event definitions from ABI
3. **ParameterDecoder** - Decodes individual parameters based on Solidity types
4. **DataDecoder** - Decodes packed data fields
5. **LogDecoder** - Handles complete log entry decoding
6. **EventDecoder** - Main decoder class that orchestrates the process
7. **EventDecoderFactory** - Factory for creating decoder instances

### Utility Classes

- **HexUtils** - Common hex string operations
- **TypeUtils** - Type-related utility functions
- **DecoderConfig** - Configuration management

### Interfaces

All components implement protocols defined in `interfaces.py` for better type safety and extensibility.

## Usage Examples

### Basic Usage

```python
from evm_sleuth.decoder import EventDecoderFactory

# Create decoder from ABI
decoder = EventDecoderFactory.create_from_abi(abi)

# Decode a log entry
decoded_event = decoder.decode_log_entry(address, topics, data)
```

### Advanced Usage

```python
from evm_sleuth.decoder import (
    EventSignatureGenerator,
    EventDefinitionBuilder,
    ParameterDecoder,
    DataDecoder,
    LogDecoder,
    EventDecoder
)

# Manual setup
signature_generator = EventSignatureGenerator()
event_builder = EventDefinitionBuilder(signature_generator)
parameter_decoder = ParameterDecoder()
data_decoder = DataDecoder(parameter_decoder)
log_decoder = LogDecoder(parameter_decoder, data_decoder)

# Create event definitions
events = event_builder.get_events(abi)
events_by_topic0 = {event.topic0: event for event in events}

# Create decoder
decoder = EventDecoder(events_by_topic0)
```

### Custom Configuration

```python
from evm_sleuth.decoder.config import DecoderConfig

config = DecoderConfig(
    param_size_bytes=32,
    param_size_hex=64,
    address_size_hex=40,
    abi_dir=Path("custom/abi"),
    event_dir=Path("custom/events")
)
```

## Key Improvements

### DRY Principles
- Common hex operations extracted to `HexUtils`
- Type checking logic centralized in `TypeUtils`
- Shared configuration management

### Modularity
- Each class has a single responsibility
- Dependencies injected through constructors
- Easy to test individual components

### Object-Oriented Design
- Clear interfaces defined with protocols
- Factory pattern for object creation
- Configuration objects instead of constants

### Extensibility
- Easy to add new parameter types
- Pluggable decoder components
- Custom configuration support

## File Structure

```
decoder/
├── __init__.py
├── decoder.py          # Main decoder classes
├── types.py           # Data classes
├── config.py          # Configuration management
├── utils.py           # Utility classes
├── interfaces.py      # Protocol definitions
├── example.py         # Usage examples
└── README.md         # This file
```

## Testing

Each component can be tested independently:

```python
# Test parameter decoder
decoder = ParameterDecoder()
result = decoder.decode_parameter("uint256", "0x64")  # Should return 100

# Test hex utils
normalized = HexUtils.normalize_hex("0x1234")  # Should return "1234"
```

## Migration from Old Code

The refactored code maintains backward compatibility through:

1. **Constants preserved** - Old constants still available
2. **Same public API** - `EventDecoder.decode_log_entry()` works the same
3. **Factory methods** - Easy migration path with `EventDecoderFactory`

Old code:
```python
decoder = EventDecoder(events_by_topic0)
```

New code:
```python
decoder = EventDecoderFactory.create_from_event_definitions(events)
``` 