"""Custom exceptions for evm_sleuth package."""


class EVMSleuthError(Exception):
    """Base exception for evm_sleuth package."""
    pass


class APIError(EVMSleuthError):
    """Exception raised for API-related errors."""
    pass


class ConfigurationError(EVMSleuthError):
    """Exception raised for configuration-related errors."""
    pass


class DecodingError(EVMSleuthError):
    """Exception raised for decoding-related errors."""
    pass


class PipelineError(EVMSleuthError):
    """Exception raised for pipeline-related errors."""
    pass