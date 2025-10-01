"""Custom exceptions for onchaindata package."""


class EVMSleuthError(Exception):
    """Base exception for onchaindata package."""

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
