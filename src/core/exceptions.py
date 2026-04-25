"""Quant Trading System — Custom Exception Hierarchy.

All custom exceptions inherit from QuantBaseError for catch-all handling.
Each layer (connector, ingestion, storage, cleaning) has its own exception family.
"""


class QuantBaseError(Exception):
    """Base exception for the entire quant trading system."""

    def __init__(self, message: str, details: dict | None = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)


# =============================================================================
# Connector Exceptions
# =============================================================================


class ConnectorError(QuantBaseError):
    """Base exception for all exchange connector errors."""


class ConnectorAuthError(ConnectorError):
    """Authentication / login failure."""


class ConnectorRateLimitError(ConnectorError):
    """Rate limit exceeded — caller should back off and retry."""

    def __init__(self, message: str, retry_after: float | None = None, **kwargs):
        super().__init__(message, **kwargs)
        self.retry_after = retry_after


class ConnectorTimeoutError(ConnectorError):
    """Request or WebSocket operation timed out."""


class ConnectorDataError(ConnectorError):
    """Unexpected data format or missing data from exchange."""


class WebSocketError(ConnectorError):
    """WebSocket connection or streaming failure."""


# =============================================================================
# Ingestion Exceptions
# =============================================================================


class IngestionError(QuantBaseError):
    """Base exception for data ingestion failures."""


class HistoricalFetchError(IngestionError):
    """Failed to fetch historical data after retries."""


class LiveStreamError(IngestionError):
    """Live data streaming failure."""


class NormalizationError(IngestionError):
    """Failed to normalize data to the unified schema."""


# =============================================================================
# Storage Exceptions
# =============================================================================


class StorageError(QuantBaseError):
    """Base exception for database / storage operations."""


class DatabaseConnectionError(StorageError):
    """Cannot connect to TimescaleDB."""


class DatabaseWriteError(StorageError):
    """Failed to write records to the database."""


class DatabaseReadError(StorageError):
    """Failed to read records from the database."""


class MigrationError(StorageError):
    """Database migration / schema setup failure."""


# =============================================================================
# Cleaning Exceptions
# =============================================================================


class CleaningError(QuantBaseError):
    """Base exception for data cleaning / validation."""


class ValidationError(CleaningError):
    """Data failed validation checks."""


class AdjustmentError(CleaningError):
    """Corporate action adjustment failure."""


# =============================================================================
# Streaming Exceptions
# =============================================================================


class StreamingError(QuantBaseError):
    """Base exception for Redis streaming operations."""


class PublishError(StreamingError):
    """Failed to publish message to Redis Stream."""


class SubscribeError(StreamingError):
    """Failed to subscribe or consume from Redis Stream."""
