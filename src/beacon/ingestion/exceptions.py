"""Typed exception hierarchy for the BEACON ingestion subsystem."""


class IngestionError(Exception):
    """Base exception for all ingestion-related errors."""


class SourceFileNotFoundError(IngestionError):
    """Raised when the specified source file does not exist or is inaccessible."""


class UnsupportedFormatError(IngestionError):
    """Raised when attempting to ingest a file with an unsupported format or extension."""


class CorruptFileError(IngestionError):
    """Raised when a file container or stream is malformed, corrupt, or unreadable."""


class RecordParsingError(IngestionError):
    """Raised internally when an individual record fails schema validation or normalization."""
