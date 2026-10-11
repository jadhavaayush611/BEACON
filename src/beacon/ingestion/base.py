"""Base class and common utilities for format-specific ingestors."""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
import hashlib
from pathlib import Path
from typing import Any, Optional

from beacon.ingestion.result import IngestionResult
from beacon.models import (
    ExtractionMethod,
    ExtractionStatus,
    SourceRecord,
    SourceType,
    ensure_utc_aware,
    generate_source_id,
)


class BaseIngestor(ABC):
    """Abstract base class for all BEACON format-specific ingestors."""

    def __init__(self, source_type: SourceType, extraction_method: ExtractionMethod) -> None:
        self.source_type = source_type
        self.extraction_method = extraction_method

    @abstractmethod
    def ingest(self, path: Path) -> IngestionResult:
        """Parse the input file at path and return an IngestionResult."""
        pass

    @staticmethod
    def compute_sha256(path: Path) -> tuple[str, int]:
        """Compute SHA-256 hex digest and file size in bytes from disk."""
        hasher = hashlib.sha256()
        total_bytes = 0
        with open(path, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
                total_bytes += len(chunk)
        return hasher.hexdigest(), total_bytes

    def create_source_record(
        self,
        path: Path,
        content_hash: Optional[str],
        status: ExtractionStatus = ExtractionStatus.PENDING,
        raw_content: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SourceRecord:
        """Create a canonical SourceRecord instance with stable source_id."""
        normalized_path = str(path).replace("\\", "/")
        source_id = generate_source_id(self.source_type, normalized_path, content_hash)
        meta = dict(metadata or {})

        return SourceRecord(
            source_id=source_id,
            source_file=normalized_path,
            source_type=self.source_type,
            extraction_method=self.extraction_method,
            ingested_at=datetime.now(timezone.utc),
            extraction_status=status,
            content_hash=content_hash,
            raw_content=raw_content,
            metadata=meta,
        )

    @staticmethod
    def parse_datetime_to_utc(val: Any) -> Optional[datetime]:
        """Parse an ISO-8601 string or datetime to a timezone-aware UTC datetime.

        Returns None if input is None, empty string, or whitespace.
        Raises ValueError if date is unparseable or lacks timezone info.
        """
        if val is None:
            return None
        if isinstance(val, datetime):
            return ensure_utc_aware(val, "datetime")
        if isinstance(val, str):
            cleaned = val.strip()
            if not cleaned:
                return None
            # Standard ISO parsing (e.g. "2026-10-12T08:00:00Z")
            cleaned = cleaned.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(cleaned)
            return ensure_utc_aware(parsed, "datetime")
        raise ValueError(f"Unsupported datetime type: {type(val).__name__}")
