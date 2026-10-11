"""Canonical Pydantic data contracts for the BEACON intelligence pipeline.

Defines the core models:
- SourceRecord: Metadata and tracking for an ingested raw source document.
- DisasterObservation: Normalized atomic claim extracted from a source document.
- UnifiedDisasterEvent: Consolidated disaster event profile synthesized from observations.
"""

from datetime import datetime, timezone
from enum import Enum
import hashlib
import re
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


# ==============================================================================
# Enumerations and Controlled Vocabularies
# ==============================================================================

class SourceType(str, Enum):
    """Supported input source document and channel types."""
    PDF = "pdf"
    JSON = "json"
    CSV = "csv"
    TXT = "txt"
    IMAGE = "image"
    API = "api"


class ExtractionMethod(str, Enum):
    """Technique used to extract observations from raw sources."""
    JSON_PARSE = "json_parse"
    CSV_PARSE = "csv_parse"
    TEXT_READ = "text_read"
    PDF_TEXT = "pdf_text"
    OCR = "ocr"
    API_FETCH = "api_fetch"


class ExtractionStatus(str, Enum):
    """Processing status of a source ingestion operation."""
    PENDING = "pending"
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"


class SeverityLevel(str, Enum):
    """Normalized categorical disaster severity rating."""
    MINOR = "minor"
    MODERATE = "moderate"
    SEVERE = "severe"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


# Standard reference disaster event categories (extensible)
STANDARD_EVENT_TYPES: set[str] = {
    "flood",
    "wildfire",
    "earthquake",
    "cyclone",
    "hurricane",
    "tornado",
    "tsunami",
    "landslide",
    "drought",
    "storm",
    "volcanic_eruption",
    "industrial_accident",
    "infrastructure_failure",
}

# Forbidden metadata keywords indicating potential secret leaks
FORBIDDEN_SECRET_KEYS: set[str] = {
    "password",
    "secret",
    "token",
    "api_key",
    "apikey",
    "access_token",
    "private_key",
    "auth_token",
    "authorization",
}


# ==============================================================================
# Timestamp and Validation Helpers
# ==============================================================================

def ensure_utc_aware(dt: Optional[datetime], field_name: str = "datetime") -> Optional[datetime]:
    """Validate that datetime is timezone-aware and convert it to UTC."""
    if dt is None:
        return None
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        raise ValueError(f"{field_name} must be timezone-aware (tzinfo is required).")
    return dt.astimezone(timezone.utc)


# ==============================================================================
# Deterministic Identifier Generators
# ==============================================================================

def generate_source_id(source_type: str | SourceType, source_file: str, content_hash: Optional[str] = None) -> str:
    """Generate a deterministic, rerun-stable source ID.

    Derived from source type, normalized filename, and optional content hash.
    """
    st_val = source_type.value if isinstance(source_type, SourceType) else str(source_type).lower()
    norm_file = source_file.replace("\\", "/").strip().lower()
    raw = f"{st_val}::{norm_file}"
    if content_hash:
        raw += f"::{content_hash.lower()}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"src_{digest}"


def generate_observation_id(
    source_id: str,
    source_reference: Optional[str] = None,
    raw_claim: Optional[str] = None,
    index: Optional[int] = None,
) -> str:
    """Generate a deterministic, rerun-stable observation ID.

    Derived from source ID, source-internal locator/reference, and claim content.
    """
    elements = [source_id]
    if source_reference:
        elements.append(source_reference.strip().lower())
    if raw_claim:
        elements.append(raw_claim.strip())
    if index is not None:
        elements.append(str(index))
    raw = "::".join(elements)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"obs_{digest}"


def generate_event_id(event_type: str, location: str, start_time: datetime) -> str:
    """Generate a deterministic, rerun-stable event ID for a consolidated disaster incident.

    Derived from normalized event type, location, and ISO-formatted UTC start time.
    """
    utc_time = ensure_utc_aware(start_time, "start_time")
    assert utc_time is not None
    time_str = utc_time.isoformat()
    raw = f"{event_type.strip().lower()}::{location.strip().lower()}::{time_str}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"evt_{digest}"


# ==============================================================================
# Canonical Data Contract Models
# ==============================================================================

class SourceRecord(BaseModel):
    """Represents an ingested raw source or source document.

    Preserves provenance, ingestion audit trail, extraction state,
    and metadata without storing sensitive credentials.
    """

    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
        extra="forbid",
    )

    source_id: str = Field(
        description="Stable deterministic identifier for the source",
    )
    source_file: str = Field(
        description="Original filename or URI reference",
    )
    source_type: SourceType = Field(
        description="Format type of the source",
    )
    extraction_method: ExtractionMethod = Field(
        description="Extraction method applied or scheduled",
    )
    ingested_at: datetime = Field(
        description="Timezone-aware UTC timestamp when document was ingested",
    )
    extraction_status: ExtractionStatus = Field(
        default=ExtractionStatus.PENDING,
        description="Execution status of the ingestion/extraction process",
    )
    content_hash: Optional[str] = Field(
        default=None,
        description="Optional SHA-256 hash of the source raw bytes",
    )
    raw_content: Optional[str] = Field(
        default=None,
        description="Extracted raw text, payload preview, or storage pointer",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Format-specific metadata (file size, mime type, headers)",
    )

    @field_validator("ingested_at")
    @classmethod
    def validate_ingested_at(cls, v: datetime) -> datetime:
        res = ensure_utc_aware(v, "ingested_at")
        assert res is not None
        return res

    @field_validator("content_hash")
    @classmethod
    def validate_content_hash(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        v = v.strip().lower()
        if not re.fullmatch(r"[a-f0-9]{64}", v):
            raise ValueError("content_hash must be a valid 64-character hexadecimal SHA-256 string")
        return v

    @field_validator("metadata")
    @classmethod
    def validate_metadata_secrets(cls, meta: dict[str, Any]) -> dict[str, Any]:
        """Verify that metadata does not contain passwords, API tokens, or secrets."""
        for key in meta.keys():
            k_lower = str(key).lower()
            for forbidden in FORBIDDEN_SECRET_KEYS:
                if forbidden in k_lower:
                    raise ValueError(
                        f"Forbidden credential key '{key}' found in metadata. "
                        "Do not store passwords, API tokens, or secrets in source metadata."
                    )
        return meta


class DisasterObservation(BaseModel):
    """Represents a claim or observation extracted from a single source.

    An observation is NOT a confirmed fact; it captures the extracted claim,
    retains original evidence, and records validation warnings without
    fabricating missing values.
    """

    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
        extra="forbid",
    )

    observation_id: str = Field(
        description="Stable deterministic identifier for the observation",
    )
    source_id: str = Field(
        description="Reference to the originating source document ID",
    )
    event_type: str = Field(
        description="Normalized category or extensible disaster event string",
    )
    location: Optional[str] = Field(
        default=None,
        description="Human-readable location reported by the source",
    )
    event_time: Optional[datetime] = Field(
        default=None,
        description="Event timestamp reported by the source. None if unstated. Never defaulted to ingestion time.",
    )
    description: Optional[str] = Field(
        default=None,
        description="Extracted textual description or narrative. Blank strings normalized to None.",
    )
    severity: Optional[str] = Field(
        default=None,
        description="Normalized severity rating (e.g. minor, moderate, severe, critical) or source-specific level",
    )
    latitude: Optional[float] = Field(
        default=None,
        description="Geographic latitude coordinate in range [-90.0, 90.0]",
    )
    longitude: Optional[float] = Field(
        default=None,
        description="Geographic longitude coordinate in range [-180.0, 180.0]",
    )
    extraction_confidence: Optional[float] = Field(
        default=None,
        description="Confidence score for this extraction in range [0.0, 1.0]",
    )
    page_number: Optional[int] = Field(
        default=None,
        description="Positive 1-based page number within source document (> 0)",
    )
    source_reference: Optional[str] = Field(
        default=None,
        description="Source locator (row number, json path, image region/bbox, table cell)",
    )
    raw_claim: Optional[str] = Field(
        default=None,
        description="Original unparsed text or verbatim claim from the source",
    )
    validation_warnings: list[str] = Field(
        default_factory=list,
        description="Non-fatal warnings recorded during ingestion and standardization",
    )

    @field_validator("event_time")
    @classmethod
    def validate_event_time(cls, v: Optional[datetime]) -> Optional[datetime]:
        return ensure_utc_aware(v, "event_time")

    @field_validator("latitude")
    @classmethod
    def validate_latitude(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (-90.0 <= v <= 90.0):
            raise ValueError(f"Latitude must be between -90.0 and 90.0 degrees, got {v}")
        return v

    @field_validator("longitude")
    @classmethod
    def validate_longitude(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (-180.0 <= v <= 180.0):
            raise ValueError(f"Longitude must be between -180.0 and 180.0 degrees, got {v}")
        return v

    @field_validator("extraction_confidence")
    @classmethod
    def validate_confidence(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (0.0 <= v <= 1.0):
            raise ValueError(f"extraction_confidence must be between 0.0 and 1.0, got {v}")
        return v

    @field_validator("page_number")
    @classmethod
    def validate_page_number(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v <= 0:
            raise ValueError(f"page_number must be a positive integer (> 0), got {v}")
        return v

    @model_validator(mode="before")
    @classmethod
    def normalize_fields_and_collect_warnings(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        warnings = list(data.get("validation_warnings") or [])

        # 1. Handle blank or whitespace-only description
        desc = data.get("description")
        if isinstance(desc, str):
            stripped_desc = desc.strip()
            if not stripped_desc:
                data["description"] = None
                warnings.append("Blank or whitespace-only description normalized to None.")
            else:
                data["description"] = stripped_desc

        # 2. Handle event_type validation and vocabulary extensibility
        ev_type = data.get("event_type")
        if ev_type is not None:
            if not isinstance(ev_type, str) or not ev_type.strip():
                raise ValueError("event_type must be a non-empty string.")
            norm_type = ev_type.strip().lower()
            data["event_type"] = norm_type
            if norm_type not in STANDARD_EVENT_TYPES:
                warnings.append(
                    f"Non-standard event category '{norm_type}' accepted as custom extensible type."
                )

        data["validation_warnings"] = warnings
        return data


class ConflictingClaim(BaseModel):
    """Retains a contradictory or divergent claim asserted by a specific source.

    Ensures that conflicting observations are not silently discarded during
    consolidation, preserving transparency and lineage.
    """

    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
        extra="forbid",
    )

    field_name: str = Field(
        description="Attribute with conflicting values (e.g. 'severity', 'casualty_count', 'event_time')",
    )
    observation_id: str = Field(
        description="Observation identifier asserting this claim",
    )
    source_id: str = Field(
        description="Source identifier origin of this claim",
    )
    claimed_value: Any = Field(
        description="Value asserted by this observation",
    )
    confidence: Optional[float] = Field(
        default=None,
        description="Extraction confidence of the conflicting claim",
    )
    resolution_status: str = Field(
        default="unresolved",
        description="Status: 'unresolved', 'accepted', 'superseded', 'flagged_for_review'",
    )
    notes: Optional[str] = Field(
        default=None,
        description="Rationale for how this conflict was weighed or flagged",
    )


class UnifiedDisasterEvent(BaseModel):
    """Consolidated disaster event synthesized from multiple observations.

    Represents a unified profile linked to contributing evidence while
    preserving contradictory claims and resolution rationale.
    """

    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
        extra="forbid",
    )

    event_id: str = Field(
        description="Stable deterministic identifier for the consolidated event",
    )
    event_type: str = Field(
        description="Consolidated disaster event category",
    )
    location: str = Field(
        description="Consolidated disaster location description",
    )
    start_time: datetime = Field(
        description="Consolidated event start time (UTC)",
    )
    end_time: Optional[datetime] = Field(
        default=None,
        description="Consolidated event conclusion time (UTC), if known",
    )
    summary: str = Field(
        description="Synthesized summary of the disaster incident",
    )
    severity: Optional[str] = Field(
        default=None,
        description="Consolidated event severity rating",
    )
    observation_ids: list[str] = Field(
        min_length=1,
        description="References to all supporting observation IDs contributing to this profile",
    )
    created_at: datetime = Field(
        description="Timestamp when event profile was first synthesized (UTC)",
    )
    updated_at: datetime = Field(
        description="Timestamp when event profile was last updated (UTC)",
    )
    conflicting_claims: list[ConflictingClaim] = Field(
        default_factory=list,
        description="Preserved contradictory claims across sources with resolution state",
    )
    resolution_rationale: Optional[str] = Field(
        default=None,
        description="Audit explanation of evidence and policies used to consolidate values",
    )
    source_ids: list[str] = Field(
        default_factory=list,
        description="List of distinct source document IDs contributing evidence",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Extensible event-level metadata (tags, coordinate bounding box)",
    )

    @field_validator("start_time", "created_at", "updated_at")
    @classmethod
    def validate_required_datetimes(cls, v: datetime, info: Any) -> datetime:
        res = ensure_utc_aware(v, info.field_name)
        assert res is not None
        return res

    @field_validator("end_time")
    @classmethod
    def validate_end_time(cls, v: Optional[datetime]) -> Optional[datetime]:
        return ensure_utc_aware(v, "end_time")

    @model_validator(mode="after")
    def validate_event_time_range(self) -> "UnifiedDisasterEvent":
        if self.end_time is not None and self.end_time < self.start_time:
            raise ValueError(
                f"end_time ({self.end_time.isoformat()}) cannot be earlier than "
                f"start_time ({self.start_time.isoformat()})"
            )
        return self
