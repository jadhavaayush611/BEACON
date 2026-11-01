"""Consistent typed result structure for BEACON ingestion operations."""

from typing import Any, Optional
from pydantic import BaseModel, ConfigDict, Field

from beacon.models import DisasterObservation, ExtractionOutcome, ExtractionStatus, SourceRecord


class IngestionResult(BaseModel):
    """Encapsulates the complete outcome of ingesting a single source file.

    Includes the primary SourceRecord, all validated DisasterObservation records,
    execution status, count metrics, semantic outcome classification, and diagnostic messages.
    """

    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
    )

    source_record: SourceRecord = Field(
        description="Source document provenance and ingestion metadata record",
    )
    observations: list[DisasterObservation] = Field(
        default_factory=list,
        description="List of valid DisasterObservation records extracted from the source",
    )
    status: ExtractionStatus = Field(
        default=ExtractionStatus.PENDING,
        description="High-level processing status (success, partial, failed)",
    )
    outcome: ExtractionOutcome = Field(
        default=ExtractionOutcome.OBSERVATIONS_EXTRACTED,
        description="Semantic outcome classification of the extraction operation",
    )
    document_metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Structured non-observation metadata extracted from document",
    )
    explanation: Optional[str] = Field(
        default=None,
        description="Concise human-readable explanation of the extraction outcome",
    )
    errors: list[str] = Field(
        default_factory=list,
        description="Actionable, non-secret diagnostic messages describing parsing or validation errors",
    )
    warnings: list[str] = Field(
        default_factory=list,
        description="Non-fatal warnings regarding unextractable content, partial records, or format limitations",
    )
    processed_count: int = Field(
        default=0,
        description="Total candidate observation records or blocks detected in the input",
    )
    accepted_count: int = Field(
        default=0,
        description="Total records successfully validated and included in observations",
    )
    rejected_count: int = Field(
        default=0,
        description="Total records that failed parsing or contract validation and were skipped",
    )

    @property
    def is_success(self) -> bool:
        """Return True if extraction completed without fatal errors."""
        return self.status == ExtractionStatus.SUCCESS

    @property
    def is_partial(self) -> bool:
        """Return True if some records succeeded and others failed."""
        return self.status == ExtractionStatus.PARTIAL

    @property
    def is_failed(self) -> bool:
        """Return True if extraction failed completely."""
        return self.status == ExtractionStatus.FAILED

    @property
    def yields_observations(self) -> bool:
        """Return True if source yielded one or more validated disaster observations."""
        return len(self.observations) > 0

    @property
    def yields_metadata_only(self) -> bool:
        """Return True if source produced valid document/dataset metadata but 0 observations."""
        return self.is_success and len(self.observations) == 0 and bool(self.document_metadata)
