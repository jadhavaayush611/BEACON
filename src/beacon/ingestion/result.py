"""Consistent typed result structure for BEACON ingestion operations."""

from pydantic import BaseModel, ConfigDict, Field

from beacon.models import DisasterObservation, ExtractionStatus, SourceRecord


class IngestionResult(BaseModel):
    """Encapsulates the complete outcome of ingesting a single source file.

    Includes the primary SourceRecord, all validated DisasterObservation records,
    execution status, count metrics, and diagnostic messages.
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
