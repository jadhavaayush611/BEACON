"""Structured results and audit metrics for the BEACON ETL pipeline."""

from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field

from beacon.ingestion.result import IngestionResult
from beacon.quality.models import QualityReport


class ETLPipelineResult(BaseModel):
    """Encapsulates the complete outcome of an ETL pipeline batch execution."""

    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    execution_id: str = Field(description="Deterministic or timestamped pipeline run identifier")
    execution_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp when the pipeline execution started",
    )
    files_discovered: int = Field(default=0, description="Total input files identified for processing")
    files_succeeded: int = Field(default=0, description="Number of files successfully ingested")
    files_failed: int = Field(default=0, description="Number of files that failed ingestion")

    observations_extracted: int = Field(default=0, description="Total raw observations extracted")
    observations_transformed: int = Field(default=0, description="Total observations normalized")
    observations_accepted: int = Field(default=0, description="Total observations passing data quality")
    observations_rejected: int = Field(default=0, description="Total observations failing data quality")
    records_persisted: int = Field(default=0, description="Total observation records committed to storage")

    storage_enabled: bool = Field(default=False, description="Whether database storage was enabled/configured")
    quality_report: QualityReport = Field(
        default_factory=QualityReport,
        description="Consolidated data quality validation audit report",
    )
    file_results: list[IngestionResult] = Field(
        default_factory=list,
        description="Individual file ingestion results",
    )
    errors: list[str] = Field(
        default_factory=list,
        description="Non-fatal and fatal pipeline execution errors",
    )
    warnings: list[str] = Field(
        default_factory=list,
        description="Pipeline execution warnings",
    )

    def to_dict(self) -> dict:
        """Serialize pipeline execution result to a dictionary representation."""
        return self.model_dump(mode="json")

    def summary(self) -> str:
        """Generate a human-readable execution summary report."""
        lines = [
            f"=== BEACON ETL Pipeline Run Summary ({self.execution_id}) ===",
            f"Timestamp: {self.execution_timestamp.isoformat()}",
            f"Files: {self.files_discovered} discovered, {self.files_succeeded} succeeded, {self.files_failed} failed",
            f"Observations: {self.observations_extracted} extracted, {self.observations_transformed} transformed",
            f"Data Quality: {self.observations_accepted} accepted, {self.observations_rejected} rejected",
            f"Persistence: {'Enabled' if self.storage_enabled else 'Disabled'} ({self.records_persisted} observations stored)",
        ]
        if self.file_results:
            lines.append("Processed Files:")
            for fr in self.file_results:
                lines.append(f"  - {fr.source_record.source_file} [{fr.source_record.extraction_status.value}] ({len(fr.observations)} obs)")
        if self.errors:
            lines.append(f"Errors ({len(self.errors)}):")
            for err in self.errors:
                lines.append(f"  - {err}")
        return "\n".join(lines)

