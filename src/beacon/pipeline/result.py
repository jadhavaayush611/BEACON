"""Structured results and audit metrics for the BEACON ETL pipeline."""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field

from beacon.ingestion.result import IngestionResult
from beacon.logging_config import redact_sensitive_text
from beacon.quality.models import QualityReport


class RunStatus(str, Enum):
    """Execution status states for pipeline runs."""

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"


class InputFileExecutionRecord(BaseModel):
    """Structured per-input execution and status outcome."""

    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    source_file: str = Field(description="Normalized or relative path to the input file")
    source_type: str = Field(description="Source data format identifier")
    status: str = Field(description="Per-input execution status: success, partial, failed, skipped")
    extracted_count: int = Field(default=0, description="Total observations extracted")
    errors: list[str] = Field(default_factory=list, description="Sanitized per-input error messages")
    warnings: list[str] = Field(default_factory=list, description="Sanitized per-input warnings")


class PipelineRunRecord(BaseModel):
    """Persistent audit model representing a single batch or retry execution run."""

    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    run_id: str = Field(description="Deterministic or timestamped pipeline run identifier")
    status: RunStatus = Field(description="Final execution outcome status")
    started_at: datetime = Field(description="UTC timestamp when the pipeline execution started")
    completed_at: Optional[datetime] = Field(
        default=None,
        description="UTC timestamp when the pipeline execution finished",
    )
    duration_seconds: Optional[float] = Field(
        default=None,
        description="Total execution wall-clock time in seconds",
    )
    trigger_type: str = Field(
        default="manual",
        description="Trigger origin: 'manual', 'batch', or 'retry'",
    )
    retry_of_run_id: Optional[str] = Field(
        default=None,
        description="Parent execution ID if this run is a retry",
    )
    input_files: list[InputFileExecutionRecord] = Field(
        default_factory=list,
        description="Structured per-input execution records",
    )
    inputs_summary: dict[str, Any] = Field(
        default_factory=dict,
        description="Structured manifest of input files, statuses, and counts",
    )
    counts_summary: dict[str, Any] = Field(
        default_factory=dict,
        description="Consolidated observation, quality, and resolution counts",
    )
    failure_details: list[str] = Field(
        default_factory=list,
        description="Redacted error messages, diagnostics, and failure traces",
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Record insertion timestamp",
    )

    def get_failed_input_files(self) -> list[str]:
        """Return structured list of failed input paths without parsing human-readable error text."""
        if self.input_files:
            return [f.source_file for f in self.input_files if f.status == "failed"]
        if "failed_inputs" in self.inputs_summary:
            return list(self.inputs_summary["failed_inputs"])
        files_meta = self.inputs_summary.get("files", [])
        return [
            f["source_file"]
            for f in files_meta
            if isinstance(f, dict) and f.get("status") == "failed" and f.get("source_file")
        ]

    def get_all_input_files(self) -> list[str]:
        """Return structured list of all input paths."""
        if self.input_files:
            return [f.source_file for f in self.input_files]
        if "files" in self.inputs_summary:
            return [
                f["source_file"]
                for f in self.inputs_summary["files"]
                if isinstance(f, dict) and f.get("source_file")
            ]
        return []


class ETLPipelineResult(BaseModel):
    """Encapsulates the complete outcome of an ETL pipeline batch execution."""

    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    execution_id: str = Field(description="Deterministic or timestamped pipeline run identifier")
    execution_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp when the pipeline execution started",
    )
    completed_at: Optional[datetime] = Field(
        default=None,
        description="UTC timestamp when the pipeline execution finished",
    )
    duration_seconds: Optional[float] = Field(
        default=None,
        description="Total execution wall-clock time in seconds",
    )
    status: str = Field(
        default=RunStatus.COMPLETED.value,
        description="Execution status ('completed', 'failed', 'partial', 'running')",
    )
    trigger_type: str = Field(
        default="manual",
        description="Trigger origin: 'manual', 'batch', or 'retry'",
    )
    retry_of_run_id: Optional[str] = Field(
        default=None,
        description="Parent execution ID if this run is a retry",
    )
    files_discovered: int = Field(default=0, description="Total input files identified for processing")
    files_succeeded: int = Field(default=0, description="Number of files successfully ingested")
    files_partially_successful: int = Field(
        default=0,
        description="Number of files partially ingested with some records accepted and some rejected",
    )
    files_failed: int = Field(default=0, description="Number of files that failed ingestion")
    files_skipped: int = Field(default=0, description="Number of files skipped during processing")

    observations_extracted: int = Field(default=0, description="Total raw observations extracted")
    observations_transformed: int = Field(default=0, description="Total observations normalized")
    observations_accepted: int = Field(default=0, description="Total observations passing data quality")
    observations_rejected: int = Field(default=0, description="Total observations failing data quality")
    records_persisted: int = Field(default=0, description="Total observation records committed to storage")
    events_synthesized: int = Field(default=0, description="Total unified events created or updated through resolution")
    events_persisted: int = Field(default=0, description="Total unified event records committed to storage")
    resolution_summary: Optional[dict] = Field(default=None, description="Summary details of event resolution")

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

    @property
    def run_id(self) -> str:
        """Alias for execution_id for consistency with pipeline run record terminology."""
        return self.execution_id

    def to_run_record(self) -> PipelineRunRecord:
        """Convert result to a persistent PipelineRunRecord with sanitized errors."""
        input_file_records = []
        for fr in self.file_results:
            src = fr.source_record
            input_file_records.append(
                InputFileExecutionRecord(
                    source_file=src.source_file,
                    source_type=src.source_type.value,
                    status=fr.status.value,
                    extracted_count=len(fr.observations),
                    errors=[redact_sensitive_text(e) for e in fr.errors],
                    warnings=[redact_sensitive_text(w) for w in fr.warnings],
                )
            )

        failed_inputs = [r.source_file for r in input_file_records if r.status == "failed"]
        succeeded_inputs = [r.source_file for r in input_file_records if r.status == "success"]

        inputs_summary = {
            "total_inputs": self.files_discovered,
            "succeeded": self.files_succeeded,
            "partially_successful": self.files_partially_successful,
            "failed": self.files_failed,
            "skipped": self.files_skipped,
            "failed_inputs": failed_inputs,
            "succeeded_inputs": succeeded_inputs,
            "files": [r.model_dump() for r in input_file_records],
        }

        counts_summary = {
            "observations_extracted": self.observations_extracted,
            "observations_transformed": self.observations_transformed,
            "observations_accepted": self.observations_accepted,
            "observations_rejected": self.observations_rejected,
            "records_persisted": self.records_persisted,
            "events_synthesized": self.events_synthesized,
            "events_persisted": self.events_persisted,
            "storage_enabled": self.storage_enabled,
        }
        if self.resolution_summary:
            counts_summary["resolution"] = self.resolution_summary

        sanitized_errors = [redact_sensitive_text(err) for err in self.errors]

        try:
            status_enum = RunStatus(self.status)
        except ValueError:
            status_enum = RunStatus.FAILED if self.errors else RunStatus.COMPLETED

        return PipelineRunRecord(
            run_id=self.execution_id,
            status=status_enum,
            started_at=self.execution_timestamp,
            completed_at=self.completed_at,
            duration_seconds=self.duration_seconds,
            trigger_type=self.trigger_type,
            retry_of_run_id=self.retry_of_run_id,
            input_files=input_file_records,
            inputs_summary=inputs_summary,
            counts_summary=counts_summary,
            failure_details=sanitized_errors,
        )

    def to_dict(self) -> dict:
        """Serialize pipeline execution result to a dictionary representation."""
        return self.model_dump(mode="json")

    def summary(self) -> str:
        """Generate a human-readable execution summary report."""
        lines = [
            f"=== BEACON ETL Pipeline Run Summary ({self.execution_id}) ===",
            f"Status: {self.status.upper()} (Trigger: {self.trigger_type})",
            f"Timestamp: {self.execution_timestamp.isoformat()}",
            f"Files: {self.files_discovered} discovered, {self.files_succeeded} succeeded, "
            f"{self.files_partially_successful} partial, {self.files_failed} failed, {self.files_skipped} skipped",
            f"Observations: {self.observations_extracted} extracted, {self.observations_transformed} transformed",
            f"Data Quality: {self.observations_accepted} accepted, {self.observations_rejected} rejected",
            f"Persistence: {'Enabled' if self.storage_enabled else 'Disabled'} ({self.records_persisted} observations stored)",
        ]
        if self.duration_seconds is not None:
            lines.append(f"Duration: {self.duration_seconds:.2f}s")
        if self.retry_of_run_id:
            lines.append(f"Retry Of: {self.retry_of_run_id}")
        if self.file_results:
            lines.append("Processed Files:")
            for fr in self.file_results:
                lines.append(f"  - {fr.source_record.source_file} [{fr.source_record.extraction_status.value}] ({len(fr.observations)} obs)")
        if self.errors:
            lines.append(f"Errors ({len(self.errors)}):")
            for err in self.errors:
                lines.append(f"  - {redact_sensitive_text(err)}")
        return "\n".join(lines)
