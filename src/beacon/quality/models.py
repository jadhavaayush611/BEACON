"""Data Quality models and outcome schemas for BEACON."""

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field

from beacon.models import DisasterObservation


class RuleSeverity(str, Enum):
    """Severity classification for data-quality validation rules."""
    ERROR = "error"      # Fatal rule breach: record must be rejected from persistence
    WARNING = "warning"  # Non-fatal quality anomaly: record accepted but flagged
    INFO = "info"        # Informational quality audit notification


class ValidationOutcome(BaseModel):
    """Machine-readable outcome for a single data-quality check on a record."""

    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    record_id: str = Field(description="Observation identifier evaluated")
    source_id: str = Field(description="Originating source document identifier")
    rule_name: str = Field(description="Standardized name of the evaluated quality rule")
    field_name: Optional[str] = Field(default=None, description="Affected field or attribute name")
    severity: RuleSeverity = Field(description="Severity classification of the outcome")
    message: str = Field(description="Human-readable explanation of the validation result")
    passed: bool = Field(description="True if the record satisfied the rule, False otherwise")
    details: dict[str, Any] = Field(default_factory=dict, description="Diagnostic contextual metadata")


class QualityReport(BaseModel):
    """Consolidated summary of data-quality evaluations for a pipeline execution batch."""

    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    total_checked: int = Field(default=0, description="Total observations evaluated")
    passed_count: int = Field(default=0, description="Count of accepted observations without fatal errors")
    failed_count: int = Field(default=0, description="Count of rejected observations with fatal errors")
    warning_count: int = Field(default=0, description="Total warning-level anomalies detected")
    outcomes: list[ValidationOutcome] = Field(default_factory=list, description="All rule execution outcomes")
    accepted_records: list[DisasterObservation] = Field(default_factory=list, description="Accepted observation records")
    rejected_record_ids: list[str] = Field(default_factory=list, description="Identifiers of rejected observation records")

    def has_errors_for(self, observation_id: str) -> bool:
        """Check if an observation has any ERROR-level rule failures."""
        return any(
            o.record_id == observation_id and not o.passed and o.severity == RuleSeverity.ERROR
            for o in self.outcomes
        )

    def get_outcomes_for(self, observation_id: str) -> list[ValidationOutcome]:
        """Return all evaluation outcomes associated with a specific observation ID."""
        return [o for o in self.outcomes if o.record_id == observation_id]
