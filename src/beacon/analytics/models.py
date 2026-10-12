"""Pydantic schemas and summary models for disaster intelligence analytics."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class AnalyticsFilter(BaseModel):
    """Optional filter criteria for slicing intelligence analytics."""

    model_config = ConfigDict(extra="forbid")

    event_type: Optional[str] = None
    severity: Optional[str] = None
    location: Optional[str] = None
    min_sources: Optional[int] = Field(default=None, ge=1)
    start_time_after: Optional[datetime] = None
    end_time_before: Optional[datetime] = None


class CoverageGaps(BaseModel):
    """Metrics indicating missing spatio-temporal data across observations and events."""

    model_config = ConfigDict(extra="forbid")

    events_missing_coordinates: int = 0
    events_unanchored_timestamps: int = 0
    observations_missing_coordinates: int = 0
    observations_missing_timestamps: int = 0


class AnalyticsSummary(BaseModel):
    """Comprehensive intelligence report summarizing resolution and event metrics."""

    model_config = ConfigDict(extra="forbid")

    total_events: int = 0
    total_observations: int = 0
    compression_ratio: float = 0.0
    multi_source_events: int = 0
    single_source_events: int = 0
    multi_source_ratio: float = 0.0
    events_by_type: dict[str, int] = Field(default_factory=dict)
    events_by_severity: dict[str, int] = Field(default_factory=dict)
    events_by_location: dict[str, int] = Field(default_factory=dict)
    conflicting_claims_count: int = 0
    conflicting_claims_by_field: dict[str, int] = Field(default_factory=dict)
    coverage_gaps: CoverageGaps = Field(default_factory=CoverageGaps)
    ambiguous_observations_count: int = 0
    merge_candidates_count: int = 0
