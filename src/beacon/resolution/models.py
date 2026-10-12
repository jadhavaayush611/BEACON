"""Domain models and outcome schemas for BEACON event resolution."""

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field

from beacon.models import UnifiedDisasterEvent


class DecisionType(str, Enum):
    """Categorical outcome of observation-to-event comparison."""
    CONFIDENT_MATCH = "confident_match"
    AMBIGUOUS_CANDIDATE = "ambiguous_candidate"
    NO_MATCH = "no_match"
    MERGE_CANDIDATE = "merge_candidate"


class FeatureScores(BaseModel):
    """Granular feature-level comparison scores and diagnostics."""

    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    event_type_score: float = Field(ge=0.0, le=1.0, description="Category match score [0.0, 1.0]")
    spatial_score: Optional[float] = Field(default=None, ge=0.0, le=1.0, description="Coordinate proximity score")
    spatial_distance_km: Optional[float] = Field(default=None, ge=0.0, description="Calculated Haversine distance in km")
    temporal_score: Optional[float] = Field(default=None, ge=0.0, le=1.0, description="Timestamp proximity score")
    temporal_distance_hours: Optional[float] = Field(default=None, ge=0.0, description="Time delta in hours")
    location_text_score: Optional[float] = Field(default=None, ge=0.0, le=1.0, description="Location text token similarity")
    description_score: Optional[float] = Field(default=None, ge=0.0, le=1.0, description="Narrative token similarity")
    composite_score: float = Field(ge=0.0, le=1.0, description="Weighted composite match score")
    missing_features: list[str] = Field(default_factory=list, description="Features absent in evaluated records")
    evidence_summary: dict[str, Any] = Field(default_factory=dict, description="Audit breakdown of weights and inputs")


class CandidateMatch(BaseModel):
    """Evaluated match candidate between an observation and an existing event."""

    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    observation_id: str
    candidate_event_id: str
    decision: DecisionType
    score: float
    feature_scores: FeatureScores
    rationale: str


class MergeCandidate(BaseModel):
    """Audit record indicating two existing event profiles may refer to the same incident."""

    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    primary_event_id: str
    secondary_event_id: str
    bridging_observation_id: Optional[str] = None
    similarity_score: float
    rationale: str
    status: str = Field(default="pending_review", description="Status: 'pending_review', 'merged', 'rejected'")


class ResolutionOutcome(BaseModel):
    """Result of resolving a single observation into an event assignment."""

    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    observation_id: str
    assigned_event_id: Optional[str] = None
    decision: DecisionType
    score: float
    is_new_event: bool = False
    rationale: str
    feature_scores: Optional[FeatureScores] = None
    candidate_matches: list[CandidateMatch] = Field(default_factory=list)


class ResolutionResult(BaseModel):
    """Consolidated summary of an event resolution run."""

    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    total_observations: int = Field(default=0)
    events_created: int = Field(default=0)
    events_updated: int = Field(default=0)
    observations_assigned: int = Field(default=0)
    ambiguous_count: int = Field(default=0)
    outcomes: list[ResolutionOutcome] = Field(default_factory=list)
    merge_candidates: list[MergeCandidate] = Field(default_factory=list)
    resolved_events: list[UnifiedDisasterEvent] = Field(default_factory=list)
