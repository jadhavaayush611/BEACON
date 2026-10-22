"""BEACON Event Resolution and Entity Linking Subsystem."""

from beacon.resolution.aggregator import EventProfileAggregator
from beacon.resolution.candidate_generator import CandidateGenerator
from beacon.resolution.distance import (
    haversine_distance_km,
    spatial_proximity_score,
    temporal_distance_hours,
    temporal_proximity_score,
    token_similarity,
)
from beacon.resolution.matcher import ObservationMatcher
from beacon.resolution.models import (
    CandidateMatch,
    DecisionType,
    FeatureScores,
    MergeCandidate,
    ResolutionOutcome,
    ResolutionResult,
)
from beacon.resolution.policy import MatchPolicy
from beacon.resolution.resolver import AmbiguousObservationAssociationError, EventResolver

__all__ = [
    "AmbiguousObservationAssociationError",
    "CandidateGenerator",
    "CandidateMatch",
    "DecisionType",
    "EventProfileAggregator",
    "EventResolver",
    "FeatureScores",
    "MatchPolicy",
    "MergeCandidate",
    "ObservationMatcher",
    "ResolutionOutcome",
    "ResolutionResult",
    "haversine_distance_km",
    "spatial_proximity_score",
    "temporal_distance_hours",
    "temporal_proximity_score",
    "token_similarity",
]
