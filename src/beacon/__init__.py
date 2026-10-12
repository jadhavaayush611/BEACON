"""BEACON: Multi-Format Disaster Intelligence Data Engineering Pipeline.

"One event, many sources, one trusted data profile."
"""

from beacon.config import Settings, get_settings
from beacon.ingestion import IngestionResult, ingest_file
from beacon.models import (
    STANDARD_EVENT_TYPES,
    ConflictingClaim,
    DisasterObservation,
    ExtractionMethod,
    ExtractionStatus,
    SeverityLevel,
    SourceRecord,
    SourceType,
    UnifiedDisasterEvent,
    generate_event_id,
    generate_observation_id,
    generate_source_id,
)
from beacon.analytics import AnalyticsFilter, AnalyticsService, AnalyticsSummary, CoverageGaps
from beacon.pipeline import ETLPipelineResult, ETLRunner, ObservationTransformer
from beacon.quality import DataQualityValidator, QualityReport, RuleSeverity, ValidationOutcome
from beacon.resolution import (
    CandidateGenerator,
    CandidateMatch,
    DecisionType,
    EventProfileAggregator,
    EventResolver,
    FeatureScores,
    MatchPolicy,
    MergeCandidate,
    ObservationMatcher,
    ResolutionOutcome,
    ResolutionResult,
    haversine_distance_km,
)
from beacon.storage import StorageRepository, create_db_engine, get_session, get_session_factory, init_db

__version__ = "0.4.0"

__all__ = [
    "__version__",
    "Settings",
    "get_settings",
    "ingest_file",
    "IngestionResult",
    "ETLRunner",
    "ETLPipelineResult",
    "ObservationTransformer",
    "DataQualityValidator",
    "QualityReport",
    "RuleSeverity",
    "ValidationOutcome",
    "StorageRepository",
    "create_db_engine",
    "get_session_factory",
    "get_session",
    "init_db",
    "SourceType",
    "ExtractionMethod",
    "ExtractionStatus",
    "SeverityLevel",
    "STANDARD_EVENT_TYPES",
    "ConflictingClaim",
    "SourceRecord",
    "DisasterObservation",
    "UnifiedDisasterEvent",
    "generate_source_id",
    "generate_observation_id",
    "generate_event_id",
    "EventResolver",
    "ObservationMatcher",
    "CandidateGenerator",
    "MatchPolicy",
    "EventProfileAggregator",
    "DecisionType",
    "ResolutionResult",
    "ResolutionOutcome",
    "CandidateMatch",
    "MergeCandidate",
    "FeatureScores",
    "haversine_distance_km",
    "AnalyticsService",
    "AnalyticsSummary",
    "AnalyticsFilter",
    "CoverageGaps",
]
