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

__version__ = "0.2.0"

__all__ = [
    "__version__",
    "Settings",
    "get_settings",
    "ingest_file",
    "IngestionResult",
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
]
