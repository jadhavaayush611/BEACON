"""Observation transformation and normalization layer for BEACON pipeline."""

import logging
import re
from typing import Optional, Sequence

from beacon.models import (
    STANDARD_EVENT_TYPES,
    DisasterObservation,
    SeverityLevel,
)

logger = logging.getLogger("beacon.pipeline.transform")

# Common domain aliases mapping extracted natural terms to standard event types
COMMON_EVENT_ALIASES: dict[str, str] = {
    "flash flood": "flood",
    "riverine inundation": "flood",
    "riverine flood": "flood",
    "urban flood": "flood",
    "brushfire": "wildfire",
    "bushfire": "wildfire",
    "forest fire": "wildfire",
    "bush fire": "wildfire",
    "cyclonic storm": "cyclone",
    "tropical storm": "cyclone",
    "tremor": "earthquake",
    "rockfall": "landslide",
    "mudslide": "landslide",
}


class ObservationTransformer:
    """Normalizes extracted DisasterObservation fields into consistent canonical forms."""

    def __init__(self) -> None:
        pass

    def transform(self, obs: DisasterObservation) -> DisasterObservation:
        """Apply deterministic, justified normalizations to an observation record.

        Normalizes:
        - Location whitespace and formatting
        - Event category alias standardization
        - Geographic coordinate floating-point precision (rounded to 6 decimal places)
        - Severity scale lowercasing and formatting
        - Description whitespace stripping
        """
        norm_location = self.normalize_location(obs.location)
        norm_event_type = self.normalize_event_type(obs.event_type)
        norm_severity = self.normalize_severity(obs.severity)
        norm_lat = self.normalize_coordinate(obs.latitude)
        norm_lon = self.normalize_coordinate(obs.longitude)
        norm_description = self.normalize_description(obs.description)

        # Build a refreshed observation instance with preserved IDs and provenance
        return DisasterObservation(
            observation_id=obs.observation_id,
            source_id=obs.source_id,
            event_type=norm_event_type,
            location=norm_location,
            event_time=obs.event_time,
            description=norm_description,
            severity=norm_severity,
            latitude=norm_lat,
            longitude=norm_lon,
            extraction_confidence=obs.extraction_confidence,
            page_number=obs.page_number,
            source_reference=obs.source_reference,
            raw_claim=obs.raw_claim,
            validation_warnings=list(obs.validation_warnings),
        )

    def transform_batch(self, observations: Sequence[DisasterObservation]) -> list[DisasterObservation]:
        """Apply transformation across an entire batch of observations."""
        return [self.transform(o) for o in observations]

    @staticmethod
    def normalize_location(loc: Optional[str]) -> Optional[str]:
        """Normalize location text by collapsing whitespace and trimming punctuation."""
        if loc is None:
            return None
        cleaned = re.sub(r"\s+", " ", loc).strip()
        cleaned = cleaned.rstrip(",;:- ")
        return cleaned or None

    @staticmethod
    def normalize_event_type(ev: str) -> str:
        """Standardize event categories, lowercasing and mapping known aliases."""
        cleaned = ev.strip().lower()
        # Direct alias replacement if recognized
        if cleaned in COMMON_EVENT_ALIASES:
            return COMMON_EVENT_ALIASES[cleaned]
        return cleaned

    @staticmethod
    def normalize_severity(sev: Optional[str]) -> Optional[str]:
        """Normalize severity level string to standard lower-case scale."""
        if sev is None:
            return None
        cleaned = sev.strip().lower()
        if cleaned in ("unknown", "none", "n/a", ""):
            return None
        return cleaned

    @staticmethod
    def normalize_coordinate(coord: Optional[float]) -> Optional[float]:
        """Round coordinates to 6 decimal places (~0.1m precision) to remove float noise."""
        if coord is None:
            return None
        return round(coord, 6)

    @staticmethod
    def normalize_description(desc: Optional[str]) -> Optional[str]:
        """Normalize narrative description by collapsing multi-line whitespace."""
        if desc is None:
            return None
        cleaned = re.sub(r"\s+", " ", desc).strip()
        return cleaned or None
