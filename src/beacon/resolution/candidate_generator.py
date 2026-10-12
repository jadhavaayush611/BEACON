"""Multi-attribute blocking and candidate generation for event resolution."""

from datetime import datetime, timedelta
import logging
from typing import Iterable, Optional

from beacon.config import Settings, get_settings
from beacon.models import DisasterObservation, UnifiedDisasterEvent
from beacon.pipeline.transform import COMMON_EVENT_ALIASES
from beacon.resolution.distance import haversine_distance_km

logger = logging.getLogger("beacon.resolution.candidate_generator")


class CandidateGenerator:
    """Filters candidate events for an observation using multi-attribute blocking."""

    def __init__(self, settings: Optional[Settings] = None) -> None:
        cfg = settings or get_settings()
        self.max_spatial_km = cfg.resolution_max_spatial_distance_km
        self.max_temporal_hours = cfg.resolution_max_temporal_distance_hours

    @staticmethod
    def normalize_type(raw_type: str) -> str:
        """Normalize event type to base form."""
        cleaned = raw_type.strip().lower()
        return COMMON_EVENT_ALIASES.get(cleaned, cleaned)

    def is_type_compatible(self, obs_type: str, event_type: str) -> bool:
        """Check if event categories are compatible."""
        t1 = self.normalize_type(obs_type)
        t2 = self.normalize_type(event_type)
        return t1 == t2 or t1 in t2 or t2 in t1

    def is_temporally_plausible(
        self,
        obs_time: Optional[datetime],
        event_start: Optional[datetime],
        event_end: Optional[datetime],
    ) -> bool:
        """Check if observation timestamp is temporally plausible with event window.

        If either has missing timestamp, returns True (cannot rule out based on time).
        """
        if obs_time is None or (event_start is None and event_end is None):
            return True

        t_min = event_start or event_end
        t_max = event_end or event_start
        if t_min is None or t_max is None:
            return True

        start_boundary = min(t_min, t_max) - timedelta(hours=self.max_temporal_hours)
        end_boundary = max(t_min, t_max) + timedelta(hours=self.max_temporal_hours)

        return start_boundary <= obs_time <= end_boundary

    def is_spatially_plausible(
        self,
        obs_lat: Optional[float],
        obs_lon: Optional[float],
        event: UnifiedDisasterEvent,
    ) -> bool:
        """Check if observation coordinates are spatially plausible with event centroid.

        If either has missing coordinates, returns True (cannot rule out based on location).
        """
        if obs_lat is None or obs_lon is None:
            return True

        centroid = (event.metadata or {}).get("centroid", {})
        ev_lat = centroid.get("latitude")
        ev_lon = centroid.get("longitude")

        if ev_lat is None or ev_lon is None:
            return True

        try:
            dist = haversine_distance_km(obs_lat, obs_lon, ev_lat, ev_lon)
            # Allow 1.5x buffer for candidate generation blocking
            return dist <= (self.max_spatial_km * 1.5)
        except ValueError:
            return True

    def generate_candidates(
        self,
        observation: DisasterObservation,
        existing_events: Iterable[UnifiedDisasterEvent],
    ) -> list[UnifiedDisasterEvent]:
        """Filter existing events down to plausible candidates for detailed scoring."""
        candidates: list[UnifiedDisasterEvent] = []

        for event in existing_events:
            # 1. Type blocking (hard filter)
            if not self.is_type_compatible(observation.event_type, event.event_type):
                continue

            # 2. Temporal blocking
            if not self.is_temporally_plausible(
                observation.event_time, event.start_time, event.end_time
            ):
                continue

            # 3. Spatial blocking
            if not self.is_spatially_plausible(
                observation.latitude, observation.longitude, event
            ):
                continue

            candidates.append(event)

        return candidates
