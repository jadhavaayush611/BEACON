"""Feature comparison and scoring engine for event resolution."""

from datetime import datetime, timezone
import logging
from typing import Any, Optional, Union

from beacon.config import Settings, get_settings
from beacon.models import DisasterObservation, UnifiedDisasterEvent
from beacon.pipeline.transform import COMMON_EVENT_ALIASES
from beacon.resolution.distance import (
    haversine_distance_km,
    spatial_proximity_score,
    temporal_distance_hours,
    temporal_proximity_score,
    token_similarity,
)
from beacon.resolution.models import FeatureScores

logger = logging.getLogger("beacon.resolution.matcher")


class ObservationMatcher:
    """Computes explainable similarity scores between observations and disaster events."""

    def __init__(self, settings: Optional[Settings] = None) -> None:
        cfg = settings or get_settings()
        self.max_spatial_km = cfg.resolution_max_spatial_distance_km
        self.max_temporal_hours = cfg.resolution_max_temporal_distance_hours
        self.type_weight = cfg.resolution_type_weight
        self.spatial_weight = cfg.resolution_spatial_weight
        self.temporal_weight = cfg.resolution_temporal_weight
        self.text_weight = cfg.resolution_location_text_weight

    def normalize_type(self, raw_type: str) -> str:
        """Normalize event type and resolve standard domain aliases."""
        cleaned = raw_type.strip().lower()
        return COMMON_EVENT_ALIASES.get(cleaned, cleaned)

    def score_event_type(self, type1: str, type2: str) -> float:
        """Evaluate categorical event compatibility.

        Returns 1.0 for match/alias equivalence, 0.0 for incompatible categories.
        """
        t1 = self.normalize_type(type1)
        t2 = self.normalize_type(type2)
        if t1 == t2:
            return 1.0
        # If one is a subtype or exact substring (e.g. "flood" and "flash flood")
        if t1 in t2 or t2 in t1:
            return 0.8
        return 0.0

    def score_spatial(
        self,
        lat1: Optional[float],
        lon1: Optional[float],
        lat2: Optional[float],
        lon2: Optional[float],
    ) -> tuple[Optional[float], Optional[float]]:
        """Calculate spatial score [0.0, 1.0] and distance in km, or None if coordinates absent."""
        if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
            return None, None
        try:
            dist_km = haversine_distance_km(lat1, lon1, lat2, lon2)
            score = spatial_proximity_score(dist_km, self.max_spatial_km)
            return score, dist_km
        except ValueError as err:
            logger.warning("Coordinate evaluation error: %s", err)
            return None, None

    def score_temporal(
        self,
        obs_time: Optional[datetime],
        target_time: Optional[datetime],
        target_end_time: Optional[datetime] = None,
    ) -> tuple[Optional[float], Optional[float]]:
        """Calculate temporal score [0.0, 1.0] and delta in hours."""
        if obs_time is None or target_time is None:
            return None, None

        # If target has a defined time span and obs_time falls inside it
        if target_end_time is not None:
            t_min = min(target_time, target_end_time)
            t_max = max(target_time, target_end_time)
            if t_min <= obs_time <= t_max:
                return 1.0, 0.0
            # Distance to nearest bound
            delta_start = temporal_distance_hours(obs_time, t_min)
            delta_end = temporal_distance_hours(obs_time, t_max)
            dist_hours = min(delta_start, delta_end)
        else:
            dist_hours = temporal_distance_hours(obs_time, target_time)

        score = temporal_proximity_score(dist_hours, self.max_temporal_hours)
        return score, dist_hours

    def compare(
        self,
        obs: DisasterObservation,
        target: Union[DisasterObservation, UnifiedDisasterEvent],
    ) -> FeatureScores:
        """Execute feature-by-feature explainable comparison against an observation or event profile."""
        missing: list[str] = []
        evidence: dict[str, Any] = {}

        # 1. Event Type
        type_score = self.score_event_type(obs.event_type, target.event_type)
        evidence["event_type"] = {
            "obs": obs.event_type,
            "target": target.event_type,
            "score": type_score,
            "weight": self.type_weight,
        }

        # Hard veto: Disasters of completely different categories cannot co-refer
        if type_score == 0.0:
            return FeatureScores(
                event_type_score=0.0,
                composite_score=0.0,
                missing_features=missing,
                evidence_summary={
                    "veto": "Incompatible disaster categories",
                    **evidence,
                },
            )

        # 2. Spatial coordinates
        if isinstance(target, DisasterObservation):
            target_lat = target.latitude
            target_lon = target.longitude
            target_time = target.event_time
            target_end = None
            target_desc = target.description
        else:
            # UnifiedDisasterEvent
            centroid = (target.metadata or {}).get("centroid", {})
            target_lat = centroid.get("latitude")
            target_lon = centroid.get("longitude")
            target_time = target.start_time
            target_end = target.end_time
            target_desc = target.summary

        spatial_score, dist_km = self.score_spatial(
            obs.latitude, obs.longitude, target_lat, target_lon
        )
        if spatial_score is not None:
            evidence["spatial"] = {
                "distance_km": round(dist_km, 2) if dist_km else 0.0,
                "score": spatial_score,
                "weight": self.spatial_weight,
            }
        else:
            missing.append("spatial_coordinates")

        # 3. Temporal
        temp_score, dist_hours = self.score_temporal(obs.event_time, target_time, target_end)
        if temp_score is not None:
            evidence["temporal"] = {
                "distance_hours": round(dist_hours, 2) if dist_hours else 0.0,
                "score": temp_score,
                "weight": self.temporal_weight,
            }
        else:
            missing.append("event_timestamp")

        # 4. Textual location
        text_score = token_similarity(obs.location, target.location)
        if obs.location and target.location:
            evidence["location_text"] = {
                "obs_location": obs.location,
                "target_location": target.location,
                "score": text_score,
                "weight": self.text_weight,
            }
        else:
            missing.append("location_text")

        # 5. Narrative description
        desc_score: Optional[float] = None
        if obs.description and target_desc:
            desc_score = token_similarity(obs.description, target_desc)
            evidence["description"] = {
                "score": desc_score,
            }
        else:
            missing.append("description")

        # 6. Weighted composite calculation over available features
        active_weight = self.type_weight
        weighted_sum = type_score * self.type_weight

        if spatial_score is not None:
            active_weight += self.spatial_weight
            weighted_sum += spatial_score * self.spatial_weight
        elif text_score > 0.0:
            # If coordinates are missing, reallocate part of spatial weight to textual location
            active_weight += self.spatial_weight * 0.5
            weighted_sum += text_score * (self.spatial_weight * 0.5)

        if temp_score is not None:
            active_weight += self.temporal_weight
            weighted_sum += temp_score * self.temporal_weight

        if obs.location and target.location:
            active_weight += self.text_weight
            weighted_sum += text_score * self.text_weight

        composite = weighted_sum / active_weight if active_weight > 0 else 0.0

        # Calibration safeguard:
        # If both coordinates and timestamps are missing, cap score to prevent false-positive matches
        # based solely on type and vague text without spatiotemporal anchors.
        if spatial_score is None and temp_score is None:
            composite = min(composite, 0.40)
            evidence["capped"] = "Insufficient spatiotemporal evidence to confirm co-reference"

        composite_score = round(min(1.0, max(0.0, composite)), 4)

        return FeatureScores(
            event_type_score=type_score,
            spatial_score=spatial_score,
            spatial_distance_km=round(dist_km, 3) if dist_km is not None else None,
            temporal_score=temp_score,
            temporal_distance_hours=round(dist_hours, 2) if dist_hours is not None else None,
            location_text_score=text_score if (obs.location and target.location) else None,
            description_score=desc_score,
            composite_score=composite_score,
            missing_features=missing,
            evidence_summary=evidence,
        )
