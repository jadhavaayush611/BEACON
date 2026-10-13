"""Canonical profile synthesis and conflict tracking for unified disaster events."""

from collections import Counter
from datetime import datetime, timezone
import logging
from typing import Any, Optional

from beacon.models import (
    ConflictingClaim,
    DisasterObservation,
    UnifiedDisasterEvent,
    generate_event_id,
)

logger = logging.getLogger("beacon.resolution.aggregator")

SEVERITY_RANKS: dict[str, int] = {
    "minor": 1,
    "moderate": 2,
    "severe": 3,
    "critical": 4,
    "catastrophic": 5,
}


class EventProfileAggregator:
    """Synthesizes deterministic UnifiedDisasterEvent profiles from observation clusters."""

    def __init__(self, conflict_coord_tolerance_km: float = 5.0) -> None:
        self.conflict_coord_tolerance_km = conflict_coord_tolerance_km

    def synthesize(
        self,
        observations: list[DisasterObservation],
        existing_event: Optional[UnifiedDisasterEvent] = None,
    ) -> UnifiedDisasterEvent:
        """Synthesize a canonical UnifiedDisasterEvent from constituent observations.

        Args:
            observations: List of DisasterObservation instances in this cluster.
            existing_event: Optional existing event being updated with new observations.

        Returns:
            Deterministic, fully-validated UnifiedDisasterEvent instance.
        """
        if not observations:
            raise ValueError("Cannot synthesize UnifiedDisasterEvent from empty observations list.")

        # Sort observations deterministically by ID
        sorted_obs = sorted(observations, key=lambda o: o.observation_id)
        obs_ids = [o.observation_id for o in sorted_obs]
        source_ids = sorted(list({o.source_id for o in sorted_obs}))

        now_utc = datetime.now(timezone.utc)
        created_at = existing_event.created_at if existing_event else now_utc
        updated_at = now_utc

        # 1. Spatiotemporal extent
        valid_times = [o.event_time for o in sorted_obs if o.event_time is not None]
        unanchored_timestamp = False
        if valid_times:
            start_time = min(valid_times)
            max_time = max(valid_times)
            end_time = max_time if max_time > start_time else (valid_times[0] if len(valid_times) > 1 else None)
        else:
            unanchored_timestamp = True
            # Fallback for unanchored observations to satisfy model contract
            start_time = existing_event.start_time if existing_event else created_at
            end_time = existing_event.end_time if existing_event else None

        # 2. Coordinates & Centroid
        coord_obs = [
            o for o in sorted_obs if o.latitude is not None and o.longitude is not None
        ]
        metadata: dict[str, Any] = {}
        if existing_event and existing_event.metadata:
            metadata.update(existing_event.metadata)

        if coord_obs:
            lats = [o.latitude for o in coord_obs]  # type: ignore[misc]
            lons = [o.longitude for o in coord_obs]  # type: ignore[misc]
            centroid_lat = round(sum(lats) / len(lats), 6)
            centroid_lon = round(sum(lons) / len(lons), 6)
            bbox = {
                "min_latitude": round(min(lats), 6),
                "max_latitude": round(max(lats), 6),
                "min_longitude": round(min(lons), 6),
                "max_longitude": round(max(lons), 6),
            }
            metadata["centroid"] = {"latitude": centroid_lat, "longitude": centroid_lon}
            metadata["bounding_box"] = bbox
            metadata["coordinate_count"] = len(coord_obs)
        else:
            metadata["missing_coordinates"] = True

        if unanchored_timestamp:
            metadata["unanchored_timestamp"] = True

        # 3. Canonical Event Type
        type_counts = Counter(o.event_type for o in sorted_obs)
        # Select most common; break ties deterministically by alphabetical order
        best_type = sorted(type_counts.keys(), key=lambda t: (-type_counts[t], t))[0]

        # 4. Canonical Location
        loc_counts = Counter(o.location.strip() for o in sorted_obs if o.location)
        if loc_counts:
            # Select most common; break ties preferring longer, more descriptive name, then alphabetical
            best_loc = sorted(loc_counts.keys(), key=lambda loc: (-loc_counts[loc], -len(loc), loc))[0]
        else:
            best_loc = "Unknown Location"

        # 5. Canonical Severity
        severities = [o.severity.lower() for o in sorted_obs if o.severity]
        if severities:
            # Prefer highest reported severity by domain rank, or majority vote
            best_sev = sorted(
                severities,
                key=lambda s: (SEVERITY_RANKS.get(s, 0), severities.count(s)),
                reverse=True,
            )[0]
        else:
            best_sev = None

        # 6. Detect and Track Conflicting Claims
        conflicting_claims: list[ConflictingClaim] = []

        # Check event_type conflicts
        if len(type_counts) > 1:
            for obs in sorted_obs:
                if obs.event_type != best_type:
                    conflicting_claims.append(
                        ConflictingClaim(
                            field_name="event_type",
                            observation_id=obs.observation_id,
                            source_id=obs.source_id,
                            claimed_value=obs.event_type,
                            confidence=obs.extraction_confidence,
                            resolution_status="superseded",
                            notes=f"Resolved to '{best_type}' by majority consensus ({type_counts[best_type]}/{len(sorted_obs)}).",
                        )
                    )

        # Check severity conflicts
        if len(set(severities)) > 1:
            for obs in sorted_obs:
                if obs.severity and obs.severity.lower() != best_sev:
                    conflicting_claims.append(
                        ConflictingClaim(
                            field_name="severity",
                            observation_id=obs.observation_id,
                            source_id=obs.source_id,
                            claimed_value=obs.severity,
                            confidence=obs.extraction_confidence,
                            resolution_status="superseded",
                            notes=f"Resolved to '{best_sev}' based on highest reported severity/plurality.",
                        )
                    )

        # Check location conflicts
        if len(loc_counts) > 1:
            for obs in sorted_obs:
                cleaned_loc = obs.location.strip()
                if cleaned_loc.lower() != best_loc.lower():
                    conflicting_claims.append(
                        ConflictingClaim(
                            field_name="location",
                            observation_id=obs.observation_id,
                            source_id=obs.source_id,
                            claimed_value=obs.location,
                            confidence=obs.extraction_confidence,
                            resolution_status="superseded",
                            notes=f"Resolved to '{best_loc}' as most descriptive consensus name.",
                        )
                    )

        # 7. Synthesize Summary & Rationale
        sev_str = f"{best_sev.title()} " if best_sev else ""
        summary = (
            f"{sev_str}{best_type.title()} in {best_loc} documented by {len(source_ids)} "
            f"distinct source(s) across {len(sorted_obs)} observation(s)."
        )

        rationale_parts = [
            f"Synthesized from {len(sorted_obs)} observation(s) across {len(source_ids)} source(s)."
        ]
        if conflicting_claims:
            rationale_parts.append(
                f"Identified and preserved {len(conflicting_claims)} conflicting claim(s) "
                f"across attributes ({', '.join(sorted({c.field_name for c in conflicting_claims}))})."
            )
        else:
            rationale_parts.append("Complete attribute consensus across all contributing sources.")

        resolution_rationale = " ".join(rationale_parts)

        # 8. Deterministic Event Identifier
        if existing_event:
            event_id = existing_event.event_id
        else:
            event_id = generate_event_id(best_type, best_loc, start_time)

        return UnifiedDisasterEvent(
            event_id=event_id,
            event_type=best_type,
            location=best_loc,
            start_time=start_time,
            end_time=end_time,
            summary=summary,
            severity=best_sev,
            observation_ids=obs_ids,
            created_at=created_at,
            updated_at=updated_at,
            conflicting_claims=conflicting_claims,
            resolution_rationale=resolution_rationale,
            source_ids=source_ids,
            metadata=metadata,
        )
