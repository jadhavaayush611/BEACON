"""Data Quality validation rules for BEACON observations."""

from typing import Optional

from beacon.models import (
    STANDARD_EVENT_TYPES,
    DisasterObservation,
    SeverityLevel,
)
from beacon.quality.models import RuleSeverity, ValidationOutcome

STANDARD_SEVERITY_VALUES: set[str] = {s.value for s in SeverityLevel}


def check_required_fields(obs: DisasterObservation) -> list[ValidationOutcome]:
    """Verify that mandatory identifiers and classification fields are present and non-empty."""
    outcomes: list[ValidationOutcome] = []

    # 1. observation_id
    if not obs.observation_id or not obs.observation_id.strip():
        outcomes.append(
            ValidationOutcome(
                record_id=obs.observation_id or "UNKNOWN",
                source_id=obs.source_id,
                rule_name="REQUIRED_OBSERVATION_ID",
                field_name="observation_id",
                severity=RuleSeverity.ERROR,
                message="observation_id must be a non-empty string",
                passed=False,
            )
        )

    # 2. source_id
    if not obs.source_id or not obs.source_id.strip():
        outcomes.append(
            ValidationOutcome(
                record_id=obs.observation_id,
                source_id=obs.source_id or "UNKNOWN",
                rule_name="REQUIRED_SOURCE_ID",
                field_name="source_id",
                severity=RuleSeverity.ERROR,
                message="source_id must be a non-empty string referencing the parent source",
                passed=False,
            )
        )

    # 3. event_type
    if not obs.event_type or not obs.event_type.strip():
        outcomes.append(
            ValidationOutcome(
                record_id=obs.observation_id,
                source_id=obs.source_id,
                rule_name="REQUIRED_EVENT_TYPE",
                field_name="event_type",
                severity=RuleSeverity.ERROR,
                message="event_type is mandatory and cannot be empty",
                passed=False,
            )
        )

    return outcomes


def check_geographic_coordinates(obs: DisasterObservation) -> list[ValidationOutcome]:
    """Verify coordinate domain bounds and paired presence."""
    outcomes: list[ValidationOutcome] = []
    lat = obs.latitude
    lon = obs.longitude

    # Paired presence check
    if (lat is not None and lon is None) or (lat is None and lon is not None):
        outcomes.append(
            ValidationOutcome(
                record_id=obs.observation_id,
                source_id=obs.source_id,
                rule_name="COORDINATES_PAIRED",
                field_name="latitude/longitude",
                severity=RuleSeverity.WARNING,
                message="Incomplete coordinate pair: latitude and longitude should both be present or both absent",
                passed=False,
                details={"latitude": lat, "longitude": lon},
            )
        )

    # Range checks
    if lat is not None and not (-90.0 <= lat <= 90.0):
        outcomes.append(
            ValidationOutcome(
                record_id=obs.observation_id,
                source_id=obs.source_id,
                rule_name="LATITUDE_BOUNDS",
                field_name="latitude",
                severity=RuleSeverity.ERROR,
                message=f"Latitude {lat} out of valid geographic range [-90.0, 90.0]",
                passed=False,
                details={"latitude": lat},
            )
        )

    if lon is not None and not (-180.0 <= lon <= 180.0):
        outcomes.append(
            ValidationOutcome(
                record_id=obs.observation_id,
                source_id=obs.source_id,
                rule_name="LONGITUDE_BOUNDS",
                field_name="longitude",
                severity=RuleSeverity.ERROR,
                message=f"Longitude {lon} out of valid geographic range [-180.0, 180.0]",
                passed=False,
                details={"longitude": lon},
            )
        )

    return outcomes


def check_temporal_bounds(obs: DisasterObservation) -> list[ValidationOutcome]:
    """Verify event timestamp timezone-awareness and temporal sanity."""
    outcomes: list[ValidationOutcome] = []
    dt = obs.event_time

    if dt is None:
        outcomes.append(
            ValidationOutcome(
                record_id=obs.observation_id,
                source_id=obs.source_id,
                rule_name="TIMESTAMP_MISSING",
                field_name="event_time",
                severity=RuleSeverity.INFO,
                message="Event timestamp is unstated in source report",
                passed=True,
            )
        )
        return outcomes

    # Timezone check
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        outcomes.append(
            ValidationOutcome(
                record_id=obs.observation_id,
                source_id=obs.source_id,
                rule_name="TIMESTAMP_TIMEZONE_AWARE",
                field_name="event_time",
                severity=RuleSeverity.ERROR,
                message="event_time must be timezone-aware (UTC)",
                passed=False,
            )
        )
        return outcomes

    # Temporal domain bounds: between 1900-01-01 and year 2100
    min_year = 1900
    max_year = 2100
    if not (min_year <= dt.year <= max_year):
        outcomes.append(
            ValidationOutcome(
                record_id=obs.observation_id,
                source_id=obs.source_id,
                rule_name="TIMESTAMP_PLAUSIBLE_RANGE",
                field_name="event_time",
                severity=RuleSeverity.ERROR,
                message=f"event_time year {dt.year} is outside plausible historical/future range [{min_year}, {max_year}]",
                passed=False,
                details={"year": dt.year, "iso": dt.isoformat()},
            )
        )

    return outcomes


def check_numeric_measurements(obs: DisasterObservation) -> list[ValidationOutcome]:
    """Verify numeric confidence and page number domain boundaries."""
    outcomes: list[ValidationOutcome] = []

    # 1. Extraction confidence [0.0, 1.0]
    conf = obs.extraction_confidence
    if conf is not None:
        if not (0.0 <= conf <= 1.0):
            outcomes.append(
                ValidationOutcome(
                    record_id=obs.observation_id,
                    source_id=obs.source_id,
                    rule_name="CONFIDENCE_BOUNDS",
                    field_name="extraction_confidence",
                    severity=RuleSeverity.ERROR,
                    message=f"extraction_confidence {conf} is outside [0.0, 1.0]",
                    passed=False,
                    details={"confidence": conf},
                )
            )
        elif conf < 0.2:
            outcomes.append(
                ValidationOutcome(
                    record_id=obs.observation_id,
                    source_id=obs.source_id,
                    rule_name="CONFIDENCE_LOW",
                    field_name="extraction_confidence",
                    severity=RuleSeverity.WARNING,
                    message=f"extraction_confidence {conf} is very low (< 0.2)",
                    passed=False,
                    details={"confidence": conf},
                )
            )

    # 2. Page number > 0
    page = obs.page_number
    if page is not None and page <= 0:
        outcomes.append(
            ValidationOutcome(
                record_id=obs.observation_id,
                source_id=obs.source_id,
                rule_name="PAGE_NUMBER_POSITIVE",
                field_name="page_number",
                severity=RuleSeverity.ERROR,
                message=f"page_number {page} must be a positive integer (> 0)",
                passed=False,
                details={"page_number": page},
            )
        )

    return outcomes


def check_vocabulary_standards(obs: DisasterObservation) -> list[ValidationOutcome]:
    """Check whether disaster category and severity adhere to standard vocabularies."""
    outcomes: list[ValidationOutcome] = []

    # Category standard
    cat = obs.event_type.strip().lower()
    if cat not in STANDARD_EVENT_TYPES:
        outcomes.append(
            ValidationOutcome(
                record_id=obs.observation_id,
                source_id=obs.source_id,
                rule_name="VOCABULARY_EVENT_CATEGORY",
                field_name="event_type",
                severity=RuleSeverity.WARNING,
                message=f"Event category '{cat}' is not in standard vocabulary; accepted as custom type",
                passed=False,
                details={"event_type": cat},
            )
        )

    # Severity standard
    sev = obs.severity
    if sev is not None:
        sev_norm = sev.strip().lower()
        if sev_norm not in STANDARD_SEVERITY_VALUES:
            outcomes.append(
                ValidationOutcome(
                    record_id=obs.observation_id,
                    source_id=obs.source_id,
                    rule_name="VOCABULARY_SEVERITY",
                    field_name="severity",
                    severity=RuleSeverity.WARNING,
                    message=f"Severity '{sev}' is not in standard scale (minor, moderate, severe, critical, unknown)",
                    passed=False,
                    details={"severity": sev},
                )
            )

    return outcomes


def check_batch_duplicates(
    observations: list[DisasterObservation],
) -> list[ValidationOutcome]:
    """Detect intra-batch duplicate records within the current processing run."""
    outcomes: list[ValidationOutcome] = []
    seen_ids: dict[str, DisasterObservation] = {}
    seen_claims: dict[tuple[str, str, Optional[str], Optional[str]], str] = {}

    for obs in observations:
        # 1. Observation ID collision check
        if obs.observation_id in seen_ids:
            outcomes.append(
                ValidationOutcome(
                    record_id=obs.observation_id,
                    source_id=obs.source_id,
                    rule_name="BATCH_DUPLICATE_ID",
                    field_name="observation_id",
                    severity=RuleSeverity.ERROR,
                    message=f"Duplicate observation_id '{obs.observation_id}' detected within current processing batch",
                    passed=False,
                    details={"first_seen_source_id": seen_ids[obs.observation_id].source_id},
                )
            )
        else:
            seen_ids[obs.observation_id] = obs

        # 2. Duplicate claim signature within same source
        # (same source_id, event_type, location, event_time)
        sig = (
            obs.source_id,
            obs.event_type.strip().lower(),
            (obs.location or "").strip().lower(),
            obs.event_time.isoformat() if obs.event_time else None,
        )
        if sig in seen_claims:
            prior_obs_id = seen_claims[sig]
            outcomes.append(
                ValidationOutcome(
                    record_id=obs.observation_id,
                    source_id=obs.source_id,
                    rule_name="BATCH_DUPLICATE_CLAIM",
                    field_name="claim_signature",
                    severity=RuleSeverity.WARNING,
                    message=f"Record claim matches prior observation '{prior_obs_id}' within same source",
                    passed=False,
                    details={"matched_observation_id": prior_obs_id},
                )
            )
        else:
            seen_claims[sig] = obs.observation_id

    return outcomes
