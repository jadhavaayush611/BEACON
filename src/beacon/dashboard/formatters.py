"""Formatting helpers for the BEACON Streamlit dashboard.

Ensures deterministic, safe rendering of nullable fields (confidence, coordinates,
timestamps, scores, compression ratios) across dashboard views in compliance
with BEACON's Non-Fabrication Guarantee.
"""

from datetime import datetime
from typing import Any, Optional

from beacon.models import ConflictingClaim, DisasterObservation


def format_confidence(confidence: Optional[float]) -> str:
    """Format extraction confidence score to 2 decimal places or 'N/A' if missing.

    Adheres to the Non-Fabrication Guarantee: missing confidence is rendered
    explicitly as 'N/A' and is never replaced with 0.0 or synthetic scores.
    """
    if confidence is not None:
        return f"{confidence:.2f}"
    return "N/A"


def format_coordinates(latitude: Optional[float], longitude: Optional[float]) -> str:
    """Format spatial coordinates as '(lat, lon)' or 'N/A' if either is missing."""
    if latitude is not None and longitude is not None:
        return f"({latitude}, {longitude})"
    return "N/A"


def format_timestamp(dt: Optional[datetime]) -> str:
    """Format datetime as ISO-8601 string or 'N/A' if missing."""
    if dt is not None:
        return dt.isoformat()
    return "N/A"


def format_score(score: Optional[float]) -> str:
    """Format match composite score to 3 decimal places or 'N/A' if missing."""
    if score is not None:
        return f"{score:.3f}"
    return "N/A"


def format_compression_ratio(ratio: Optional[float]) -> str:
    """Format compression ratio as 'X.XXx' or 'N/A' if missing.

    Derivation formula: Total Accepted Observations / Total Unified Events.
    Safely renders 0.0 as '0.00x' and None as 'N/A' adhering to the
    Non-Fabrication Guarantee.
    """
    if ratio is not None:
        return f"{ratio:.2f}x"
    return "N/A"


def explain_compression_ratio(
    total_observations: int, total_events: int, ratio: Optional[float]
) -> str:
    """Return human-readable explanation of the compression ratio and its derivation.

    Formula: Total Accepted Observations / Total Unified Events (observations per unified event).
    When total_events is 0, safely returns 0.00x without fabricating synthetic ratios.
    """
    formatted = format_compression_ratio(ratio)
    if total_events <= 0:
        return f"{formatted} (0 events loaded; baseline ratio is 0.00x)"
    return f"{formatted} ({total_observations} observations ÷ {total_events} unified events)"


def format_observation_row(obs: DisasterObservation) -> dict[str, Any]:
    """Build a display dictionary for a constituent observation in Event Explorer."""
    return {
        "Obs ID": obs.observation_id,
        "Source ID": obs.source_id,
        "Type": obs.event_type,
        "Location": obs.location or "N/A",
        "Severity": obs.severity or "unspecified",
        "Coordinates": format_coordinates(obs.latitude, obs.longitude),
        "Timestamp": format_timestamp(obs.event_time),
        "Confidence": format_confidence(obs.extraction_confidence),
    }


def format_decision_row(decision: dict[str, Any]) -> dict[str, Any]:
    """Build a display dictionary for a match decision in Event Explorer."""
    return {
        "Obs ID": decision.get("observation_id", "N/A"),
        "Decision": decision.get("decision_type", "N/A"),
        "Score": format_score(decision.get("composite_score")),
        "Rationale": decision.get("rationale") or "N/A",
    }


def format_conflict_claim_row(claim: ConflictingClaim) -> dict[str, Any]:
    """Build a display dictionary for a conflicting claim, safely handling missing notes."""
    notes = (
        claim.notes.strip()
        if claim.notes and claim.notes.strip()
        else "No resolution notes recorded."
    )
    return {
        "Field": claim.field_name,
        "Claimed Value": claim.claimed_value,
        "Observation ID": claim.observation_id,
        "Source ID": claim.source_id,
        "Status": claim.resolution_status,
        "Resolution Notes": notes,
    }


def format_conflict_claim_markdown(claim: ConflictingClaim) -> str:
    """Format an accessible markdown bullet for a conflicting claim with fallback for missing notes."""
    notes = (
        claim.notes.strip()
        if claim.notes and claim.notes.strip()
        else "No resolution notes recorded."
    )
    return (
        f"- **{claim.field_name.title()}** (`{claim.claimed_value}` from `{claim.source_id}`, "
        f"status: `{claim.resolution_status}`): {notes}"
    )
