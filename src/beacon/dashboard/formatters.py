"""Formatting helpers for the BEACON Streamlit dashboard.

Ensures deterministic, safe rendering of nullable fields (confidence, coordinates,
timestamps, scores, compression ratios) across dashboard views in compliance
with BEACON's Non-Fabrication Guarantee.
"""

from datetime import datetime
from typing import Any, Optional

from beacon.models import DisasterObservation


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
    """Format compression ratio as 'X.XXx' or 'N/A' if missing."""
    if ratio is not None:
        return f"{ratio:.2f}x"
    return "N/A"


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
