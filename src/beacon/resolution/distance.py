"""Geographic, temporal, and textual distance metrics for event resolution."""

from datetime import datetime, timezone
import math
import re
from typing import Optional

EARTH_RADIUS_KM: float = 6371.0

# Generic location stop-words that do not differentiate specific places
LOCATION_STOP_WORDS: set[str] = {
    "the", "at", "in", "near", "of", "and", "or", "on", "by", "to", "from",
    "area", "zone", "sector", "region", "district", "metro", "county", "city",
    "environs", "vicinity", "perimeter", "division", "block", "site",
}


def haversine_distance_km(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    """Calculate the great-circle distance between two points on the Earth's surface in kilometers.

    Coordinates must be in decimal degrees:
    lat in [-90.0, 90.0], lon in [-180.0, 180.0].
    """
    if not (-90.0 <= lat1 <= 90.0 and -90.0 <= lat2 <= 90.0):
        raise ValueError(f"Latitude out of bounds [-90, 90]: lat1={lat1}, lat2={lat2}")
    if not (-180.0 <= lon1 <= 180.0 and -180.0 <= lon2 <= 180.0):
        raise ValueError(f"Longitude out of bounds [-180, 180]: lon1={lon1}, lon2={lon2}")

    # Identical points
    if lat1 == lat2 and lon1 == lon2:
        return 0.0

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * (math.sin(delta_lambda / 2.0) ** 2)
    )
    # Clamp to [0.0, 1.0] to prevent math domain error in asin due to float rounding
    a_clamped = min(1.0, max(0.0, a))
    c = 2.0 * math.atan2(math.sqrt(a_clamped), math.sqrt(1.0 - a_clamped))

    return EARTH_RADIUS_KM * c


def temporal_distance_hours(t1: datetime, t2: datetime) -> float:
    """Calculate absolute difference between two datetimes in decimal hours.

    Both timestamps must be timezone-aware or normalized to UTC.
    """
    if t1.tzinfo is None or t1.tzinfo.utcoffset(t1) is None:
        raise ValueError("t1 must be timezone-aware")
    if t2.tzinfo is None or t2.tzinfo.utcoffset(t2) is None:
        raise ValueError("t2 must be timezone-aware")

    t1_utc = t1.astimezone(timezone.utc)
    t2_utc = t2.astimezone(timezone.utc)
    delta_seconds = abs((t1_utc - t2_utc).total_seconds())
    return delta_seconds / 3600.0


def tokenize_text(text: Optional[str]) -> set[str]:
    """Tokenize and normalize text into clean distinctive lowercase words."""
    if not text:
        return set()
    cleaned = re.sub(r"[^\w\s]", " ", text.lower())
    tokens = {tok.strip() for tok in cleaned.split() if len(tok.strip()) > 1}
    # Keep substantive tokens
    substantive = {tok for tok in tokens if tok not in LOCATION_STOP_WORDS}
    return substantive if substantive else tokens


def token_similarity(text1: Optional[str], text2: Optional[str]) -> float:
    """Calculate combined Jaccard and containment similarity [0.0, 1.0] between text strings."""
    if not text1 or not text2:
        return 0.0

    t1 = tokenize_text(text1)
    t2 = tokenize_text(text2)

    if not t1 or not t2:
        return 0.0

    intersection = t1 & t2
    if not intersection:
        return 0.0

    union = t1 | t2
    jaccard = len(intersection) / len(union)
    containment = len(intersection) / min(len(t1), len(t2))

    # Balanced score giving credit to subset matches (e.g. "Riverside" vs "Riverside Sector 4")
    return round(0.5 * jaccard + 0.5 * containment, 4)


def spatial_proximity_score(dist_km: float, max_km: float = 50.0) -> float:
    """Compute normalized spatial similarity score [0.0, 1.0] from distance in km."""
    if dist_km <= 0.0:
        return 1.0
    if dist_km >= max_km:
        return 0.0
    return round(1.0 - (dist_km / max_km), 4)


def temporal_proximity_score(dist_hours: float, max_hours: float = 48.0) -> float:
    """Compute normalized temporal similarity score [0.0, 1.0] from time delta in hours."""
    if dist_hours <= 0.0:
        return 1.0
    if dist_hours >= max_hours:
        return 0.0
    return round(1.0 - (dist_hours / max_hours), 4)
