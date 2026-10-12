"""Unit tests for spatial, temporal, and text distance calculation helpers."""

from datetime import datetime, timezone, timedelta
import math
import pytest

from beacon.resolution.distance import (
    haversine_distance_km,
    spatial_proximity_score,
    temporal_distance_hours,
    temporal_proximity_score,
    tokenize_text,
    token_similarity,
)


class TestHaversineDistance:
    """Tests for haversine_distance_km."""

    def test_zero_distance_identical_coordinates(self) -> None:
        dist = haversine_distance_km(37.7749, -122.4194, 37.7749, -122.4194)
        assert dist == 0.0

    def test_known_distance(self) -> None:
        # Distance between London (51.5074, -0.1278) and Paris (48.8566, 2.3522) is approx 343 km
        dist = haversine_distance_km(51.5074, -0.1278, 48.8566, 2.3522)
        assert 340.0 < dist < 350.0

    def test_antipodal_points_clamping(self) -> None:
        # Exact antipodal points: North Pole to South Pole ~ 20015 km
        dist = haversine_distance_km(90.0, 0.0, -90.0, 0.0)
        assert math.isclose(dist, 20015.087, rel_tol=1e-3)

    def test_invalid_latitude_raises(self) -> None:
        with pytest.raises(ValueError, match="Latitude"):
            haversine_distance_km(95.0, 0.0, 10.0, 10.0)
        with pytest.raises(ValueError, match="Latitude"):
            haversine_distance_km(0.0, 0.0, -95.0, 10.0)

    def test_invalid_longitude_raises(self) -> None:
        with pytest.raises(ValueError, match="Longitude"):
            haversine_distance_km(0.0, 185.0, 10.0, 10.0)
        with pytest.raises(ValueError, match="Longitude"):
            haversine_distance_km(0.0, 0.0, 10.0, -190.0)


class TestSpatialProximityScore:
    """Tests for spatial_proximity_score."""

    def test_zero_distance_full_score(self) -> None:
        assert spatial_proximity_score(0.0, 50.0) == 1.0

    def test_max_distance_zero_score(self) -> None:
        assert spatial_proximity_score(50.0, 50.0) == 0.0
        assert spatial_proximity_score(100.0, 50.0) == 0.0

    def test_intermediate_score(self) -> None:
        score = spatial_proximity_score(25.0, 50.0)
        assert 0.0 < score < 1.0


class TestTemporalDistance:
    """Tests for temporal_distance_hours."""

    def test_same_timestamp_zero_distance(self) -> None:
        t = datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc)
        assert temporal_distance_hours(t, t) == 0.0

    def test_cross_day_delta(self) -> None:
        t1 = datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 5, 16, 12, 0, 0, tzinfo=timezone.utc)
        assert temporal_distance_hours(t1, t2) == 24.0

    def test_naive_timestamp_raises(self) -> None:
        t1 = datetime(2026, 5, 15, 12, 0, 0)
        t2 = datetime(2026, 5, 15, 14, 0, 0, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="timezone-aware"):
            temporal_distance_hours(t1, t2)


class TestTemporalProximityScore:
    """Tests for temporal_proximity_score."""

    def test_zero_hours_full_score(self) -> None:
        assert temporal_proximity_score(0.0, 48.0) == 1.0

    def test_max_hours_zero_score(self) -> None:
        assert temporal_proximity_score(48.0, 48.0) == 0.0
        assert temporal_proximity_score(72.0, 48.0) == 0.0

    def test_intermediate_score(self) -> None:
        score = temporal_proximity_score(24.0, 48.0)
        assert 0.0 < score < 1.0


class TestTextSimilarity:
    """Tests for tokenization and token_similarity."""

    def test_tokenize_text(self) -> None:
        tokens = tokenize_text("Riverside Green Valley #4!")
        assert "riverside" in tokens
        assert "green" in tokens
        assert "valley" in tokens
        stop_tokens = tokenize_text("Riverside District")
        assert "district" not in stop_tokens

    def test_exact_match(self) -> None:
        assert token_similarity("Riverside Metro", "Riverside Metro") == 1.0

    def test_partial_containment_match(self) -> None:
        score = token_similarity("Riverside Metro", "Riverside Metro District")
        assert score > 0.70

    def test_disjoint_strings(self) -> None:
        assert token_similarity("Riverside Metro", "Coastal Ridge Harbor") == 0.0

    def test_empty_string(self) -> None:
        assert token_similarity("", "Riverside") == 0.0
        assert token_similarity("", "") == 0.0
