"""Unit tests for ObservationMatcher feature comparison and scoring."""

from datetime import datetime, timezone
import pytest

from beacon.config import Settings
from beacon.models import DisasterObservation, UnifiedDisasterEvent
from beacon.resolution.matcher import ObservationMatcher


@pytest.fixture
def test_settings() -> Settings:
    return Settings(
        resolution_match_threshold=0.75,
        resolution_ambiguity_threshold=0.50,
        resolution_max_spatial_distance_km=50.0,
        resolution_max_temporal_distance_hours=48.0,
        resolution_type_weight=0.35,
        resolution_spatial_weight=0.30,
        resolution_temporal_weight=0.20,
        resolution_location_text_weight=0.15,
    )


@pytest.fixture
def base_obs() -> DisasterObservation:
    return DisasterObservation(
        observation_id="obs_001",
        source_id="src_001",
        event_type="flood",
        location="Riverside Metro",
        event_time=datetime(2026, 5, 15, 10, 0, 0, tzinfo=timezone.utc),
        latitude=34.0522,
        longitude=-118.2437,
        severity="severe",
        description="Severe flooding across Riverside Metro.",
    )


class TestObservationMatcher:
    """Tests for ObservationMatcher scoring and calibration."""

    def test_identical_observation_high_score(
        self, test_settings: Settings, base_obs: DisasterObservation
    ) -> None:
        matcher = ObservationMatcher(test_settings)
        scores = matcher.compare(base_obs, base_obs)

        assert scores.composite_score >= 0.95
        assert scores.event_type_score == 1.0
        assert scores.spatial_score == 1.0
        assert scores.temporal_score == 1.0
        assert scores.location_text_score == 1.0
        assert scores.missing_features == []

    def test_incompatible_disaster_category_hard_veto(
        self, test_settings: Settings, base_obs: DisasterObservation
    ) -> None:
        matcher = ObservationMatcher(test_settings)
        wildfire_obs = DisasterObservation(
            observation_id="obs_002",
            source_id="src_002",
            event_type="wildfire",
            location="Riverside Metro",
            event_time=datetime(2026, 5, 15, 10, 0, 0, tzinfo=timezone.utc),
            latitude=34.0522,
            longitude=-118.2437,
        )

        scores = matcher.compare(base_obs, wildfire_obs)
        assert scores.composite_score == 0.0
        assert scores.event_type_score == 0.0
        assert "veto" in scores.evidence_summary

    def test_event_type_alias_equivalence(
        self, test_settings: Settings, base_obs: DisasterObservation
    ) -> None:
        matcher = ObservationMatcher(test_settings)
        flash_flood_obs = DisasterObservation(
            observation_id="obs_003",
            source_id="src_003",
            event_type="flash flood",
            location="Riverside Metro",
            event_time=datetime(2026, 5, 15, 10, 0, 0, tzinfo=timezone.utc),
            latitude=34.0522,
            longitude=-118.2437,
        )

        scores = matcher.compare(base_obs, flash_flood_obs)
        # flash flood resolves as alias/subtype of flood
        assert scores.event_type_score >= 0.80
        assert scores.composite_score >= 0.85

    def test_missing_coordinates_dynamic_reweighting(
        self, test_settings: Settings, base_obs: DisasterObservation
    ) -> None:
        matcher = ObservationMatcher(test_settings)
        no_coord_obs = DisasterObservation(
            observation_id="obs_004",
            source_id="src_004",
            event_type="flood",
            location="Riverside Metro",
            event_time=datetime(2026, 5, 15, 10, 0, 0, tzinfo=timezone.utc),
            latitude=None,
            longitude=None,
        )

        scores = matcher.compare(base_obs, no_coord_obs)
        assert "spatial_coordinates" in scores.missing_features
        assert scores.spatial_score is None
        # Score is rebalanced over available type, time, and text features
        assert scores.composite_score >= 0.75

    def test_missing_spatiotemporal_anchors_calibration_cap(
        self, test_settings: Settings
    ) -> None:
        matcher = ObservationMatcher(test_settings)
        obs_a = DisasterObservation(
            observation_id="obs_005",
            source_id="src_005",
            event_type="flood",
            location="City Center",
            event_time=None,
            latitude=None,
            longitude=None,
        )
        obs_b = DisasterObservation(
            observation_id="obs_006",
            source_id="src_006",
            event_type="flood",
            location="City Center",
            event_time=None,
            latitude=None,
            longitude=None,
        )

        scores = matcher.compare(obs_a, obs_b)
        assert "spatial_coordinates" in scores.missing_features
        assert "event_timestamp" in scores.missing_features
        # Capped to 0.40 to prevent false-positive merges when spatiotemporal evidence is absent
        assert scores.composite_score <= 0.40

    def test_compare_with_unified_event(
        self, test_settings: Settings, base_obs: DisasterObservation
    ) -> None:
        matcher = ObservationMatcher(test_settings)
        event = UnifiedDisasterEvent(
            event_id="evt_001",
            event_type="flood",
            location="Riverside Metro District",
            start_time=datetime(2026, 5, 15, 8, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2026, 5, 15, 18, 0, 0, tzinfo=timezone.utc),
            summary="Severe flood in Riverside Metro.",
            severity="severe",
            observation_ids=["obs_anchor"],
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            metadata={
                "centroid": {"latitude": 34.0522, "longitude": -118.2437}
            },
        )

        scores = matcher.compare(base_obs, event)
        assert scores.composite_score >= 0.85
        assert scores.spatial_score == 1.0
        assert scores.temporal_score == 1.0
