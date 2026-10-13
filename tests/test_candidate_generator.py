"""Unit tests for CandidateGenerator multi-attribute blocking."""

from datetime import datetime, timezone
import pytest

from beacon.config import Settings
from beacon.models import DisasterObservation, UnifiedDisasterEvent
from beacon.resolution.candidate_generator import CandidateGenerator


@pytest.fixture
def generator() -> CandidateGenerator:
    cfg = Settings(
        resolution_max_spatial_distance_km=50.0,
        resolution_max_temporal_distance_hours=48.0,
    )
    return CandidateGenerator(cfg)


@pytest.fixture
def base_event() -> UnifiedDisasterEvent:
    return UnifiedDisasterEvent(
        event_id="evt_flood_riverside",
        event_type="flood",
        location="Riverside Metro",
        start_time=datetime(2026, 5, 15, 10, 0, 0, tzinfo=timezone.utc),
        end_time=datetime(2026, 5, 15, 18, 0, 0, tzinfo=timezone.utc),
        summary="Flood in Riverside Metro.",
        severity="moderate",
        observation_ids=["obs_01"],
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        metadata={
            "centroid": {"latitude": 34.0522, "longitude": -118.2437}
        },
    )


class TestCandidateGenerator:
    """Tests for CandidateGenerator blocking."""

    def test_matching_candidate_included(
        self, generator: CandidateGenerator, base_event: UnifiedDisasterEvent
    ) -> None:
        obs = DisasterObservation(
            observation_id="obs_matching",
            source_id="src_01",
            event_type="flood",
            location="Riverside Metro",
            event_time=datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc),
            latitude=34.0600,
            longitude=-118.2500,
        )
        candidates = generator.generate_candidates(obs, [base_event])
        assert len(candidates) == 1
        assert candidates[0].event_id == base_event.event_id

    def test_incompatible_type_blocked(
        self, generator: CandidateGenerator, base_event: UnifiedDisasterEvent
    ) -> None:
        obs = DisasterObservation(
            observation_id="obs_earthquake",
            source_id="src_01",
            event_type="earthquake",
            location="Riverside Metro",
            event_time=datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc),
            latitude=34.0522,
            longitude=-118.2437,
        )
        candidates = generator.generate_candidates(obs, [base_event])
        assert len(candidates) == 0

    def test_distant_time_blocked(
        self, generator: CandidateGenerator, base_event: UnifiedDisasterEvent
    ) -> None:
        obs = DisasterObservation(
            observation_id="obs_future_time",
            source_id="src_01",
            event_type="flood",
            location="Riverside Metro",
            # 5 days later (> 48h limit)
            event_time=datetime(2026, 5, 20, 12, 0, 0, tzinfo=timezone.utc),
            latitude=34.0522,
            longitude=-118.2437,
        )
        candidates = generator.generate_candidates(obs, [base_event])
        assert len(candidates) == 0

    def test_distant_coordinates_blocked(
        self, generator: CandidateGenerator, base_event: UnifiedDisasterEvent
    ) -> None:
        obs = DisasterObservation(
            observation_id="obs_far_coords",
            source_id="src_01",
            event_type="flood",
            location="Far Away Town",
            event_time=datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc),
            # Coordinates hundreds of km away (San Francisco vs LA)
            latitude=37.7749,
            longitude=-122.4194,
        )
        candidates = generator.generate_candidates(obs, [base_event])
        assert len(candidates) == 0

    def test_missing_coordinates_preserved_as_candidate(
        self, generator: CandidateGenerator, base_event: UnifiedDisasterEvent
    ) -> None:
        obs = DisasterObservation(
            observation_id="obs_no_coords",
            source_id="src_01",
            event_type="flood",
            location="Riverside Metro",
            event_time=datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc),
            latitude=None,
            longitude=None,
        )
        candidates = generator.generate_candidates(obs, [base_event])
        # Missing coordinates cannot be ruled out by spatial blocking
        assert len(candidates) == 1
