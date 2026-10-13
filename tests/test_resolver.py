"""Unit and end-to-end integration tests for EventResolver orchestrator."""

from datetime import datetime, timezone
from pathlib import Path
import pytest

from beacon.config import Settings
from beacon.ingestion import ingest_file
from beacon.models import DisasterObservation
from beacon.pipeline.transform import ObservationTransformer
from beacon.resolution.resolver import EventResolver

SAMPLE_DIR = Path(__file__).parent.parent / "data" / "sample"


@pytest.fixture
def resolver() -> EventResolver:
    cfg = Settings(
        resolution_match_threshold=0.75,
        resolution_ambiguity_threshold=0.50,
        resolution_max_spatial_distance_km=50.0,
        resolution_max_temporal_distance_hours=48.0,
    )
    return EventResolver(settings=cfg)


class TestEventResolver:
    """Tests for EventResolver orchestration and cross-source sample dataset co-reference."""

    def test_single_observation_creates_new_event(self, resolver: EventResolver) -> None:
        obs = DisasterObservation(
            observation_id="obs_single",
            source_id="src_single",
            event_type="wildfire",
            location="Pine Valley Forest",
            event_time=datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
            latitude=35.1234,
            longitude=-119.5678,
            severity="moderate",
        )

        result = resolver.resolve_observations([obs])

        assert result.total_observations == 1
        assert result.events_created == 1
        assert result.events_updated == 0
        assert result.observations_assigned == 1
        assert len(result.resolved_events) == 1

        ev = result.resolved_events[0]
        assert ev.event_type == "wildfire"
        assert ev.location == "Pine Valley Forest"
        assert ev.observation_ids == ["obs_single"]
        assert ev.source_ids == ["src_single"]

    def test_distinct_observations_create_distinct_events(
        self, resolver: EventResolver
    ) -> None:
        obs_flood = DisasterObservation(
            observation_id="obs_flood",
            source_id="src_01",
            event_type="flood",
            location="Riverside Metro",
            event_time=datetime(2026, 5, 15, 10, 0, 0, tzinfo=timezone.utc),
            latitude=34.0522,
            longitude=-118.2437,
        )
        obs_earthquake = DisasterObservation(
            observation_id="obs_quake",
            source_id="src_02",
            event_type="earthquake",
            location="Coastal Ridge",
            event_time=datetime(2026, 5, 15, 11, 0, 0, tzinfo=timezone.utc),
            latitude=36.0000,
            longitude=-121.0000,
        )

        result = resolver.resolve_observations([obs_flood, obs_earthquake])

        assert result.total_observations == 2
        assert result.events_created == 2
        assert len(result.resolved_events) == 2
        event_types = {e.event_type for e in result.resolved_events}
        assert event_types == {"flood", "earthquake"}

    def test_sample_files_cross_source_coreference(self, resolver: EventResolver) -> None:
        """Ground truth test: All sample flood reports coalesce into one UnifiedDisasterEvent."""
        sample_files = [
            SAMPLE_DIR / "events.json",
            SAMPLE_DIR / "events.csv",
            SAMPLE_DIR / "events.txt",
            SAMPLE_DIR / "sample_report.pdf",
        ]

        transformer = ObservationTransformer()
        all_obs: list[DisasterObservation] = []

        for p in sample_files:
            ingest_res = ingest_file(p)
            assert ingest_res.is_success, f"Ingestion failed for {p.name}"
            for raw_obs in ingest_res.observations:
                transformed_obs = transformer.transform(raw_obs)
                all_obs.append(transformed_obs)

        assert len(all_obs) >= 4

        # Run resolution over all extracted observations
        result = resolver.resolve_observations(all_obs)

        # Confirm compression: multiple source reports coalesced into 1 flood event (+ any secondary events)
        flood_events = [
            e for e in result.resolved_events if e.event_type == "flood"
        ]
        assert len(flood_events) == 1, (
            f"Expected exactly 1 consolidated flood event, got {len(flood_events)}"
        )

        unified_flood = flood_events[0]
        # Should contain constituent observation IDs from all contributing files
        assert len(unified_flood.observation_ids) >= 4
        # Should link all distinct sources
        assert len(unified_flood.source_ids) >= 4

        # Conflicting claim check: CSV moderate vs JSON severe
        severity_claims = [
            c for c in unified_flood.conflicting_claims if c.field_name == "severity"
        ]
        assert len(severity_claims) >= 1
        assert any(c.claimed_value == "moderate" for c in severity_claims)

    def test_rerun_idempotency_preserves_event_count(
        self, resolver: EventResolver
    ) -> None:
        obs1 = DisasterObservation(
            observation_id="obs_idem_1",
            source_id="src_01",
            event_type="flood",
            location="Riverside Metro",
            event_time=datetime(2026, 5, 15, 10, 0, 0, tzinfo=timezone.utc),
            latitude=34.0522,
            longitude=-118.2437,
        )
        obs2 = DisasterObservation(
            observation_id="obs_idem_2",
            source_id="src_02",
            event_type="flood",
            location="Riverside Metro",
            event_time=datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc),
            latitude=34.0525,
            longitude=-118.2440,
        )

        res1 = resolver.resolve_observations([obs1, obs2])
        assert len(res1.resolved_events) == 1
        initial_event_id = res1.resolved_events[0].event_id

        # Re-run resolution passing the same observations and existing event
        res2 = resolver.resolve_observations(
            [obs1, obs2], existing_events=res1.resolved_events
        )
        assert len(res2.resolved_events) == 1
        assert res2.resolved_events[0].event_id == initial_event_id
        assert len(res2.resolved_events[0].observation_ids) == 2

    def test_reprocess_observation_replaces_stale_instance_attributes(
        self, resolver: EventResolver
    ) -> None:
        """Verify that reprocessing an observation with corrected attributes updates the cluster."""
        obs1 = DisasterObservation(
            observation_id="obs_corr_1",
            source_id="src_01",
            event_type="flood",
            location="Riverside Metro",
            event_time=datetime(2026, 5, 15, 10, 0, 0, tzinfo=timezone.utc),
            latitude=34.0500,
            longitude=-118.2400,
            severity="moderate",
        )
        obs2 = DisasterObservation(
            observation_id="obs_corr_2",
            source_id="src_02",
            event_type="flood",
            location="Riverside Metro",
            event_time=datetime(2026, 5, 15, 11, 0, 0, tzinfo=timezone.utc),
            latitude=34.0520,
            longitude=-118.2420,
            severity="moderate",
        )

        res1 = resolver.resolve_observations([obs1, obs2])
        assert len(res1.resolved_events) == 1
        initial_event = res1.resolved_events[0]
        initial_event_id = initial_event.event_id
        assert initial_event.severity == "moderate"

        # Corrected obs1: upgraded severity to catastrophic and updated coordinates
        obs1_corrected = DisasterObservation(
            observation_id="obs_corr_1",  # Same observation ID
            source_id="src_01",
            event_type="flood",
            location="Riverside Metro",
            event_time=datetime(2026, 5, 15, 10, 0, 0, tzinfo=timezone.utc),
            latitude=34.0600,  # Corrected coordinates
            longitude=-118.2500,
            severity="catastrophic",  # Corrected severity
        )

        # Reprocess with corrected observation and existing cluster state
        res2 = resolver.resolve_observations(
            [obs1_corrected],
            existing_events=[initial_event],
            existing_event_observations={initial_event_id: [obs1, obs2]},
        )

        assert len(res2.resolved_events) == 1
        updated_event = res2.resolved_events[0]

        # Event identity preserved
        assert updated_event.event_id == initial_event_id
        # Observation count remains exactly 2 (not duplicated to 3)
        assert len(updated_event.observation_ids) == 2
        assert updated_event.observation_ids == ["obs_corr_1", "obs_corr_2"]

        # Profile aggregation updated using the corrected attributes
        assert updated_event.severity == "catastrophic"

        # Centroid updated using corrected obs1 coordinates: (34.0600 + 34.0520) / 2 = 34.0560
        centroid = updated_event.metadata.get("centroid", {})
        assert centroid.get("latitude") == 34.056
