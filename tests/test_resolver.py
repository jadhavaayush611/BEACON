"""Unit and end-to-end integration tests for EventResolver orchestrator."""

from datetime import datetime, timezone
from pathlib import Path
import pytest

from beacon.config import Settings
from beacon.ingestion import ingest_file
from beacon.models import DisasterObservation, UnifiedDisasterEvent
from beacon.pipeline.transform import ObservationTransformer
from beacon.resolution.models import DecisionType
from beacon.resolution.resolver import AmbiguousObservationAssociationError
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

    def test_unanchored_observation_processed_once_and_repeatedly(
        self, resolver: EventResolver
    ) -> None:
        """Verify an unanchored observation creates one event and re-resolves idempotently."""
        obs_unanchored = DisasterObservation(
            observation_id="obs_unanchored_001",
            source_id="src_txt_01",
            event_type="wildfire",
            location="North Forest Perimeter",
            event_time=None,
            latitude=None,
            longitude=None,
        )

        # Run 1: initial resolution creates exactly one unified event
        res1 = resolver.resolve_observations([obs_unanchored])
        assert len(res1.resolved_events) == 1
        assert res1.events_created == 1
        assert res1.events_updated == 0
        assert res1.observations_assigned == 1

        initial_event = res1.resolved_events[0]
        initial_event_id = initial_event.event_id
        assert initial_event.observation_ids == ["obs_unanchored_001"]

        # Run 2: repeat resolution with identical unanchored observation
        res2 = resolver.resolve_observations(
            [obs_unanchored],
            existing_events=res1.resolved_events,
        )

        # Idempotency invariants: exactly 1 event, identical ID, zero duplicates
        assert len(res2.resolved_events) == 1
        assert res2.events_created == 0
        assert res2.events_updated == 1
        assert res2.observations_assigned == 1
        assert res2.resolved_events[0].event_id == initial_event_id
        assert res2.resolved_events[0].observation_ids == ["obs_unanchored_001"]
        assert res2.outcomes[0].decision == DecisionType.IDEMPOTENT_RE_OBSERVATION
        assert res2.outcomes[0].score <= 0.40  # Preserves conservative unanchored score cap
        assert "Idempotent re-observation" in res2.outcomes[0].rationale

    def test_different_unanchored_observations_remain_separate(
        self, resolver: EventResolver
    ) -> None:
        """Verify distinct unanchored observations are never merged falsely without evidence."""
        obs1 = DisasterObservation(
            observation_id="obs_diff_unanchored_1",
            source_id="src_01",
            event_type="wildfire",
            location="North Forest Perimeter",
            event_time=None,
            latitude=None,
            longitude=None,
        )
        obs2 = DisasterObservation(
            observation_id="obs_diff_unanchored_2",
            source_id="src_02",
            event_type="wildfire",
            location="North Forest Perimeter",
            event_time=None,
            latitude=None,
            longitude=None,
        )

        # Run 1: process obs1
        res1 = resolver.resolve_observations([obs1])
        assert len(res1.resolved_events) == 1
        event1_id = res1.resolved_events[0].event_id

        # Run 2: process obs2 with event1 existing
        res2 = resolver.resolve_observations([obs2], existing_events=res1.resolved_events)

        # Constraint: two distinct unanchored records must NOT merge based solely on text
        assert len(res2.resolved_events) == 2
        event_ids = {e.event_id for e in res2.resolved_events}
        assert event1_id in event_ids
        assert len(event_ids) == 2

    def test_genuinely_new_observation_processed_with_existing_associations(
        self, resolver: EventResolver
    ) -> None:
        """Verify new observations are properly resolved alongside idempotent re-observations."""
        obs_existing = DisasterObservation(
            observation_id="obs_exist_01",
            source_id="src_01",
            event_type="wildfire",
            location="North Forest Perimeter",
            event_time=None,
            latitude=None,
            longitude=None,
        )
        obs_new = DisasterObservation(
            observation_id="obs_brand_new_02",
            source_id="src_02",
            event_type="flood",
            location="Riverside Metro",
            event_time=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
            latitude=34.05,
            longitude=-118.25,
        )

        res1 = resolver.resolve_observations([obs_existing])
        assert len(res1.resolved_events) == 1
        exist_eid = res1.resolved_events[0].event_id

        # Batch contains existing re-observation AND brand new observation
        res2 = resolver.resolve_observations(
            [obs_existing, obs_new],
            existing_events=res1.resolved_events,
        )

        assert len(res2.resolved_events) == 2
        assert res2.events_created == 1
        assert res2.events_updated == 1
        assert res2.observations_assigned == 2

        outcomes_by_obs = {o.observation_id: o for o in res2.outcomes}
        assert outcomes_by_obs["obs_exist_01"].assigned_event_id == exist_eid
        assert outcomes_by_obs["obs_exist_01"].decision == DecisionType.IDEMPOTENT_RE_OBSERVATION
        assert outcomes_by_obs["obs_exist_01"].score <= 0.40
        assert "Idempotent re-observation" in outcomes_by_obs["obs_exist_01"].rationale
        assert outcomes_by_obs["obs_brand_new_02"].is_new_event

    def test_ambiguous_observation_association_across_multiple_events_raises_error(
        self, resolver: EventResolver
    ) -> None:
        """Verify an observation associated with multiple distinct events raises an integrity error."""
        now = datetime.now(timezone.utc)
        ev1 = UnifiedDisasterEvent(
            event_id="evt_01",
            event_type="wildfire",
            location="Perimeter A",
            start_time=now,
            summary="Event 1",
            observation_ids=["obs_shared_01"],
            created_at=now,
            updated_at=now,
        )
        ev2 = UnifiedDisasterEvent(
            event_id="evt_02",
            event_type="wildfire",
            location="Perimeter B",
            start_time=now,
            summary="Event 2",
            observation_ids=["obs_shared_01"],
            created_at=now,
            updated_at=now,
        )
        obs = DisasterObservation(
            observation_id="obs_shared_01",
            source_id="src_01",
            event_type="wildfire",
            location="Perimeter A",
        )

        import pytest
        with pytest.raises(AmbiguousObservationAssociationError) as exc_info:
            resolver.resolve_observations([obs], existing_events=[ev1, ev2])

        err = exc_info.value
        assert "obs_shared_01" in err.ambiguous_associations
        assert set(err.ambiguous_associations["obs_shared_01"]) == {"evt_01", "evt_02"}
        assert "Data integrity violation" in str(err)

    def test_duplicate_observation_links_within_same_event_is_handled_safely(
        self, resolver: EventResolver
    ) -> None:
        """Verify duplicate references to an observation within the same event are handled cleanly."""
        now = datetime.now(timezone.utc)
        ev1 = UnifiedDisasterEvent(
            event_id="evt_single_01",
            event_type="flood",
            location="Riverside",
            start_time=now,
            summary="Single Event",
            observation_ids=["obs_dup_within_01", "obs_dup_within_01"],  # Duplicate within same event
            created_at=now,
            updated_at=now,
        )
        obs = DisasterObservation(
            observation_id="obs_dup_within_01",
            source_id="src_01",
            event_type="flood",
            location="Riverside",
            latitude=34.05,
            longitude=-118.25,
            event_time=now,
        )

        # Must NOT raise AmbiguousObservationAssociationError because both links belong to evt_single_01
        res = resolver.resolve_observations([obs], existing_events=[ev1])
        assert len(res.resolved_events) == 1
        assert res.resolved_events[0].event_id == "evt_single_01"
        assert res.outcomes[0].decision == DecisionType.IDEMPOTENT_RE_OBSERVATION

    def test_distinguish_idempotent_reobservation_from_confident_match(
        self, resolver: EventResolver
    ) -> None:
        """Verify idempotent re-observations are distinct from genuine spatiotemporal confident matches."""
        t0 = datetime(2026, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
        obs_anchored_1 = DisasterObservation(
            observation_id="obs_anc_01",
            source_id="src_sensor_01",
            event_type="flood",
            location="Downtown Bayview",
            latitude=37.7749,
            longitude=-122.4194,
            event_time=t0,
        )
        obs_anchored_2 = DisasterObservation(
            observation_id="obs_anc_02",
            source_id="src_sensor_02",
            event_type="flood",
            location="Downtown Bayview",
            latitude=37.7750,
            longitude=-122.4195,
            event_time=t0,
        )
        obs_unanchored = DisasterObservation(
            observation_id="obs_unanc_01",
            source_id="src_txt_01",
            event_type="wildfire",
            location="North Basin",
            latitude=None,
            longitude=None,
            event_time=None,
        )

        # 1. Initial resolution: obs_anchored_1 creates Event A, obs_unanchored creates Event B
        res1 = resolver.resolve_observations([obs_anchored_1, obs_unanchored])
        assert len(res1.resolved_events) == 2
        assert res1.outcomes[0].decision == DecisionType.NO_MATCH  # Created event
        assert res1.outcomes[1].decision == DecisionType.NO_MATCH  # Created event

        # 2. Second resolution:
        # - obs_anchored_2 is a NEW observation that spatiotemporally matches Event A
        # - obs_anchored_1 is an IDEMPOTENT re-observation of Event A
        # - obs_unanchored is an IDEMPOTENT re-observation of Event B
        res2 = resolver.resolve_observations(
            [obs_anchored_2, obs_anchored_1, obs_unanchored],
            existing_events=res1.resolved_events,
        )

        outcomes = {o.observation_id: o for o in res2.outcomes}

        # Genuine spatiotemporal match
        assert outcomes["obs_anc_02"].decision == DecisionType.CONFIDENT_MATCH
        assert outcomes["obs_anc_02"].score >= 0.80

        # Idempotent re-observation of anchored record
        assert outcomes["obs_anc_01"].decision == DecisionType.IDEMPOTENT_RE_OBSERVATION
        assert outcomes["obs_anc_01"].score >= 0.80  # Genuine spatiotemporal score against its own event

        # Idempotent re-observation of unanchored record: PRESERVES 0.40 CAP
        assert outcomes["obs_unanc_01"].decision == DecisionType.IDEMPOTENT_RE_OBSERVATION
        assert outcomes["obs_unanc_01"].score <= 0.40  # Must NEVER be fabricated as 1.0!
        assert "0.400" in outcomes["obs_unanc_01"].rationale
