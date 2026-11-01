"""Tests for Universal Observation Architecture: Incident vs Context Observations."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from beacon.analytics.service import AnalyticsService
from beacon.models import (
    DisasterObservation,
    ExtractionMethod,
    ExtractionStatus,
    ObservationType,
    SourceRecord,
    SourceType,
    UnifiedDisasterEvent,
    generate_observation_id,
)
from beacon.quality.validator import DataQualityValidator
from beacon.resolution.resolver import EventResolver
from beacon.storage.repository import StorageRepository
from beacon.storage.schema import Base


def _make_source(source_id: str, filename: str) -> SourceRecord:
    import hashlib
    h = hashlib.sha256(source_id.encode()).hexdigest()
    return SourceRecord(
        source_id=source_id,
        source_file=filename,
        source_type=SourceType.JSON,
        extraction_method=ExtractionMethod.JSON_PARSE,
        ingested_at=datetime.now(timezone.utc),
        extraction_status=ExtractionStatus.SUCCESS,
        content_hash=h,
        raw_content="{}",
        metadata={},
    )


class TestUniversalObservationModels:
    """Unit tests for ObservationType and DisasterObservation domain model."""

    def test_observation_type_enum_members(self) -> None:
        """Verify all required observation types exist with snake_case values."""
        assert ObservationType.INCIDENT.value == "incident"
        assert ObservationType.DATASET_METADATA.value == "dataset_metadata"
        assert ObservationType.DOCUMENT_CONTEXT.value == "document_context"
        assert ObservationType.IMAGE_CONTEXT.value == "image_context"
        assert ObservationType.IMAGE_INCIDENT.value == "image_incident"

    def test_incident_observation_properties(self) -> None:
        """INCIDENT observations are event-eligible and incident-classified."""
        obs = DisasterObservation(
            observation_id="obs_001",
            source_id="src_1",
            raw_claim=json.dumps({"type": "flood"}),
            location="River Valley",
            event_type="flood",
            severity="severe",
            observation_type=ObservationType.INCIDENT,
        )
        assert obs.is_incident is True
        assert obs.is_event_eligible is True
        assert obs.event_type == "flood"
        assert obs.context_payload == {}

    def test_context_observation_properties(self) -> None:
        """Context observations are not event-eligible and allow event_type=None."""
        obs = DisasterObservation(
            observation_id="obs_manifest",
            source_id="src_manifest",
            raw_claim=json.dumps({"dataset": "BEACON Demo"}),
            location="Dataset Metadata",
            event_type=None,
            observation_type=ObservationType.DATASET_METADATA,
            context_payload={"title": "Demo Dataset", "file_count": 7},
        )
        assert obs.is_incident is False
        assert obs.is_event_eligible is False
        assert obs.event_type is None
        assert obs.context_payload["title"] == "Demo Dataset"

    def test_incident_requires_event_type(self) -> None:
        """INCIDENT observations must provide a non-empty event_type."""
        with pytest.raises(ValueError, match="event_type must be a non-empty string for incident"):
            DisasterObservation(
                observation_id="obs_invalid",
                source_id="src_invalid",
                raw_claim="{}",
                location="River Valley",
                event_type=None,
                observation_type=ObservationType.INCIDENT,
            )

    def test_observation_id_generation_includes_type(self) -> None:
        """Observation IDs incorporate observation_type to avoid cross-type collisions."""
        id_incident = generate_observation_id("src_1", "flood", "Loc A", None, "incident")
        id_context = generate_observation_id("src_1", "flood", "Loc A", None, "dataset_metadata")
        assert id_incident != id_context


class TestUniversalObservationQualityValidation:
    """Verify DataQualityValidator handles incident vs context observations appropriately."""

    def test_context_observation_skips_vocabulary_check(self) -> None:
        """Context observations with event_type=None pass data quality validation."""
        validator = DataQualityValidator()
        obs = DisasterObservation(
            observation_id="obs_manifest_dq",
            source_id="src_manifest",
            raw_claim=json.dumps({"name": "manifest.json"}),
            location="Dataset Manifest",
            event_type=None,
            observation_type=ObservationType.DATASET_METADATA,
            context_payload={"version": "1.0"},
        )
        report = validator.validate([obs])
        assert len(report.accepted_records) == 1
        assert len(report.rejected_record_ids) == 0
        assert report.accepted_records[0].observation_id == obs.observation_id

    def test_incident_observation_enforces_vocabulary_check(self) -> None:
        """Incident observations with unknown hazard type trigger quality warnings."""
        validator = DataQualityValidator()
        obs = DisasterObservation(
            observation_id="obs_unknown_hazard",
            source_id="src_test",
            raw_claim=json.dumps({"type": "unrecognized_hazard"}),
            location="Somewhere",
            event_type="unrecognized_hazard",
            observation_type=ObservationType.INCIDENT,
        )
        report = validator.validate([obs])
        assert len(report.accepted_records) == 1
        assert report.warning_count >= 1
        all_warnings = [outcome.message for outcome in report.outcomes if not outcome.passed or outcome.severity.value == "warning"]
        assert any("category" in msg.lower() or "vocab" in msg.lower() for msg in all_warnings)


class TestUniversalObservationResolutionExclusion:
    """Verify EventResolver strictly excludes context observations from disaster events."""

    def test_resolver_skips_context_observations(self) -> None:
        """Resolver does not create disaster events or decisions for context observations."""
        resolver = EventResolver()
        context_obs = [
            DisasterObservation(
                observation_id="ctx_1",
                source_id="src_manifest",
                raw_claim="{}",
                location="Manifest Location",
                event_type=None,
                observation_type=ObservationType.DATASET_METADATA,
                context_payload={"title": "Manifest"},
            ),
            DisasterObservation(
                observation_id="ctx_2",
                source_id="src_readme",
                raw_claim="{}",
                location="Documentation",
                event_type=None,
                observation_type=ObservationType.DOCUMENT_CONTEXT,
                context_payload={"title": "README"},
            ),
            DisasterObservation(
                observation_id="ctx_3",
                source_id="src_image",
                raw_claim="{}",
                location="Image metadata",
                event_type=None,
                observation_type=ObservationType.IMAGE_CONTEXT,
                context_payload={"width": 100, "height": 100},
            ),
        ]
        result = resolver.resolve_observations(context_obs, existing_events=[])
        assert len(result.resolved_events) == 0
        assert len(result.outcomes) == 0
        assert len(result.merge_candidates) == 0

    def test_resolver_processes_mixed_incident_and_context(self) -> None:
        """In a mixed batch, only incident observations form unified disaster events."""
        resolver = EventResolver()
        now = datetime.now(timezone.utc)
        incident_obs = DisasterObservation(
            observation_id="inc_001",
            source_id="src_telemetry",
            raw_claim=json.dumps({"event": "flood"}),
            location="Harbor District",
            event_type="flood",
            event_time=now,
            latitude=37.7749,
            longitude=-122.4194,
            observation_type=ObservationType.INCIDENT,
        )
        context_obs = DisasterObservation(
            observation_id="ctx_001",
            source_id="src_doc",
            raw_claim="{}",
            location="Harbor District",
            event_type=None,
            observation_type=ObservationType.DOCUMENT_CONTEXT,
            context_payload={"summary": "Field inspection notes"},
        )

        result = resolver.resolve_observations([incident_obs, context_obs], existing_events=[])
        assert len(result.resolved_events) == 1
        assert result.resolved_events[0].event_type == "flood"
        assert result.resolved_events[0].observation_ids == [incident_obs.observation_id]
        # Context observation must not be linked to the event
        assert context_obs.observation_id not in result.resolved_events[0].observation_ids


class TestUniversalObservationStorageAndAnalytics:
    """Storage repository and analytics integration tests for Universal Observation Architecture."""

    @pytest.fixture
    def sqlite_session_factory(self) -> sessionmaker:
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        return sessionmaker(bind=engine)

    def test_persist_and_retrieve_context_observations(self, sqlite_session_factory: sessionmaker) -> None:
        """Context observations persist with observation_type, is_event_eligible, and context_payload."""
        repo = StorageRepository()
        now = datetime.now(timezone.utc)

        s1 = _make_source("src_1", "data.json")
        sm = _make_source("src_m", "manifest.json")
        si = _make_source("src_img", "image.png")

        incident = DisasterObservation(
            observation_id="obs_inc_stored",
            source_id="src_1",
            raw_claim=json.dumps({"type": "wildfire"}),
            location="Pine Ridge",
            event_type="wildfire",
            event_time=now,
            observation_type=ObservationType.INCIDENT,
        )
        manifest_context = DisasterObservation(
            observation_id="obs_man_stored",
            source_id="src_m",
            raw_claim=json.dumps({"dataset": "California Fire 2026"}),
            location="Dataset Manifest",
            event_type=None,
            observation_type=ObservationType.DATASET_METADATA,
            context_payload={"version": "2.0.1", "license": "MIT"},
        )
        image_context = DisasterObservation(
            observation_id="obs_img_stored",
            source_id="src_img",
            raw_claim=json.dumps({"file": "drone_photo.jpg"}),
            location="Aerial Drone",
            event_type=None,
            observation_type=ObservationType.IMAGE_CONTEXT,
            context_payload={"width": 4096, "height": 2160, "format": "JPEG"},
        )

        with sqlite_session_factory() as session:
            for s in [s1, sm, si]:
                repo.save_source(s, session)
            repo.save_observations([incident, manifest_context, image_context], session)
            session.commit()

        with sqlite_session_factory() as session:
            # All observations
            all_obs = repo.get_all_observations(session)
            assert len(all_obs) == 3

            # Filter by type
            meta_obs = repo.get_all_observations(session, observation_type=ObservationType.DATASET_METADATA)
            assert len(meta_obs) == 1
            assert meta_obs[0].context_payload["license"] == "MIT"
            assert meta_obs[0].is_event_eligible is False

            img_obs = repo.get_all_observations(session, observation_type=ObservationType.IMAGE_CONTEXT)
            assert len(img_obs) == 1
            assert img_obs[0].context_payload["width"] == 4096

            # Filter by event_eligible_only
            eligible_obs = repo.get_all_observations(session, event_eligible_only=True)
            assert len(eligible_obs) == 1
            assert eligible_obs[0].observation_id == incident.observation_id

    def test_unassigned_observations_excludes_context_records(self, sqlite_session_factory: sessionmaker) -> None:
        """repo.get_unassigned_observations() never returns context observations as unassigned disasters."""
        repo = StorageRepository()
        s_inc = _make_source("src_inc", "inc.json")
        s_doc = _make_source("src_doc", "doc.txt")

        incident_unassigned = DisasterObservation(
            observation_id="obs_inc_unassigned",
            source_id="src_inc",
            raw_claim=json.dumps({"type": "storm"}),
            location="Coastal Sector",
            event_type="storm",
            observation_type=ObservationType.INCIDENT,
        )
        doc_context = DisasterObservation(
            observation_id="obs_doc_context",
            source_id="src_doc",
            raw_claim=json.dumps({"doc": "handbook.pdf"}),
            location="Reference Guide",
            event_type=None,
            observation_type=ObservationType.DOCUMENT_CONTEXT,
            context_payload={"sections": ["Chapter 1"]},
        )

        with sqlite_session_factory() as session:
            repo.save_source(s_inc, session)
            repo.save_source(s_doc, session)
            repo.save_observations([incident_unassigned, doc_context], session)
            session.commit()

            unassigned = repo.get_unassigned_observations(session)

            # Only the incident observation is considered unassigned disaster intelligence
            assert len(unassigned) == 1
            assert unassigned[0].observation_id == incident_unassigned.observation_id
            assert unassigned[0].observation_type == ObservationType.INCIDENT

    def test_analytics_service_calculates_compression_on_incident_observations_only(
        self, sqlite_session_factory: sessionmaker
    ) -> None:
        """Analytics compression ratio and coordinate gap counters ignore context observations."""
        analytics = AnalyticsService()
        now = datetime.now(timezone.utc)

        s1 = _make_source("src_1", "feed1.json")
        s2 = _make_source("src_2", "feed2.json")
        sm = _make_source("src_m", "manifest.json")
        sr = _make_source("src_r", "readme.txt")
        si = _make_source("src_i", "image.png")

        # 2 incident observations merging into 1 disaster event
        obs1 = DisasterObservation(
            observation_id="obs_inc_1",
            source_id="src_1",
            raw_claim="{}",
            location="East Valley",
            event_type="flood",
            event_time=now,
            latitude=34.05,
            longitude=-118.24,
            observation_type=ObservationType.INCIDENT,
        )
        obs2 = DisasterObservation(
            observation_id="obs_inc_2",
            source_id="src_2",
            raw_claim="{}",
            location="East Valley",
            event_type="flood",
            event_time=now,
            latitude=34.06,
            longitude=-118.25,
            observation_type=ObservationType.INCIDENT,
        )
        # 3 context observations (no coordinates, no timestamps)
        context1 = DisasterObservation(
            observation_id="obs_ctx_1",
            source_id="src_m",
            raw_claim="{}",
            location="Manifest",
            event_type=None,
            observation_type=ObservationType.DATASET_METADATA,
        )
        context2 = DisasterObservation(
            observation_id="obs_ctx_2",
            source_id="src_r",
            raw_claim="{}",
            location="Readme",
            event_type=None,
            observation_type=ObservationType.DOCUMENT_CONTEXT,
        )
        context3 = DisasterObservation(
            observation_id="obs_ctx_3",
            source_id="src_i",
            raw_claim="{}",
            location="Image",
            event_type=None,
            observation_type=ObservationType.IMAGE_CONTEXT,
        )

        event = UnifiedDisasterEvent(
            event_id="evt_001",
            event_type="flood",
            location="East Valley",
            start_time=now,
            summary="Flooding in East Valley",
            observation_ids=[obs1.observation_id, obs2.observation_id],
            source_ids=[obs1.source_id, obs2.source_id],
            created_at=now,
            updated_at=now,
        )

        all_obs = [obs1, obs2, context1, context2, context3]

        # In-memory summarize
        summary_in_mem = analytics.summarize(events=[event], observations=all_obs)
        # Compression ratio must be 2 incident obs / 1 event = 2.0 (NOT 5 / 1 = 5.0)
        assert summary_in_mem.total_observations == 2
        assert summary_in_mem.compression_ratio == 2.0
        assert summary_in_mem.coverage_gaps.observations_missing_coordinates == 0
        assert summary_in_mem.coverage_gaps.observations_missing_timestamps == 0

        # Database-backed summarize_db
        repo = StorageRepository()
        with sqlite_session_factory() as session:
            for s in [s1, s2, sm, sr, si]:
                repo.save_source(s, session)
            repo.save_observations(all_obs, session)
            repo.save_event(event, session)
            session.commit()

            summary_db = analytics.summarize_db(session)
            assert summary_db.total_observations == 2
            assert summary_db.total_events == 1
            assert summary_db.compression_ratio == 2.0
            assert summary_db.coverage_gaps.observations_missing_coordinates == 0
            assert summary_db.coverage_gaps.observations_missing_timestamps == 0


class TestEventResolutionIsolation:
    """Verification that all context observation types are strictly excluded from event resolution.

    Context observations must never participate in:
    - Event clustering and candidate generation.
    - Event creation.
    - Event updates.
    - Event merging.
    - Disaster analytics.
    And all 12 incident observations must remain eligible for normal resolution.
    """

    @pytest.fixture
    def sample_demo_observations(self) -> tuple[list[DisasterObservation], list[DisasterObservation]]:
        """Load observations from the 7-file demonstration dataset.

        Returns (incident_observations [12], context_observations [3]).
        """
        from pathlib import Path
        from beacon.config import Settings
        from beacon.dashboard.services import DashboardService

        demo_dir = Path(r"C:\Users\NWNB0000\Downloads\BEACON_MultiFormat_Pipeline_Demo")
        if not demo_dir.exists():
            pytest.skip("Demonstration dataset directory not found")

        uploads = [(f.name, f.read_bytes()) for f in sorted(demo_dir.iterdir()) if f.is_file()]
        service = DashboardService(settings=Settings(database_url=None), session_factory=None)
        res = service.ingest_and_process_uploads(uploads, enable_resolution=False, persist_db=False)
        all_obs = res["observations"]

        incident_obs = [o for o in all_obs if o.observation_type == ObservationType.INCIDENT]
        context_obs = [o for o in all_obs if o.observation_type != ObservationType.INCIDENT]
        return incident_obs, context_obs

    def test_all_twelve_incident_observations_remain_eligible_for_resolution(
        self, sample_demo_observations: tuple[list[DisasterObservation], list[DisasterObservation]]
    ) -> None:
        """Verify the 7-file dataset contains exactly 12 incident observations, all event-eligible."""
        incident_obs, context_obs = sample_demo_observations
        assert len(incident_obs) == 12
        assert len(context_obs) == 3

        # All 12 incident observations are eligible for event resolution
        for obs in incident_obs:
            assert obs.is_incident is True
            assert obs.is_event_eligible is True
            assert obs.event_type is not None
            assert len(obs.event_type.strip()) > 0

        # Normal resolution processes all 12 incident observations
        resolver = EventResolver()
        result = resolver.resolve_observations(incident_obs, existing_events=[])
        assert len(result.resolved_events) > 0
        resolved_obs_ids = {oid for ev in result.resolved_events for oid in ev.observation_ids}
        for obs in incident_obs:
            assert obs.observation_id in resolved_obs_ids

    def test_context_types_excluded_from_event_clustering_and_candidate_generation(self) -> None:
        """Context observation types are never clustered or matched against existing events."""
        from beacon.resolution.candidate_generator import CandidateGenerator

        generator = CandidateGenerator()
        now = datetime.now(timezone.utc)

        existing_event = UnifiedDisasterEvent(
            event_id="evt_flood_01",
            event_type="flood",
            location="San Francisco Metro",
            start_time=now,
            summary="Flooding in SF",
            observation_ids=["obs_inc_01"],
            source_ids=["src_01"],
            created_at=now,
            updated_at=now,
        )

        context_obs_list = [
            DisasterObservation(
                observation_id="ctx_meta_1",
                source_id="src_meta",
                raw_claim="{}",
                location="San Francisco Metro",
                event_type=None,
                event_time=now,
                latitude=37.7749,
                longitude=-122.4194,
                observation_type=ObservationType.DATASET_METADATA,
                context_payload={"title": "SF Dataset"},
            ),
            DisasterObservation(
                observation_id="ctx_doc_1",
                source_id="src_doc",
                raw_claim="{}",
                location="San Francisco Metro",
                event_type=None,
                event_time=now,
                latitude=37.7749,
                longitude=-122.4194,
                observation_type=ObservationType.DOCUMENT_CONTEXT,
                context_payload={"readme": "SF Notes"},
            ),
            DisasterObservation(
                observation_id="ctx_img_1",
                source_id="src_img",
                raw_claim="{}",
                location="San Francisco Metro",
                event_type=None,
                event_time=now,
                latitude=37.7749,
                longitude=-122.4194,
                observation_type=ObservationType.IMAGE_CONTEXT,
                context_payload={"filename": "photo.jpg"},
            ),
        ]

        # Context observations do not generate candidate pairs
        for ctx in context_obs_list:
            candidates = generator.generate_candidates(ctx, [existing_event])
            assert len(candidates) == 0, f"Context type {ctx.observation_type} generated candidate pairs"

    def test_context_types_excluded_from_event_creation(self) -> None:
        """Context observations passed alone never create any unified disaster events."""
        resolver = EventResolver()

        context_batch = [
            DisasterObservation(
                observation_id="ctx_c1",
                source_id="src_1",
                raw_claim="{}",
                location="Sector 7",
                event_type=None,
                observation_type=ObservationType.DATASET_METADATA,
            ),
            DisasterObservation(
                observation_id="ctx_c2",
                source_id="src_2",
                raw_claim="{}",
                location="Sector 7",
                event_type=None,
                observation_type=ObservationType.DOCUMENT_CONTEXT,
            ),
            DisasterObservation(
                observation_id="ctx_c3",
                source_id="src_3",
                raw_claim="{}",
                location="Sector 7",
                event_type=None,
                observation_type=ObservationType.IMAGE_CONTEXT,
            ),
        ]

        result = resolver.resolve_observations(context_batch, existing_events=[])
        assert len(result.resolved_events) == 0
        assert result.events_created == 0
        assert len(result.outcomes) == 0
        assert len(result.merge_candidates) == 0

    def test_context_types_excluded_from_event_updates(self) -> None:
        """Context observations matching an existing event location/time never mutate or update it."""
        resolver = EventResolver()
        now = datetime.now(timezone.utc)

        original_event = UnifiedDisasterEvent(
            event_id="evt_existing_01",
            event_type="wildfire",
            location="Pine Valley",
            start_time=now,
            summary="Pine Valley wildfire",
            observation_ids=["obs_inc_pine_01"],
            source_ids=["src_pine_01"],
            created_at=now,
            updated_at=now,
        )

        matching_context = DisasterObservation(
            observation_id="ctx_pine_doc",
            source_id="src_doc",
            raw_claim="{}",
            location="Pine Valley",
            event_type=None,
            event_time=now,
            latitude=34.0,
            longitude=-118.0,
            observation_type=ObservationType.DOCUMENT_CONTEXT,
            context_payload={"doc_type": "evacuation_guide"},
        )

        result = resolver.resolve_observations(
            [matching_context], existing_events=[original_event]
        )
        assert result.events_updated == 0
        assert result.events_created == 0
        # The existing event's observation IDs must remain completely untouched
        assert original_event.observation_ids == ["obs_inc_pine_01"]
        assert "ctx_pine_doc" not in original_event.observation_ids

    def test_context_types_excluded_from_event_merging(self) -> None:
        """Context observations never generate merge candidates or link separate events."""
        resolver = EventResolver()
        now = datetime.now(timezone.utc)

        ev1 = UnifiedDisasterEvent(
            event_id="evt_bridge_1",
            event_type="flood",
            location="Zone A",
            start_time=now,
            summary="Zone A Flood",
            observation_ids=["obs_1"],
            source_ids=["s1"],
            created_at=now,
            updated_at=now,
        )
        ev2 = UnifiedDisasterEvent(
            event_id="evt_bridge_2",
            event_type="flood",
            location="Zone B",
            start_time=now,
            summary="Zone B Flood",
            observation_ids=["obs_2"],
            source_ids=["s2"],
            created_at=now,
            updated_at=now,
        )

        bridge_context = DisasterObservation(
            observation_id="ctx_bridge",
            source_id="src_meta",
            raw_claim="{}",
            location="Zone A and Zone B Bridge Notes",
            event_type=None,
            observation_type=ObservationType.DATASET_METADATA,
            context_payload={"zones": ["Zone A", "Zone B"]},
        )

        result = resolver.resolve_observations(
            [bridge_context], existing_events=[ev1, ev2]
        )
        assert len(result.merge_candidates) == 0
        assert result.events_updated == 0
        assert result.events_created == 0

    def test_context_types_excluded_from_disaster_analytics(self) -> None:
        """Context observations are strictly excluded from all disaster analytics metrics."""
        analytics = AnalyticsService()
        now = datetime.now(timezone.utc)

        inc_obs = DisasterObservation(
            observation_id="obs_inc_ana",
            source_id="src_1",
            raw_claim="{}",
            location="Alpha Zone",
            event_type="earthquake",
            event_time=now,
            latitude=35.0,
            longitude=-119.0,
            severity="moderate",
            observation_type=ObservationType.INCIDENT,
        )

        # Context observations with missing timestamps and coordinates
        ctx_obs_list = [
            DisasterObservation(
                observation_id=f"ctx_ana_{t.value}",
                source_id=f"src_{t.value}",
                raw_claim="{}",
                location="Metadata Zone",
                event_type=None,
                event_time=None,
                latitude=None,
                longitude=None,
                observation_type=t,
            )
            for t in [
                ObservationType.DATASET_METADATA,
                ObservationType.DOCUMENT_CONTEXT,
                ObservationType.IMAGE_CONTEXT,
            ]
        ]

        ev = UnifiedDisasterEvent(
            event_id="evt_ana_01",
            event_type="earthquake",
            location="Alpha Zone",
            start_time=now,
            summary="Earthquake Alpha",
            severity="moderate",
            observation_ids=[inc_obs.observation_id],
            source_ids=[inc_obs.source_id],
            created_at=now,
            updated_at=now,
        )

        all_obs = [inc_obs] + ctx_obs_list
        summary = analytics.summarize(events=[ev], observations=all_obs)

        # Only the 1 incident observation is counted in disaster analytics
        assert summary.total_observations == 1
        assert summary.total_events == 1
        assert summary.compression_ratio == 1.0
        assert summary.coverage_gaps.observations_missing_coordinates == 0
        assert summary.coverage_gaps.observations_missing_timestamps == 0


class TestUniversalObservationBackwardCompatibility:
    """Backward compatibility tests ensuring v1.0 incident observations function seamlessly in v1.1."""

    def test_v1_0_observation_defaults_and_eligibility(self) -> None:
        """Existing v1.0 incident observations naturally default to INCIDENT and event-eligible."""
        obs = DisasterObservation(
            observation_id="obs_v1_0_legacy",
            source_id="src_legacy",
            raw_claim=json.dumps({"event": "flood", "legacy_field": "preserved"}),
            location="Legacy District",
            event_type="flood",
            event_time=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
            severity="moderate",
            latitude=34.05,
            longitude=-118.25,
            source_reference="Page 12, line 4",
        )
        # Verify backward-compatible classification defaults
        assert obs.observation_type == ObservationType.INCIDENT
        assert obs.is_incident is True
        assert obs.is_event_eligible is True
        assert obs.context_payload == {}
        # Verify all v1.0 fields are preserved verbatim
        assert obs.observation_id == "obs_v1_0_legacy"
        assert obs.source_id == "src_legacy"
        assert obs.source_reference == "Page 12, line 4"
        assert obs.event_type == "flood"
        assert obs.severity == "moderate"

    def test_v1_0_storage_repository_compatibility(self) -> None:
        """StorageRepository loads and queries v1.0-style observations with event associations."""
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        factory = sessionmaker(bind=engine)
        repo = StorageRepository()

        now = datetime.now(timezone.utc)
        src = _make_source("src_leg", "legacy_feed.json")
        obs_legacy = DisasterObservation(
            observation_id="obs_leg_01",
            source_id="src_leg",
            raw_claim="{}",
            location="Metro City",
            event_type="cyclone",
            event_time=now,
            latitude=20.0,
            longitude=85.0,
            severity="severe",
            source_reference="legacy_report.txt:42",
        )
        evt_legacy = UnifiedDisasterEvent(
            event_id="evt_leg_01",
            event_type="cyclone",
            location="Metro City",
            start_time=now,
            summary="Cyclone in Metro City",
            severity="severe",
            observation_ids=[obs_legacy.observation_id],
            source_ids=[obs_legacy.source_id],
            created_at=now,
            updated_at=now,
        )

        with factory() as session:
            repo.save_source(src, session)
            repo.save_observations([obs_legacy], session)
            repo.save_event(evt_legacy, session)
            repo.link_observation_to_event(evt_legacy.event_id, obs_legacy.observation_id, 0.95, session)
            session.commit()

        # Reload using fresh session
        with factory() as session:
            all_loaded = repo.get_all_observations(session)
            assert len(all_loaded) == 1
            loaded_obs = all_loaded[0]
            assert loaded_obs is not None
            assert loaded_obs.observation_id == "obs_leg_01"
            assert loaded_obs.source_reference == "legacy_report.txt:42"
            assert loaded_obs.observation_type == ObservationType.INCIDENT
            assert loaded_obs.is_event_eligible is True
            assert loaded_obs.is_incident is True

            loaded_evt = repo.get_event(evt_legacy.event_id, session)
            assert loaded_evt is not None
            assert loaded_evt.observation_ids == ["obs_leg_01"]


class TestUniversalObservationRepeatIngestion:
    """Verification of repeat-ingestion idempotency on the 7-file demonstration batch."""

    def test_seven_file_repeat_ingestion_counts_and_stability(self) -> None:
        """Ingesting the 7-file dataset twice yields identical counts and zero growth."""
        from pathlib import Path
        from beacon.config import Settings
        from beacon.dashboard.services import DashboardService

        demo_dir = Path(r"C:\Users\NWNB0000\Downloads\BEACON_MultiFormat_Pipeline_Demo")
        if not demo_dir.exists():
            pytest.skip("Demonstration dataset directory not found")

        uploads = [(f.name, f.read_bytes()) for f in sorted(demo_dir.iterdir()) if f.is_file()]

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        factory = sessionmaker(bind=engine)
        repo = StorageRepository()

        service = DashboardService(
            settings=Settings(database_url="sqlite:///:memory:"),
            session_factory=factory,
        )

        # Run 1
        res1 = service.ingest_and_process_uploads(uploads, enable_resolution=True, persist_db=True)
        obs_run1 = res1["observations"]
        events_run1 = res1["events"]

        # Run 1 assertions: 15 total, 12 incident, 1 metadata, 1 doc, 1 image
        assert len(obs_run1) == 15
        type_counts_1 = {
            t: sum(1 for o in obs_run1 if o.observation_type.value == t)
            for t in ["incident", "dataset_metadata", "document_context", "image_context"]
        }
        assert type_counts_1["incident"] == 12
        assert type_counts_1["dataset_metadata"] == 1
        assert type_counts_1["document_context"] == 1
        assert type_counts_1["image_context"] == 1
        assert len(events_run1) == 8

        # Run 2 (repeat ingestion with exact same files)
        res2 = service.ingest_and_process_uploads(uploads, enable_resolution=True, persist_db=True)
        obs_run2 = res2["observations"]
        events_run2 = res2["events"]
        assert len(events_run2) == 8

        # Verify storage counts
        with factory() as session:
            db_obs = repo.get_all_observations(session)
            db_events = repo.get_all_events(session)
            assert len(db_obs) == 15, f"Expected 15 persisted observations, found {len(db_obs)}"
            assert len(db_events) == 8, f"Expected 8 persisted events, found {len(db_events)}"

            # Verify no duplicate IDs
            obs_ids = [o.observation_id for o in db_obs]
            assert len(obs_ids) == len(set(obs_ids))

            # Verify exact ID set equality between run 1 and run 2
            run1_ids = {o.observation_id for o in obs_run1}
            run2_ids = {o.observation_id for o in obs_run2}
            assert run1_ids == run2_ids

