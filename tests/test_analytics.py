"""Unit tests for AnalyticsService and disaster intelligence summaries."""

from datetime import datetime, timezone
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from beacon.analytics.models import AnalyticsFilter
from beacon.analytics.service import AnalyticsService
from beacon.models import (
    ConflictingClaim,
    DisasterObservation,
    UnifiedDisasterEvent,
)
from beacon.resolution.models import MergeCandidate
from beacon.storage.repository import StorageRepository
from beacon.storage.schema import Base


@pytest.fixture
def analytics_service() -> AnalyticsService:
    return AnalyticsService()


@pytest.fixture
def sample_events() -> list[UnifiedDisasterEvent]:
    return [
        UnifiedDisasterEvent(
            event_id="evt_01",
            event_type="flood",
            location="Riverside Metro",
            start_time=datetime(2026, 5, 15, 10, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2026, 5, 15, 18, 0, 0, tzinfo=timezone.utc),
            summary="Multi-source flood event.",
            severity="severe",
            observation_ids=["obs_01", "obs_02", "obs_03"],
            source_ids=["src_json", "src_csv", "src_txt"],
            conflicting_claims=[
                ConflictingClaim(
                    field_name="severity",
                    observation_id="obs_02",
                    source_id="src_csv",
                    claimed_value="moderate",
                    resolution_status="superseded",
                )
            ],
            metadata={"centroid": {"latitude": 34.05, "longitude": -118.24}},
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        ),
        UnifiedDisasterEvent(
            event_id="evt_02",
            event_type="wildfire",
            location="Pine Valley",
            start_time=datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
            summary="Single-source wildfire event.",
            severity="moderate",
            observation_ids=["obs_04"],
            source_ids=["src_txt"],
            conflicting_claims=[],
            metadata={"unanchored_timestamp": True},  # No centroid either
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        ),
    ]


class TestAnalyticsService:
    """Tests for AnalyticsService calculations and filtering."""

    def test_in_memory_summary_metrics(
        self, analytics_service: AnalyticsService, sample_events: list[UnifiedDisasterEvent]
    ) -> None:
        summary = analytics_service.summarize(sample_events)

        assert summary.total_events == 2
        assert summary.total_observations == 4
        assert summary.compression_ratio == 2.0
        assert summary.multi_source_events == 1
        assert summary.single_source_events == 1
        assert summary.multi_source_ratio == 0.5

        assert summary.events_by_type == {"flood": 1, "wildfire": 1}
        assert summary.events_by_severity == {"severe": 1, "moderate": 1}

        # Conflicting claims
        assert summary.conflicting_claims_count == 1
        assert summary.conflicting_claims_by_field == {"severity": 1}

        # Coverage gaps
        assert summary.coverage_gaps.events_missing_coordinates == 1  # evt_02 has no centroid
        assert summary.coverage_gaps.events_unanchored_timestamps == 1

    def test_in_memory_filtering(
        self, analytics_service: AnalyticsService, sample_events: list[UnifiedDisasterEvent]
    ) -> None:
        # Filter by min_sources = 2
        filter_multi = AnalyticsFilter(min_sources=2)
        summary = analytics_service.summarize(sample_events, filter_criteria=filter_multi)
        assert summary.total_events == 1
        assert summary.events_by_type == {"flood": 1}

        # Filter by event_type
        filter_type = AnalyticsFilter(event_type="wildfire")
        summary_wf = analytics_service.summarize(sample_events, filter_criteria=filter_type)
        assert summary_wf.total_events == 1
        assert summary_wf.events_by_type == {"wildfire": 1}

    def test_db_backed_summary(
        self, analytics_service: AnalyticsService, sample_events: list[UnifiedDisasterEvent]
    ) -> None:
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        repo = StorageRepository()

        with Session(engine) as session:
            for ev in sample_events:
                repo.save_event(ev, session)
            session.commit()

            summary = analytics_service.summarize_db(session)
            assert summary.total_events == 2
            assert summary.multi_source_events == 1
            assert summary.single_source_events == 1
            assert summary.events_by_type == {"flood": 1, "wildfire": 1}
            assert summary.conflicting_claims_count == 1

    def test_db_backed_filtered_analytics(
        self, analytics_service: AnalyticsService
    ) -> None:
        """Verify DB-backed analytics correctly scope observation metrics to filtered events."""
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        repo = StorageRepository()

        # Two events of different types
        ev_flood = UnifiedDisasterEvent(
            event_id="evt_fl_100",
            event_type="flood",
            location="Riverside",
            start_time=datetime(2026, 5, 15, 10, 0, tzinfo=timezone.utc),
            summary="Flood event with 2 observations and 2 sources.",
            severity="severe",
            observation_ids=["obs_fl_1", "obs_fl_2"],
            source_ids=["src_json", "src_csv"],
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            metadata={"centroid": {"latitude": 34.05, "longitude": -118.24}},
        )
        ev_wildfire = UnifiedDisasterEvent(
            event_id="evt_wf_200",
            event_type="wildfire",
            location="Pine Ridge",
            start_time=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
            summary="Wildfire event with 1 observation and 1 source.",
            severity="moderate",
            observation_ids=["obs_wf_1"],
            source_ids=["src_txt"],
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            metadata={"unanchored_timestamp": True},  # Missing centroid and unanchored
        )

        # Observations across both events
        obs_fl_1 = DisasterObservation(
            observation_id="obs_fl_1",
            source_id="src_json",
            event_type="flood",
            location="Riverside",
            event_time=datetime(2026, 5, 15, 10, 0, tzinfo=timezone.utc),
            latitude=34.05,
            longitude=-118.24,
        )
        obs_fl_2 = DisasterObservation(
            observation_id="obs_fl_2",
            source_id="src_csv",
            event_type="flood",
            location="Riverside",
            event_time=datetime(2026, 5, 15, 11, 0, tzinfo=timezone.utc),
            latitude=None,  # Missing coordinates
            longitude=None,
        )
        obs_wf_1 = DisasterObservation(
            observation_id="obs_wf_1",
            source_id="src_txt",
            event_type="wildfire",
            location="Pine Ridge",
            event_time=None,  # Missing timestamp
            latitude=None,   # Missing coordinates
            longitude=None,
        )

        with Session(engine) as session:
            repo.save_event(ev_flood, session)
            repo.save_event(ev_wildfire, session)
            repo.save_observations([obs_fl_1, obs_fl_2, obs_wf_1], session)
            repo.link_observation_to_event(ev_flood.event_id, obs_fl_1.observation_id, 0.95, session)
            repo.link_observation_to_event(ev_flood.event_id, obs_fl_2.observation_id, 0.85, session)
            repo.link_observation_to_event(ev_wildfire.event_id, obs_wf_1.observation_id, 0.90, session)
            session.commit()

            # 1. Unfiltered summary
            unfiltered = analytics_service.summarize_db(session)
            assert unfiltered.total_events == 2
            assert unfiltered.total_observations == 3
            assert unfiltered.compression_ratio == 1.5
            assert unfiltered.multi_source_events == 1
            assert unfiltered.single_source_events == 1
            assert unfiltered.coverage_gaps.observations_missing_coordinates == 2
            assert unfiltered.coverage_gaps.observations_missing_timestamps == 1

            # 2. Filtered by event_type="flood" -> Scoped only to flood event and its 2 observations
            flood_filter = AnalyticsFilter(event_type="flood")
            flood_summary = analytics_service.summarize_db(session, filter_criteria=flood_filter)
            assert flood_summary.total_events == 1
            assert flood_summary.total_observations == 2
            assert flood_summary.compression_ratio == 2.0
            assert flood_summary.multi_source_events == 1
            assert flood_summary.single_source_events == 0
            assert flood_summary.multi_source_ratio == 1.0
            assert flood_summary.coverage_gaps.events_missing_coordinates == 0
            assert flood_summary.coverage_gaps.observations_missing_coordinates == 1  # only obs_fl_2
            assert flood_summary.coverage_gaps.observations_missing_timestamps == 0   # neither flood obs missing time

            # 3. Filtered by event_type="wildfire" -> Scoped only to wildfire event and its 1 observation
            wf_filter = AnalyticsFilter(event_type="wildfire")
            wf_summary = analytics_service.summarize_db(session, filter_criteria=wf_filter)
            assert wf_summary.total_events == 1
            assert wf_summary.total_observations == 1
            assert wf_summary.compression_ratio == 1.0
            assert wf_summary.multi_source_events == 0
            assert wf_summary.single_source_events == 1
            assert wf_summary.coverage_gaps.events_missing_coordinates == 1
            assert wf_summary.coverage_gaps.observations_missing_coordinates == 1
            assert wf_summary.coverage_gaps.observations_missing_timestamps == 1

    def test_db_backed_filter_matching_zero_events(
        self, analytics_service: AnalyticsService, sample_events: list[UnifiedDisasterEvent]
    ) -> None:
        """Verify filters matching zero events handle compression ratio without division by zero."""
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        repo = StorageRepository()

        with Session(engine) as session:
            for ev in sample_events:
                repo.save_event(ev, session)
            session.commit()

            no_match_filter = AnalyticsFilter(event_type="cyclone")
            summary = analytics_service.summarize_db(session, filter_criteria=no_match_filter)

            assert summary.total_events == 0
            assert summary.total_observations == 0
            assert summary.compression_ratio == 0.0
            assert summary.multi_source_events == 0
            assert summary.single_source_events == 0
            assert summary.multi_source_ratio == 0.0
            assert summary.events_by_type == {}
