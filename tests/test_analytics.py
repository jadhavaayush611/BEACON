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
    ExtractionMethod,
    ExtractionStatus,
    SourceRecord,
    SourceType,
    UnifiedDisasterEvent,
)
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

    def test_analytics_metrics_stability_under_repeated_resolution_decisions(
        self, analytics_service: AnalyticsService
    ) -> None:
        """Verify ambiguous_observations_count and merge_candidates_count remain stable across repeated runs."""
        from beacon.resolution.models import DecisionType, FeatureScores, MergeCandidate, ResolutionOutcome

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        repo = StorageRepository()

        ev = UnifiedDisasterEvent(
            event_id="evt_stab_001",
            event_type="flood",
            location="Metro Bay",
            start_time=datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
            summary="Test event for stability.",
            observation_ids=["obs_stab_001"],
            source_ids=["src_stab"],
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )

        with Session(engine) as session:
            repo.save_event(ev, session)

            # Observation 1 has an ambiguous decision recorded in Run 1
            outcome1 = ResolutionOutcome(
                observation_id="obs_stab_001",
                assigned_event_id="evt_stab_001",
                decision=DecisionType.AMBIGUOUS_CANDIDATE,
                score=0.65,
                is_new_event=False,
                rationale="Candidate in ambiguous range.",
                feature_scores=FeatureScores(event_type_score=1.0, composite_score=0.65, missing_features=[]),
            )
            repo.save_resolution_decision(outcome1, session)

            # In Run 2, the exact same observation is re-evaluated and produces another ambiguous decision
            outcome2 = ResolutionOutcome(
                observation_id="obs_stab_001",
                assigned_event_id="evt_stab_001",
                decision=DecisionType.AMBIGUOUS_CANDIDATE,
                score=0.66,
                is_new_event=False,
                rationale="Re-evaluated candidate still in ambiguous range.",
                feature_scores=FeatureScores(event_type_score=1.0, composite_score=0.66, missing_features=[]),
            )
            repo.save_resolution_decision(outcome2, session)

            # Persist a merge candidate twice
            mc = MergeCandidate(
                primary_event_id="evt_stab_001",
                secondary_event_id="evt_stab_002",
                bridging_observation_id="obs_stab_001",
                similarity_score=0.82,
                rationale="Bridge detected.",
            )
            repo.save_merge_candidates([mc], session)
            repo.save_merge_candidates([mc], session)
            session.commit()

            # Global unfiltered summary
            summary = analytics_service.summarize_db(session)
            # Must count 1 distinct ambiguous observation, NOT 2 audit decision rows
            assert summary.ambiguous_observations_count == 1
            # Merge candidate count must be 1, NOT 2
            assert summary.merge_candidates_count == 1

            # Filtered summary
            flt = AnalyticsFilter(event_type="flood")
            filtered_summary = analytics_service.summarize_db(session, filter_criteria=flt)
            assert filtered_summary.ambiguous_observations_count == 1
            assert filtered_summary.merge_candidates_count == 1

    def test_dashboard_event_detail_decision_ordering(
        self
    ) -> None:
        """Verify get_event_detail returns resolution decisions in chronological descending order."""
        from sqlalchemy.orm import sessionmaker
        from beacon.dashboard.services import DashboardService
        from beacon.resolution.models import DecisionType, ResolutionOutcome

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        repo = StorageRepository()
        factory = sessionmaker(bind=engine, expire_on_commit=False)

        ev = UnifiedDisasterEvent(
            event_id="evt_order_001",
            event_type="flood",
            location="Harbor Side",
            start_time=datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
            summary="Ordering test event.",
            observation_ids=["obs_ord_001"],
            source_ids=["src_ord"],
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )

        with factory() as session:
            repo.save_event(ev, session)
            d1 = ResolutionOutcome(
                observation_id="obs_ord_001",
                assigned_event_id=ev.event_id,
                decision=DecisionType.CONFIDENT_MATCH,
                score=0.90,
                is_new_event=False,
                rationale="Initial match.",
            )
            repo.save_resolution_decision(d1, session)
            session.commit()

            d2 = ResolutionOutcome(
                observation_id="obs_ord_001",
                assigned_event_id=ev.event_id,
                decision=DecisionType.IDEMPOTENT_RE_OBSERVATION,
                score=0.92,
                is_new_event=False,
                rationale="Re-observation.",
            )
            repo.save_resolution_decision(d2, session)
            session.commit()

        service = DashboardService(session_factory=factory, repository=repo)
        detail = service.get_event_detail(ev.event_id)
        assert detail is not None
        decisions = detail["decisions"]
        assert len(decisions) == 2
        # Latest audit event (idempotent_re_observation) must appear first
        assert decisions[0]["decision_type"] == "idempotent_re_observation"
        assert decisions[1]["decision_type"] == "confident_match"

    def test_unassigned_ambiguous_observation_lifecycle_and_queries(
        self, analytics_service: AnalyticsService
    ) -> None:
        """Verify unassigned ambiguous observations are traceable, queried, and summarized accurately."""
        from sqlalchemy.orm import sessionmaker
        from beacon.resolution.models import DecisionType, ResolutionOutcome, ResolutionResult

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        repo = StorageRepository()
        factory = sessionmaker(bind=engine, expire_on_commit=False)

        with factory() as session:
            # 1. Store a source record
            src = SourceRecord(
                source_id="src_ambig_test",
                source_file="data/raw/ambig.json",
                source_type=SourceType.JSON,
                extraction_method=ExtractionMethod.JSON_PARSE,
                content_hash="a" * 64,
                extraction_status=ExtractionStatus.SUCCESS,
                ingested_at=datetime.now(timezone.utc),
            )
            repo.save_source(src, session)

            # 2. Store an observation that will be ambiguous
            obs_ambig = DisasterObservation(
                observation_id="obs_ambig_001",
                source_id=src.source_id,
                event_type="flood",
                location="Border Zone",
                event_time=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
                severity="moderate",
            )
            repo.save_observations([obs_ambig], session)

            # 3. Save resolution result with ambiguous decision (assigned_event_id=None)
            decision = ResolutionOutcome(
                observation_id=obs_ambig.observation_id,
                assigned_event_id=None,
                decision=DecisionType.AMBIGUOUS_CANDIDATE,
                score=0.45,
                is_new_event=False,
                rationale="Candidate score below threshold; quarantined for review.",
            )
            res_result = ResolutionResult(
                resolved_events=[],
                outcomes=[decision],
                merge_candidates=[],
            )
            repo.save_resolution_result(res_result, session)
            session.commit()

            # 4. Verify observation is unassigned via repository queries
            unassigned = repo.get_unassigned_observations(session)
            assert len(unassigned) == 1
            assert unassigned[0].observation_id == "obs_ambig_001"

            unassigned_with_dec = repo.get_unassigned_observations_with_decisions(session)
            assert len(unassigned_with_dec) == 1
            assert unassigned_with_dec[0]["observation"].observation_id == "obs_ambig_001"
            assert unassigned_with_dec[0]["decision"]["decision_type"] == "ambiguous_candidate"
            assert unassigned_with_dec[0]["decision"]["rationale"] == "Candidate score below threshold; quarantined for review."

            # 5. Verify analytics summary counts ambiguous observations
            summary = analytics_service.summarize_db(session)
            assert summary.total_events == 0
            assert summary.total_observations == 1
            assert summary.ambiguous_observations_count == 1

            # 6. Verify event-filtered summary does not misattribute to arbitrary event
            flt = AnalyticsFilter(event_type="earthquake")
            filtered_summary = analytics_service.summarize_db(session, filter_criteria=flt)
            assert filtered_summary.total_events == 0
            assert filtered_summary.total_observations == 0

    def test_unassigned_observation_later_assignment(
        self
    ) -> None:
        """Verify an unassigned observation can later be assigned when corroborating evidence matches."""
        from sqlalchemy.orm import sessionmaker
        from beacon.resolution.models import DecisionType, ResolutionOutcome, ResolutionResult

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        repo = StorageRepository()
        factory = sessionmaker(bind=engine, expire_on_commit=False)

        with factory() as session:
            src = SourceRecord(
                source_id="src_later_test",
                source_file="data/raw/later.json",
                source_type=SourceType.JSON,
                extraction_method=ExtractionMethod.JSON_PARSE,
                content_hash="b" * 64,
                extraction_status=ExtractionStatus.SUCCESS,
                ingested_at=datetime.now(timezone.utc),
            )
            repo.save_source(src, session)

            obs = DisasterObservation(
                observation_id="obs_later_001",
                source_id=src.source_id,
                event_type="wildfire",
                location="Timber Ridge",
                event_time=datetime(2026, 7, 10, 14, 0, tzinfo=timezone.utc),
                severity="moderate",
            )
            repo.save_observations([obs], session)

            # Initially ambiguous
            d_init = ResolutionOutcome(
                observation_id=obs.observation_id,
                assigned_event_id=None,
                decision=DecisionType.AMBIGUOUS_CANDIDATE,
                score=0.48,
                is_new_event=False,
                rationale="Uncertain match initially.",
            )
            repo.save_resolution_result(
                ResolutionResult(resolved_events=[], outcomes=[d_init], merge_candidates=[]),
                session,
            )
            session.commit()

            assert len(repo.get_unassigned_observations(session)) == 1

            # Later assignment to a confirmed event
            ev = UnifiedDisasterEvent(
                event_id="evt_timber_ridge",
                event_type="wildfire",
                location="Timber Ridge",
                start_time=datetime(2026, 7, 10, 14, 0, tzinfo=timezone.utc),
                summary="Timber Ridge Wildfire Event.",
                observation_ids=[obs.observation_id],
                source_ids=[src.source_id],
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
            d_match = ResolutionOutcome(
                observation_id=obs.observation_id,
                assigned_event_id=ev.event_id,
                decision=DecisionType.CONFIDENT_MATCH,
                score=0.95,
                is_new_event=False,
                rationale="Corroborated by high-confidence ground report.",
            )
            repo.save_resolution_result(
                ResolutionResult(resolved_events=[ev], outcomes=[d_match], merge_candidates=[]),
                session,
            )
            session.commit()

            # Now observation is assigned: get_unassigned_observations must return 0
            assert len(repo.get_unassigned_observations(session)) == 0
            assert len(repo.get_unassigned_observations(session)) == 0
