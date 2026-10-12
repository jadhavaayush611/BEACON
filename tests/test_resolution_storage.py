"""Unit tests for StorageRepository Phase 4 resolution and event operations."""

from datetime import datetime, timezone
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import UniqueConstraint

from beacon.models import (
    ConflictingClaim,
    DisasterObservation,
    UnifiedDisasterEvent,
)
from beacon.resolution.models import (
    DecisionType,
    FeatureScores,
    MergeCandidate,
    ResolutionOutcome,
    ResolutionResult,
)
from beacon.storage.repository import StorageRepository
from beacon.storage.schema import (
    Base,
    EventModel,
    EventObservationModel,
    MergeCandidateModel,
    ObservationModel,
    ResolutionDecisionModel,
    SourceModel,
)


@pytest.fixture
def mem_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


@pytest.fixture
def sample_event() -> UnifiedDisasterEvent:
    return UnifiedDisasterEvent(
        event_id="evt_test_01",
        event_type="flood",
        location="Riverside Metro",
        start_time=datetime(2026, 5, 15, 10, 0, 0, tzinfo=timezone.utc),
        end_time=datetime(2026, 5, 15, 18, 0, 0, tzinfo=timezone.utc),
        summary="Flood event profile.",
        severity="severe",
        observation_ids=["obs_01", "obs_02"],
        source_ids=["src_01", "src_02"],
        conflicting_claims=[
            ConflictingClaim(
                field_name="severity",
                observation_id="obs_02",
                source_id="src_02",
                claimed_value="moderate",
            )
        ],
        metadata={"centroid": {"latitude": 34.05, "longitude": -118.24}},
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )


class TestStorageResolutionUnit:
    """Tests for StorageRepository Phase 4 methods."""

    def test_save_event_and_get_event(
        self, mem_session: Session, sample_event: UnifiedDisasterEvent
    ) -> None:
        repo = StorageRepository()
        repo.save_event(sample_event, mem_session)
        # Link observations
        repo.link_observation_to_event(sample_event.event_id, "obs_01", 0.95, mem_session)
        repo.link_observation_to_event(sample_event.event_id, "obs_02", 0.88, mem_session)
        mem_session.commit()

        retrieved = repo.get_event(sample_event.event_id, mem_session)
        assert retrieved is not None
        assert retrieved.event_id == sample_event.event_id
        assert retrieved.event_type == "flood"
        assert retrieved.location == "Riverside Metro"
        assert retrieved.severity == "severe"
        assert len(retrieved.observation_ids) == 2
        assert len(retrieved.conflicting_claims) == 1
        assert retrieved.conflicting_claims[0].field_name == "severity"
        assert retrieved.conflicting_claims[0].claimed_value == "moderate"

    def test_save_event_idempotent_update(
        self, mem_session: Session, sample_event: UnifiedDisasterEvent
    ) -> None:
        repo = StorageRepository()
        repo.save_event(sample_event, mem_session)
        mem_session.commit()

        # Update event
        updated_event = sample_event.model_copy(
            update={"summary": "Updated summary.", "severity": "catastrophic"}
        )
        repo.save_event(updated_event, mem_session)
        mem_session.commit()

        retrieved = repo.get_event(sample_event.event_id, mem_session)
        assert retrieved is not None
        assert retrieved.summary == "Updated summary."
        assert retrieved.severity == "catastrophic"

    def test_save_resolution_decision_and_merge_candidates(
        self, mem_session: Session
    ) -> None:
        repo = StorageRepository()
        outcome = ResolutionOutcome(
            observation_id="obs_01",
            assigned_event_id="evt_01",
            decision=DecisionType.CONFIDENT_MATCH,
            score=0.88,
            is_new_event=False,
            rationale="High similarity score across features.",
            feature_scores=FeatureScores(
                event_type_score=1.0,
                composite_score=0.88,
                missing_features=[],
            ),
        )
        repo.save_resolution_decision(outcome, mem_session)

        merges = [
            MergeCandidate(
                primary_event_id="evt_01",
                secondary_event_id="evt_02",
                bridging_observation_id="obs_01",
                similarity_score=0.82,
                rationale="Bridging observation matches both events.",
            )
        ]
        repo.save_merge_candidates(merges, mem_session)
        mem_session.commit()

        stored_merges = repo.get_merge_candidates(mem_session)
        assert len(stored_merges) == 1
        assert stored_merges[0].primary_event_id == "evt_01"
        assert stored_merges[0].secondary_event_id == "evt_02"
        assert stored_merges[0].status == "pending_review"

    def test_atomic_save_resolution_result(
        self, mem_session: Session, sample_event: UnifiedDisasterEvent
    ) -> None:
        repo = StorageRepository()
        result = ResolutionResult(
            total_observations=2,
            events_created=1,
            events_updated=0,
            observations_assigned=2,
            ambiguous_count=0,
            outcomes=[
                ResolutionOutcome(
                    observation_id="obs_01",
                    assigned_event_id=sample_event.event_id,
                    decision=DecisionType.NO_MATCH,
                    score=0.0,
                    is_new_event=True,
                    rationale="New anchor observation.",
                ),
                ResolutionOutcome(
                    observation_id="obs_02",
                    assigned_event_id=sample_event.event_id,
                    decision=DecisionType.CONFIDENT_MATCH,
                    score=0.89,
                    is_new_event=False,
                    rationale="Confident match.",
                ),
            ],
            merge_candidates=[],
            resolved_events=[sample_event],
        )

        counts = repo.save_resolution_result(result, mem_session)
        mem_session.commit()

        assert counts["events_persisted"] == 1
        assert counts["links_persisted"] == 2
        assert counts["decisions_persisted"] == 2
        assert counts["merge_candidates_persisted"] == 0

        event_list = repo.list_events(mem_session)
        assert len(event_list) == 1
        assert event_list[0].event_id == sample_event.event_id

    def test_event_observation_unique_constraint_metadata(self) -> None:
        """Verify ORM metadata defines uq_event_observation on (event_id, observation_id)."""
        constraints = [
            c for c in EventObservationModel.__table__.constraints
            if isinstance(c, UniqueConstraint)
        ]
        assert len(constraints) >= 1
        uq = next(c for c in constraints if c.name == "uq_event_observation")
        col_names = {col.name for col in uq.columns}
        assert col_names == {"event_id", "observation_id"}

    def test_event_observation_duplicate_rejection_at_db_level(
        self, mem_session: Session, sample_event: UnifiedDisasterEvent
    ) -> None:
        """Verify duplicate (event_id, observation_id) links are rejected at DB level."""
        repo = StorageRepository()
        repo.save_event(sample_event, mem_session)
        mem_session.commit()

        link1 = EventObservationModel(
            event_id=sample_event.event_id,
            observation_id="obs_unique_test",
            confidence_score=0.9,
        )
        mem_session.add(link1)
        mem_session.commit()

        # Attempt duplicate insert directly to trigger DB constraint
        link2 = EventObservationModel(
            event_id=sample_event.event_id,
            observation_id="obs_unique_test",
            confidence_score=0.95,
        )
        mem_session.add(link2)
        with pytest.raises(IntegrityError):
            mem_session.commit()
        mem_session.rollback()

    def test_repository_idempotent_linking_preserves_unique_integrity(
        self, mem_session: Session, sample_event: UnifiedDisasterEvent
    ) -> None:
        """Verify StorageRepository.link_observation_to_event operates idempotently."""
        repo = StorageRepository()
        repo.save_event(sample_event, mem_session)
        mem_session.commit()

        # Link first time
        first_link = repo.link_observation_to_event(
            sample_event.event_id, "obs_idem_link", 0.85, mem_session
        )
        mem_session.commit()
        assert first_link.confidence_score == 0.85

        # Link second time with updated confidence
        second_link = repo.link_observation_to_event(
            sample_event.event_id, "obs_idem_link", 0.95, mem_session
        )
        mem_session.commit()
        assert second_link.id == first_link.id
        assert second_link.confidence_score == 0.95
