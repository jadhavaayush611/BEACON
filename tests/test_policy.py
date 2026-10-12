"""Unit tests for MatchPolicy decision logic and bridge detection."""

from datetime import datetime, timezone
import pytest

from beacon.config import Settings
from beacon.models import DisasterObservation
from beacon.resolution.models import (
    CandidateMatch,
    DecisionType,
    FeatureScores,
)
from beacon.resolution.policy import MatchPolicy


@pytest.fixture
def policy() -> MatchPolicy:
    cfg = Settings(
        resolution_match_threshold=0.75,
        resolution_ambiguity_threshold=0.50,
    )
    return MatchPolicy(settings=cfg, tie_margin=0.08)


@pytest.fixture
def sample_obs() -> DisasterObservation:
    return DisasterObservation(
        observation_id="obs_test",
        source_id="src_test",
        event_type="flood",
        location="Riverside Metro",
        event_time=datetime(2026, 5, 15, 10, 0, 0, tzinfo=timezone.utc),
    )


def make_dummy_scores(score: float) -> FeatureScores:
    return FeatureScores(
        event_type_score=1.0,
        composite_score=score,
        missing_features=[],
        evidence_summary={},
    )


class TestMatchPolicy:
    """Tests for MatchPolicy decision logic."""

    def test_no_candidates_returns_no_match(
        self, policy: MatchPolicy, sample_obs: DisasterObservation
    ) -> None:
        outcome, merges = policy.evaluate_candidates(sample_obs, [])
        assert outcome.decision == DecisionType.NO_MATCH
        assert outcome.is_new_event is True
        assert outcome.assigned_event_id is None
        assert merges == []

    def test_top_candidate_below_ambiguity_threshold(
        self, policy: MatchPolicy, sample_obs: DisasterObservation
    ) -> None:
        candidates = [
            CandidateMatch(
                observation_id=sample_obs.observation_id,
                candidate_event_id="evt_01",
                decision=DecisionType.NO_MATCH,
                score=0.45,
                feature_scores=make_dummy_scores(0.45),
                rationale="Low score",
            )
        ]
        outcome, merges = policy.evaluate_candidates(sample_obs, candidates)
        assert outcome.decision == DecisionType.NO_MATCH
        assert outcome.is_new_event is True
        assert outcome.assigned_event_id is None

    def test_candidate_in_ambiguity_range(
        self, policy: MatchPolicy, sample_obs: DisasterObservation
    ) -> None:
        candidates = [
            CandidateMatch(
                observation_id=sample_obs.observation_id,
                candidate_event_id="evt_01",
                decision=DecisionType.AMBIGUOUS_CANDIDATE,
                score=0.65,
                feature_scores=make_dummy_scores(0.65),
                rationale="Ambiguous range",
            )
        ]
        outcome, merges = policy.evaluate_candidates(sample_obs, candidates)
        assert outcome.decision == DecisionType.AMBIGUOUS_CANDIDATE
        assert outcome.is_new_event is False
        assert outcome.assigned_event_id is None

    def test_near_tie_ambiguity(
        self, policy: MatchPolicy, sample_obs: DisasterObservation
    ) -> None:
        candidates = [
            CandidateMatch(
                observation_id=sample_obs.observation_id,
                candidate_event_id="evt_01",
                decision=DecisionType.CONFIDENT_MATCH,
                score=0.78,
                feature_scores=make_dummy_scores(0.78),
                rationale="Top match",
            ),
            CandidateMatch(
                observation_id=sample_obs.observation_id,
                candidate_event_id="evt_02",
                decision=DecisionType.CONFIDENT_MATCH,
                score=0.73,
                feature_scores=make_dummy_scores(0.73),
                rationale="Runner up close match",
            ),
        ]
        outcome, merges = policy.evaluate_candidates(sample_obs, candidates)
        assert outcome.decision == DecisionType.AMBIGUOUS_CANDIDATE
        assert "Near-tie" in outcome.rationale

    def test_confident_match(
        self, policy: MatchPolicy, sample_obs: DisasterObservation
    ) -> None:
        candidates = [
            CandidateMatch(
                observation_id=sample_obs.observation_id,
                candidate_event_id="evt_clear_winner",
                decision=DecisionType.CONFIDENT_MATCH,
                score=0.88,
                feature_scores=make_dummy_scores(0.88),
                rationale="High score",
            ),
            CandidateMatch(
                observation_id=sample_obs.observation_id,
                candidate_event_id="evt_distant",
                decision=DecisionType.NO_MATCH,
                score=0.30,
                feature_scores=make_dummy_scores(0.30),
                rationale="Low score",
            ),
        ]
        outcome, merges = policy.evaluate_candidates(sample_obs, candidates)
        assert outcome.decision == DecisionType.CONFIDENT_MATCH
        assert outcome.assigned_event_id == "evt_clear_winner"
        assert outcome.score == 0.88
        assert merges == []

    def test_bridge_detection_multiple_high_matches(
        self, policy: MatchPolicy, sample_obs: DisasterObservation
    ) -> None:
        candidates = [
            CandidateMatch(
                observation_id=sample_obs.observation_id,
                candidate_event_id="evt_primary",
                decision=DecisionType.CONFIDENT_MATCH,
                score=0.92,
                feature_scores=make_dummy_scores(0.92),
                rationale="Primary strong match",
            ),
            CandidateMatch(
                observation_id=sample_obs.observation_id,
                candidate_event_id="evt_secondary",
                decision=DecisionType.CONFIDENT_MATCH,
                score=0.85,
                feature_scores=make_dummy_scores(0.85),
                rationale="Secondary strong match",
            ),
        ]
        outcome, merges = policy.evaluate_candidates(sample_obs, candidates)
        assert outcome.decision == DecisionType.MERGE_CANDIDATE
        assert outcome.assigned_event_id == "evt_primary"
        assert len(merges) == 1
        assert merges[0].primary_event_id == "evt_primary"
        assert merges[0].secondary_event_id == "evt_secondary"
        assert merges[0].bridging_observation_id == sample_obs.observation_id
        assert merges[0].status == "pending_review"
