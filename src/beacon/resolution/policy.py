"""Deterministic match classification and decision policy for event resolution."""

import logging
from typing import Optional

from beacon.config import Settings, get_settings
from beacon.models import DisasterObservation
from beacon.resolution.models import (
    CandidateMatch,
    DecisionType,
    MergeCandidate,
    ResolutionOutcome,
)

logger = logging.getLogger("beacon.resolution.policy")


class MatchPolicy:
    """Evaluates candidate match scores and applies deterministic decision rules."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        tie_margin: float = 0.08,
        assign_ambiguous: bool = False,
    ) -> None:
        cfg = settings or get_settings()
        self.match_threshold = cfg.resolution_match_threshold
        self.ambiguity_threshold = cfg.resolution_ambiguity_threshold
        self.tie_margin = tie_margin
        self.assign_ambiguous = assign_ambiguous

    def evaluate_candidates(
        self,
        observation: DisasterObservation,
        candidates: list[CandidateMatch],
    ) -> tuple[ResolutionOutcome, list[MergeCandidate]]:
        """Classify candidate matches into a single resolution outcome and detect merge candidates.

        Returns:
            A tuple of (ResolutionOutcome, list of MergeCandidate).
        """
        merge_candidates: list[MergeCandidate] = []

        if not candidates:
            return (
                ResolutionOutcome(
                    observation_id=observation.observation_id,
                    assigned_event_id=None,
                    decision=DecisionType.NO_MATCH,
                    score=0.0,
                    is_new_event=True,
                    rationale="No candidate events passed blocking criteria. New event profile created.",
                    feature_scores=None,
                    candidate_matches=[],
                ),
                merge_candidates,
            )

        # Sort descending by composite score
        sorted_candidates = sorted(candidates, key=lambda c: c.score, reverse=True)
        top = sorted_candidates[0]
        top_score = top.score

        # Check for Bridge / Multi-Match Merge Candidates
        high_matches = [c for c in sorted_candidates if c.score >= self.match_threshold]
        if len(high_matches) > 1:
            # Bridging observation matches multiple distinct existing events with high confidence
            for secondary in high_matches[1:]:
                merge_cand = MergeCandidate(
                    primary_event_id=top.candidate_event_id,
                    secondary_event_id=secondary.candidate_event_id,
                    bridging_observation_id=observation.observation_id,
                    similarity_score=secondary.score,
                    rationale=(
                        f"Observation {observation.observation_id} matched both event "
                        f"{top.candidate_event_id} (score {top.score:.3f}) and event "
                        f"{secondary.candidate_event_id} (score {secondary.score:.3f}) "
                        f"above match threshold ({self.match_threshold:.2f})."
                    ),
                    status="pending_review",
                )
                merge_candidates.append(merge_cand)

            # Assign to the highest scoring event but flag as MERGE_CANDIDATE
            return (
                ResolutionOutcome(
                    observation_id=observation.observation_id,
                    assigned_event_id=top.candidate_event_id,
                    decision=DecisionType.MERGE_CANDIDATE,
                    score=top_score,
                    is_new_event=False,
                    rationale=(
                        f"Observation bridges events {top.candidate_event_id} (score {top_score:.3f}) "
                        f"and {high_matches[1].candidate_event_id} (score {high_matches[1].score:.3f}). "
                        f"Assigned to highest match; merge candidate logged for audit."
                    ),
                    feature_scores=top.feature_scores,
                    candidate_matches=sorted_candidates,
                ),
                merge_candidates,
            )

        # Check for No Match
        if top_score < self.ambiguity_threshold:
            return (
                ResolutionOutcome(
                    observation_id=observation.observation_id,
                    assigned_event_id=None,
                    decision=DecisionType.NO_MATCH,
                    score=top_score,
                    is_new_event=True,
                    rationale=(
                        f"Top candidate score ({top_score:.3f}) with event {top.candidate_event_id} "
                        f"is below ambiguity threshold ({self.ambiguity_threshold:.2f}). "
                        f"New event profile created."
                    ),
                    feature_scores=top.feature_scores,
                    candidate_matches=sorted_candidates,
                ),
                merge_candidates,
            )

        # Check for Near-Tie Ambiguity between top candidates
        if len(sorted_candidates) > 1:
            runner_up = sorted_candidates[1]
            diff = top_score - runner_up.score
            if diff < self.tie_margin and runner_up.score >= self.ambiguity_threshold:
                assigned_id = top.candidate_event_id if self.assign_ambiguous else None
                return (
                    ResolutionOutcome(
                        observation_id=observation.observation_id,
                        assigned_event_id=assigned_id,
                        decision=DecisionType.AMBIGUOUS_CANDIDATE,
                        score=top_score,
                        is_new_event=False,
                        rationale=(
                            f"Near-tie ambiguous candidate: top event {top.candidate_event_id} "
                            f"(score {top_score:.3f}) and event {runner_up.candidate_event_id} "
                            f"(score {runner_up.score:.3f}) differ by only {diff:.3f} "
                            f"(< margin {self.tie_margin:.2f}). Flagged for manual review."
                        ),
                        feature_scores=top.feature_scores,
                        candidate_matches=sorted_candidates,
                    ),
                    merge_candidates,
                )

        # Check for Single Candidate in Ambiguity Range [ambiguity_threshold, match_threshold)
        if top_score < self.match_threshold:
            assigned_id = top.candidate_event_id if self.assign_ambiguous else None
            return (
                ResolutionOutcome(
                    observation_id=observation.observation_id,
                    assigned_event_id=assigned_id,
                    decision=DecisionType.AMBIGUOUS_CANDIDATE,
                    score=top_score,
                    is_new_event=False,
                    rationale=(
                        f"Candidate score ({top_score:.3f}) with event {top.candidate_event_id} "
                        f"is between ambiguity threshold ({self.ambiguity_threshold:.2f}) "
                        f"and match threshold ({self.match_threshold:.2f}). "
                        f"Evidence insufficient for confident merge; flagged for review."
                    ),
                    feature_scores=top.feature_scores,
                    candidate_matches=sorted_candidates,
                ),
                merge_candidates,
            )

        # Confident Match
        return (
            ResolutionOutcome(
                observation_id=observation.observation_id,
                assigned_event_id=top.candidate_event_id,
                decision=DecisionType.CONFIDENT_MATCH,
                score=top_score,
                is_new_event=False,
                rationale=(
                    f"Confident match with event {top.candidate_event_id} "
                    f"(score {top_score:.3f} >= threshold {self.match_threshold:.2f})."
                ),
                feature_scores=top.feature_scores,
                candidate_matches=sorted_candidates,
            ),
            merge_candidates,
        )
