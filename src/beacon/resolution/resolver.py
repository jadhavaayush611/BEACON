"""Event resolution coordinator for matching and consolidating disaster observations."""

import logging
from typing import Optional

from beacon.config import Settings, get_settings
from beacon.models import DisasterObservation, UnifiedDisasterEvent
from beacon.resolution.aggregator import EventProfileAggregator
from beacon.resolution.candidate_generator import CandidateGenerator
from beacon.resolution.matcher import ObservationMatcher
from beacon.resolution.models import (
    CandidateMatch,
    DecisionType,
    MergeCandidate,
    ResolutionOutcome,
    ResolutionResult,
)
from beacon.resolution.policy import MatchPolicy

logger = logging.getLogger("beacon.resolution.resolver")


class EventResolver:
    """Orchestrates candidate generation, feature comparison, decision policy, and profile aggregation."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        matcher: Optional[ObservationMatcher] = None,
        candidate_generator: Optional[CandidateGenerator] = None,
        policy: Optional[MatchPolicy] = None,
        aggregator: Optional[EventProfileAggregator] = None,
    ) -> None:
        cfg = settings or get_settings()
        self.settings = cfg
        self.matcher = matcher or ObservationMatcher(cfg)
        self.candidate_generator = candidate_generator or CandidateGenerator(cfg)
        self.policy = policy or MatchPolicy(cfg)
        self.aggregator = aggregator or EventProfileAggregator()

    def resolve_observations(
        self,
        observations: list[DisasterObservation],
        existing_events: Optional[list[UnifiedDisasterEvent]] = None,
        existing_event_observations: Optional[dict[str, list[DisasterObservation]]] = None,
    ) -> ResolutionResult:
        """Resolve a batch of observations against existing and dynamically created events.

        Args:
            observations: List of DisasterObservation records to resolve.
            existing_events: Optional list of preexisting UnifiedDisasterEvent profiles.
            existing_event_observations: Optional mapping of event_id to preexisting constituent observations.

        Returns:
            A ResolutionResult detailing outcomes, merge candidates, and final synthesized events.
        """
        events_by_id: dict[str, UnifiedDisasterEvent] = {}
        if existing_events:
            for ev in existing_events:
                events_by_id[ev.event_id] = ev

        obs_by_event: dict[str, list[DisasterObservation]] = {}
        if existing_event_observations:
            for eid, obs_list in existing_event_observations.items():
                obs_by_event[eid] = list(obs_list)

        outcomes: list[ResolutionOutcome] = []
        all_merge_candidates: list[MergeCandidate] = []
        events_created_count = 0
        events_updated_count = 0
        ambiguous_count = 0
        assigned_count = 0

        for obs in observations:
            # 1. Blocking & Candidate Generation
            candidate_events = self.candidate_generator.generate_candidates(
                obs, events_by_id.values()
            )

            # 2. Pairwise feature scoring
            candidate_matches: list[CandidateMatch] = []
            for cand_event in candidate_events:
                feat_scores = self.matcher.compare(obs, cand_event)
                match_cand = CandidateMatch(
                    observation_id=obs.observation_id,
                    candidate_event_id=cand_event.event_id,
                    decision=DecisionType.CONFIDENT_MATCH
                    if feat_scores.composite_score >= self.policy.match_threshold
                    else (
                        DecisionType.AMBIGUOUS_CANDIDATE
                        if feat_scores.composite_score >= self.policy.ambiguity_threshold
                        else DecisionType.NO_MATCH
                    ),
                    score=feat_scores.composite_score,
                    feature_scores=feat_scores,
                    rationale=f"Composite score: {feat_scores.composite_score:.3f} across evaluated features.",
                )
                candidate_matches.append(match_cand)

            # 3. Policy evaluation
            outcome, merges = self.policy.evaluate_candidates(obs, candidate_matches)
            all_merge_candidates.extend(merges)

            # 4. Act on decision
            if outcome.decision in (DecisionType.CONFIDENT_MATCH, DecisionType.MERGE_CANDIDATE):
                target_event_id = outcome.assigned_event_id
                assert target_event_id is not None
                target_event = events_by_id[target_event_id]

                # Add or replace observation in cluster matching by observation_id
                cluster = obs_by_event.setdefault(target_event_id, [])
                existing_idx = next(
                    (i for i, o in enumerate(cluster) if o.observation_id == obs.observation_id),
                    None,
                )
                if existing_idx is not None:
                    cluster[existing_idx] = obs
                else:
                    cluster.append(obs)

                # Re-synthesize unified profile
                updated_event = self.aggregator.synthesize(cluster, existing_event=target_event)
                events_by_id[target_event_id] = updated_event
                events_updated_count += 1
                assigned_count += 1

            elif outcome.decision == DecisionType.NO_MATCH:
                # Synthesize new UnifiedDisasterEvent profile
                new_event = self.aggregator.synthesize([obs])
                events_by_id[new_event.event_id] = new_event
                obs_by_event[new_event.event_id] = [obs]

                outcome.assigned_event_id = new_event.event_id
                events_created_count += 1
                assigned_count += 1

            elif outcome.decision == DecisionType.AMBIGUOUS_CANDIDATE:
                ambiguous_count += 1
                if self.policy.assign_ambiguous and outcome.assigned_event_id:
                    target_event_id = outcome.assigned_event_id
                    target_event = events_by_id[target_event_id]
                    cluster = obs_by_event.setdefault(target_event_id, [])
                    existing_idx = next(
                        (i for i, o in enumerate(cluster) if o.observation_id == obs.observation_id),
                        None,
                    )
                    if existing_idx is not None:
                        cluster[existing_idx] = obs
                    else:
                        cluster.append(obs)
                    events_by_id[target_event_id] = self.aggregator.synthesize(
                        cluster, existing_event=target_event
                    )
                    events_updated_count += 1
                    assigned_count += 1

            outcomes.append(outcome)

        return ResolutionResult(
            total_observations=len(observations),
            events_created=events_created_count,
            events_updated=events_updated_count,
            observations_assigned=assigned_count,
            ambiguous_count=ambiguous_count,
            outcomes=outcomes,
            merge_candidates=all_merge_candidates,
            resolved_events=list(events_by_id.values()),
        )
