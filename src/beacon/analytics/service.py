"""Analytics service for disaster event intelligence, conflict metrics, and coverage audits."""

from collections import Counter
from datetime import datetime
import logging
from typing import Optional, Sequence

from sqlalchemy import distinct, func, select
from sqlalchemy.orm import Session

from beacon.analytics.models import AnalyticsFilter, AnalyticsSummary, CoverageGaps
from beacon.models import DisasterObservation, UnifiedDisasterEvent
from beacon.resolution.models import MergeCandidate
from beacon.storage.schema import (
    EventModel,
    EventObservationModel,
    MergeCandidateModel,
    ObservationModel,
    ResolutionDecisionModel,
)

logger = logging.getLogger("beacon.analytics.service")


class AnalyticsService:
    """Computes aggregated disaster intelligence and data quality metrics."""

    def __init__(self) -> None:
        pass

    def filter_events(
        self,
        events: Sequence[UnifiedDisasterEvent],
        criteria: Optional[AnalyticsFilter] = None,
    ) -> list[UnifiedDisasterEvent]:
        """Apply in-memory filters to a sequence of events."""
        if not criteria:
            return list(events)

        filtered = []
        for ev in events:
            if criteria.event_type and ev.event_type.lower() != criteria.event_type.lower():
                continue
            if criteria.severity and (ev.severity or "").lower() != criteria.severity.lower():
                continue
            if criteria.location and criteria.location.lower() not in ev.location.lower():
                continue
            if criteria.min_sources and len(ev.source_ids) < criteria.min_sources:
                continue
            if criteria.start_time_after and ev.start_time < criteria.start_time_after:
                continue
            if criteria.end_time_before:
                ev_end = ev.end_time or ev.start_time
                if ev_end > criteria.end_time_before:
                    continue
            filtered.append(ev)

        return filtered

    def summarize(
        self,
        events: Sequence[UnifiedDisasterEvent],
        observations: Optional[Sequence[DisasterObservation]] = None,
        merge_candidates: Optional[Sequence[MergeCandidate]] = None,
        ambiguous_count: int = 0,
        filter_criteria: Optional[AnalyticsFilter] = None,
    ) -> AnalyticsSummary:
        """Compute an AnalyticsSummary from in-memory objects."""
        filtered_events = self.filter_events(events, filter_criteria)
        total_events = len(filtered_events)

        # Observations mapping
        if observations:
            total_obs = len(observations)
            obs_missing_coords = sum(
                1 for o in observations if o.latitude is None or o.longitude is None
            )
            obs_missing_times = sum(1 for o in observations if o.event_time is None)
        else:
            # Derived from event constituent IDs if observations not supplied
            total_obs = sum(len(e.observation_ids) for e in filtered_events)
            obs_missing_coords = 0
            obs_missing_times = 0

        compression_ratio = (
            round(total_obs / total_events, 2) if total_events > 0 else 0.0
        )

        multi_source = sum(1 for e in filtered_events if len(e.source_ids) >= 2)
        single_source = total_events - multi_source
        multi_ratio = (
            round(multi_source / total_events, 4) if total_events > 0 else 0.0
        )

        by_type = dict(Counter(e.event_type for e in filtered_events))
        by_sev = dict(
            Counter(e.severity or "unknown" for e in filtered_events)
        )
        by_loc = dict(Counter(e.location for e in filtered_events))

        # Conflicting claims
        conflict_field_counts: Counter[str] = Counter()
        total_conflicts = 0
        for e in filtered_events:
            for claim in e.conflicting_claims:
                conflict_field_counts[claim.field_name] += 1
                total_conflicts += 1

        # Coverage gaps
        events_missing_coords = sum(
            1
            for e in filtered_events
            if not (e.metadata or {}).get("centroid")
        )
        events_unanchored_times = sum(
            1
            for e in filtered_events
            if (e.metadata or {}).get("unanchored_timestamp")
        )

        gaps = CoverageGaps(
            events_missing_coordinates=events_missing_coords,
            events_unanchored_timestamps=events_unanchored_times,
            observations_missing_coordinates=obs_missing_coords,
            observations_missing_timestamps=obs_missing_times,
        )

        merge_count = len(merge_candidates) if merge_candidates else 0

        return AnalyticsSummary(
            total_events=total_events,
            total_observations=total_obs,
            compression_ratio=compression_ratio,
            multi_source_events=multi_source,
            single_source_events=single_source,
            multi_source_ratio=multi_ratio,
            events_by_type=by_type,
            events_by_severity=by_sev,
            events_by_location=by_loc,
            conflicting_claims_count=total_conflicts,
            conflicting_claims_by_field=dict(conflict_field_counts),
            coverage_gaps=gaps,
            ambiguous_observations_count=ambiguous_count,
            merge_candidates_count=merge_count,
        )

    def summarize_db(
        self,
        session: Session,
        filter_criteria: Optional[AnalyticsFilter] = None,
    ) -> AnalyticsSummary:
        """Compute an AnalyticsSummary from database tables."""
        # Query events
        stmt = select(EventModel)
        if filter_criteria:
            if filter_criteria.event_type:
                stmt = stmt.where(EventModel.event_type == filter_criteria.event_type.lower())
            if filter_criteria.severity:
                stmt = stmt.where(EventModel.severity == filter_criteria.severity.lower())
            if filter_criteria.location:
                stmt = stmt.where(EventModel.location.ilike(f"%{filter_criteria.location}%"))
            if filter_criteria.start_time_after:
                stmt = stmt.where(EventModel.start_time >= filter_criteria.start_time_after)
            if filter_criteria.end_time_before:
                stmt = stmt.where(
                    (EventModel.end_time <= filter_criteria.end_time_before)
                    | (EventModel.start_time <= filter_criteria.end_time_before)
                )

        event_models = session.execute(stmt).scalars().all()

        # Build in-memory representations of fetched events to compute distributions
        from beacon.storage.repository import StorageRepository

        repo = StorageRepository()
        events = [repo.get_event(m.event_id, session) for m in event_models]
        valid_events = [e for e in events if e is not None]

        # Apply min_sources filter if specified
        if filter_criteria and filter_criteria.min_sources:
            valid_events = [
                e for e in valid_events if len(e.source_ids) >= filter_criteria.min_sources
            ]

        valid_event_ids = [e.event_id for e in valid_events]
        total_events = len(valid_events)

        is_filtered = filter_criteria is not None and any(
            getattr(filter_criteria, f) is not None
            for f in (
                "event_type",
                "severity",
                "location",
                "min_sources",
                "start_time_after",
                "end_time_before",
            )
        )

        if is_filtered:
            if valid_event_ids:
                linked_obs_ids = list(
                    session.execute(
                        select(EventObservationModel.observation_id)
                        .where(EventObservationModel.event_id.in_(valid_event_ids))
                        .distinct()
                    ).scalars().all()
                )
                total_obs = len(linked_obs_ids)

                if linked_obs_ids:
                    obs_missing_coords = session.execute(
                        select(func.count(distinct(ObservationModel.observation_id))).where(
                            ObservationModel.observation_id.in_(linked_obs_ids),
                            (ObservationModel.latitude.is_(None)) | (ObservationModel.longitude.is_(None)),
                        )
                    ).scalar() or 0
                    obs_missing_times = session.execute(
                        select(func.count(distinct(ObservationModel.observation_id))).where(
                            ObservationModel.observation_id.in_(linked_obs_ids),
                            ObservationModel.event_time.is_(None),
                        )
                    ).scalar() or 0
                else:
                    obs_missing_coords = 0
                    obs_missing_times = 0

                ambiguous_count = session.execute(
                    select(func.count(distinct(ResolutionDecisionModel.id))).where(
                        ResolutionDecisionModel.decision_type == "ambiguous_candidate",
                        ResolutionDecisionModel.event_id.in_(valid_event_ids),
                    )
                ).scalar() or 0

                merge_count = session.execute(
                    select(func.count(distinct(MergeCandidateModel.id))).where(
                        (MergeCandidateModel.primary_event_id.in_(valid_event_ids))
                        | (MergeCandidateModel.secondary_event_id.in_(valid_event_ids))
                    )
                ).scalar() or 0
            else:
                total_obs = 0
                obs_missing_coords = 0
                obs_missing_times = 0
                ambiguous_count = 0
                merge_count = 0
        else:
            # Global unfiltered observation queries
            total_obs = session.execute(
                select(func.count(distinct(ObservationModel.observation_id)))
            ).scalar() or 0
            obs_missing_coords = session.execute(
                select(func.count(distinct(ObservationModel.observation_id))).where(
                    (ObservationModel.latitude.is_(None)) | (ObservationModel.longitude.is_(None))
                )
            ).scalar() or 0
            obs_missing_times = session.execute(
                select(func.count(distinct(ObservationModel.observation_id))).where(
                    ObservationModel.event_time.is_(None)
                )
            ).scalar() or 0

            ambiguous_count = session.execute(
                select(func.count(distinct(ResolutionDecisionModel.id))).where(
                    ResolutionDecisionModel.decision_type == "ambiguous_candidate"
                )
            ).scalar() or 0

            merge_count = session.execute(
                select(func.count(distinct(MergeCandidateModel.id)))
            ).scalar() or 0

        compression_ratio = (
            round(total_obs / total_events, 2) if total_events > 0 else 0.0
        )
        multi_source = sum(1 for e in valid_events if len(e.source_ids) >= 2)
        single_source = total_events - multi_source
        multi_ratio = (
            round(multi_source / total_events, 4) if total_events > 0 else 0.0
        )

        by_type = dict(Counter(e.event_type for e in valid_events))
        by_sev = dict(
            Counter(e.severity or "unknown" for e in valid_events)
        )
        by_loc = dict(Counter(e.location for e in valid_events))

        conflict_field_counts: Counter[str] = Counter()
        total_conflicts = 0
        for e in valid_events:
            for claim in e.conflicting_claims:
                conflict_field_counts[claim.field_name] += 1
                total_conflicts += 1

        events_missing_coords = sum(
            1 for e in valid_events if not (e.metadata or {}).get("centroid")
        )
        events_unanchored_times = sum(
            1 for e in valid_events if (e.metadata or {}).get("unanchored_timestamp")
        )

        gaps = CoverageGaps(
            events_missing_coordinates=events_missing_coords,
            events_unanchored_timestamps=events_unanchored_times,
            observations_missing_coordinates=obs_missing_coords,
            observations_missing_timestamps=obs_missing_times,
        )

        return AnalyticsSummary(
            total_events=total_events,
            total_observations=total_obs,
            compression_ratio=compression_ratio,
            multi_source_events=multi_source,
            single_source_events=single_source,
            multi_source_ratio=multi_ratio,
            events_by_type=by_type,
            events_by_severity=by_sev,
            events_by_location=by_loc,
            conflicting_claims_count=total_conflicts,
            conflicting_claims_by_field=dict(conflict_field_counts),
            coverage_gaps=gaps,
            ambiguous_observations_count=ambiguous_count,
            merge_candidates_count=merge_count,
        )
