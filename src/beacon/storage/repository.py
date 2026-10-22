"""Data access and repository operations for sources, observations, and data quality."""

from datetime import datetime, timezone
import logging
from typing import Optional, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from beacon.models import (
    ConflictingClaim,
    DisasterObservation,
    ExtractionMethod,
    ExtractionStatus,
    SourceRecord,
    SourceType,
    UnifiedDisasterEvent,
)
from beacon.quality.models import ValidationOutcome
from beacon.resolution.models import (
    MergeCandidate,
    ResolutionOutcome,
    ResolutionResult,
)
from beacon.storage.schema import (
    EventModel,
    EventObservationModel,
    MergeCandidateModel,
    ObservationModel,
    QualityResultModel,
    ResolutionDecisionModel,
    SourceModel,
)

logger = logging.getLogger("beacon.storage.repository")


def _ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """Ensure a datetime retrieved from database is timezone-aware in UTC."""
    if dt is None:
        return None
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)



class StorageRepository:
    """Handles idempotent persistence and querying for sources and observations."""

    def __init__(self) -> None:
        pass

    def save_source(self, source_rec: SourceRecord, session: Session) -> SourceModel:
        """Persist or update a SourceRecord idempotently.

        If the source_id already exists, updates status and metadata.
        """
        existing = session.execute(
            select(SourceModel).where(SourceModel.source_id == source_rec.source_id)
        ).scalar_one_or_none()

        if existing:
            logger.debug("Source '%s' exists; updating mutable metadata", source_rec.source_id)
            existing.extraction_status = source_rec.extraction_status.value
            existing.metadata_json = dict(source_rec.metadata)
            existing.updated_at = datetime.now(timezone.utc)
            return existing

        model = SourceModel(
            source_id=source_rec.source_id,
            source_file=source_rec.source_file,
            source_type=source_rec.source_type.value,
            extraction_method=source_rec.extraction_method.value,
            ingested_at=source_rec.ingested_at,
            extraction_status=source_rec.extraction_status.value,
            content_hash=source_rec.content_hash,
            raw_content=source_rec.raw_content,
            metadata_json=dict(source_rec.metadata),
        )
        session.add(model)
        return model

    def save_observations(
        self,
        observations: Sequence[DisasterObservation],
        session: Session,
    ) -> int:
        """Idempotently persist a batch of DisasterObservation records.

        Uses stable observation_id to update existing records or insert new ones.
        Returns the total number of observations processed.
        """
        persisted = 0
        for obs in observations:
            existing = session.execute(
                select(ObservationModel).where(ObservationModel.observation_id == obs.observation_id)
            ).scalar_one_or_none()

            if existing:
                logger.debug("Observation '%s' exists; updating record", obs.observation_id)
                existing.event_type = obs.event_type
                existing.location = obs.location
                existing.event_time = obs.event_time
                existing.description = obs.description
                existing.severity = obs.severity
                existing.latitude = obs.latitude
                existing.longitude = obs.longitude
                existing.extraction_confidence = obs.extraction_confidence
                existing.page_number = obs.page_number
                existing.source_reference = obs.source_reference
                existing.raw_claim = obs.raw_claim
                existing.validation_warnings = list(obs.validation_warnings)
                existing.updated_at = datetime.now(timezone.utc)
            else:
                model = ObservationModel(
                    observation_id=obs.observation_id,
                    source_id=obs.source_id,
                    event_type=obs.event_type,
                    location=obs.location,
                    event_time=obs.event_time,
                    description=obs.description,
                    severity=obs.severity,
                    latitude=obs.latitude,
                    longitude=obs.longitude,
                    extraction_confidence=obs.extraction_confidence,
                    page_number=obs.page_number,
                    source_reference=obs.source_reference,
                    raw_claim=obs.raw_claim,
                    validation_warnings=list(obs.validation_warnings),
                )
                session.add(model)
            persisted += 1

        return persisted

    def save_quality_results(
        self,
        outcomes: Sequence[ValidationOutcome],
        session: Session,
    ) -> int:
        """Persist data quality evaluation outcomes for audit lineage."""
        saved = 0
        for outcome in outcomes:
            model = QualityResultModel(
                record_id=outcome.record_id if outcome.record_id != "UNKNOWN" else None,
                source_id=outcome.source_id if outcome.source_id != "UNKNOWN" else "UNKNOWN",
                rule_name=outcome.rule_name,
                field_name=outcome.field_name,
                severity=outcome.severity.value,
                message=outcome.message,
                passed=outcome.passed,
                details=dict(outcome.details),
                checked_at=datetime.now(timezone.utc),
            )
            session.add(model)
            saved += 1
        return saved

    def save_batch(
        self,
        source_record: SourceRecord,
        accepted_observations: Sequence[DisasterObservation],
        quality_outcomes: Sequence[ValidationOutcome],
        session: Session,
    ) -> dict[str, int]:
        """Atomically persist a source record, accepted observations, and all DQ outcomes."""
        self.save_source(source_record, session)
        session.flush()

        obs_count = self.save_observations(accepted_observations, session)
        session.flush()

        dq_count = self.save_quality_results(quality_outcomes, session)
        return {"observations_persisted": obs_count, "quality_results_persisted": dq_count}

    def get_source(self, source_id: str, session: Session) -> Optional[SourceRecord]:
        """Fetch a SourceRecord by identifier."""
        model = session.execute(
            select(SourceModel).where(SourceModel.source_id == source_id)
        ).scalar_one_or_none()
        if not model:
            return None

        return SourceRecord(
            source_id=model.source_id,
            source_file=model.source_file,
            source_type=SourceType(model.source_type),
            extraction_method=ExtractionMethod(model.extraction_method),
            ingested_at=_ensure_utc(model.ingested_at),
            extraction_status=ExtractionStatus(model.extraction_status),
            content_hash=model.content_hash,
            raw_content=model.raw_content,
            metadata=dict(model.metadata_json or {}),
        )

    def get_observations_for_source(
        self,
        source_id: str,
        session: Session,
    ) -> list[DisasterObservation]:
        """Fetch all stored DisasterObservation records for a given source."""
        models = session.execute(
            select(ObservationModel).where(ObservationModel.source_id == source_id)
        ).scalars().all()

        return [
            DisasterObservation(
                observation_id=m.observation_id,
                source_id=m.source_id,
                event_type=m.event_type,
                location=m.location,
                event_time=_ensure_utc(m.event_time),
                description=m.description,
                severity=m.severity,
                latitude=m.latitude,
                longitude=m.longitude,
                extraction_confidence=m.extraction_confidence,
                page_number=m.page_number,
                source_reference=m.source_reference,
                raw_claim=m.raw_claim,
                validation_warnings=list(m.validation_warnings or []),
            )
            for m in models
        ]

    def save_event(
        self,
        event: UnifiedDisasterEvent,
        session: Session,
    ) -> EventModel:
        """Idempotently persist or update a UnifiedDisasterEvent profile."""
        existing = session.execute(
            select(EventModel).where(EventModel.event_id == event.event_id)
        ).scalar_one_or_none()

        claims_json = [c.model_dump() for c in event.conflicting_claims]
        meta_dict = dict(event.metadata)
        src_ids_list = list(event.source_ids)

        if existing:
            logger.debug("Event '%s' exists; updating profile", event.event_id)
            existing.event_type = event.event_type
            existing.location = event.location
            existing.start_time = event.start_time
            existing.end_time = event.end_time
            existing.summary = event.summary
            existing.severity = event.severity
            existing.resolution_rationale = event.resolution_rationale
            existing.conflicting_claims = claims_json
            existing.source_ids = src_ids_list
            existing.metadata_json = meta_dict
            existing.updated_at = datetime.now(timezone.utc)
            return existing

        model = EventModel(
            event_id=event.event_id,
            event_type=event.event_type,
            location=event.location,
            start_time=event.start_time,
            end_time=event.end_time,
            summary=event.summary,
            severity=event.severity,
            resolution_rationale=event.resolution_rationale,
            conflicting_claims=claims_json,
            source_ids=src_ids_list,
            metadata_json=meta_dict,
            created_at=event.created_at,
            updated_at=event.updated_at,
        )
        session.add(model)
        return model

    def link_observation_to_event(
        self,
        event_id: str,
        observation_id: str,
        confidence_score: Optional[float],
        session: Session,
    ) -> EventObservationModel:
        """Idempotently link an observation to a unified event."""
        existing = session.execute(
            select(EventObservationModel).where(
                EventObservationModel.event_id == event_id,
                EventObservationModel.observation_id == observation_id,
            )
        ).scalar_one_or_none()

        if existing:
            if confidence_score is not None:
                existing.confidence_score = confidence_score
            return existing

        link = EventObservationModel(
            event_id=event_id,
            observation_id=observation_id,
            confidence_score=confidence_score,
            assigned_at=datetime.now(timezone.utc),
        )
        session.add(link)
        return link

    def save_resolution_decision(
        self,
        outcome: ResolutionOutcome,
        session: Session,
    ) -> ResolutionDecisionModel:
        """Record an explainable resolution decision for audit lineage."""
        scores_json = (
            outcome.feature_scores.model_dump() if outcome.feature_scores else {}
        )
        model = ResolutionDecisionModel(
            observation_id=outcome.observation_id,
            event_id=outcome.assigned_event_id,
            decision_type=outcome.decision.value,
            composite_score=outcome.score,
            feature_scores=scores_json,
            rationale=outcome.rationale,
            created_at=datetime.now(timezone.utc),
        )
        session.add(model)
        return model

    def save_merge_candidates(
        self,
        merge_candidates: Sequence[MergeCandidate],
        session: Session,
    ) -> int:
        """Record potential multi-event bridge / merge candidates."""
        saved = 0
        for mc in merge_candidates:
            model = MergeCandidateModel(
                primary_event_id=mc.primary_event_id,
                secondary_event_id=mc.secondary_event_id,
                bridging_observation_id=mc.bridging_observation_id,
                similarity_score=mc.similarity_score,
                rationale=mc.rationale,
                status=mc.status,
                created_at=datetime.now(timezone.utc),
            )
            session.add(model)
            saved += 1
        return saved

    def save_resolution_result(
        self,
        result: ResolutionResult,
        session: Session,
    ) -> dict[str, int]:
        """Atomically persist an entire resolution result including events, links, and audits."""
        # 1. Save all synthesized events
        events_saved = 0
        for ev in result.resolved_events:
            self.save_event(ev, session)
            events_saved += 1
        session.flush()

        # 2. Save observation links
        links_saved = 0
        for outcome in result.outcomes:
            if outcome.assigned_event_id:
                self.link_observation_to_event(
                    event_id=outcome.assigned_event_id,
                    observation_id=outcome.observation_id,
                    confidence_score=outcome.score,
                    session=session,
                )
                links_saved += 1
        session.flush()

        # 3. Save resolution decision audit records
        decisions_saved = 0
        for outcome in result.outcomes:
            self.save_resolution_decision(outcome, session)
            decisions_saved += 1
        session.flush()

        # 4. Save merge candidates
        merges_saved = self.save_merge_candidates(result.merge_candidates, session)
        session.flush()

        return {
            "events_persisted": events_saved,
            "links_persisted": links_saved,
            "decisions_persisted": decisions_saved,
            "merge_candidates_persisted": merges_saved,
        }

    def get_event(
        self,
        event_id: str,
        session: Session,
    ) -> Optional[UnifiedDisasterEvent]:
        """Fetch a UnifiedDisasterEvent profile by identifier with linked observations and sources."""
        model = session.execute(
            select(EventModel).where(EventModel.event_id == event_id)
        ).scalar_one_or_none()
        if not model:
            return None

        # Fetch constituent observation IDs
        obs_ids = session.execute(
            select(EventObservationModel.observation_id).where(
                EventObservationModel.event_id == event_id
            )
        ).scalars().all()
        obs_id_list = list(obs_ids)

        # Fetch source IDs
        source_id_list: list[str] = []
        if obs_id_list:
            src_ids = session.execute(
                select(ObservationModel.source_id).where(
                    ObservationModel.observation_id.in_(obs_id_list)
                )
            ).scalars().all()
            source_id_list = sorted(list(set(src_ids)))

        if not source_id_list and model.source_ids:
            source_id_list = list(model.source_ids)

        claims = [
            ConflictingClaim(**c)
            for c in (model.conflicting_claims or [])
        ]

        # Ensure observation_ids is non-empty for contract validation
        final_obs_ids = obs_id_list if obs_id_list else ["obs_unknown"]

        return UnifiedDisasterEvent(
            event_id=model.event_id,
            event_type=model.event_type,
            location=model.location,
            start_time=_ensure_utc(model.start_time),  # type: ignore[arg-type]
            end_time=_ensure_utc(model.end_time),
            summary=model.summary,
            severity=model.severity,
            observation_ids=final_obs_ids,
            created_at=_ensure_utc(model.created_at),  # type: ignore[arg-type]
            updated_at=_ensure_utc(model.updated_at),  # type: ignore[arg-type]
            conflicting_claims=claims,
            resolution_rationale=model.resolution_rationale,
            source_ids=source_id_list,
            metadata=dict(model.metadata_json or {}),
        )

    def list_events(
        self,
        session: Session,
        limit: Optional[int] = 100,
        offset: int = 0,
    ) -> list[UnifiedDisasterEvent]:
        """List unified events ordered by start time descending.

        If limit is None, retrieves all unified events without pagination limits.
        """
        stmt = select(EventModel).order_by(EventModel.start_time.desc()).offset(offset)
        if limit is not None:
            stmt = stmt.limit(limit)

        models = session.execute(stmt).scalars().all()

        events: list[UnifiedDisasterEvent] = []
        for m in models:
            ev = self.get_event(m.event_id, session)
            if ev is not None:
                events.append(ev)
        return events

    def get_all_events(
        self,
        session: Session,
    ) -> list[UnifiedDisasterEvent]:
        """Retrieve every unified event in storage without pagination limits."""
        return self.list_events(session, limit=None)

    def get_all_events_with_observations(
        self,
        session: Session,
    ) -> tuple[list[UnifiedDisasterEvent], dict[str, list[DisasterObservation]]]:
        """Retrieve all events and their constituent observations without pagination limits."""
        events = self.get_all_events(session)
        obs_by_event: dict[str, list[DisasterObservation]] = {}
        for ev in events:
            obs_by_event[ev.event_id] = self.get_observations_for_event(ev.event_id, session)
        return events, obs_by_event

    def get_observations_for_event(
        self,
        event_id: str,
        session: Session,
    ) -> list[DisasterObservation]:
        """Fetch all constituent DisasterObservation records linked to an event."""
        obs_ids = session.execute(
            select(EventObservationModel.observation_id).where(
                EventObservationModel.event_id == event_id
            )
        ).scalars().all()

        if not obs_ids:
            return []

        models = session.execute(
            select(ObservationModel).where(ObservationModel.observation_id.in_(list(obs_ids)))
        ).scalars().all()

        return [
            DisasterObservation(
                observation_id=m.observation_id,
                source_id=m.source_id,
                event_type=m.event_type,
                location=m.location,
                event_time=_ensure_utc(m.event_time),
                description=m.description,
                severity=m.severity,
                latitude=m.latitude,
                longitude=m.longitude,
                extraction_confidence=m.extraction_confidence,
                page_number=m.page_number,
                source_reference=m.source_reference,
                raw_claim=m.raw_claim,
                validation_warnings=list(m.validation_warnings or []),
            )
            for m in models
        ]

    def get_merge_candidates(
        self,
        session: Session,
        status: Optional[str] = None,
    ) -> list[MergeCandidate]:
        """Query merge candidates with optional status filter."""
        stmt = select(MergeCandidateModel)
        if status:
            stmt = stmt.where(MergeCandidateModel.status == status)

        models = session.execute(stmt).scalars().all()
        return [
            MergeCandidate(
                primary_event_id=m.primary_event_id,
                secondary_event_id=m.secondary_event_id,
                bridging_observation_id=m.bridging_observation_id,
                similarity_score=m.similarity_score,
                rationale=m.rationale,
                status=m.status,
            )
            for m in models
        ]
