"""Data access and repository operations for sources, observations, and data quality."""

from datetime import datetime, timezone
import logging
from typing import Optional, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from beacon.models import (
    DisasterObservation,
    ExtractionMethod,
    ExtractionStatus,
    SourceRecord,
    SourceType,
)
from beacon.quality.models import RuleSeverity, ValidationOutcome
from beacon.storage.schema import ObservationModel, QualityResultModel, SourceModel

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
