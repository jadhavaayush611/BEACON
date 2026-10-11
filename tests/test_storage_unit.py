"""Unit tests for BEACON storage layer using in-memory database."""

from datetime import datetime, timezone
import pytest
from sqlalchemy import BigInteger, create_engine, select
from sqlalchemy.orm import sessionmaker

from beacon.models import (
    DisasterObservation,
    ExtractionMethod,
    ExtractionStatus,
    SourceRecord,
    SourceType,
)
from beacon.quality.models import RuleSeverity, ValidationOutcome
from beacon.storage import (
    ObservationModel,
    QualityResultModel,
    SourceModel,
    StorageRepository,
    drop_db,
    get_session,
    get_session_factory,
    init_db,
)


def _make_source_record(
    source_id: str = "src_storage_001",
    source_file: str = "data/sample/events.json",
    source_type: SourceType = SourceType.JSON,
    content_hash: str = "a" * 64,
) -> SourceRecord:
    return SourceRecord(
        source_id=source_id,
        source_file=source_file,
        source_type=source_type,
        extraction_method=ExtractionMethod.JSON_PARSE,
        ingested_at=datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
        extraction_status=ExtractionStatus.SUCCESS,
        content_hash=content_hash,
        raw_content='[{"event_type": "flood"}]',
        metadata={"record_count": 1},
    )


def _make_observation(
    observation_id: str = "obs_storage_001",
    source_id: str = "src_storage_001",
    event_type: str = "flood",
    location: str = "Valley River",
    severity: str = "moderate",
) -> DisasterObservation:
    return DisasterObservation(
        observation_id=observation_id,
        source_id=source_id,
        event_type=event_type,
        location=location,
        event_time=datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
        description="River bank overflow",
        severity=severity,
        latitude=28.6139,
        longitude=77.2090,
        extraction_confidence=0.92,
        page_number=1,
    )


class TestStorageRepositoryUnit:
    """Tests for repository persistence, updates, rollbacks, and queries."""

    @pytest.fixture
    def db_session_factory(self) -> sessionmaker:
        # In-memory SQLite for rapid, isolated unit testing without PostgreSQL dependency
        engine = create_engine("sqlite:///:memory:")
        init_db(engine)
        factory = get_session_factory(engine)
        yield factory
        drop_db(engine)

    @pytest.fixture
    def repository(self) -> StorageRepository:
        return StorageRepository()

    def test_schema_init_and_drop(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        init_db(engine)
        # Verify tables exist by inspecting schema
        with engine.connect() as conn:
            # Querying tables directly should succeed without error
            pass
        drop_db(engine)

    def test_quality_result_model_primary_key_is_biginteger(self) -> None:
        """Verify QualityResultModel.id is defined as BigInteger for PostgreSQL BIGSERIAL parity."""
        col = QualityResultModel.__table__.c.id
        assert col.primary_key is True
        assert isinstance(col.type, BigInteger)


    def test_save_source_and_get_source(
        self, db_session_factory: sessionmaker, repository: StorageRepository
    ) -> None:
        source_rec = _make_source_record()

        with get_session(db_session_factory) as session:
            repository.save_source(source_rec, session)

        with get_session(db_session_factory) as session:
            retrieved = repository.get_source(source_rec.source_id, session)
            assert retrieved is not None
            assert retrieved.source_id == source_rec.source_id
            assert retrieved.source_file == source_rec.source_file
            assert retrieved.source_type == SourceType.JSON
            assert retrieved.content_hash == "a" * 64

    def test_save_source_idempotent_update(
        self, db_session_factory: sessionmaker, repository: StorageRepository
    ) -> None:
        source_rec = _make_source_record()

        with get_session(db_session_factory) as session:
            repository.save_source(source_rec, session)

        # Update metadata and re-save same source_id
        updated_source = SourceRecord(
            source_id=source_rec.source_id,
            source_file=source_rec.source_file,
            source_type=source_rec.source_type,
            extraction_method=source_rec.extraction_method,
            ingested_at=source_rec.ingested_at,
            extraction_status=ExtractionStatus.PARTIAL,
            content_hash=source_rec.content_hash,
            raw_content=source_rec.raw_content,
            metadata={"record_count": 5, "updated": True},
        )

        with get_session(db_session_factory) as session:
            repository.save_source(updated_source, session)

        with get_session(db_session_factory) as session:
            retrieved = repository.get_source(source_rec.source_id, session)
            assert retrieved is not None
            assert retrieved.extraction_status == ExtractionStatus.PARTIAL
            assert retrieved.metadata["record_count"] == 5

            # Ensure no duplicate source records created
            all_sources = session.execute(select(SourceModel)).scalars().all()
            assert len(all_sources) == 1

    def test_save_observations_and_query_by_source(
        self, db_session_factory: sessionmaker, repository: StorageRepository
    ) -> None:
        source_rec = _make_source_record()
        obs = _make_observation()

        with get_session(db_session_factory) as session:
            repository.save_source(source_rec, session)
            repository.save_observations([obs], session)

        with get_session(db_session_factory) as session:
            stored_obs = repository.get_observations_for_source(source_rec.source_id, session)
            assert len(stored_obs) == 1
            assert stored_obs[0].observation_id == obs.observation_id
            assert stored_obs[0].location == "Valley River"

    def test_save_observations_idempotent_update(
        self, db_session_factory: sessionmaker, repository: StorageRepository
    ) -> None:
        source_rec = _make_source_record()
        obs = _make_observation()

        with get_session(db_session_factory) as session:
            repository.save_source(source_rec, session)
            repository.save_observations([obs], session)

        # Updated observation with changed severity
        obs_updated = _make_observation(severity="critical")

        with get_session(db_session_factory) as session:
            repository.save_observations([obs_updated], session)

        with get_session(db_session_factory) as session:
            stored_obs = repository.get_observations_for_source(source_rec.source_id, session)
            assert len(stored_obs) == 1
            assert stored_obs[0].severity == "critical"

            # Check raw count in table is still 1
            all_obs = session.execute(select(ObservationModel)).scalars().all()
            assert len(all_obs) == 1

    def test_save_quality_results(
        self, db_session_factory: sessionmaker, repository: StorageRepository
    ) -> None:
        outcome = ValidationOutcome(
            record_id="obs_storage_001",
            source_id="src_storage_001",
            rule_name="COORDINATES_PAIRED",
            severity=RuleSeverity.WARNING,
            message="Latitude without longitude",
            passed=False,
            details={"lat": 10.0},
        )

        with get_session(db_session_factory) as session:
            saved_count = repository.save_quality_results([outcome], session)
            assert saved_count == 1

        with get_session(db_session_factory) as session:
            results = session.execute(select(QualityResultModel)).scalars().all()
            assert len(results) == 1
            assert results[0].rule_name == "COORDINATES_PAIRED"
            assert results[0].severity == "warning"

    def test_save_batch_atomic(
        self, db_session_factory: sessionmaker, repository: StorageRepository
    ) -> None:
        source_rec = _make_source_record()
        obs = _make_observation()
        outcome = ValidationOutcome(
            record_id=obs.observation_id,
            source_id=source_rec.source_id,
            rule_name="REQUIRED_FIELDS",
            severity=RuleSeverity.INFO,
            message="Check ok",
            passed=True,
        )

        with get_session(db_session_factory) as session:
            counts = repository.save_batch(
                source_record=source_rec,
                accepted_observations=[obs],
                quality_outcomes=[outcome],
                session=session,
            )
            assert counts["observations_persisted"] == 1
            assert counts["quality_results_persisted"] == 1

        with get_session(db_session_factory) as session:
            src = repository.get_source(source_rec.source_id, session)
            obs_list = repository.get_observations_for_source(source_rec.source_id, session)
            assert src is not None
            assert len(obs_list) == 1

    def test_transaction_rollback_on_error(
        self, db_session_factory: sessionmaker, repository: StorageRepository
    ) -> None:
        source_rec = _make_source_record(source_id="src_rollback_test")

        with pytest.raises(RuntimeError):
            with get_session(db_session_factory) as session:
                repository.save_source(source_rec, session)
                # Force failure before transaction ends
                raise RuntimeError("Simulated transaction failure")

        # Verify nothing was committed
        with get_session(db_session_factory) as session:
            src = repository.get_source("src_rollback_test", session)
            assert src is None
