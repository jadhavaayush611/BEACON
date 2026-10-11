"""PostgreSQL integration tests for BEACON storage layer.

These tests run only when an explicit PostgreSQL test database is provided via
the BEACON_TEST_DATABASE_URL environment variable. When absent, the entire module
skips gracefully to maintain dependency-free local and CI test execution.
"""

from datetime import datetime, timezone
import os
import pytest
from sqlalchemy import create_engine, select

from beacon.config import Settings
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

TEST_DB_URL = os.getenv("BEACON_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not TEST_DB_URL,
    reason="BEACON_TEST_DATABASE_URL is not set; skipping live PostgreSQL integration tests",
)


@pytest.fixture(scope="module")
def pg_session_factory():
    """Setup and teardown a live PostgreSQL database schema for integration testing."""
    engine = create_engine(TEST_DB_URL)
    init_db(engine)
    factory = get_session_factory(engine)
    yield factory
    drop_db(engine)


class TestPostgresStorageIntegration:
    """Live PostgreSQL database integration tests."""

    def test_postgres_table_creation_and_batch_persistence(
        self, pg_session_factory
    ) -> None:
        repository = StorageRepository()
        source_rec = SourceRecord(
            source_id="src_pg_integration_001",
            source_file="data/sample/events.json",
            source_type=SourceType.JSON,
            extraction_method=ExtractionMethod.JSON_PARSE,
            ingested_at=datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
            extraction_status=ExtractionStatus.SUCCESS,
            content_hash="b" * 64,
            raw_content='[{"event_type": "wildfire"}]',
            metadata={"source": "postgres_integration"},
        )
        obs = DisasterObservation(
            observation_id="obs_pg_integration_001",
            source_id="src_pg_integration_001",
            event_type="wildfire",
            location="Pine Forest",
            event_time=datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
            description="Active blaze spreading rapidly",
            severity="severe",
            latitude=39.7392,
            longitude=-104.9903,
            extraction_confidence=0.95,
            page_number=1,
        )
        outcome = ValidationOutcome(
            record_id=obs.observation_id,
            source_id=source_rec.source_id,
            rule_name="REQUIRED_FIELDS",
            severity=RuleSeverity.INFO,
            message="Verified required fields",
            passed=True,
        )

        with get_session(pg_session_factory) as session:
            counts = repository.save_batch(
                source_record=source_rec,
                accepted_observations=[obs],
                quality_outcomes=[outcome],
                session=session,
            )
            assert counts["observations_persisted"] == 1
            assert counts["quality_results_persisted"] == 1

        with get_session(pg_session_factory) as session:
            retrieved_src = repository.get_source("src_pg_integration_001", session)
            assert retrieved_src is not None
            assert retrieved_src.source_id == "src_pg_integration_001"

            stored_obs = repository.get_observations_for_source("src_pg_integration_001", session)
            assert len(stored_obs) == 1
            assert stored_obs[0].observation_id == "obs_pg_integration_001"
            assert stored_obs[0].event_type == "wildfire"
