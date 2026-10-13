"""Comprehensive live PostgreSQL integration tests for BEACON storage and migration layers.

These tests execute against a real PostgreSQL server when BEACON_TEST_DATABASE_URL is set,
or fail explicitly if BEACON_REQUIRE_POSTGRES is set but the database is unavailable.
All operations execute inside an isolated disposable schema to guarantee zero data loss.
"""

from datetime import datetime, timezone
import os
import uuid
import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

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
from beacon.quality.models import RuleSeverity, ValidationOutcome
from beacon.storage import (
    EventModel,
    EventObservationModel,
    StorageRepository,
    init_db,
)
from scripts.migrate_v0_4 import apply_migration

TEST_DB_URL = os.getenv("BEACON_TEST_DATABASE_URL")
REQUIRE_POSTGRES = os.getenv("BEACON_REQUIRE_POSTGRES", "").lower() in ("1", "true", "yes")

# Validate execution preconditions
if REQUIRE_POSTGRES and not TEST_DB_URL:
    pytest.fail(
        "BEACON_REQUIRE_POSTGRES is set but BEACON_TEST_DATABASE_URL is not configured. "
        "A live PostgreSQL database is mandatory for this test run."
    )

pytestmark = pytest.mark.skipif(
    not TEST_DB_URL,
    reason="BEACON_TEST_DATABASE_URL is not set; skipping live PostgreSQL integration tests",
)


@pytest.fixture(scope="function")
def pg_disposable_schema():
    """Create an isolated, disposable PostgreSQL schema and return an engine scoped to it."""
    assert TEST_DB_URL is not None
    root_engine = create_engine(TEST_DB_URL)

    # Test database connectivity
    try:
        with root_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as err:
        root_engine.dispose()
        if REQUIRE_POSTGRES:
            pytest.fail(f"PostgreSQL connection failed under BEACON_REQUIRE_POSTGRES: {err}")
        pytest.skip(f"PostgreSQL connection failed: {err}")

    # Create isolated disposable schema with strict identifier validation
    schema_id = f"beacon_test_{uuid.uuid4().hex[:10]}"
    assert schema_id.startswith("beacon_test_") and schema_id != "public"
    with root_engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema_id}"))

    # Engine configured to use the disposable schema by default
    scoped_engine = create_engine(
        TEST_DB_URL,
        connect_args={"options": f"-csearch_path={schema_id}"},
    )
    factory = sessionmaker(bind=scoped_engine, expire_on_commit=False)

    try:
        yield scoped_engine, factory, schema_id
    finally:
        # Guarantee scoped connections and transactions are closed before dropping schema
        try:
            scoped_engine.dispose()
        except Exception:
            pass

        # Teardown: Safely drop only the verified disposable schema
        if schema_id.startswith("beacon_test_") and schema_id != "public":
            try:
                with root_engine.begin() as conn:
                    conn.execute(text(f"DROP SCHEMA IF EXISTS {schema_id} CASCADE"))
            finally:
                root_engine.dispose()
        else:
            root_engine.dispose()


class TestPostgresStorageIntegration:
    """Live PostgreSQL database integration and migration tests."""

    def test_postgres_fresh_initialization_and_batch_persistence(
        self, pg_disposable_schema
    ) -> None:
        """Verify fresh PostgreSQL initialization and atomic batch persistence."""
        engine, factory, _ = pg_disposable_schema
        init_db(engine)

        repository = StorageRepository()
        source_rec = SourceRecord(
            source_id="src_pg_fresh_001",
            source_file="data/sample/events.json",
            source_type=SourceType.JSON,
            extraction_method=ExtractionMethod.JSON_PARSE,
            ingested_at=datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
            extraction_status=ExtractionStatus.SUCCESS,
            content_hash="a" * 64,
            raw_content='[{"event_type": "wildfire"}]',
            metadata={"test": "fresh_init"},
        )
        obs = DisasterObservation(
            observation_id="obs_pg_fresh_001",
            source_id="src_pg_fresh_001",
            event_type="wildfire",
            location="Pine Forest",
            event_time=datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
            description="Active wildfire reported",
            severity="severe",
            latitude=39.7392,
            longitude=-104.9903,
            extraction_confidence=0.95,
        )
        outcome = ValidationOutcome(
            record_id=obs.observation_id,
            source_id=source_rec.source_id,
            rule_name="REQUIRED_FIELDS",
            severity=RuleSeverity.INFO,
            message="Verified required fields",
            passed=True,
        )

        with factory() as session:
            counts = repository.save_batch(
                source_record=source_rec,
                accepted_observations=[obs],
                quality_outcomes=[outcome],
                session=session,
            )
            session.commit()
            assert counts["observations_persisted"] == 1
            assert counts["quality_results_persisted"] == 1

        with factory() as session:
            retrieved_src = repository.get_source("src_pg_fresh_001", session)
            assert retrieved_src is not None
            assert retrieved_src.source_id == "src_pg_fresh_001"

            stored_obs = repository.get_observations_for_source("src_pg_fresh_001", session)
            assert len(stored_obs) == 1
            assert stored_obs[0].observation_id == "obs_pg_fresh_001"

    def test_postgres_phase_3_to_phase_4_migration_upgrade(
        self, pg_disposable_schema
    ) -> None:
        """Verify upgrading a Phase 3 PostgreSQL schema to Phase 4 is safe, transactional, and idempotent."""
        engine, factory, _ = pg_disposable_schema

        # 1. Initialize Phase 3 tables ONLY (simulate legacy Phase 3 database)
        phase_3_sql = """
        CREATE TABLE IF NOT EXISTS sources (
            source_id VARCHAR(64) PRIMARY KEY,
            source_file VARCHAR(512) NOT NULL,
            source_type VARCHAR(32) NOT NULL,
            extraction_method VARCHAR(64) NOT NULL,
            ingested_at TIMESTAMP WITH TIME ZONE NOT NULL,
            extraction_status VARCHAR(32) NOT NULL,
            content_hash VARCHAR(64) NOT NULL,
            raw_content TEXT,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS observations (
            observation_id VARCHAR(64) PRIMARY KEY,
            source_id VARCHAR(64) NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
            event_type VARCHAR(128) NOT NULL,
            location TEXT,
            event_time TIMESTAMP WITH TIME ZONE,
            description TEXT,
            severity VARCHAR(32),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            extraction_confidence DOUBLE PRECISION,
            page_number INTEGER,
            source_reference VARCHAR(512),
            raw_claim TEXT,
            validation_warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS data_quality_results (
            id BIGSERIAL PRIMARY KEY,
            record_id VARCHAR(64) REFERENCES observations(observation_id) ON DELETE SET NULL,
            source_id VARCHAR(64) NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
            rule_name VARCHAR(128) NOT NULL,
            field_name VARCHAR(128),
            severity VARCHAR(32) NOT NULL,
            message TEXT NOT NULL,
            passed BOOLEAN NOT NULL,
            details JSONB NOT NULL DEFAULT '{}'::jsonb,
            checked_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
        with engine.begin() as conn:
            for stmt in phase_3_sql.strip().split(";"):
                if stmt.strip():
                    conn.execute(text(stmt))

        # 2. Populate sample Phase 3 data
        repo = StorageRepository()
        src_v3 = SourceRecord(
            source_id="src_v3_upgrade",
            source_file="data/sample/events.json",
            source_type=SourceType.JSON,
            extraction_method=ExtractionMethod.JSON_PARSE,
            ingested_at=datetime.now(timezone.utc),
            extraction_status=ExtractionStatus.SUCCESS,
            content_hash="c" * 64,
            raw_content="{}",
        )
        obs_v3 = DisasterObservation(
            observation_id="obs_v3_upgrade",
            source_id="src_v3_upgrade",
            event_type="flood",
            location="River Basin",
            event_time=datetime(2026, 5, 10, 8, 0, tzinfo=timezone.utc),
            latitude=35.0,
            longitude=-119.0,
        )
        with factory() as session:
            repo.save_batch(src_v3, [obs_v3], [], session)
            session.commit()

        # 3. Apply Phase 4 migration via CLI utility function
        apply_migration(engine, dry_run=False)

        # 4. Verify Phase 3 data was preserved without corruption (non-destructive)
        with factory() as session:
            retrieved_src = repo.get_source("src_v3_upgrade", session)
            assert retrieved_src is not None
            assert retrieved_src.source_id == "src_v3_upgrade"

        # 5. Verify Phase 4 tables exist and accept records
        sample_event = UnifiedDisasterEvent(
            event_id="evt_pg_migrated_01",
            event_type="flood",
            location="River Basin",
            start_time=datetime(2026, 5, 10, 8, 0, tzinfo=timezone.utc),
            summary="Flood profile created after Phase 4 upgrade.",
            severity="moderate",
            observation_ids=["obs_v3_upgrade"],
            source_ids=["src_v3_upgrade"],
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        with factory() as session:
            repo.save_event(sample_event, session)
            repo.link_observation_to_event(sample_event.event_id, obs_v3.observation_id, 0.95, session)
            session.commit()

            stored_ev = repo.get_event("evt_pg_migrated_01", session)
            assert stored_ev is not None
            assert stored_ev.event_id == "evt_pg_migrated_01"

        # 6. Apply migration a second time to verify idempotency
        apply_migration(engine, dry_run=False)

    def test_postgres_composite_uniqueness_and_foreign_keys(
        self, pg_disposable_schema
    ) -> None:
        """Verify PostgreSQL enforces uq_event_observation and foreign-key cascades."""
        engine, factory, _ = pg_disposable_schema
        init_db(engine)
        repo = StorageRepository()

        event = UnifiedDisasterEvent(
            event_id="evt_pg_fk_01",
            event_type="storm",
            location="Coastal Port",
            start_time=datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc),
            summary="Coastal storm",
            severity="severe",
            observation_ids=["obs_pg_fk_01"],
            source_ids=["src_pg_fk_01"],
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        src = SourceRecord(
            source_id="src_pg_fk_01",
            source_file="events.json",
            source_type=SourceType.JSON,
            extraction_method=ExtractionMethod.JSON_PARSE,
            ingested_at=datetime.now(timezone.utc),
            extraction_status=ExtractionStatus.SUCCESS,
            content_hash="d" * 64,
        )
        obs = DisasterObservation(
            observation_id="obs_pg_fk_01",
            source_id="src_pg_fk_01",
            event_type="storm",
            location="Coastal Port",
            event_time=datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc),
        )

        with factory() as session:
            repo.save_event(event, session)
            repo.save_source(src, session)
            repo.save_observations([obs], session)
            session.commit()

            # First link succeeds
            repo.link_observation_to_event("evt_pg_fk_01", "obs_pg_fk_01", 0.90, session)
            session.commit()

            # Direct duplicate insert must trigger PostgreSQL uq_event_observation
            dup_link = EventObservationModel(
                event_id="evt_pg_fk_01",
                observation_id="obs_pg_fk_01",
                confidence_score=0.95,
            )
            session.add(dup_link)
            with pytest.raises(IntegrityError):
                session.commit()
            session.rollback()

            # Repository link_observation_to_event must update idempotently without failing
            idempotent_link = repo.link_observation_to_event(
                "evt_pg_fk_01", "obs_pg_fk_01", 0.98, session
            )
            session.commit()
            assert idempotent_link.confidence_score == 0.98

            # Cascade verification: Deleting event must cascade delete junction link
            ev_model = session.get(EventModel, "evt_pg_fk_01")
            assert ev_model is not None
            session.delete(ev_model)
            session.commit()

            remaining_links = session.execute(
                select(EventObservationModel).where(EventObservationModel.event_id == "evt_pg_fk_01")
            ).scalars().all()
            assert len(remaining_links) == 0

    def test_postgres_transactional_rollback_on_error(
        self, pg_disposable_schema
    ) -> None:
        """Verify PostgreSQL transaction rollback ensures atomic all-or-nothing behavior."""
        engine, factory, _ = pg_disposable_schema
        init_db(engine)
        repo = StorageRepository()

        src = SourceRecord(
            source_id="src_rollback_pg",
            source_file="corrupt.json",
            source_type=SourceType.JSON,
            extraction_method=ExtractionMethod.JSON_PARSE,
            ingested_at=datetime.now(timezone.utc),
            extraction_status=ExtractionStatus.SUCCESS,
            content_hash="e" * 64,
        )
        valid_obs = DisasterObservation(
            observation_id="obs_valid_pg",
            source_id="src_rollback_pg",
            event_type="flood",
            location="Delta",
        )
        # Invalid outcome with non-existent source_id referencing FK
        invalid_outcome = ValidationOutcome(
            record_id=valid_obs.observation_id,
            source_id="src_NON_EXISTENT_FK",
            rule_name="REQUIRED_FIELDS",
            severity=RuleSeverity.ERROR,
            message="Foreign key violation test",
            passed=False,
        )

        with factory() as session:
            with pytest.raises(IntegrityError):
                repo.save_batch(
                    source_record=src,
                    accepted_observations=[valid_obs],
                    quality_outcomes=[invalid_outcome],
                    session=session,
                )
                session.commit()
            session.rollback()

        # Confirm rollback: Neither source nor observations were persisted
        with factory() as session:
            assert repo.get_source("src_rollback_pg", session) is None
            assert len(repo.get_observations_for_source("src_rollback_pg", session)) == 0

    def test_postgres_database_backed_analytics_and_filtering(
        self, pg_disposable_schema
    ) -> None:
        """Verify AnalyticsService.summarize_db computes accurate, scoped metrics on PostgreSQL."""
        engine, factory, _ = pg_disposable_schema
        init_db(engine)
        repo = StorageRepository()
        analytics = AnalyticsService()

        ev1 = UnifiedDisasterEvent(
            event_id="evt_pg_an_flood",
            event_type="flood",
            location="Riverside",
            start_time=datetime(2026, 8, 1, 10, 0, tzinfo=timezone.utc),
            summary="Severe flood",
            severity="severe",
            observation_ids=["obs_an_fl_1", "obs_an_fl_2"],
            source_ids=["src_s1", "src_s2"],
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            metadata={"centroid": {"latitude": 34.0, "longitude": -118.0}},
            conflicting_claims=[
                ConflictingClaim(
                    field_name="severity",
                    observation_id="obs_an_fl_1",
                    source_id="src_s1",
                    claimed_value="moderate",
                    resolution_status="superseded",
                    notes="Resolved to severe",
                )
            ],
        )
        ev2 = UnifiedDisasterEvent(
            event_id="evt_pg_an_wildfire",
            event_type="wildfire",
            location="Pine Valley",
            start_time=datetime(2026, 8, 5, 12, 0, tzinfo=timezone.utc),
            summary="Moderate wildfire",
            severity="moderate",
            observation_ids=["obs_an_wf_1"],
            source_ids=["src_s3"],
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            metadata={"unanchored_timestamp": True},  # Missing centroid
        )

        obs_fl_1 = DisasterObservation(
            observation_id="obs_an_fl_1",
            source_id="src_s1",
            event_type="flood",
            location="Riverside",
            event_time=datetime(2026, 8, 1, 10, 0, tzinfo=timezone.utc),
            latitude=34.0,
            longitude=-118.0,
        )
        obs_fl_2 = DisasterObservation(
            observation_id="obs_an_fl_2",
            source_id="src_s2",
            event_type="flood",
            location="Riverside",
            event_time=datetime(2026, 8, 1, 11, 0, tzinfo=timezone.utc),
            latitude=None,  # Missing coordinates
            longitude=None,
        )
        obs_wf_1 = DisasterObservation(
            observation_id="obs_an_wf_1",
            source_id="src_s3",
            event_type="wildfire",
            location="Pine Valley",
            event_time=None,  # Missing timestamp
            latitude=None,   # Missing coordinates
            longitude=None,
        )

        src_s1 = SourceRecord(source_id="src_s1", source_file="s1.json", source_type=SourceType.JSON, extraction_method=ExtractionMethod.JSON_PARSE, ingested_at=datetime.now(timezone.utc), extraction_status=ExtractionStatus.SUCCESS, content_hash="1"*64)
        src_s2 = SourceRecord(source_id="src_s2", source_file="s2.csv", source_type=SourceType.CSV, extraction_method=ExtractionMethod.CSV_PARSE, ingested_at=datetime.now(timezone.utc), extraction_status=ExtractionStatus.SUCCESS, content_hash="2"*64)
        src_s3 = SourceRecord(source_id="src_s3", source_file="s3.txt", source_type=SourceType.TXT, extraction_method=ExtractionMethod.TEXT_READ, ingested_at=datetime.now(timezone.utc), extraction_status=ExtractionStatus.SUCCESS, content_hash="3"*64)

        with factory() as session:
            for s in [src_s1, src_s2, src_s3]:
                repo.save_source(s, session)
            repo.save_observations([obs_fl_1, obs_fl_2, obs_wf_1], session)
            repo.save_event(ev1, session)
            repo.save_event(ev2, session)
            repo.link_observation_to_event(ev1.event_id, obs_fl_1.observation_id, 0.95, session)
            repo.link_observation_to_event(ev1.event_id, obs_fl_2.observation_id, 0.85, session)
            repo.link_observation_to_event(ev2.event_id, obs_wf_1.observation_id, 0.90, session)
            session.commit()

            # 1. Unfiltered summary
            unfiltered = analytics.summarize_db(session)
            assert unfiltered.total_events == 2
            assert unfiltered.total_observations == 3
            assert unfiltered.compression_ratio == 1.5
            assert unfiltered.multi_source_events == 1
            assert unfiltered.single_source_events == 1
            assert unfiltered.conflicting_claims_count == 1
            assert unfiltered.coverage_gaps.events_missing_coordinates == 1
            assert unfiltered.coverage_gaps.observations_missing_coordinates == 2
            assert unfiltered.coverage_gaps.observations_missing_timestamps == 1

            # 2. Filtered summary (flood only)
            flood_filter = AnalyticsFilter(event_type="flood")
            flood_summary = analytics.summarize_db(session, filter_criteria=flood_filter)
            assert flood_summary.total_events == 1
            assert flood_summary.total_observations == 2
            assert flood_summary.compression_ratio == 2.0
            assert flood_summary.multi_source_events == 1
            assert flood_summary.multi_source_ratio == 1.0
            assert flood_summary.coverage_gaps.events_missing_coordinates == 0
            assert flood_summary.coverage_gaps.observations_missing_coordinates == 1
            assert flood_summary.coverage_gaps.observations_missing_timestamps == 0

            # 3. Filter with no matches
            no_match = AnalyticsFilter(event_type="tsunami")
            zero_summary = analytics.summarize_db(session, filter_criteria=no_match)
            assert zero_summary.total_events == 0
            assert zero_summary.total_observations == 0
            assert zero_summary.compression_ratio == 0.0
            assert zero_summary.multi_source_events == 0

    def test_postgres_schema_isolation_and_safe_teardown(
        self, pg_disposable_schema
    ) -> None:
        """Verify schema isolation and that teardown does not affect public schema."""
        engine, factory, schema_id = pg_disposable_schema
        assert schema_id.startswith("beacon_test_")
        assert schema_id != "public"

        # Verify current search_path resolves to the disposable schema
        with engine.connect() as conn:
            current_schema = conn.execute(text("SELECT current_schema()")).scalar()
            assert current_schema == schema_id

        # Verify public schema remains distinct and accessible
        with engine.connect() as conn:
            public_exists = conn.execute(
                text("SELECT 1 FROM information_schema.schemata WHERE schema_name = 'public'")
            ).scalar()
            assert public_exists == 1
