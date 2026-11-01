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
from collections import Counter
import json
from scripts.migrate_v0_4 import apply_migration
from scripts.migrate_v1_1 import apply_migration as apply_v1_1_migration
from beacon.models import ObservationType

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
    root_engine = create_engine(TEST_DB_URL, connect_args={"connect_timeout": 3})

    # Test database connectivity and strictly assert target is beacon_test, not beacon_dev
    try:
        with root_engine.connect() as conn:
            current_db = conn.execute(text("SELECT current_database()")).scalar()
            if current_db != "beacon_test":
                raise RuntimeError(
                    f"SAFETY POLICY VIOLATION: Integration tests must target 'beacon_test', "
                    f"but connected to '{current_db}'! Aborting."
                )
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

        from sqlalchemy import inspect
        inspector = inspect(engine)
        tables = set(inspector.get_table_names())
        assert len(tables) == 8, f"Expected 8 tables, got {tables}"
        assert "pipeline_runs" in tables, "pipeline_runs table missing from initialized PostgreSQL schema"

        from beacon.dashboard.services import DashboardService
        service = DashboardService(session_factory=factory)
        diag = service.test_database_connection()
        assert diag["connected"] is True
        assert diag["schema_ready"] is True
        assert diag["ready_tables"] == 8
        assert diag["tables"]["pipeline_runs"] >= 0

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
    def test_postgres_schema_inspection_respects_different_schemas(
        self, pg_disposable_schema
    ) -> None:
        """Verify readiness inspection correctly respects current_schema and isolates different schemas."""
        engine, factory, schema_a = pg_disposable_schema
        init_db(engine)

        # Create a second empty disposable schema B
        schema_b = f"beacon_test_{uuid.uuid4().hex[:10]}"
        with engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA {schema_b}"))

        engine_b = create_engine(
            TEST_DB_URL,
            connect_args={"options": f"-csearch_path={schema_b}"},
        )
        factory_b = sessionmaker(bind=engine_b, expire_on_commit=False)

        try:
            from beacon.config import Settings
            from beacon.dashboard.services import DashboardService

            # Service pointing to schema B (empty schema) reports uninitialized
            service_b = DashboardService(
                settings=Settings(database_url=TEST_DB_URL),
                session_factory=factory_b,
            )
            diag_b = service_b.test_database_connection()
            assert diag_b["connected"] is True
            assert diag_b["schema_ready"] is False
            assert diag_b["status"] == "uninitialized"
            assert diag_b["ready_tables"] == 0

            # Service pointing to schema A (all 8 tables initialized) reports healthy
            service_a = DashboardService(
                settings=Settings(database_url=TEST_DB_URL),
                session_factory=factory,
            )
            diag_a = service_a.test_database_connection()
            assert diag_a["connected"] is True
            assert diag_a["schema_ready"] is True
            assert diag_a["status"] == "healthy"
            assert diag_a["ready_tables"] == 8
        finally:
            engine_b.dispose()
            with engine.begin() as conn:
                conn.execute(text(f"DROP SCHEMA IF EXISTS {schema_b} CASCADE"))

    def test_postgres_missing_pipeline_runs_diagnostic_and_in_place_migration(
        self, pg_disposable_schema
    ) -> None:
        """Verify PostgreSQL reports missing pipeline_runs in inspected schema and in-place migration restores readiness."""
        engine, factory, schema_id = pg_disposable_schema
        init_db(engine)

        # Drop pipeline_runs to simulate legacy Phase 2E schema in disposable schema
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE pipeline_runs;"))

        from beacon.config import Settings
        from beacon.dashboard.services import DashboardService
        from scripts.migrate_v1_0 import apply_migration as apply_v1_0_migration

        service = DashboardService(
            settings=Settings(database_url=TEST_DB_URL),
            session_factory=factory,
        )

        # Check diagnostic guidance on PostgreSQL
        diag = service.test_database_connection()
        assert diag["connected"] is True
        assert diag["schema_ready"] is False
        assert diag["status"] == "uninitialized"
        assert diag["tables"]["pipeline_runs"] == -1
        assert "pipeline_runs" in diag["message"]
        assert "scripts/migrate_v1_0.py" in diag["message"]

        # Overview correctly falls back to in-memory mode without raising
        ov = service.get_system_overview()
        assert ov["is_live_db"] is False
        assert ov["storage_mode"] == "in_memory"
        assert ov["uninitialized_db"] is True
        assert ov["events_count"] == 0

        # Execute migration in-place within the disposable schema
        apply_v1_0_migration(engine)

        # Re-check on the SAME service instance without stale caching
        assert service.is_schema_ready is True
        diag_after = service.test_database_connection()
        assert diag_after["schema_ready"] is True
        assert diag_after["status"] == "healthy"
        assert diag_after["ready_tables"] == 8
        assert diag_after["tables"]["pipeline_runs"] >= 0

    def test_postgres_v1_1_universal_observation_migration_and_types(
        self, pg_disposable_schema
    ) -> None:
        """Verify v1.1 additive migration against live PostgreSQL disposable schema.

        Validates:
        - Connection strictly targets beacon_test.
        - Previous database version schema (v1.0) can be populated with existing observations and events.
        - v1.1 migration executes cleanly.
        - New columns (observation_type, is_event_eligible, context_payload) and indexes exist.
        - Existing rows and IDs are preserved.
        - Existing incident observations retain INCIDENT classification and event eligibility.
        - Context observations can be inserted and reloaded.
        - JSON context payloads survive persistence.
        - Event associations remain intact.
        - Transaction rollback behaves correctly on error.
        - Reload verified using a fresh SQLAlchemy session.
        """
        engine, factory, schema_id = pg_disposable_schema

        # 1. Verify connection targets beacon_test
        with engine.connect() as conn:
            target_db = conn.execute(text("SELECT current_database()")).scalar()
            assert target_db == "beacon_test", f"Target DB was {target_db}, expected beacon_test"

        # 2. Initialize full base schema
        init_db(engine)

        # 3. Alter table back to v1.0 state to simulate legacy database before v1.1 migration
        with engine.begin() as conn:
            conn.execute(text("DROP INDEX IF EXISTS ix_observations_observation_type;"))
            conn.execute(text("DROP INDEX IF EXISTS ix_observations_is_event_eligible;"))
            conn.execute(text("ALTER TABLE observations DROP COLUMN IF EXISTS observation_type;"))
            conn.execute(text("ALTER TABLE observations DROP COLUMN IF EXISTS is_event_eligible;"))
            conn.execute(text("ALTER TABLE observations DROP COLUMN IF EXISTS context_payload;"))
            conn.execute(text("ALTER TABLE observations ALTER COLUMN event_type SET NOT NULL;"))

        # Verify observations table is in v1.0 state
        with engine.connect() as conn:
            cols = [
                row[0] for row in conn.execute(
                    text("SELECT column_name FROM information_schema.columns WHERE table_schema = :s AND table_name = 'observations'"),
                    {"s": schema_id}
                ).fetchall()
            ]
            assert "observation_type" not in cols
            assert "is_event_eligible" not in cols
            assert "context_payload" not in cols

        # Populate with representative existing v1.0 incident observation and event
        obs_id_leg = "obs_legacy_pg_001"
        evt_id_leg = "evt_legacy_pg_001"
        src_id_leg = "src_legacy_pg_001"

        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO sources (source_id, source_file, source_type, extraction_method, ingested_at, extraction_status, content_hash, raw_content, metadata_json, created_at, updated_at)
                    VALUES (:s_id, 'legacy_feed.json', 'json', 'json_parse', NOW(), 'success', 'hash123', '{}', '{}'::jsonb, NOW(), NOW())
                """),
                {"s_id": src_id_leg}
            )
            conn.execute(
                text("""
                    INSERT INTO observations (observation_id, source_id, event_type, location, event_time, description, severity, latitude, longitude, extraction_confidence, page_number, source_reference, raw_claim, validation_warnings, created_at, updated_at)
                    VALUES (:o_id, :s_id, 'flood', 'River District', NOW(), 'Severe river flood', 'severe', 34.05, -118.25, 0.95, 1, 'feed.json:1', '{"event":"flood"}', '[]'::jsonb, NOW(), NOW())
                """),
                {"o_id": obs_id_leg, "s_id": src_id_leg}
            )
            conn.execute(
                text("""
                    INSERT INTO events (event_id, event_type, location, start_time, end_time, summary, severity, resolution_rationale, conflicting_claims, source_ids, metadata_json, created_at, updated_at)
                    VALUES (:e_id, 'flood', 'River District', NOW(), NULL, 'River flood event', 'severe', 'Resolved from legacy feed', '[]'::jsonb, :src_ids::jsonb, '{}'::jsonb, NOW(), NOW())
                """),
                {"e_id": evt_id_leg, "src_ids": json.dumps([src_id_leg])}
            )
            conn.execute(
                text("""
                    INSERT INTO event_observations (event_id, observation_id, confidence_score, assigned_at)
                    VALUES (:e_id, :o_id, 0.95, NOW())
                """),
                {"e_id": evt_id_leg, "o_id": obs_id_leg}
            )

        # 4. Apply v1.1 migration
        apply_v1_1_migration(engine)

        # 5. Verify new columns exist
        with engine.connect() as conn:
            cols_after = [
                row[0] for row in conn.execute(
                    text("SELECT column_name FROM information_schema.columns WHERE table_schema = :s AND table_name = 'observations'"),
                    {"s": schema_id}
                ).fetchall()
            ]
            assert "observation_type" in cols_after
            assert "is_event_eligible" in cols_after
            assert "context_payload" in cols_after

            # Verify indexes exist
            indexes = [
                row[0] for row in conn.execute(
                    text("SELECT indexname FROM pg_indexes WHERE schemaname = :s AND tablename = 'observations'"),
                    {"s": schema_id}
                ).fetchall()
            ]
            assert "ix_observations_observation_type" in indexes
            assert "ix_observations_is_event_eligible" in indexes

        # 6. Verify existing rows and IDs are preserved with defaults
        repo = StorageRepository()
        with factory() as session:
            legacy_obs = repo.get_all_observations(session)
            assert len(legacy_obs) == 1
            lo = legacy_obs[0]
            assert lo.observation_id == obs_id_leg
            assert lo.observation_type == ObservationType.INCIDENT
            assert lo.is_event_eligible is True
            assert lo.is_incident is True
            assert lo.context_payload == {}
            assert lo.event_type == "flood"

            # Event association remains intact
            event_obs = repo.get_observations_for_event(evt_id_leg, session)
            assert len(event_obs) == 1
            assert event_obs[0].observation_id == obs_id_leg

        # 7. Insert and reload context observations with JSON payloads using fresh session
        ctx_meta_id = "obs_ctx_meta_pg_001"
        ctx_doc_id = "obs_ctx_doc_pg_001"
        meta_payload = {"title": "PostgreSQL Integration Manifest", "file_count": 7, "nested": {"key": "value"}}
        doc_payload = {"document_type": "sitrep", "author": "Emergency Command", "sections": ["Overview", "Impact"]}

        ctx_meta = DisasterObservation(
            observation_id=ctx_meta_id,
            source_id=src_id_leg,
            raw_claim="{}",
            location="HQ Manifest",
            event_type=None,
            observation_type=ObservationType.DATASET_METADATA,
            context_payload=meta_payload,
        )
        ctx_doc = DisasterObservation(
            observation_id=ctx_doc_id,
            source_id=src_id_leg,
            raw_claim="{}",
            location="HQ SitRep",
            event_type=None,
            observation_type=ObservationType.DOCUMENT_CONTEXT,
            context_payload=doc_payload,
        )

        with factory() as session:
            repo.save_observations([ctx_meta, ctx_doc], session)
            session.commit()

        # Fresh session reload verification
        fresh_session = factory()
        try:
            all_reloaded = repo.get_all_observations(fresh_session)
            assert len(all_reloaded) == 3

            meta_reloaded = next(o for o in all_reloaded if o.observation_id == ctx_meta_id)
            assert meta_reloaded.observation_type == ObservationType.DATASET_METADATA
            assert meta_reloaded.is_event_eligible is False
            assert meta_reloaded.is_incident is False
            assert meta_reloaded.context_payload == meta_payload
            assert meta_reloaded.context_payload["nested"]["key"] == "value"

            doc_reloaded = next(o for o in all_reloaded if o.observation_id == ctx_doc_id)
            assert doc_reloaded.observation_type == ObservationType.DOCUMENT_CONTEXT
            assert doc_reloaded.is_event_eligible is False
            assert doc_reloaded.context_payload["sections"] == ["Overview", "Impact"]
        finally:
            fresh_session.close()

        # 8. Verify transactional rollback behaves correctly
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(text("INSERT INTO observations (observation_id, source_id, event_type) VALUES ('obs_fail', 'nonexistent_src', 'flood');"))

        # Verify no partial rows survived rollback
        with factory() as session:
            check_failed = repo.get_all_observations(session)
            assert not any(o.observation_id == "obs_fail" for o in check_failed)

    def test_postgres_repeat_ingestion_and_event_portfolio_stability(
        self, pg_disposable_schema
    ) -> None:
        """Run the exact seven-file dataset twice using isolated PostgreSQL storage.

        Verifies:
        - 12 INCIDENT observations.
        - 1 DATASET_METADATA observation.
        - 1 DOCUMENT_CONTEXT observation.
        - 1 IMAGE_CONTEXT observation.
        - 15 total observations.
        - Zero additional records after second identical run.
        - No duplicate IDs.
        - Event portfolio stability (no unexpected event creation or portfolio growth).
        """
        from pathlib import Path
        from beacon.config import Settings
        from beacon.dashboard.services import DashboardService

        demo_dir = Path(r"C:\Users\NWNB0000\Downloads\BEACON_MultiFormat_Pipeline_Demo")
        if not demo_dir.exists():
            pytest.skip("Demonstration dataset directory not found")

        uploads = [(f.name, f.read_bytes()) for f in sorted(demo_dir.iterdir()) if f.is_file()]

        engine, factory, schema_id = pg_disposable_schema
        init_db(engine)

        service = DashboardService(
            settings=Settings(database_url=TEST_DB_URL),
            session_factory=factory,
        )

        # Run 1
        res1 = service.ingest_and_process_uploads(uploads, enable_resolution=True, persist_db=True)
        obs1 = res1["observations"]
        events1 = res1["events"]

        assert len(obs1) == 15
        assert len(events1) == 8
        counts_1 = Counter(o.observation_type.value for o in obs1)
        assert counts_1["incident"] == 12
        assert counts_1["dataset_metadata"] == 1
        assert counts_1["document_context"] == 1
        assert counts_1["image_context"] == 1

        repo = StorageRepository()
        with factory() as session:
            db_obs_1 = repo.get_all_observations(session)
            db_events_1 = repo.get_all_events(session)
            assert len(db_obs_1) == 15
            assert len(db_events_1) == 8
            obs_ids_1 = {o.observation_id for o in db_obs_1}

        # Run 2 (repeat ingestion)
        res2 = service.ingest_and_process_uploads(uploads, enable_resolution=True, persist_db=True)
        obs2 = res2["observations"]
        events2 = res2["events"]
        assert len(obs2) == 15
        assert len(events2) == 8

        with factory() as session:
            db_obs_2 = repo.get_all_observations(session)
            db_events_2 = repo.get_all_events(session)
            assert len(db_obs_2) == 15, "Repeat ingestion created additional observation rows"
            assert len(db_events_2) == 8, "Repeat ingestion created unexpected event portfolio growth"
            obs_ids_2 = {o.observation_id for o in db_obs_2}
            assert obs_ids_1 == obs_ids_2, "Observation IDs shifted between repeat runs"

    def test_postgres_context_exclusion_from_resolution_and_analytics(
        self, pg_disposable_schema
    ) -> None:
        """Verify context types are excluded from resolution and analytics on PostgreSQL.

        Proves all context types are excluded from clustering, creation, updates, merging,
        and disaster analytics, while 12 incident observations remain eligible.
        """
        from pathlib import Path
        from beacon.config import Settings
        from beacon.dashboard.services import DashboardService

        demo_dir = Path(r"C:\Users\NWNB0000\Downloads\BEACON_MultiFormat_Pipeline_Demo")
        if not demo_dir.exists():
            pytest.skip("Demonstration dataset directory not found")

        uploads = [(f.name, f.read_bytes()) for f in sorted(demo_dir.iterdir()) if f.is_file()]

        engine, factory, schema_id = pg_disposable_schema
        init_db(engine)

        service = DashboardService(
            settings=Settings(database_url=TEST_DB_URL),
            session_factory=factory,
        )

        res = service.ingest_and_process_uploads(uploads, enable_resolution=True, persist_db=True)
        obs_all = res["observations"]
        events = res["events"]

        # Verify all 12 incident observations are resolved
        incident_obs = [o for o in obs_all if o.observation_type == ObservationType.INCIDENT]
        context_obs = [o for o in obs_all if o.observation_type != ObservationType.INCIDENT]
        assert len(incident_obs) == 12
        assert len(context_obs) == 3

        # All 12 are event eligible
        assert all(o.is_event_eligible for o in incident_obs)
        assert all(not o.is_event_eligible for o in context_obs)

        # No context observations are linked to any event
        all_linked_obs_ids = {oid for e in events for oid in e.observation_ids}
        for ctx in context_obs:
            assert ctx.observation_id not in all_linked_obs_ids

        # Analytics on PostgreSQL excludes context observations
        analytics = AnalyticsService()
        with factory() as session:
            summary = analytics.summarize_db(session)
            assert summary.total_observations == 12, "Analytics counted context observations"
            assert summary.total_events == 8
            assert summary.compression_ratio == 1.5
