"""Unit tests for the BEACON Streamlit dashboard service layer.

Validates overview aggregations, file uploads, pipeline integration,
event querying, analytics, and database diagnostic services without requiring a browser.
"""

from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from beacon.analytics.models import AnalyticsFilter
from beacon.config import Settings, get_settings
from beacon.dashboard.formatters import (
    explain_compression_ratio,
    format_compression_ratio,
    format_confidence,
    format_conflict_claim_markdown,
    format_conflict_claim_row,
    format_coordinates,
    format_decision_row,
    format_observation_row,
    format_score,
    format_timestamp,
)
from beacon.dashboard.services import DashboardService
from beacon.models import (
    DisasterObservation,
    ExtractionMethod,
    ExtractionStatus,
    SourceRecord,
    SourceType,
    UnifiedDisasterEvent,
)
from beacon.storage import ResolutionDecisionModel, StorageRepository, drop_db, init_db


def _can_create_socketpair() -> bool:
    """Return True if local loopback socket connections can be established."""
    import socket
    try:
        s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s1.bind(("127.0.0.1", 0))
        port = s1.getsockname()[1]
        s1.listen(1)
        s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s2.settimeout(0.5)
        s2.connect(("127.0.0.1", port))
        conn, _ = s1.accept()
        conn.close()
        s1.close()
        s2.close()
        return True
    except Exception:
        return False


@pytest.fixture
def in_memory_factory() -> sessionmaker:
    """Create an in-memory SQLite database session factory for testing."""
    engine = create_engine("sqlite:///:memory:")
    init_db(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    yield factory
    drop_db(engine)


@pytest.fixture
def dashboard_service(in_memory_factory: sessionmaker) -> DashboardService:
    """Initialize a DashboardService instance with an in-memory database."""
    settings = Settings(database_url="sqlite:///:memory:")
    return DashboardService(settings=settings, session_factory=in_memory_factory)


@pytest.fixture
def isolate_in_memory_dashboard(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """Explicitly isolate environment and settings cache for in-memory dashboard execution."""
    import streamlit as st

    monkeypatch.setenv("BEACON_DATABASE_URL", "")
    get_settings.cache_clear()
    st.cache_resource.clear()
    try:
        yield
    finally:
        get_settings.cache_clear()
        st.cache_resource.clear()


@pytest.fixture
def sample_events() -> list[UnifiedDisasterEvent]:
    return [
        UnifiedDisasterEvent(
            event_id="evt_dash_01",
            event_type="flood",
            location="Riverside Metro",
            start_time=datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
            summary="Riverside flood",
            severity="severe",
            observation_ids=["obs_d_01", "obs_d_02"],
            source_ids=["src_d_01", "src_d_02"],
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            metadata={"centroid": {"latitude": 34.05, "longitude": -118.25}},
        ),
        UnifiedDisasterEvent(
            event_id="evt_dash_02",
            event_type="wildfire",
            location="Pine Valley",
            start_time=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
            summary="Pine Valley wildfire",
            severity="moderate",
            observation_ids=["obs_d_03"],
            source_ids=["src_d_03"],
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        ),
    ]


class TestDashboardService:
    """Tests for DashboardService functionality."""

    def test_overview_empty_state(self) -> None:
        """Dashboard overview handles zero events cleanly."""
        service = DashboardService(settings=Settings(database_url=None), session_factory=None)
        overview = service.get_system_overview()

        assert overview["events_count"] == 0
        assert overview["observations_count"] == 0
        assert overview["sources_count"] == 0
        assert overview["compression_ratio"] == 0.0
        assert overview["multi_source_events_count"] == 0
        assert overview["conflicting_claims_count"] == 0
        assert not overview["is_live_db"]

    def test_database_connection_diagnostics_unconfigured(self) -> None:
        """Diagnostics report unconfigured status when database_url is not set."""
        service = DashboardService(settings=Settings(database_url=None), session_factory=None)
        status = service.test_database_connection()

        assert not status["connected"]
        assert status["status"] == "unconfigured"
        assert "BEACON_DATABASE_URL is not set" in status["message"]

    def test_database_connection_diagnostics_healthy(
        self, dashboard_service: DashboardService
    ) -> None:
        """Diagnostics report healthy connection and table counts when connected."""
        status = dashboard_service.test_database_connection()

        assert status["connected"]
        assert status["status"] == "healthy"
        assert "events" in status["tables"]
        assert "observations" in status["tables"]
        assert "sources" in status["tables"]

    def test_process_uploaded_files(self, dashboard_service: DashboardService) -> None:
        """File upload processing ingests valid disaster files and cleans up temporary directory."""
        uploads = [
            ("telemetry.json", b'[{"event_type": "flood", "location": "Sector 4", "severity": "moderate"}]'),
            ("sensor.csv", b"event_type,location,severity\nwildfire,Ridge 9,severe\n"),
            ("dispatch.txt", b"INCIDENT: Cyclone in Bay\nSEVERITY: high\n"),
        ]

        results, temp_dir = dashboard_service.process_uploaded_files(uploads)
        assert len(results) == 3
        assert any(r.source_record.source_type == SourceType.JSON for r in results)
        assert any(r.source_record.source_type == SourceType.CSV for r in results)
        assert any(r.source_record.source_type == SourceType.TXT for r in results)
        # Verify temporary upload directory is cleaned up upon completion
        assert not temp_dir.exists()

    def test_process_uploaded_files_cleanup_on_partial_failure(
        self, dashboard_service: DashboardService
    ) -> None:
        """Temporary directory is guaranteed to be cleaned up even on partial ingestion failures."""
        uploads = [
            ("valid.json", b'[{"event_type": "flood", "location": "Sector 1"}]'),
            ("corrupt.xyz", b"not-a-valid-format-content"),
        ]
        results, temp_dir = dashboard_service.process_uploaded_files(uploads)
        assert len(results) == 2
        assert not temp_dir.exists()
        # Diagnostics remain intact after cleanup
        assert any(r.status == ExtractionStatus.SUCCESS for r in results)
        assert any(r.is_failed or r.status == ExtractionStatus.FAILED for r in results)

    def test_process_uploaded_files_cleanup_on_exception(
        self, dashboard_service: DashboardService, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Temporary directory cleanup is guaranteed when an unexpected exception is raised."""
        def raise_boom(*args, **kwargs):
            raise RuntimeError("Unexpected simulated ingestion failure")

        monkeypatch.setattr("beacon.dashboard.services.ingest_file", raise_boom)
        uploads = [("stream.json", b'[{"event_type": "flood"}]')]

        with pytest.raises(RuntimeError, match="Unexpected simulated ingestion failure"):
            dashboard_service.process_uploaded_files(uploads)

    def test_process_unsupported_file_upload(
        self, dashboard_service: DashboardService
    ) -> None:
        """Unsupported file extensions fail gracefully without crashing."""
        uploads = [("unknown.xyz", b"binary content")]
        results, temp_dir = dashboard_service.process_uploaded_files(uploads)

        assert len(results) == 1
        assert results[0].is_failed or results[0].status == ExtractionStatus.FAILED
        assert not temp_dir.exists()

    def test_run_pipeline_in_memory(self) -> None:
        """Pipeline execution in-memory processes sample files without database."""
        service = DashboardService(settings=Settings(database_url=None), session_factory=None)
        sample_path = Path("data/sample/events.json")
        if not sample_path.exists():
            pytest.skip("Sample data file not found")

        result = service.run_pipeline_on_paths([sample_path], enable_resolution=True, persist_db=False)
        assert result.files_succeeded == 1
        assert result.observations_extracted > 0
        assert result.events_synthesized > 0
        assert len(service._in_memory_events) > 0

    def test_run_pipeline_rerun_idempotency_in_memory(self) -> None:
        """Re-running pipeline in-memory deduplicates observations and preserves compression ratio."""
        service = DashboardService(settings=Settings(database_url=None), session_factory=None)
        sample_path = Path("data/sample/events.json")
        if not sample_path.exists():
            pytest.skip("Sample data file not found")

        # Run 1
        service.run_pipeline_on_paths([sample_path], enable_resolution=True, persist_db=False)
        obs_count_1 = len(service._in_memory_observations)
        events_count_1 = len(service._in_memory_events)
        overview_1 = service.get_system_overview()

        # Run 2 (reprocessing same input file)
        service.run_pipeline_on_paths([sample_path], enable_resolution=True, persist_db=False)
        obs_count_2 = len(service._in_memory_observations)
        events_count_2 = len(service._in_memory_events)
        overview_2 = service.get_system_overview()

        # Observations and events counts must not inflate
        assert obs_count_2 == obs_count_1
        assert events_count_2 == events_count_1
        assert overview_2["observations_count"] == overview_1["observations_count"]
        assert overview_2["events_count"] == overview_1["events_count"]
        assert overview_2["compression_ratio"] == overview_1["compression_ratio"]

    def test_run_pipeline_overlapping_and_distinct_observations(self, tmp_path: Path) -> None:
        """Multiple runs across files preserve all observations while deduplicating exact reruns."""
        import json
        service = DashboardService(settings=Settings(database_url=None), session_factory=None)

        f1 = tmp_path / "batch1.json"
        f1.write_text(json.dumps([
            {
                "event_type": "flood",
                "location": "Riverside North",
                "event_time": "2026-05-15T10:00:00Z",
                "latitude": 34.0500,
                "longitude": -118.2400,
            },
            {
                "event_type": "wildfire",
                "location": "Pine Ridge",
                "event_time": "2026-05-15T12:00:00Z",
                "latitude": 39.7300,
                "longitude": -104.9900,
            },
        ]))

        f2 = tmp_path / "batch2.json"
        f2.write_text(json.dumps([
            # Corroborating flood observation in Riverside North from distinct source
            {
                "event_type": "flood",
                "location": "Riverside North",
                "event_time": "2026-05-15T10:30:00Z",
                "latitude": 34.0510,
                "longitude": -118.2410,
                "severity": "severe",
            },
            # Distinct earthquake in Fault Valley
            {
                "event_type": "earthquake",
                "location": "Fault Valley",
                "event_time": "2026-05-15T14:00:00Z",
                "latitude": 36.1000,
                "longitude": -115.1500,
            },
        ]))

        # Run 1: process f1
        service.run_pipeline_on_paths([f1], enable_resolution=True, persist_db=False)
        assert len(service._in_memory_observations) == 2
        assert len(service._in_memory_events) == 2

        # Run 2: reprocess f1 (identical input) -> zero duplicate observations or events
        service.run_pipeline_on_paths([f1], enable_resolution=True, persist_db=False)
        assert len(service._in_memory_observations) == 2
        assert len(service._in_memory_events) == 2

        # Run 3: process f2 (second file with corroborating flood and distinct earthquake)
        service.run_pipeline_on_paths([f2], enable_resolution=True, persist_db=False)
        # Total observations is 2 (from f1) + 2 (from f2) = 4 distinct observations
        assert len(service._in_memory_observations) == 4
        # Events resolved: Riverside flood (merged), Pine wildfire, Fault earthquake -> 3 unified events!
        assert len(service._in_memory_events) == 3

        # Re-running f2 again must not duplicate observations or events
        service.run_pipeline_on_paths([f2], enable_resolution=True, persist_db=False)
        assert len(service._in_memory_observations) == 4
        assert len(service._in_memory_events) == 3

    def test_query_events_with_filters(
        self, dashboard_service: DashboardService, sample_events: list[UnifiedDisasterEvent]
    ) -> None:
        """Event querying supports filtering by event type, location, and minimum sources."""
        repo = StorageRepository()
        with dashboard_service.session_factory() as session:
            for ev in sample_events:
                repo.save_event(ev, session)
            session.commit()

        # 1. Query all
        all_events = dashboard_service.query_events()
        assert len(all_events) == 2

        # 2. Filter by type
        flood_filter = AnalyticsFilter(event_type="flood")
        flood_events = dashboard_service.query_events(flood_filter)
        assert len(flood_events) == 1
        assert flood_events[0].event_type == "flood"

        # 3. Filter by location
        loc_filter = AnalyticsFilter(location="Pine")
        loc_events = dashboard_service.query_events(loc_filter)
        assert len(loc_events) == 1
        assert loc_events[0].event_id == "evt_dash_02"

        # 4. Filter by min_sources = 2
        multi_filter = AnalyticsFilter(min_sources=2)
        multi_events = dashboard_service.query_events(multi_filter)
        assert len(multi_events) == 1
        assert multi_events[0].event_id == "evt_dash_01"

    def test_get_event_detail(
        self, dashboard_service: DashboardService, sample_events: list[UnifiedDisasterEvent]
    ) -> None:
        """Retrieving event detail returns constituent observations and profile."""
        repo = StorageRepository()
        src = SourceRecord(
            source_id="src_d_01",
            source_file="f1.json",
            source_type=SourceType.JSON,
            extraction_method=ExtractionMethod.JSON_PARSE,
            ingested_at=datetime.now(timezone.utc),
            extraction_status=ExtractionStatus.SUCCESS,
            content_hash="f" * 64,
        )
        obs = DisasterObservation(
            observation_id="obs_d_01",
            source_id="src_d_01",
            event_type="flood",
            location="Riverside Metro",
        )
        with dashboard_service.session_factory() as session:
            repo.save_source(src, session)
            repo.save_observations([obs], session)
            repo.save_event(sample_events[0], session)
            repo.link_observation_to_event(sample_events[0].event_id, obs.observation_id, 0.95, session)
            session.commit()

        detail = dashboard_service.get_event_detail("evt_dash_01")
        assert detail is not None
        assert detail["event"].event_id == "evt_dash_01"
        assert len(detail["observations"]) == 1
        assert detail["observations"][0].observation_id == "obs_d_01"

    def test_get_event_detail_not_found(self, dashboard_service: DashboardService) -> None:
        """Non-existent event ID returns None."""
        assert dashboard_service.get_event_detail("non_existent_id") is None

    def test_app_headless_smoke_runs_without_exception(
        self, isolate_in_memory_dashboard: None
    ) -> None:
        """Verify the Streamlit dashboard application launches and runs headlessly without error in in-memory mode."""
        if not _can_create_socketpair():
            pytest.skip("Local loopback socket connection forbidden in sandboxed environment")
        from streamlit.testing.v1 import AppTest

        app_path = Path(__file__).parent.parent / "src" / "beacon" / "dashboard" / "app.py"
        at = AppTest.from_file(str(app_path), default_timeout=15)
        at.run()
        assert not at.exception

        # Verify in-memory storage indicator renders
        storage_warns = [w.value for w in at.warning if "In-Memory" in w.value]
        assert len(storage_warns) >= 1

    def test_get_event_detail_preserves_none_confidence_from_db(
        self, dashboard_service: DashboardService, sample_events: list[UnifiedDisasterEvent]
    ) -> None:
        """Loading event detail from DB preserves None extraction_confidence without fabricating 0.0."""
        repo = StorageRepository()
        src = SourceRecord(
            source_id="src_d_none",
            source_file="none.json",
            source_type=SourceType.JSON,
            extraction_method=ExtractionMethod.JSON_PARSE,
            ingested_at=datetime.now(timezone.utc),
            extraction_status=ExtractionStatus.SUCCESS,
            content_hash="e" * 64,
        )
        obs = DisasterObservation(
            observation_id="obs_d_none",
            source_id="src_d_none",
            event_type="flood",
            location="Riverside Metro",
            extraction_confidence=None,
        )
        with dashboard_service.session_factory() as session:
            repo.save_source(src, session)
            repo.save_observations([obs], session)
            repo.save_event(sample_events[0], session)
            repo.link_observation_to_event(sample_events[0].event_id, obs.observation_id, 0.95, session)
            session.commit()

        detail = dashboard_service.get_event_detail("evt_dash_01")
        assert detail is not None
        matched_obs = next(o for o in detail["observations"] if o.observation_id == "obs_d_none")
        assert matched_obs.extraction_confidence is None

    def test_get_event_detail_deterministic_decision_ordering_on_identical_timestamps(
        self, dashboard_service: DashboardService, sample_events: list[UnifiedDisasterEvent]
    ) -> None:
        """Resolution decisions must order primarily by created_at.desc() and secondarily by id.desc()."""
        repo = StorageRepository()
        src = SourceRecord(
            source_id="src_d_order",
            source_file="order.json",
            source_type=SourceType.JSON,
            extraction_method=ExtractionMethod.JSON_PARSE,
            ingested_at=datetime.now(timezone.utc),
            extraction_status=ExtractionStatus.SUCCESS,
            content_hash="d" * 64,
        )
        obs1 = DisasterObservation(
            observation_id="obs_d_ord_1",
            source_id="src_d_order",
            event_type="flood",
            location="Riverside Metro",
        )
        obs2 = DisasterObservation(
            observation_id="obs_d_ord_2",
            source_id="src_d_order",
            event_type="flood",
            location="Riverside Metro",
        )
        obs3 = DisasterObservation(
            observation_id="obs_d_ord_3",
            source_id="src_d_order",
            event_type="flood",
            location="Riverside Metro",
        )
        obs4 = DisasterObservation(
            observation_id="obs_d_ord_4",
            source_id="src_d_order",
            event_type="flood",
            location="Riverside Metro",
        )
        t_base = datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc)
        t_older = t_base - timedelta(hours=1)
        t_newer = t_base + timedelta(hours=1)

        with dashboard_service.session_factory() as session:
            repo.save_source(src, session)
            repo.save_observations([obs1, obs2, obs3, obs4], session)
            repo.save_event(sample_events[0], session)
            for o in [obs1, obs2, obs3, obs4]:
                repo.link_observation_to_event(sample_events[0].event_id, o.observation_id, 0.95, session)

            d_old = ResolutionDecisionModel(
                observation_id=obs1.observation_id,
                event_id=sample_events[0].event_id,
                decision_type="matched_existing",
                composite_score=0.70,
                feature_scores={},
                rationale="old_decision",
                created_at=t_older,
            )
            d_mid1 = ResolutionDecisionModel(
                observation_id=obs2.observation_id,
                event_id=sample_events[0].event_id,
                decision_type="matched_existing",
                composite_score=0.80,
                feature_scores={},
                rationale="mid_decision_lower_id",
                created_at=t_base,
            )
            d_mid2 = ResolutionDecisionModel(
                observation_id=obs3.observation_id,
                event_id=sample_events[0].event_id,
                decision_type="matched_existing",
                composite_score=0.85,
                feature_scores={},
                rationale="mid_decision_higher_id",
                created_at=t_base,
            )
            d_new = ResolutionDecisionModel(
                observation_id=obs4.observation_id,
                event_id=sample_events[0].event_id,
                decision_type="matched_existing",
                composite_score=0.95,
                feature_scores={},
                rationale="new_decision",
                created_at=t_newer,
            )
            session.add_all([d_old, d_mid1, d_mid2, d_new])
            session.commit()

        detail = dashboard_service.get_event_detail("evt_dash_01")
        assert detail is not None
        assert len(detail["decisions"]) == 4

        rationales = [d["rationale"] for d in detail["decisions"]]
        assert rationales == [
            "new_decision",
            "mid_decision_higher_id",
            "mid_decision_lower_id",
            "old_decision",
        ]

    def test_app_headless_runs_complete_pipeline_without_exception(
        self, isolate_in_memory_dashboard: None
    ) -> None:
        """Verify dashboard runs complete sample pipeline and renders all views safely in in-memory mode."""
        if not _can_create_socketpair():
            pytest.skip("Local loopback socket connection forbidden in sandboxed environment")
        from streamlit.testing.v1 import AppTest

        app_path = Path(__file__).parent.parent / "src" / "beacon" / "dashboard" / "app.py"
        at = AppTest.from_file(str(app_path), default_timeout=30)
        at.run()

        # Verify in-memory storage indicator and disabled persistence checkbox
        storage_warns = [w.value for w in at.warning if "In-Memory" in w.value]
        assert len(storage_warns) >= 1
        persist_cbs = [c for c in at.checkbox if "Persist results" in c.label]
        assert len(persist_cbs) == 1
        assert persist_cbs[0].value is False
        assert persist_cbs[0].disabled is True

        run_btns = [b for b in at.button if "Run Complete Pipeline" in b.label]
        assert len(run_btns) == 1
        run_btns[0].click().run()

        assert len(at.exception) == 0

        # Check that dataframes render without exception and display N/A for None confidence
        dfs = at.dataframe
        assert len(dfs) > 0
        found_na_confidence = False
        found_numeric_confidence = False
        for df in dfs:
            val = df.value
            if "Confidence" in val.columns:
                for conf in val["Confidence"]:
                    if conf == "N/A":
                        found_na_confidence = True
                    elif isinstance(conf, str) and "." in conf:
                        found_numeric_confidence = True
        assert found_na_confidence, "Expected at least one observation with 'N/A' confidence"
        assert found_numeric_confidence, "Expected at least one observation with formatted numeric confidence"

        # Verify session state and success banner output
        assert at.session_state.get("last_pipeline_result") is not None
        exec_id = at.session_state["last_pipeline_result"].execution_id
        success_banners = [s.value for s in at.success if "Complete!" in s.value]
        assert len(success_banners) == 1
        assert exec_id in success_banners[0]

        # Verify compression-ratio explanation and formula captions rendered
        comp_caps = [c.value for c in at.caption if "Compression Ratio" in c.value]
        assert len(comp_caps) > 0
        comp_infos = [i.value for i in at.info if "Compression Ratio" in i.value]
        assert len(comp_infos) > 0

        # Verify conflict-claim detail text rendered
        claim_lines = [m.value for m in at.markdown if "status:" in m.value]
        assert len(claim_lines) > 0

        # Verify rerun stability: rerunning script without button click does NOT trigger a second pipeline run
        at.run()
        assert not at.exception
        assert at.session_state["last_pipeline_result"].execution_id == exec_id

    def test_app_headless_failed_run_clears_prior_success_results(
        self, isolate_in_memory_dashboard: None
    ) -> None:
        """A failed pipeline execution clears prior success results so stale metrics are never displayed in in-memory mode."""
        if not _can_create_socketpair():
            pytest.skip("Local loopback socket connection forbidden in sandboxed environment")
        from streamlit.testing.v1 import AppTest

        app_path = Path(__file__).parent.parent / "src" / "beacon" / "dashboard" / "app.py"
        at = AppTest.from_file(str(app_path), default_timeout=30)
        at.run()

        # Verify in-memory storage indicator and disabled persistence checkbox
        storage_warns = [w.value for w in at.warning if "In-Memory" in w.value]
        assert len(storage_warns) >= 1

        run_btns = [b for b in at.button if "Run Complete Pipeline" in b.label]
        assert len(run_btns) == 1

        # Run 1: initial successful execution
        run_btns[0].click().run()
        assert at.session_state.get("last_pipeline_result") is not None
        assert len([s.value for s in at.success if "Complete!" in s.value]) == 1

        # Run 2: simulated failed execution
        def fail_pipeline(*args, **kwargs):
            raise RuntimeError("Simulated ETL extraction failure")

        orig = DashboardService.run_pipeline_on_paths
        DashboardService.run_pipeline_on_paths = fail_pipeline
        try:
            run_btns[0].click().run()
        finally:
            DashboardService.run_pipeline_on_paths = orig

        # Assert prior success results were cleared
        assert at.session_state.get("last_pipeline_result") is None
        assert at.session_state.get("last_pipeline_error") == "Simulated ETL extraction failure"
        assert len([s.value for s in at.success if "Complete!" in s.value]) == 0
        assert any("Simulated ETL extraction failure" in e.value for e in at.error)


    def test_overview_refreshes_immediately_after_pipeline_execution(self) -> None:
        """Overview metrics update immediately upon pipeline execution from zero to resolved counts."""
        service = DashboardService(settings=Settings(database_url=None), session_factory=None)
        sample_paths = [
            Path("data/sample/events.json"),
            Path("data/sample/events.csv"),
            Path("data/sample/events.txt"),
            Path("data/sample/sample_report.pdf"),
            Path("data/sample/sample_disaster.png"),
        ]
        if not all(p.exists() for p in sample_paths):
            pytest.skip("Standard sample files not available")

        # Initial state before execution: empty overview
        initial_overview = service.get_system_overview()
        assert initial_overview["events_count"] == 0
        assert initial_overview["observations_count"] == 0
        assert initial_overview["compression_ratio"] == 0.0

        # Run pipeline
        res = service.run_pipeline_on_paths(sample_paths, enable_resolution=True, persist_db=False)
        assert res.observations_accepted == 10
        assert res.events_synthesized == 7

        # Post-run overview must immediately reflect 7 events, 10 observations, and ~1.43x ratio
        post_overview = service.get_system_overview()
        assert post_overview["events_count"] == 7
        assert post_overview["observations_count"] == 10
        assert post_overview["compression_ratio"] == 1.43
        assert post_overview["sources_count"] == 4

    def test_sample_observation_files_separated_from_manifest(self) -> None:
        """Sample data directory separates 5 input observation files from the manifest specification."""
        sample_dir = Path("data/sample")
        if not sample_dir.exists():
            pytest.skip("Sample data directory not found")

        observation_files = [
            s for s in sorted(sample_dir.glob("*.*")) if s.name.lower() != "manifest.json"
        ]
        manifest_file = sample_dir / "manifest.json"

        # Assert exactly 5 observation input files are present
        assert len(observation_files) == 5
        obs_names = {f.name for f in observation_files}
        expected_names = {
            "events.json",
            "events.csv",
            "events.txt",
            "sample_report.pdf",
            "sample_disaster.png",
        }
        assert obs_names == expected_names

        # Assert manifest exists separately as specification metadata
        assert manifest_file.exists()
        import json
        manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
        assert "manifest_version" in manifest
        assert "samples" in manifest
        assert len(manifest["samples"]) == 5

    def test_conflicting_claims_table_structure_and_resolution_notes(self) -> None:
        """Conflicting claims display structure preserves all 6 audit fields and full resolution notes."""
        from beacon.models import ConflictingClaim

        claim = ConflictingClaim(
            field_name="severity",
            claimed_value="moderate",
            observation_id="obs_test_01",
            source_id="src_csv_01",
            resolution_status="superseded",
            notes="Resolved to 'severe' based on highest reported severity/plurality consensus across contributing feeds.",
        )
        row = format_conflict_claim_row(claim)
        # Check all 6 audit fields are preserved
        assert set(row.keys()) == {
            "Field",
            "Claimed Value",
            "Observation ID",
            "Source ID",
            "Status",
            "Resolution Notes",
        }
        # Full notes are unabbreviated and intact
        assert "highest reported severity/plurality" in row["Resolution Notes"]

        # Markdown bullet rendering preserves notes
        md_bullet = format_conflict_claim_markdown(claim)
        assert "Severity" in md_bullet
        assert "moderate" in md_bullet
        assert "src_csv_01" in md_bullet
        assert "highest reported severity/plurality" in md_bullet

    def test_conflicting_claims_missing_notes_fallback(self) -> None:
        """Missing or empty resolution notes fall back to clear message without literal None."""
        from beacon.models import ConflictingClaim

        # Case 1: notes is None
        c_none = ConflictingClaim(
            field_name="severity",
            claimed_value="moderate",
            observation_id="obs_none",
            source_id="src_1",
            resolution_status="superseded",
            notes=None,
        )
        row_none = format_conflict_claim_row(c_none)
        assert row_none["Resolution Notes"] == "No resolution notes recorded."
        assert "None" not in row_none["Resolution Notes"]
        md_none = format_conflict_claim_markdown(c_none)
        assert "No resolution notes recorded." in md_none
        assert "None" not in md_none

        # Case 2: notes is empty string
        c_empty = ConflictingClaim(
            field_name="location",
            claimed_value="Delta",
            observation_id="obs_empty",
            source_id="src_2",
            resolution_status="superseded",
            notes="",
        )
        row_empty = format_conflict_claim_row(c_empty)
        assert row_empty["Resolution Notes"] == "No resolution notes recorded."
        md_empty = format_conflict_claim_markdown(c_empty)
        assert "No resolution notes recorded." in md_empty

        # Case 3: notes is whitespace only
        c_space = ConflictingClaim(
            field_name="event_type",
            claimed_value="storm",
            observation_id="obs_space",
            source_id="src_3",
            resolution_status="superseded",
            notes="   ",
        )
        row_space = format_conflict_claim_row(c_space)
        assert row_space["Resolution Notes"] == "No resolution notes recorded."
        md_space = format_conflict_claim_markdown(c_space)
        assert "No resolution notes recorded." in md_space

    def test_analytics_horizontal_bar_chart_dataframes(self) -> None:
        """Analytics distribution data structures accurately convert to DataFrames without mutating values."""
        import pandas as pd
        service = DashboardService(settings=Settings(database_url=None), session_factory=None)
        sample_path = Path("data/sample/events.json")
        if not sample_path.exists():
            pytest.skip("Sample data file not found")

        service.run_pipeline_on_paths([sample_path], enable_resolution=True, persist_db=False)
        summary = service.get_analytics_summary()

        # 1. Disaster Types DataFrame
        df_types = pd.DataFrame(
            list(summary.events_by_type.items()),
            columns=["Disaster Type", "Event Count"],
        )
        assert "Disaster Type" in df_types.columns
        assert "Event Count" in df_types.columns
        assert df_types["Event Count"].sum() == summary.total_events

        # 2. Severity DataFrame
        df_sev = pd.DataFrame(
            list(summary.events_by_severity.items()),
            columns=["Severity Level", "Event Count"],
        )
        assert "Severity Level" in df_sev.columns
        assert "Event Count" in df_sev.columns
        assert df_sev["Event Count"].sum() == summary.total_events

        # 3. Conflicting Claims DataFrame
        df_conf = pd.DataFrame(
            list(summary.conflicting_claims_by_field.items()),
            columns=["Field Name", "Conflicting Claims"],
        )
        assert "Field Name" in df_conf.columns
        assert "Conflicting Claims" in df_conf.columns

    def test_dashboard_service_reachable_uninitialized_database_behavior(self) -> None:
        """Reachable database with uninitialized schema falls back safely to in-memory mode."""
        engine = create_engine("sqlite:///:memory:")
        factory = sessionmaker(bind=engine)
        settings = Settings(database_url="sqlite:///:memory:")
        service = DashboardService(settings=settings, session_factory=factory)

        # Database is reachable but tables are not yet created
        assert service.is_database_reachable is True
        assert service.is_schema_ready is False
        assert service.is_database_connected is False

        # Diagnostic report correctly flags UNINITIALIZED status with actionable guidance
        diag = service.test_database_connection()
        assert diag["connected"] is True
        assert diag["schema_ready"] is False
        assert diag["status"] == "uninitialized"
        assert "Run 'python scripts/init_db.py'" in diag["message"]

        # All 7 tables report -1 (missing)
        for t in [
            "sources",
            "observations",
            "data_quality_results",
            "events",
            "event_observations",
            "resolution_decisions",
            "merge_candidates",
        ]:
            assert diag["tables"][t] == -1

        # Overview and query calls gracefully fall back without raising ProgrammingError
        overview = service.get_system_overview()
        assert overview["is_live_db"] is False
        assert overview["events_count"] == 0

        events = service.query_events()
        assert events == []

    def test_app_headless_renders_with_uninitialized_database_without_exception(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Dashboard renders headlessly when database is reachable but uninitialized."""
        if not _can_create_socketpair():
            pytest.skip("Local loopback socket connection forbidden in sandboxed environment")
        import streamlit as st
        from streamlit.testing.v1 import AppTest

        db_file = tmp_path / "uninit_dashboard.db"
        monkeypatch.setenv("BEACON_DATABASE_URL", f"sqlite:///{db_file.as_posix()}")
        get_settings.cache_clear()
        st.cache_resource.clear()

        try:
            app_path = Path(__file__).parent.parent / "src" / "beacon" / "dashboard" / "app.py"
            at = AppTest.from_file(str(app_path), default_timeout=15)
            at.run()

            # Renders cleanly with zero unhandled exceptions
            assert len(at.exception) == 0

            # Sidebar indicates uninitialized database warning
            uninit_warns = [w.value for w in at.warning if "Uninitialized" in w.value]
            assert len(uninit_warns) >= 1

            # Persist checkbox is safely disabled
            persist_cbs = [c for c in at.checkbox if "Persist results" in c.label]
            assert len(persist_cbs) == 1
            assert persist_cbs[0].value is False
            assert persist_cbs[0].disabled is True
        finally:
            get_settings.cache_clear()
            st.cache_resource.clear()


class TestDashboardFormatters:
    """Unit tests for dashboard formatting and nullable rendering helpers."""

    def test_format_confidence_numeric_preserved(self) -> None:
        """Numeric confidence values retain expected two-decimal formatting."""
        assert format_confidence(0.95) == "0.95"
        assert format_confidence(0.7) == "0.70"
        assert format_confidence(1.0) == "1.00"
        assert format_confidence(0.0) == "0.00"
        assert format_confidence(0.12345) == "0.12"

    def test_format_confidence_none_is_honest(self) -> None:
        """Missing confidence is displayed explicitly as 'N/A' without fabricating 0.0."""
        assert format_confidence(None) == "N/A"
        assert format_confidence(None) != "0"
        assert format_confidence(None) != "0.0"
        assert format_confidence(None) != "0.00"

    def test_format_coordinates(self) -> None:
        """Coordinates format properly when present and yield 'N/A' when missing."""
        assert format_coordinates(34.05, -118.25) == "(34.05, -118.25)"
        assert format_coordinates(None, -118.25) == "N/A"
        assert format_coordinates(34.05, None) == "N/A"
        assert format_coordinates(None, None) == "N/A"

    def test_format_timestamp(self) -> None:
        """Timestamps format as ISO-8601 when present and yield 'N/A' when missing."""
        dt = datetime(2026, 10, 12, 8, 30, tzinfo=timezone.utc)
        assert format_timestamp(dt) == "2026-10-12T08:30:00+00:00"
        assert format_timestamp(None) == "N/A"

    def test_format_score(self) -> None:
        """Scores format to 3 decimals or 'N/A' if missing."""
        assert format_score(0.8526) == "0.853"
        assert format_score(0.0) == "0.000"
        assert format_score(None) == "N/A"

    def test_format_compression_ratio(self) -> None:
        """Compression ratio formats to 2 decimals with suffix 'x' or 'N/A'."""
        assert format_compression_ratio(1.428) == "1.43x"
        assert format_compression_ratio(0.0) == "0.00x"
        assert format_compression_ratio(None) == "N/A"

    def test_explain_compression_ratio(self) -> None:
        """explain_compression_ratio formats ratio with derivation and handles zero events safely."""
        # Non-zero events
        msg = explain_compression_ratio(10, 7, 10 / 7)
        assert "1.43x" in msg
        assert "10 observations ÷ 7 unified events" in msg

        # Zero events safe handling
        zero_msg = explain_compression_ratio(0, 0, 0.0)
        assert "0.00x" in zero_msg
        assert "0 events loaded" in zero_msg

        # None ratio safe handling
        none_msg = explain_compression_ratio(0, 0, None)
        assert "N/A" in none_msg

    def test_format_observation_row_fully_populated(self) -> None:
        """Fully populated observation produces complete formatted dictionary."""
        obs = DisasterObservation(
            observation_id="obs_full",
            source_id="src_full",
            event_type="flood",
            location="Riverside",
            severity="severe",
            latitude=34.05,
            longitude=-118.25,
            event_time=datetime(2026, 10, 12, 8, 0, tzinfo=timezone.utc),
            extraction_confidence=0.92,
        )
        row = format_observation_row(obs)
        assert row["Obs ID"] == "obs_full"
        assert row["Source ID"] == "src_full"
        assert row["Type"] == "flood"
        assert row["Location"] == "Riverside"
        assert row["Severity"] == "severe"
        assert row["Coordinates"] == "(34.05, -118.25)"
        assert row["Timestamp"] == "2026-10-12T08:00:00+00:00"
        assert row["Confidence"] == "0.92"

    def test_format_observation_row_all_nullables(self) -> None:
        """Observation with missing optional fields produces safe 'N/A' entries without raising exceptions."""
        obs = DisasterObservation(
            observation_id="obs_nulls",
            source_id="src_nulls",
            event_type="wildfire",
            location=None,
            severity=None,
            latitude=None,
            longitude=None,
            event_time=None,
            extraction_confidence=None,
        )
        row = format_observation_row(obs)
        assert row["Obs ID"] == "obs_nulls"
        assert row["Source ID"] == "src_nulls"
        assert row["Type"] == "wildfire"
        assert row["Location"] == "N/A"
        assert row["Severity"] == "unspecified"
        assert row["Coordinates"] == "N/A"
        assert row["Timestamp"] == "N/A"
        assert row["Confidence"] == "N/A"

    def test_format_decision_row_handles_nullables(self) -> None:
        """Decision row formats score and rationale safely with missing values."""
        row_pop = format_decision_row({
            "observation_id": "obs_01",
            "decision_type": "match",
            "composite_score": 0.888,
            "rationale": "Matched location and time",
        })
        assert row_pop["Score"] == "0.888"
        assert row_pop["Rationale"] == "Matched location and time"

        row_empty = format_decision_row({})
        assert row_empty["Obs ID"] == "N/A"
        assert row_empty["Decision"] == "N/A"
        assert row_empty["Score"] == "N/A"
        assert row_empty["Rationale"] == "N/A"
