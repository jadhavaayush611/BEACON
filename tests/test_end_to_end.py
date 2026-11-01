"""End-to-End (E2E) integration test suite for BEACON v1.0 MVP.

Validates the full pipeline flow:
Synthetic heterogeneous inputs (JSON, CSV, TXT, PDF, PNG)
  ➔ Multi-format Ingestion
  ➔ Data Normalization (Transform)
  ➔ Quality Validation
  ➔ Event Resolution & Synthesis
  ➔ Database Persistence (SQLite / PostgreSQL)
  ➔ Analytics Aggregation
  ➔ Dashboard Query Services
"""

import json
from pathlib import Path
from PIL import Image
import pytest
from pypdf import PdfWriter
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from beacon.analytics.models import AnalyticsFilter
from beacon.analytics.service import AnalyticsService
from beacon.config import Settings
from beacon.dashboard.services import DashboardService
from beacon.pipeline.runner import ETLRunner
from beacon.storage import (
    StorageRepository,
    drop_db,
    init_db,
)


@pytest.fixture
def synthetic_data_dir(tmp_path: Path) -> Path:
    """Generate deterministic synthetic multi-format disaster files in all five formats."""
    # 1. JSON Telemetry (Flood event)
    json_path = tmp_path / "stream.json"
    json_path.write_text(
        json.dumps([
            {
                "event_type": "flood",
                "location": "Bayview District",
                "event_time": "2026-06-15T08:00:00Z",
                "severity": "severe",
                "latitude": 37.7749,
                "longitude": -122.4194,
                "confidence": 0.95,
            }
        ]),
        encoding="utf-8",
    )

    # 2. CSV Sensor Data (Corroborating flood with conflicting severity)
    csv_path = tmp_path / "sensor.csv"
    csv_path.write_text(
        "event_type,location,event_time,severity,latitude,longitude,confidence\n"
        "flood,Bayview District,2026-06-15T09:30:00Z,moderate,37.7752,-122.4198,0.85\n",
        encoding="utf-8",
    )

    # 3. TXT Situation Dispatch
    txt_path = tmp_path / "dispatch.txt"
    txt_path.write_text(
        "[REPORT 2026-06-15-001]\n"
        "TIMESTAMP: 2026-06-15T11:00:00Z\n"
        "LOCATION: Bayview District\n"
        "EVENT: Flood\n"
        "SEVERITY: Severe\n"
        "NARRATIVE: Rising flood water in Bayview industrial sector.\n",
        encoding="utf-8",
    )

    # 4. Valid PDF Report
    pdf_path = tmp_path / "report.pdf"
    writer = PdfWriter()
    writer.add_blank_page(width=72, height=72)
    with open(pdf_path, "wb") as f:
        writer.write(f)

    # 5. Valid Image File
    img_path = tmp_path / "aerial.png"
    img = Image.new("RGB", (64, 64), color=(100, 150, 200))
    img.save(img_path)

    return tmp_path


@pytest.fixture
def db_session_factory():
    """Create a temporary database session factory for end-to-end persistence testing."""
    engine = create_engine("sqlite:///:memory:")
    init_db(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    yield factory
    drop_db(engine)


class TestEndToEndPipeline:
    """Comprehensive E2E tests validating the complete data engineering pipeline."""

    def test_e2e_multi_format_ingest_to_dashboard(
        self, synthetic_data_dir: Path, db_session_factory: sessionmaker
    ) -> None:
        """Execute full workflow from raw files to storage, analytics, and dashboard services."""
        files = [
            synthetic_data_dir / "stream.json",
            synthetic_data_dir / "sensor.csv",
            synthetic_data_dir / "dispatch.txt",
            synthetic_data_dir / "report.pdf",
            synthetic_data_dir / "aerial.png",
        ]

        # 1. Run Pipeline with Event Resolution and Database Persistence
        runner = ETLRunner(session_factory=db_session_factory)
        result = runner.run_complete_pipeline(files)

        # Assert Pipeline Result metrics
        assert result.files_discovered == 5
        assert result.files_succeeded >= 4  # JSON, CSV, TXT, PNG succeed
        assert result.observations_extracted >= 3
        assert result.observations_accepted >= 3
        assert result.records_persisted >= 3
        assert result.events_synthesized >= 1
        assert result.events_persisted >= 1

        # 2. Verify Storage State
        with db_session_factory() as session:
            repo = StorageRepository()
            stored_events = repo.list_events(session)
            assert len(stored_events) >= 1

            # Find the Bayview flood event
            bayview_ev = next(
                (e for e in stored_events if "bayview" in e.location.lower()),
                None,
            )
            assert bayview_ev is not None
            assert bayview_ev.event_type == "flood"
            # Multi-source corroboration: JSON + CSV + TXT
            assert len(bayview_ev.source_ids) >= 3
            assert len(bayview_ev.observation_ids) >= 3

            # Conflicting claim check: CSV reported 'moderate' vs JSON 'severe'
            severity_conflicts = [
                c for c in bayview_ev.conflicting_claims if c.field_name == "severity"
            ]
            assert len(severity_conflicts) >= 1
            assert any(c.claimed_value == "moderate" for c in severity_conflicts)

        # 3. Verify Database-Backed Analytics
        analytics = AnalyticsService()
        with db_session_factory() as session:
            summary = analytics.summarize_db(session)
            assert summary.total_events >= 1
            assert summary.total_observations >= 3
            assert summary.compression_ratio >= 1.0
            assert summary.multi_source_events >= 1
            assert summary.conflicting_claims_count >= 1

        # 4. Verify Dashboard Service Integration
        dashboard = DashboardService(
            settings=Settings(database_url="sqlite:///:memory:"),
            session_factory=db_session_factory,
        )
        overview = dashboard.get_system_overview()
        assert overview["events_count"] >= 1
        assert overview["observations_count"] >= 3
        assert overview["multi_source_events_count"] >= 1
        assert overview["conflicting_claims_count"] >= 1

        # Query events through dashboard service
        filtered = dashboard.query_events(AnalyticsFilter(event_type="flood"))
        assert len(filtered) >= 1
        assert filtered[0].event_type == "flood"

        # Event detail drilldown
        detail = dashboard.get_event_detail(bayview_ev.event_id)
        assert detail is not None
        assert detail["event"].event_id == bayview_ev.event_id
        assert len(detail["observations"]) >= 3

    def test_e2e_partial_failure_isolation(
        self, tmp_path: Path, db_session_factory: sessionmaker
    ) -> None:
        """Partial failure in one file isolates cleanly and does not halt valid file processing."""
        valid_json = tmp_path / "valid.json"
        valid_json.write_text(
            json.dumps([{"event_type": "flood", "location": "North Basin"}]),
            encoding="utf-8",
        )

        malformed_json = tmp_path / "malformed.json"
        malformed_json.write_text("NOT_VALID_JSON{invalid", encoding="utf-8")

        unsupported_file = tmp_path / "unknown.bin"
        unsupported_file.write_bytes(b"\x00\x01\x02\x03")

        runner = ETLRunner(session_factory=db_session_factory)
        result = runner.run_complete_pipeline([valid_json, malformed_json, unsupported_file])

        assert result.files_discovered == 3
        assert result.files_succeeded == 1
        assert result.files_failed == 2
        assert result.observations_accepted >= 1
        assert len(result.errors) >= 1

        # Valid file was still committed to storage
        with db_session_factory() as session:
            repo = StorageRepository()
            stored = repo.list_events(session)
            assert len(stored) >= 1

    def test_e2e_rerun_idempotency(
        self, synthetic_data_dir: Path, db_session_factory: sessionmaker
    ) -> None:
        """Re-running the pipeline over identical files is idempotent with zero record duplication."""
        target_files = [
            synthetic_data_dir / "stream.json",
            synthetic_data_dir / "sensor.csv",
        ]
        runner = ETLRunner(session_factory=db_session_factory)

        # Run 1
        res1 = runner.run_complete_pipeline(target_files)
        assert res1.files_succeeded == 2
        initial_events_count = res1.events_persisted

        with db_session_factory() as session:
            repo = StorageRepository()
            count_ev_1 = len(repo.list_events(session))
        assert count_ev_1 == initial_events_count

        # Run 2 (re-run over exact same files)
        res2 = runner.run_complete_pipeline(target_files)
        assert res2.files_succeeded == 2

        with db_session_factory() as session:
            repo = StorageRepository()
            count_ev_2 = len(repo.list_events(session))

        # Event count must remain identical (no duplicated ghost events)
        assert count_ev_1 == count_ev_2

    def test_e2e_repeat_pipeline_idempotency_all_sample_formats(
        self, db_session_factory: sessionmaker
    ) -> None:
        """Verify repeated execution over all 5 heterogeneous sample files produces stable records.

        Asserts that observations, events, and event-observation link counts remain strictly
        identical without ghost duplication across multiple identical runs.
        """
        sample_files = [
            Path("data/sample/events.json"),
            Path("data/sample/events.csv"),
            Path("data/sample/events.txt"),
            Path("data/sample/sample_report.pdf"),
            Path("data/sample/sample_disaster.png"),
        ]
        if not all(p.exists() for p in sample_files):
            pytest.skip("Standard sample files not available")

        from sqlalchemy import select
        from beacon.storage import EventModel, EventObservationModel, ObservationModel

        runner = ETLRunner(session_factory=db_session_factory)

        # Run 1: initial ingestion & resolution
        res1 = runner.run_complete_pipeline(sample_files)
        assert res1.files_succeeded == 5
        assert res1.observations_accepted == 11
        assert res1.events_synthesized == 7

        with db_session_factory() as session:
            obs_1 = sorted(session.execute(select(ObservationModel.observation_id)).scalars().all())
            ev_1 = sorted(session.execute(select(EventModel.event_id, EventModel.start_time)).all())
            links_1 = sorted(session.execute(select(EventObservationModel.event_id, EventObservationModel.observation_id)).all())

        assert len(obs_1) == 11
        assert len(ev_1) == 7
        assert len(links_1) == 10

        # Run 2: repeat execution with exact same files
        res2 = runner.run_complete_pipeline(sample_files)
        assert res2.files_succeeded == 5

        with db_session_factory() as session:
            obs_2 = sorted(session.execute(select(ObservationModel.observation_id)).scalars().all())
            ev_2 = sorted(session.execute(select(EventModel.event_id, EventModel.start_time)).all())
            links_2 = sorted(session.execute(select(EventObservationModel.event_id, EventObservationModel.observation_id)).all())

        assert len(obs_2) == 11
        assert len(ev_2) == 7
        assert len(links_2) == 10
        # Assert complete record identity stability
        assert obs_2 == obs_1
        assert ev_2 == ev_1
        assert links_2 == links_1

        # Run 3: third execution to confirm sustained idempotency
        runner.run_complete_pipeline(sample_files)
        with db_session_factory() as session:
            obs_3 = sorted(session.execute(select(ObservationModel.observation_id)).scalars().all())
            ev_3 = sorted(session.execute(select(EventModel.event_id, EventModel.start_time)).all())
            links_3 = sorted(session.execute(select(EventObservationModel.event_id, EventObservationModel.observation_id)).all())

        assert len(obs_3) == 11
        assert len(ev_3) == 7
        assert len(links_3) == 10
        assert obs_3 == obs_1
        assert ev_3 == ev_1
        assert links_3 == links_1

    def test_e2e_in_memory_repeat_pipeline_idempotency_all_sample_formats(self) -> None:
        """Verify repeated execution in in-memory mode preserves identical counts across all 5 files."""
        sample_files = [
            Path("data/sample/events.json"),
            Path("data/sample/events.csv"),
            Path("data/sample/events.txt"),
            Path("data/sample/sample_report.pdf"),
            Path("data/sample/sample_disaster.png"),
        ]
        if not all(p.exists() for p in sample_files):
            pytest.skip("Standard sample files not available")

        service = DashboardService(settings=Settings(database_url=None), session_factory=None)

        # Run 1
        service.run_pipeline_on_paths(sample_files, enable_resolution=True, persist_db=False)
        obs_1 = len(service._in_memory_observations)
        ev_1 = len(service._in_memory_events)
        assert obs_1 == 11
        assert ev_1 == 7

        # Run 2
        service.run_pipeline_on_paths(sample_files, enable_resolution=True, persist_db=False)
        obs_2 = len(service._in_memory_observations)
        ev_2 = len(service._in_memory_events)
        assert obs_2 == 11
        assert ev_2 == 7

        # Run 3
        service.run_pipeline_on_paths(sample_files, enable_resolution=True, persist_db=False)
        obs_3 = len(service._in_memory_observations)
        ev_3 = len(service._in_memory_events)
        assert obs_3 == 11
        assert ev_3 == 7
class TestPhase2GAcceptanceWorkflow:
    """Rigorous Phase 2G acceptance gate tests covering end-to-end integration."""

    def test_phase2g_multi_format_ingestion_with_jpeg_and_ocr_boundary(
        self, tmp_path: Path, db_session_factory: sessionmaker
    ) -> None:
        """Verify all supported formats (JSON, CSV, TXT, PDF, PNG, JPEG) ingest with OCR boundary respected."""
        # 1. JSON
        json_file = tmp_path / "feed.json"
        json_file.write_text(
            json.dumps([
                {
                    "event_type": "flood",
                    "location": "Delta Sector",
                    "event_time": "2026-10-29T12:00:00Z",
                    "severity": "severe",
                    "latitude": 29.9792,
                    "longitude": 31.1342,
                    "confidence": 0.92,
                }
            ]),
            encoding="utf-8",
        )

        # 2. CSV
        csv_file = tmp_path / "telemetry.csv"
        csv_file.write_text(
            "event_type,location,event_time,severity,latitude,longitude,confidence\n"
            "flood,Delta Sector,2026-10-29T12:15:00Z,severe,29.9795,31.1345,0.88\n",
            encoding="utf-8",
        )

        # 3. TXT
        txt_file = tmp_path / "dispatch.txt"
        txt_file.write_text(
            "[REPORT 2026-10-29-D01]\n"
            "TIMESTAMP: 2026-10-29T12:30:00Z\n"
            "LOCATION: Delta Sector\n"
            "EVENT: Flood\n"
            "SEVERITY: Severe\n"
            "NARRATIVE: Rising flood waters reported along primary canal.\n",
            encoding="utf-8",
        )

        # 4. PDF
        pdf_file = tmp_path / "brief.pdf"
        writer = PdfWriter()
        writer.add_blank_page(width=72, height=72)
        with open(pdf_file, "wb") as f:
            writer.write(f)

        # 5. PNG Image
        png_file = tmp_path / "satellite.png"
        img_png = Image.new("RGB", (128, 128), color=(50, 100, 150))
        img_png.save(png_file, format="PNG")

        # 6. JPEG Image
        jpeg_file = tmp_path / "recon.jpeg"
        img_jpeg = Image.new("RGB", (256, 256), color=(200, 120, 80))
        img_jpeg.save(jpeg_file, format="JPEG")

        all_fixtures = [json_file, csv_file, txt_file, pdf_file, png_file, jpeg_file]

        runner = ETLRunner(session_factory=db_session_factory)
        result = runner.run_complete_pipeline(all_fixtures)

        assert result.files_discovered == 6
        assert result.files_succeeded == 6
        assert result.files_failed == 0

        # Assert image validation integrity without OCR fabrication
        img_results = [r for r in result.file_results if r.source_record.source_type.value == "image"]
        assert len(img_results) == 2
        for img_res in img_results:
            assert img_res.status.value == "success"
            assert img_res.processed_count == 1  # 1 image context observation
            assert len(img_res.observations) == 1
            assert img_res.observations[0].observation_type.value == "image_context"
            assert not img_res.observations[0].is_event_eligible
            # Explicit OCR deferral notice in warnings
            assert any("deferred to OCR/computer-vision" in w for w in img_res.warnings)
            meta = img_res.source_record.metadata
            assert meta["width"] in (128, 256)
            assert meta["color_mode"] == "RGB"

        # Assert observations extracted and resolved from text/tabular sources
        assert result.observations_accepted >= 3
        assert result.events_synthesized >= 1

    def test_phase2g_ambiguous_observation_quarantined_and_manual_resolution_audit(
        self, db_session_factory: sessionmaker
    ) -> None:
        """Verify ambiguous observations remain unassigned and manual resolution preserves audit history."""
        from datetime import datetime, timezone
        from beacon.models import DisasterObservation, UnifiedDisasterEvent
        from beacon.resolution.models import DecisionType, FeatureScores, ResolutionOutcome

        with db_session_factory() as session:
            repo = StorageRepository()

            # 1. Existing Event
            now = datetime.now(timezone.utc)
            ev = UnifiedDisasterEvent(
                event_id="ev_delta_001",
                event_type="wildfire",
                location="North Ridge Sector",
                start_time=datetime(2026, 10, 29, 8, 0, tzinfo=timezone.utc),
                end_time=datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc),
                summary="North Ridge Fire incident report.",
                severity="severe",
                source_ids=["src_alpha"],
                observation_ids=["obs_seed_001"],
                created_at=now,
                updated_at=now,
            )
            repo.save_event(ev, session)

            # Seed observation linked to event
            obs_seed = DisasterObservation(
                observation_id="obs_seed_001",
                source_id="src_alpha",
                event_type="wildfire",
                location="North Ridge Sector",
                event_time=datetime(2026, 10, 29, 8, 0, tzinfo=timezone.utc),
                latitude=34.2000,
                longitude=-118.5000,
                severity="severe",
            )
            repo.save_observations([obs_seed], session)
            repo.link_observation_to_event("ev_delta_001", "obs_seed_001", 1.0, session)

            # 2. Ingest ambiguous observation (location slightly offset, moderate score)
            obs_ambig = DisasterObservation(
                observation_id="obs_ambig_999",
                source_id="src_beta",
                event_type="wildfire",
                location="North Ridge Upper Basin",
                event_time=datetime(2026, 10, 29, 9, 30, tzinfo=timezone.utc),
                latitude=34.2250,
                longitude=-118.5150,
                severity="moderate",
            )
            repo.save_observations([obs_ambig], session)

            # Record ambiguous resolution outcome: unassigned
            ambig_outcome = ResolutionOutcome(
                observation_id="obs_ambig_999",
                assigned_event_id=None,
                decision=DecisionType.AMBIGUOUS_CANDIDATE,
                score=0.62,
                is_new_event=False,
                rationale="Score 0.62 is within ambiguous range [0.45, 0.75). Quarantined for review.",
                feature_scores=FeatureScores(
                    event_type_score=1.0,
                    spatial_score=0.70,
                    temporal_score=0.85,
                    location_text_score=0.60,
                    description_score=0.50,
                    composite_score=0.62,
                ),
            )
            repo.save_resolution_decision(ambig_outcome, session)
            session.commit()

            # 3. Verify it is unassigned in quarantine
            unassigned_before = repo.get_unassigned_observations_with_decisions(session)
            unassigned_ids = [u["observation"].observation_id for u in unassigned_before]
            assert "obs_ambig_999" in unassigned_ids
            assert "obs_seed_001" not in unassigned_ids

            # 4. Operator manual resolution: link to existing event
            repo.link_observation_to_event("ev_delta_001", "obs_ambig_999", 0.95, session)
            manual_outcome = ResolutionOutcome(
                observation_id="obs_ambig_999",
                assigned_event_id="ev_delta_001",
                decision=DecisionType.CONFIDENT_MATCH,
                score=0.95,
                is_new_event=False,
                rationale="Operator confirmed North Ridge Upper Basin is constituent zone of ev_delta_001.",
            )
            repo.save_resolution_decision(manual_outcome, session)
            session.commit()

            # 5. Verify observation is now assigned and unassigned list is empty
            unassigned_after = repo.get_unassigned_observations_with_decisions(session)
            assert "obs_ambig_999" not in [u["observation"].observation_id for u in unassigned_after]

            # 6. Verify audit decision history is append-only (both decisions preserved)
            from sqlalchemy import select
            from beacon.storage import ResolutionDecisionModel
            decisions = session.execute(
                select(ResolutionDecisionModel)
                .where(ResolutionDecisionModel.observation_id == "obs_ambig_999")
                .order_by(ResolutionDecisionModel.created_at.asc())
            ).scalars().all()

            assert len(decisions) == 2
            assert decisions[0].decision_type == "ambiguous_candidate"
            assert decisions[0].event_id is None
            assert decisions[1].decision_type == "confident_match"
            assert decisions[1].event_id == "ev_delta_001"

    def test_phase2g_batch_error_attribution_and_partial_isolation(
        self, tmp_path: Path, db_session_factory: sessionmaker
    ) -> None:
        """Verify failures are attributed strictly to failed files while valid inputs succeed."""
        # 1. Valid JSON
        f_val_json = tmp_path / "good.json"
        f_val_json.write_text(
            json.dumps([{"event_type": "flood", "location": "Valid Pier"}]),
            encoding="utf-8",
        )

        # 2. Syntax-error JSON
        f_bad_json = tmp_path / "broken.json"
        f_bad_json.write_text("{invalid json", encoding="utf-8")

        # 3. Valid CSV
        f_val_csv = tmp_path / "good.csv"
        f_val_csv.write_text("event_type,location\nflood,Valid Pier\n", encoding="utf-8")

        # 4. Unsupported binary
        f_bad_bin = tmp_path / "unsupported.bin"
        f_bad_bin.write_bytes(b"\x00\xFF\x00")

        runner = ETLRunner(session_factory=db_session_factory)
        result = runner.run_complete_pipeline([f_val_json, f_bad_json, f_val_csv, f_bad_bin])

        assert result.files_discovered == 4
        assert result.files_succeeded == 2
        assert result.files_failed == 2
        assert result.records_persisted >= 2

        # Error attribution check
        error_files = [err for err in result.errors if "broken.json" in err or "unsupported.bin" in err]
        assert len(error_files) >= 2
        assert not any("good.json" in err for err in result.errors)
        assert not any("good.csv" in err for err in result.errors)

    def test_phase2g_pipeline_run_records_execution_duration_and_retry_linkage(
        self, tmp_path: Path, db_session_factory: sessionmaker
    ) -> None:
        """Verify run records capture monotonic duration, file attribution, and parent retry linkage."""
        good_file = tmp_path / "p2g_good.json"
        good_file.write_text(json.dumps([{"event_type": "earthquake", "location": "Fault Line"}]), encoding="utf-8")

        bad_file = tmp_path / "p2g_bad.json"
        bad_file.write_text("{bad syntax", encoding="utf-8")

        runner = ETLRunner(session_factory=db_session_factory)
        res_initial = runner.run_complete_pipeline([good_file, bad_file])

        assert res_initial.status == "partial"
        assert res_initial.duration_seconds > 0.0
        assert res_initial.files_succeeded == 1
        assert res_initial.files_failed == 1

        # Retrieve run record from storage
        with db_session_factory() as session:
            repo = StorageRepository()
            run_model = repo.get_pipeline_run(res_initial.execution_id, session)
            assert run_model is not None
            assert run_model.status == "partial"
            assert run_model.duration_seconds > 0.0
            assert run_model.retry_of_run_id is None
            assert len(run_model.input_files) == 2

            # Trigger retry of failed inputs only
            res_retry = runner.retry(
                failed_run_id=res_initial.execution_id,
                file_paths=[bad_file],
                failed_inputs_only=True,
            )

            assert res_retry.retry_of_run_id == res_initial.execution_id
            assert res_retry.trigger_type == "retry"

            retry_model = repo.get_pipeline_run(res_retry.execution_id, session)
            assert retry_model is not None
            assert retry_model.retry_of_run_id == res_initial.execution_id

    def test_e2e_repeat_partial_batch_seven_files_stability_and_metric_separation(
        self, db_session_factory: sessionmaker, tmp_path: Path
    ) -> None:
        """Verify 7-file partial batch run consecutively through ETLRunner produces stable metrics.

        Run 1: 5 succeeded, 2 failed. 7 observations accepted.
               events_created > 0, events_updated == 0, active_portfolio == X.
        Run 2: 5 succeeded, 2 failed. 7 observations accepted.
               events_created == 0, events_updated == 7, active_portfolio == X (strictly no drift).
        Persistence entities in database remain non-duplicated across runs.
        """
        import json
        from sqlalchemy import select
        from beacon.storage.schema import EventModel, ObservationModel

        files_dir = tmp_path / "seven_files"
        files_dir.mkdir()

        # 5 valid files yielding 7 observations
        f1 = files_dir / "events1.json"
        f1.write_text(json.dumps([
            {"event_type": "flood", "location": "Zone 1", "event_time": "2026-10-29T10:00:00Z"},
            {"event_type": "flood", "location": "Zone 2", "event_time": "2026-10-29T10:30:00Z"},
        ]), encoding="utf-8")

        f2 = files_dir / "events2.json"
        f2.write_text(json.dumps([
            {"event_type": "wildfire", "location": "Forest Area 1", "event_time": "2026-10-29T11:00:00Z"},
            {"event_type": "wildfire", "location": "Forest Area 2", "event_time": "2026-10-29T11:30:00Z"},
        ]), encoding="utf-8")

        f3 = files_dir / "data.csv"
        f3.write_text("event_type,location,event_time\nearthquake,Epicenter,2026-10-29T12:00:00Z\n", encoding="utf-8")

        f4 = files_dir / "report.csv"
        f4.write_text("event_type,location,event_time\ntornado,Valley Road,2026-10-29T13:00:00Z\n", encoding="utf-8")

        f5 = files_dir / "sensors.csv"
        f5.write_text("event_type,location,event_time\ntsunami,Port Facility,2026-10-29T14:00:00Z\n", encoding="utf-8")

        # 2 failing files
        f6 = files_dir / "corrupt.json"
        f6.write_text("{unclosed json payload", encoding="utf-8")

        f7 = files_dir / "unsupported.bin"
        f7.write_bytes(b"\x00\x01\x02\xFF")

        target_paths = [f1, f2, f3, f4, f5, f6, f7]
        runner = ETLRunner(session_factory=db_session_factory)

        # Run 1
        res1 = runner.run(target_paths, enable_resolution=True)
        assert res1.files_discovered == 7
        assert res1.files_succeeded == 5
        assert res1.files_failed == 2
        assert res1.observations_accepted == 7
        assert res1.events_created >= 1
        assert res1.events_created + res1.events_updated == 7
        assert res1.active_portfolio == res1.events_created
        assert res1.events_persisted_this_run == res1.events_created

        with db_session_factory() as session:
            db_evts_1 = len(session.execute(select(EventModel)).scalars().all())
            db_obs_1 = len(session.execute(select(ObservationModel)).scalars().all())
        assert db_evts_1 == res1.active_portfolio
        assert db_obs_1 == 7

        # Run 2 (repeat over identical 7 files)
        res2 = runner.run(target_paths, enable_resolution=True)
        assert res2.files_discovered == 7
        assert res2.files_succeeded == 5
        assert res2.files_failed == 2
        assert res2.observations_accepted == 7
        assert res2.events_created == 0
        assert res2.events_updated == 7
        assert res2.active_portfolio == res1.active_portfolio  # STRICTLY IDENTICAL (NO DRIFT)
        assert res2.events_persisted_this_run == res1.active_portfolio

        with db_session_factory() as session:
            db_evts_2 = len(session.execute(select(EventModel)).scalars().all())
            db_obs_2 = len(session.execute(select(ObservationModel)).scalars().all())
        assert db_evts_2 == db_evts_1  # NO DUPLICATION
        assert db_obs_2 == db_obs_1
