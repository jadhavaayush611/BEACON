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
