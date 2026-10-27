"""Comprehensive unit and integration tests for BEACON Phase 2F: Data Operations and Orchestration."""

from datetime import timezone
from pathlib import Path
from unittest.mock import patch
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from beacon.config import Settings
from beacon.dashboard.services import DashboardService
from beacon.logging_config import redact_sensitive_dict, redact_sensitive_text
from beacon.pipeline import ETLPipelineResult, ETLRunner, InputFileExecutionRecord, RunStatus
from beacon.storage import (
    EventModel,
    MergeCandidateModel,
    ObservationModel,
    PipelineRunModel,
    ResolutionDecisionModel,
    SourceModel,
    drop_db,
    get_session_factory,
    init_db,
)


@pytest.fixture
def sample_files() -> list[Path]:
    sample_dir = Path("data/sample")
    return [
        sample_dir / "events.json",
        sample_dir / "events.csv",
        sample_dir / "events.txt",
    ]


@pytest.fixture
def in_memory_db() -> sessionmaker:
    engine = create_engine("sqlite:///:memory:")
    init_db(engine)
    factory = get_session_factory(engine)
    yield factory
    drop_db(engine)


# ==============================================================================
# 1. Pipeline Run Records & Metadata
# ==============================================================================

class TestPipelineRunRecords:
    """Tests for stable execution IDs, start/end timestamps, statuses, and counts."""

    def test_pipeline_run_record_fields_and_persistence(
        self, sample_files: list[Path], in_memory_db: sessionmaker
    ) -> None:
        runner = ETLRunner(session_factory=in_memory_db)
        result = runner.run([sample_files[0]], enable_resolution=True)

        assert isinstance(result, ETLPipelineResult)
        assert result.execution_id.startswith("run_")
        assert result.run_id == result.execution_id
        assert result.status == RunStatus.COMPLETED.value
        assert result.trigger_type == "manual"
        assert result.retry_of_run_id is None
        assert result.execution_timestamp.tzinfo == timezone.utc
        assert result.completed_at is not None
        assert result.completed_at.tzinfo == timezone.utc
        assert result.completed_at >= result.execution_timestamp
        assert result.duration_seconds is not None
        assert result.duration_seconds >= 0.0

        # Verify persistence of PipelineRunModel in database
        with in_memory_db() as session:
            runs = session.execute(select(PipelineRunModel)).scalars().all()
            assert len(runs) == 1
            run = runs[0]
            assert run.run_id == result.execution_id
            assert run.status == "completed"
            assert run.trigger_type == "manual"
            assert run.retry_of_run_id is None
            assert run.inputs_summary["total_inputs"] == 1
            assert run.inputs_summary["succeeded"] == 1
            assert run.inputs_summary["failed"] == 0
            assert len(run.inputs_summary["files"]) == 1
            assert run.inputs_summary["files"][0]["status"] == "success"
            assert run.counts_summary["records_persisted"] == result.records_persisted
            assert run.counts_summary["events_synthesized"] == result.events_synthesized
            assert run.failure_details == []

    def test_secret_redaction_in_pipeline_runs_and_errors(
        self, tmp_path: Path, in_memory_db: sessionmaker
    ) -> None:
        """Secrets, credentials, and sensitive database URLs are scrubbed from errors and run records."""
        # 1. Test direct text redaction helper
        raw_error = "Connection failed to postgresql://beacon_user:super_secret_pw123@db.prod.internal:5432/beacon_prod"
        cleaned_error = redact_sensitive_text(raw_error)
        assert "super_secret_pw123" not in cleaned_error
        assert "postgresql://beacon_user:[REDACTED]@db.prod.internal:5432/beacon_prod" in cleaned_error

        raw_auth_error = "Authentication header token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9 and password=AdminPass123"
        cleaned_auth = redact_sensitive_text(raw_auth_error)
        assert "AdminPass123" not in cleaned_auth
        assert "eyJhbGci" not in cleaned_auth
        assert "password=[REDACTED]" in cleaned_auth
        assert "token=[REDACTED]" in cleaned_auth

        # 2. Test dictionary scrubbing helper
        raw_dict = {
            "database_url": "postgresql://usr:secret_pass@host/db",
            "api_key": "prod_key_9999",
            "safe_field": "valid_value",
        }
        cleaned_dict = redact_sensitive_dict(raw_dict)
        assert cleaned_dict["api_key"] == "[REDACTED]"
        assert cleaned_dict["safe_field"] == "valid_value"

        # 3. Test that simulated sensitive exception in runner is scrubbed in result and persistent run record
        runner = ETLRunner(session_factory=in_memory_db)
        sensitive_msg = "Database persistence failed: postgresql://admin:p@ssw0rd99@10.0.0.1:5432/beacon_dev"

        with patch.object(
            runner.repository, "save_batch", side_effect=RuntimeError(sensitive_msg)
        ):
            res = runner.run([tmp_path / "dummy.json"], persist_run_record=True)

        assert res.status == RunStatus.FAILED.value
        assert any("p@ssw0rd99" not in err for err in res.errors)
        assert not any("p@ssw0rd99" in err for err in res.errors)
        assert any("[REDACTED]" in err for err in res.errors)

        with in_memory_db() as session:
            run_records = session.execute(select(PipelineRunModel)).scalars().all()
            assert len(run_records) == 1
            for err in run_records[0].failure_details:
                assert "p@ssw0rd99" not in err


# ==============================================================================
# 2. Safe Retries & Reprocessing Idempotency
# ==============================================================================

class TestSafeRetriesAndReprocessing:
    """Tests for execution retries, parent-child linkage, and non-duplication."""

    def test_retry_creates_distinct_execution_with_parent_linkage(
        self, sample_files: list[Path], in_memory_db: sessionmaker
    ) -> None:
        runner = ETLRunner(session_factory=in_memory_db)

        # Initial run
        run1 = runner.run([sample_files[0], sample_files[1]])
        assert run1.trigger_type == "manual"
        assert run1.retry_of_run_id is None

        # Retry of initial run
        run2 = runner.retry(failed_run_id=run1.execution_id)
        assert run2.trigger_type == "retry"
        assert run2.retry_of_run_id == run1.execution_id
        assert run2.execution_id.startswith("retry_")
        assert run2.execution_id != run1.execution_id

        # Verify DB records both runs with proper parent linkage
        with in_memory_db() as session:
            all_runs = runner.repository.list_pipeline_runs(session)
            assert len(all_runs) == 2
            # Ordered newest first
            assert all_runs[0].run_id == run2.execution_id
            assert all_runs[0].retry_of_run_id == run1.execution_id
            assert all_runs[0].trigger_type == "retry"

            assert all_runs[1].run_id == run1.execution_id
            assert all_runs[1].retry_of_run_id is None
            assert all_runs[1].trigger_type == "manual"

    def test_retry_idempotency_does_not_duplicate_entities(
        self, sample_files: list[Path], in_memory_db: sessionmaker
    ) -> None:
        """Retrying an execution reprocesses inputs without inflating sources, observations, or events."""
        runner = ETLRunner(session_factory=in_memory_db)

        # Initial run with event resolution
        run1 = runner.run(sample_files, enable_resolution=True)
        assert run1.records_persisted > 0
        assert run1.events_persisted > 0

        with in_memory_db() as session:
            src_count_1 = len(session.execute(select(SourceModel)).scalars().all())
            obs_count_1 = len(session.execute(select(ObservationModel)).scalars().all())
            evt_count_1 = len(session.execute(select(EventModel)).scalars().all())
            merges_count_1 = len(session.execute(select(MergeCandidateModel)).scalars().all())
            decisions_count_1 = len(session.execute(select(ResolutionDecisionModel)).scalars().all())

        # Retry the execution
        run2 = runner.retry(failed_run_id=run1.execution_id, enable_resolution=True)
        assert run2.records_persisted == run1.records_persisted
        assert run2.events_persisted == run1.events_persisted

        with in_memory_db() as session:
            src_count_2 = len(session.execute(select(SourceModel)).scalars().all())
            obs_count_2 = len(session.execute(select(ObservationModel)).scalars().all())
            evt_count_2 = len(session.execute(select(EventModel)).scalars().all())
            merges_count_2 = len(session.execute(select(MergeCandidateModel)).scalars().all())
            decisions_count_2 = len(session.execute(select(ResolutionDecisionModel)).scalars().all())

            # Data entities MUST NOT duplicate
            assert src_count_2 == src_count_1
            assert obs_count_2 == obs_count_1
            assert evt_count_2 == evt_count_1
            assert merges_count_2 == merges_count_1

            # Audit history MUST append (preserving immutable provenance decisions)
            assert decisions_count_2 > decisions_count_1

    def test_retry_failed_inputs_only_filters_target_files(
        self, sample_files: list[Path], tmp_path: Path, in_memory_db: sessionmaker
    ) -> None:
        """When failed_inputs_only is True, retry filters to only inputs that failed in parent run."""
        corrupt_file = tmp_path / "corrupt_data.pdf"
        corrupt_file.write_bytes(b"")  # 0-byte PDF fails ingestion

        mixed_inputs = [sample_files[0], corrupt_file]
        runner = ETLRunner(session_factory=in_memory_db)

        initial_run = runner.run(mixed_inputs)
        assert initial_run.files_succeeded == 1
        assert initial_run.files_failed == 1
        assert initial_run.status == RunStatus.PARTIAL.value

        # Retry with failed_inputs_only=True
        retry_run = runner.retry(
            failed_run_id=initial_run.execution_id,
            failed_inputs_only=True,
        )
        assert retry_run.files_discovered == 1
        assert Path(retry_run.file_results[0].source_record.source_file) == corrupt_file
        assert retry_run.status == RunStatus.FAILED.value

    def test_structured_input_execution_records_and_failed_inputs_retrieval(
        self, sample_files: list[Path], tmp_path: Path, in_memory_db: sessionmaker
    ) -> None:
        """Verify structured per-input execution records and stable identifier extraction."""
        corrupt_file = tmp_path / "bad.pdf"
        corrupt_file.write_bytes(b"")

        runner = ETLRunner(session_factory=in_memory_db)
        res = runner.run([sample_files[0], corrupt_file])
        run_record = res.to_run_record()

        # Verify input_files are structured instances
        assert len(run_record.input_files) == 2
        assert all(isinstance(f, InputFileExecutionRecord) for f in run_record.input_files)

        # Verify failed inputs retrieval does NOT parse human-readable error strings
        failed = run_record.get_failed_input_files()
        assert len(failed) == 1
        assert Path(failed[0]) == corrupt_file

        all_inputs = run_record.get_all_input_files()
        assert len(all_inputs) == 2
        assert Path(sample_files[0]) in [Path(p) for p in all_inputs]

        # Verify persisted record restores input_files structure from DB
        with in_memory_db() as session:
            retrieved = runner.repository.get_pipeline_run(run_record.run_id, session)
            assert retrieved is not None
            assert len(retrieved.input_files) == 2
            assert retrieved.get_failed_input_files() == failed

    def test_retry_with_explicit_file_paths_and_failed_inputs_only(
        self, sample_files: list[Path], tmp_path: Path, in_memory_db: sessionmaker
    ) -> None:
        """When explicit file_paths and failed_inputs_only are passed, filter against parent run."""
        corrupt_file = tmp_path / "corrupt.json"
        corrupt_file.write_text("NOT_JSON{bad", encoding="utf-8")

        runner = ETLRunner(session_factory=in_memory_db)
        parent = runner.run([sample_files[0], corrupt_file])
        assert parent.files_failed == 1

        # Passing all files to retry but with failed_inputs_only=True
        retry_run = runner.retry(
            failed_run_id=parent.execution_id,
            file_paths=[sample_files[0], corrupt_file],
            failed_inputs_only=True,
        )
        assert retry_run.files_discovered == 1
        assert Path(retry_run.file_results[0].source_record.source_file) == corrupt_file


# ==============================================================================
# 3. Batch Execution & Partial Failure Isolation
# ==============================================================================

class TestBatchExecutionAndFailureIsolation:
    """Tests for multi-input batch orchestration and granular error attribution."""

    def test_batch_execution_reports_total_succeeded_partial_and_failed(
        self, sample_files: list[Path], tmp_path: Path, in_memory_db: sessionmaker
    ) -> None:
        # 1. Full success file
        valid_json = sample_files[0]

        # 2. Partial success file: 1 valid report block, 1 block missing EVENT
        partial_txt = tmp_path / "partial.txt"
        partial_txt.write_text(
            "[REPORT 001]\nEVENT: Flood\nLOCATION: Basin\n\n"
            "[REPORT 002]\nLOCATION: Missing Event\n",
            encoding="utf-8",
        )

        # 3. Completely failed file
        failed_img = tmp_path / "empty.png"
        failed_img.write_bytes(b"")

        runner = ETLRunner(session_factory=in_memory_db)
        batch_result = runner.run([valid_json, partial_txt, failed_img])

        assert batch_result.files_discovered == 3
        assert batch_result.files_succeeded == 1
        assert batch_result.files_partially_successful == 1
        assert batch_result.files_failed == 1
        assert batch_result.status == RunStatus.PARTIAL.value

        # Error attribution: check error messages reference specific files
        error_files = [err.split("]:")[0].strip("[") for err in batch_result.errors]
        assert "empty.png" in error_files
        assert "partial.txt" in error_files

    def test_persistence_failure_transaction_rolls_back_and_records_run_failure(
        self, sample_files: list[Path], in_memory_db: sessionmaker
    ) -> None:
        runner = ETLRunner(session_factory=in_memory_db)

        with patch.object(
            runner.repository, "save_resolution_result", side_effect=RuntimeError("Simulated Storage Disconnect")
        ):
            result = runner.run([sample_files[0]], enable_resolution=True)

        assert result.status == RunStatus.FAILED.value
        assert result.records_persisted == 0
        assert result.events_persisted == 0
        assert any("Simulated Storage Disconnect" in err for err in result.errors)

        with in_memory_db() as session:
            # Data entities were cleanly rolled back
            assert len(session.execute(select(SourceModel)).scalars().all()) == 0
            assert len(session.execute(select(ObservationModel)).scalars().all()) == 0
            assert len(session.execute(select(EventModel)).scalars().all()) == 0

            # Persistent run record captures the failure diagnostic
            runs = session.execute(select(PipelineRunModel)).scalars().all()
            assert len(runs) == 1
            assert runs[0].status == "failed"
            assert any("Simulated Storage Disconnect" in f for f in runs[0].failure_details)


# ==============================================================================
# 4. Dashboard Service Run History & Retry Integration
# ==============================================================================

class TestDashboardServiceOrchestration:
    """Tests for DashboardService execution history and safe retry capabilities."""

    def test_dashboard_service_run_history_in_memory(self, sample_files: list[Path]) -> None:
        service = DashboardService(settings=Settings(database_url=None), session_factory=None)
        assert len(service.list_pipeline_runs()) == 0

        res1 = service.run_pipeline_on_paths([sample_files[0]], persist_db=False)
        res2 = service.run_pipeline_on_paths([sample_files[1]], persist_db=False)

        runs = service.list_pipeline_runs()
        assert len(runs) == 2
        # Order is newest first
        assert runs[0].run_id == res2.execution_id
        assert runs[1].run_id == res1.execution_id

        fetched = service.get_pipeline_run(res1.execution_id)
        assert fetched is not None
        assert fetched.run_id == res1.execution_id

    def test_dashboard_service_retry_pipeline_run_in_memory(self, sample_files: list[Path]) -> None:
        service = DashboardService(settings=Settings(database_url=None), session_factory=None)
        res1 = service.run_pipeline_on_paths([sample_files[0]], persist_db=False)

        # Retry the run
        retry_res = service.retry_pipeline_run(res1.execution_id, persist_db=False)
        assert retry_res.trigger_type == "retry"
        assert retry_res.retry_of_run_id == res1.execution_id

        runs = service.list_pipeline_runs()
        assert len(runs) == 2
        assert runs[0].run_id == retry_res.execution_id
        assert runs[0].retry_of_run_id == res1.execution_id

    def test_dashboard_service_run_history_database_backed(
        self, sample_files: list[Path], in_memory_db: sessionmaker
    ) -> None:
        settings = Settings(database_url="sqlite:///:memory:")
        service = DashboardService(settings=settings, session_factory=in_memory_db)

        res = service.run_pipeline_on_paths([sample_files[0]], persist_db=True)

        runs = service.list_pipeline_runs()
        assert len(runs) == 1
        assert runs[0].run_id == res.execution_id
        assert runs[0].status == RunStatus.COMPLETED
