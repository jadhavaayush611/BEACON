"""Unit tests for BEACON ETL pipeline orchestrator (Extract, Transform, Validate, Load, Report)."""

from pathlib import Path
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from beacon.config import Settings
from beacon.pipeline import ETLPipelineResult, ETLRunner
from beacon.storage import (
    ObservationModel,
    QualityResultModel,
    SourceModel,
    drop_db,
    get_session_factory,
    init_db,
)


class TestETLPipelineUnit:
    """Tests for ETLRunner pipeline flow, error isolation, idempotency, and reporting."""

    @pytest.fixture
    def sample_files(self) -> list[Path]:
        sample_dir = Path("data/sample")
        return [
            sample_dir / "events.json",
            sample_dir / "events.csv",
            sample_dir / "events.txt",
        ]

    @pytest.fixture
    def in_memory_db(self) -> sessionmaker:
        engine = create_engine("sqlite:///:memory:")
        init_db(engine)
        factory = get_session_factory(engine)
        yield factory
        drop_db(engine)

    def test_pipeline_without_storage(self, sample_files: list[Path]) -> None:
        """Pipeline runs extract -> transform -> validate without DB persistence if not configured."""
        settings = Settings(database_url=None)
        runner = ETLRunner(settings=settings, session_factory=None)

        result = runner.run([sample_files[0]])  # events.json

        assert isinstance(result, ETLPipelineResult)
        assert result.files_discovered == 1
        assert result.files_succeeded == 1
        assert result.files_failed == 0
        assert result.observations_extracted > 0
        assert result.observations_transformed == result.observations_extracted
        assert result.observations_accepted > 0
        assert not result.storage_enabled
        assert result.records_persisted == 0

        # Check summary formatting
        summary = result.summary()
        assert "BEACON ETL Pipeline Run Summary" in summary
        assert "events.json" in summary

    def test_pipeline_with_storage_end_to_end(
        self, sample_files: list[Path], in_memory_db: sessionmaker
    ) -> None:
        """Pipeline persists extracted and accepted observations into database."""
        runner = ETLRunner(session_factory=in_memory_db)

        result = runner.run(sample_files)

        assert result.files_discovered == 3
        assert result.files_succeeded == 3
        assert result.files_failed == 0
        assert result.observations_accepted > 0
        assert result.storage_enabled
        assert result.records_persisted == result.observations_accepted

        # Verify DB records
        with in_memory_db() as session:
            sources = session.execute(select(SourceModel)).scalars().all()
            assert len(sources) == 3

            observations = session.execute(select(ObservationModel)).scalars().all()
            assert len(observations) == result.records_persisted

            dq_results = session.execute(select(QualityResultModel)).scalars().all()
            assert len(dq_results) > 0

    def test_pipeline_rerun_idempotence(
        self, sample_files: list[Path], in_memory_db: sessionmaker
    ) -> None:
        """Rerunning pipeline over identical data must succeed and maintain stable record counts."""
        runner = ETLRunner(session_factory=in_memory_db)

        # Run 1
        result1 = runner.run([sample_files[0]])
        # Run 2
        result2 = runner.run([sample_files[0]])

        assert result1.records_persisted == result2.records_persisted

        with in_memory_db() as session:
            sources = session.execute(select(SourceModel)).scalars().all()
            assert len(sources) == 1

            observations = session.execute(select(ObservationModel)).scalars().all()
            assert len(observations) == result1.records_persisted

    def test_pipeline_partial_file_failure_isolation(
        self, sample_files: list[Path], in_memory_db: sessionmaker, tmp_path: Path
    ) -> None:
        """Missing or malformed file does not halt ingestion and persistence of valid files."""
        missing_file = tmp_path / "does_not_exist.json"
        mixed_paths = [sample_files[0], missing_file]

        runner = ETLRunner(session_factory=in_memory_db)
        result = runner.run(mixed_paths)

        assert result.files_discovered == 2
        assert result.files_succeeded == 1
        assert result.files_failed == 1
        assert len(result.errors) >= 1
        assert result.records_persisted > 0

        with in_memory_db() as session:
            sources = session.execute(select(SourceModel)).scalars().all()
            assert len(sources) == 2
            statuses = {s.extraction_status for s in sources}
            assert "success" in statuses
            assert "failed" in statuses

    def test_pipeline_data_quality_rejection_isolation(
        self, in_memory_db: sessionmaker, tmp_path: Path
    ) -> None:
        """Records failing fatal DQ rules are rejected while valid records in the batch persist."""
        # Create a JSON file with one valid record and duplicate records causing fatal collision
        raw_json = tmp_path / "collision.json"
        raw_json.write_text(
            """[
                {
                    "event_type": "flood",
                    "location": "River City",
                    "timestamp": "2026-05-01T10:00:00Z",
                    "severity": "moderate"
                },
                {
                    "event_type": "earthquake",
                    "location": "Fault Line",
                    "timestamp": "2026-05-01T10:00:00Z",
                    "severity": "severe"
                }
            ]""",
            encoding="utf-8",
        )

        runner = ETLRunner(session_factory=in_memory_db)
        result = runner.run([raw_json])

        assert result.files_succeeded == 1
        assert result.observations_accepted == 2
        assert result.observations_rejected == 0
        assert result.records_persisted == 2

    def test_pipeline_result_to_dict_serialization(self, sample_files: list[Path]) -> None:
        """Verify machine-readable serialization of pipeline run result."""
        runner = ETLRunner(session_factory=None)
        result = runner.run([sample_files[0]])

        data = result.to_dict()
        assert "execution_id" in data
        assert "files_discovered" in data
        assert "observations_accepted" in data
        assert "quality_report" in data
        assert isinstance(data["quality_report"], dict)
