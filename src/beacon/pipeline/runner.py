"""Main ETL pipeline orchestrator: Extract, Transform, Validate, Load, Report."""

from datetime import datetime, timezone
import hashlib
import logging
from pathlib import Path
from typing import Any, Optional, Sequence, Union

from sqlalchemy.orm import Session, sessionmaker

from beacon.config import Settings, get_settings
from beacon.ingestion import IngestionResult, ingest_file
from beacon.logging_config import setup_logging
from beacon.models import DisasterObservation
from beacon.pipeline.result import ETLPipelineResult
from beacon.pipeline.transform import ObservationTransformer
from beacon.quality import DataQualityValidator, QualityReport
from beacon.storage import StorageRepository, create_db_engine, get_session, get_session_factory

logger = logging.getLogger("beacon.pipeline.runner")


class ETLRunner:
    """Executes the complete BEACON ETL pipeline over candidate disaster files."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        session_factory: Optional[sessionmaker[Session]] = None,
        transformer: Optional[ObservationTransformer] = None,
        validator: Optional[DataQualityValidator] = None,
        repository: Optional[StorageRepository] = None,
        resolver: Optional[Any] = None,
    ) -> None:
        self.settings = settings or get_settings()
        setup_logging(self.settings)
        self.transformer = transformer or ObservationTransformer()
        self.validator = validator or DataQualityValidator()
        self.repository = repository or StorageRepository()
        self.session_factory = session_factory

        from beacon.resolution.resolver import EventResolver
        self.resolver = resolver or EventResolver(settings=self.settings)

        # Initialize session_factory lazily from settings if configured and not explicitly provided
        if self.session_factory is None and self.settings.database_url:
            try:
                engine = create_db_engine(settings=self.settings)
                self.session_factory = get_session_factory(engine)
            except Exception as err:
                logger.warning("Could not initialize database session factory: %s", err)

    def extract(self, file_paths: Sequence[Union[str, Path]]) -> list[IngestionResult]:
        """Extract: Ingest raw files into IngestionResult instances using the ingestion subsystem."""
        results: list[IngestionResult] = []
        for path in file_paths:
            target = Path(path)
            logger.info("Extracting observations from: %s", target)
            res = ingest_file(target, raise_on_unsupported=False, raise_on_missing=False)
            results.append(res)
        return results

    def transform(self, observations: Sequence[DisasterObservation]) -> list[DisasterObservation]:
        """Transform: Normalize extracted observations into consistent canonical representations."""
        return self.transformer.transform_batch(observations)

    def validate(self, observations: Sequence[DisasterObservation]) -> QualityReport:
        """Validate: Apply machine-readable data quality rules across the observation batch."""
        return self.validator.validate(observations)

    def load(
        self,
        file_results: Sequence[IngestionResult],
        quality_report: QualityReport,
        session: Session,
    ) -> int:
        """Load: Idempotently persist source records, quality results, and accepted observations."""
        persisted_observations = 0
        accepted_lookup = {o.observation_id: o for o in quality_report.accepted_records}

        for file_res in file_results:
            source_rec = file_res.source_record
            # Filter accepted observations that originated from this source
            source_accepted = [
                obs for obs in file_res.observations
                if obs.observation_id in accepted_lookup
            ]
            # Replace with transformed versions from accepted_lookup
            source_accepted_transformed = [accepted_lookup[obs.observation_id] for obs in source_accepted]
            source_outcomes = [
                o for o in quality_report.outcomes
                if o.source_id == source_rec.source_id
            ]

            logger.info(
                "Persisting source '%s' with %d accepted observations and %d DQ audit records",
                source_rec.source_id,
                len(source_accepted_transformed),
                len(source_outcomes),
            )

            counts = self.repository.save_batch(
                source_record=source_rec,
                accepted_observations=source_accepted_transformed,
                quality_outcomes=source_outcomes,
                session=session,
            )
            persisted_observations += counts["observations_persisted"]

        return persisted_observations

    def run(
        self,
        file_paths: Sequence[Union[str, Path]],
        enable_resolution: bool = False,
    ) -> ETLPipelineResult:
        """Execute the end-to-end pipeline: Extract -> Transform -> Validate -> Load -> Report.

        Safe to rerun and isolates partial failures.
        """
        paths = [Path(p) for p in file_paths]
        run_ts = datetime.now(timezone.utc)
        exec_id = f"run_{hashlib.sha256(run_ts.isoformat().encode('utf-8')).hexdigest()[:12]}"

        logger.info("Starting BEACON ETL Run '%s' for %d files", exec_id, len(paths))

        errors: list[str] = []
        warnings: list[str] = []

        # 1. Extract
        ingestion_results = self.extract(paths)
        files_succeeded = sum(1 for r in ingestion_results if r.is_success or r.is_partial)
        files_failed = sum(1 for r in ingestion_results if r.is_failed)

        all_extracted_observations: list[DisasterObservation] = []
        for r in ingestion_results:
            all_extracted_observations.extend(r.observations)
            if r.errors:
                errors.extend([f"[{Path(r.source_record.source_file).name}]: {e}" for e in r.errors])
            if r.warnings:
                warnings.extend([f"[{Path(r.source_record.source_file).name}]: {w}" for w in r.warnings])

        # 2. Transform
        transformed_observations = self.transform(all_extracted_observations)

        # 3. Validate
        quality_report = self.validate(transformed_observations)

        # 4. Load & Resolution
        records_persisted = 0
        events_synthesized = 0
        events_persisted = 0
        res_summary = None
        storage_enabled = self.session_factory is not None

        if storage_enabled:
            assert self.session_factory is not None
            try:
                # Execute all storage operations in a single atomic transaction boundary
                with get_session(self.session_factory) as session:
                    records_persisted = self.load(
                        file_results=ingestion_results,
                        quality_report=quality_report,
                        session=session,
                    )

                    if enable_resolution and quality_report.accepted_records:
                        logger.info(
                            "Resolving %d accepted observations into unified events",
                            len(quality_report.accepted_records),
                        )
                        existing_events, existing_event_obs = (
                            self.repository.get_all_events_with_observations(session)
                        )
                        res_result = self.resolver.resolve_observations(
                            quality_report.accepted_records,
                            existing_events=existing_events,
                            existing_event_observations=existing_event_obs,
                        )
                        events_synthesized = len(res_result.resolved_events)
                        res_summary = {
                            "total_observations": res_result.total_observations,
                            "events_created": res_result.events_created,
                            "events_updated": res_result.events_updated,
                            "observations_assigned": res_result.observations_assigned,
                            "ambiguous_count": res_result.ambiguous_count,
                            "merge_candidates_count": len(res_result.merge_candidates),
                            "resolved_events_count": len(res_result.resolved_events),
                        }
                        res_counts = self.repository.save_resolution_result(res_result, session)
                        events_persisted = res_counts.get("events_persisted", 0)
            except Exception as db_err:
                err_msg = f"Database persistence failed during ETL transaction: {db_err}"
                logger.error(err_msg)
                errors.append(err_msg)
                records_persisted = 0
                events_persisted = 0
        else:
            logger.info("Database storage not configured; skipping persistence step")
            if enable_resolution and quality_report.accepted_records:
                logger.info(
                    "Resolving %d accepted observations in-memory",
                    len(quality_report.accepted_records),
                )
                res_result = self.resolver.resolve_observations(
                    quality_report.accepted_records,
                    existing_events=[],
                    existing_event_observations={},
                )
                events_synthesized = len(res_result.resolved_events)
                res_summary = {
                    "total_observations": res_result.total_observations,
                    "events_created": res_result.events_created,
                    "events_updated": res_result.events_updated,
                    "observations_assigned": res_result.observations_assigned,
                    "ambiguous_count": res_result.ambiguous_count,
                    "merge_candidates_count": len(res_result.merge_candidates),
                    "resolved_events_count": len(res_result.resolved_events),
                }

        # 6. Report
        return ETLPipelineResult(
            execution_id=exec_id,
            execution_timestamp=run_ts,
            files_discovered=len(paths),
            files_succeeded=files_succeeded,
            files_failed=files_failed,
            observations_extracted=len(all_extracted_observations),
            observations_transformed=len(transformed_observations),
            observations_accepted=len(quality_report.accepted_records),
            observations_rejected=len(quality_report.rejected_record_ids),
            records_persisted=records_persisted,
            events_synthesized=events_synthesized,
            events_persisted=events_persisted,
            resolution_summary=res_summary,
            storage_enabled=storage_enabled,
            quality_report=quality_report,
            file_results=ingestion_results,
            errors=errors,
            warnings=warnings,
        )

    def run_complete_pipeline(
        self, file_paths: Sequence[Union[str, Path]]
    ) -> ETLPipelineResult:
        """Execute complete end-to-end pipeline with event resolution enabled."""
        return self.run(file_paths, enable_resolution=True)
