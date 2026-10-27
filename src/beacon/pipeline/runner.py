"""Main ETL pipeline orchestrator: Extract, Transform, Validate, Load, Report.

Orchestration & Scheduling Note:
Recurring daemon/cron background scheduling is explicitly deferred in BEACON v1.0.0 MVP.
Batch execution operations are controlled, idempotent, and deterministic,
triggered on-demand via the CLI or dashboard service with complete audit trail persistence.
"""

from datetime import datetime, timezone
import hashlib
import logging
from pathlib import Path
from typing import Any, Optional, Sequence, Union

from sqlalchemy.orm import Session, sessionmaker

from beacon.config import Settings, get_settings
from beacon.ingestion import IngestionResult, ingest_file
from beacon.logging_config import redact_sensitive_text, setup_logging
from beacon.models import DisasterObservation
from beacon.pipeline.result import ETLPipelineResult, RunStatus
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
        trigger_type: str = "manual",
        retry_of_run_id: Optional[str] = None,
        persist_run_record: bool = True,
    ) -> ETLPipelineResult:
        """Execute the end-to-end pipeline: Extract -> Transform -> Validate -> Load -> Report.

        Safe to rerun, isolates partial failures, and guarantees atomic transactional persistence.
        """
        paths = [Path(p) for p in file_paths]
        run_ts = datetime.now(timezone.utc)
        if retry_of_run_id:
            exec_id = f"retry_{retry_of_run_id[:16]}_{hashlib.sha256(run_ts.isoformat().encode('utf-8')).hexdigest()[:8]}"
            trigger_type = "retry"
        else:
            exec_id = f"run_{hashlib.sha256(run_ts.isoformat().encode('utf-8')).hexdigest()[:12]}"

        logger.info(
            "Starting BEACON ETL Run '%s' (trigger: %s) for %d files",
            exec_id,
            trigger_type,
            len(paths),
        )

        errors: list[str] = []
        warnings: list[str] = []

        # 1. Extract
        ingestion_results = self.extract(paths)
        files_succeeded = sum(1 for r in ingestion_results if r.is_success)
        files_partially_successful = sum(1 for r in ingestion_results if r.is_partial)
        files_failed = sum(1 for r in ingestion_results if r.is_failed)
        files_skipped = sum(1 for r in ingestion_results if getattr(r, "is_skipped", False))

        all_extracted_observations: list[DisasterObservation] = []
        for r in ingestion_results:
            all_extracted_observations.extend(r.observations)
            if r.errors:
                errors.extend([
                    f"[{Path(r.source_record.source_file).name}]: {redact_sensitive_text(e)}"
                    for e in r.errors
                ])
            if r.warnings:
                warnings.extend([
                    f"[{Path(r.source_record.source_file).name}]: {redact_sensitive_text(w)}"
                    for w in r.warnings
                ])

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
        persistence_failed = False

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
                persistence_failed = True
                err_msg = f"Database persistence failed during ETL transaction: {redact_sensitive_text(str(db_err))}"
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

        # 5. Status & Timing
        completed_at = datetime.now(timezone.utc)
        duration_seconds = max(0.0, (completed_at - run_ts).total_seconds())

        if persistence_failed:
            final_status = RunStatus.FAILED
        elif errors:
            if files_succeeded == 0 and files_partially_successful == 0:
                final_status = RunStatus.FAILED
            else:
                final_status = RunStatus.PARTIAL
        elif files_partially_successful > 0:
            final_status = RunStatus.PARTIAL
        else:
            final_status = RunStatus.COMPLETED

        # 6. Report
        result = ETLPipelineResult(
            execution_id=exec_id,
            execution_timestamp=run_ts,
            completed_at=completed_at,
            duration_seconds=duration_seconds,
            status=final_status.value,
            trigger_type=trigger_type,
            retry_of_run_id=retry_of_run_id,
            files_discovered=len(paths),
            files_succeeded=files_succeeded,
            files_partially_successful=files_partially_successful,
            files_failed=files_failed,
            files_skipped=files_skipped,
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

        # 7. Persist Execution Run Record (independent audit transaction)
        if storage_enabled and persist_run_record:
            try:
                assert self.session_factory is not None
                with get_session(self.session_factory) as session:
                    self.repository.save_pipeline_run(result.to_run_record(), session)
            except Exception as run_save_err:
                logger.warning(
                    "Could not persist pipeline run record: %s",
                    redact_sensitive_text(str(run_save_err)),
                )

        return result

    def retry(
        self,
        failed_run_id: str,
        file_paths: Optional[Sequence[Union[str, Path]]] = None,
        failed_inputs_only: bool = False,
        enable_resolution: bool = False,
    ) -> ETLPipelineResult:
        """Retry execution of a previously executed run safely.

        If file_paths is not provided, queries the parent run record to discover its inputs.
        If failed_inputs_only is True, filters targets to only files that failed in the parent run.
        Ensures retry lineage is preserved via retry_of_run_id without duplicating records.
        """
        target_paths: list[Path] = []
        if file_paths:
            explicit_paths = [Path(p) for p in file_paths]
            if failed_inputs_only and self.session_factory is not None:
                try:
                    with get_session(self.session_factory) as session:
                        parent_run = self.repository.get_pipeline_run(failed_run_id, session)
                        if parent_run:
                            failed_set = {
                                Path(p).resolve() for p in parent_run.get_failed_input_files()
                            }
                            failed_names = {
                                Path(p).name for p in parent_run.get_failed_input_files()
                            }
                            target_paths = [
                                p for p in explicit_paths
                                if p.resolve() in failed_set or p.name in failed_names
                            ]
                        else:
                            target_paths = explicit_paths
                except Exception:
                    target_paths = explicit_paths
            else:
                target_paths = explicit_paths
        elif self.session_factory is not None:
            try:
                with get_session(self.session_factory) as session:
                    parent_run = self.repository.get_pipeline_run(failed_run_id, session)
                    if parent_run:
                        candidate_files = (
                            parent_run.get_failed_input_files()
                            if failed_inputs_only
                            else parent_run.get_all_input_files()
                        )
                        target_paths = [Path(f) for f in candidate_files]
            except Exception as lookup_err:
                logger.warning(
                    "Failed to query parent run '%s' for retry: %s",
                    failed_run_id,
                    lookup_err,
                )

        if not target_paths:
            logger.warning(
                "Retry requested for run '%s' but no target paths could be discovered",
                failed_run_id,
            )

        return self.run(
            target_paths,
            enable_resolution=enable_resolution,
            trigger_type="retry",
            retry_of_run_id=failed_run_id,
        )

    def run_complete_pipeline(
        self, file_paths: Sequence[Union[str, Path]]
    ) -> ETLPipelineResult:
        """Execute complete end-to-end pipeline with event resolution enabled."""
        return self.run(file_paths, enable_resolution=True)
