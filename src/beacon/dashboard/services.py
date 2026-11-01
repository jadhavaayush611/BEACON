"""Service layer for the BEACON Streamlit dashboard.

Encapsulates data retrieval, pipeline execution, file ingestion,
and analytics aggregation so UI components remain presentation-only.
"""

from datetime import timezone
import logging
from pathlib import Path
import shutil
import tempfile
import time
from typing import Any, Optional, Sequence, Union

from sqlalchemy import distinct, func, inspect, select, text
from sqlalchemy.orm import Session, sessionmaker

from beacon.analytics.models import AnalyticsFilter, AnalyticsSummary
from beacon.analytics.service import AnalyticsService
from beacon.config import Settings, get_settings
from beacon.ingestion import IngestionResult, ingest_file
from beacon.logging_config import redact_sensitive_text
from beacon.models import DisasterObservation, UnifiedDisasterEvent
from beacon.pipeline.result import ETLPipelineResult, PipelineRunRecord
from beacon.pipeline.runner import ETLRunner
from beacon.storage import (
    EventModel,
    EventObservationModel,
    MergeCandidateModel,
    ObservationModel,
    PipelineRunModel,
    QualityResultModel,
    ResolutionDecisionModel,
    SourceModel,
    StorageRepository,
    create_db_engine,
    get_session,
    get_session_factory,
)

logger = logging.getLogger("beacon.dashboard.services")

REQUIRED_TABLE_MODELS: tuple[tuple[type, str], ...] = (
    (SourceModel, "sources"),
    (ObservationModel, "observations"),
    (QualityResultModel, "data_quality_results"),
    (EventModel, "events"),
    (EventObservationModel, "event_observations"),
    (ResolutionDecisionModel, "resolution_decisions"),
    (MergeCandidateModel, "merge_candidates"),
    (PipelineRunModel, "pipeline_runs"),
)

REQUIRED_TABLES: tuple[str, ...] = tuple(name for _, name in REQUIRED_TABLE_MODELS)


class DashboardService:
    """Provides backend data and pipeline services to the BEACON dashboard."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        session_factory: Optional[sessionmaker[Session]] = None,
        repository: Optional[StorageRepository] = None,
        analytics_service: Optional[AnalyticsService] = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.repository = repository or StorageRepository()
        self.analytics_service = analytics_service or AnalyticsService()
        self.session_factory = session_factory
        self._explicit_session_factory = session_factory is not None
        self._bound_database_url: Optional[str] = (
            self._extract_engine_url(session_factory) if session_factory else self.settings.database_url
        )

        # Initialize session factory lazily if database_url is configured
        if self.session_factory is None and self.settings.database_url:
            try:
                engine = create_db_engine(settings=self.settings)
                self.session_factory = get_session_factory(engine)
                self._bound_database_url = self.settings.database_url
            except Exception as err:
                logger.warning("Dashboard could not connect to configured database: %s", err)

        # In-memory storage for file-based runs when database is not connected
        self._in_memory_events: list[UnifiedDisasterEvent] = []
        self._in_memory_observations: list[DisasterObservation] = []
        self._in_memory_resolution_outcomes: list[Any] = []
        self._in_memory_pipeline_runs: list[PipelineRunRecord] = []
        self._last_pipeline_result: Optional[ETLPipelineResult] = None

    def _extract_engine_url(self, factory: Optional[sessionmaker[Session]]) -> Optional[str]:
        """Safely extract the bound engine URL string from a sessionmaker if available."""
        if factory is None:
            return None
        bind = getattr(factory, "kw", {}).get("bind")
        if bind is not None and hasattr(bind, "url"):
            return str(bind.url)
        return None

    def _ensure_session_factory(self) -> Optional[sessionmaker[Session]]:
        """Ensure session factory is initialized and synchronized with current settings."""
        target_url = self.settings.database_url
        if not target_url:
            if not self._explicit_session_factory:
                self.session_factory = None
                self._bound_database_url = None
            return self.session_factory

        if self.session_factory is not None and not self._explicit_session_factory:
            current_bound = self._bound_database_url or self._extract_engine_url(self.session_factory)
            if current_bound != target_url:
                logger.info("Database URL changed; refreshing session factory")
                self.session_factory = None
                self._bound_database_url = None

        if self.session_factory is None:
            try:
                engine = create_db_engine(settings=self.settings)
                self.session_factory = get_session_factory(engine)
                self._bound_database_url = target_url
            except Exception as err:
                logger.debug(
                    "Failed to initialize session factory for '%s': %s",
                    redact_sensitive_text(target_url),
                    err,
                )
                return None

        return self.session_factory

    @property
    def is_database_reachable(self) -> bool:
        """Return True if a database session factory is available and responds to a ping query."""
        factory = self._ensure_session_factory()
        if factory is None:
            return False
        try:
            with get_session(factory) as session:
                session.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    @property
    def is_schema_ready(self) -> bool:
        """Return True if database is reachable and all required BEACON schema tables exist."""
        factory = self._ensure_session_factory()
        if factory is None:
            return False
        try:
            with get_session(factory) as session:
                bind = session.get_bind()
                inspector = inspect(bind)
                current_schema = (
                    session.execute(text("SELECT current_schema()")).scalar()
                    if bind.dialect.name == "postgresql"
                    else None
                )
                existing = set(inspector.get_table_names(schema=current_schema))
                return set(REQUIRED_TABLES).issubset(existing)
        except Exception as err:
            logger.debug("Schema readiness check failed: %s", err)
            return False

    @property
    def is_database_connected(self) -> bool:
        """Return True if database is reachable and schema tables are ready for persistence.

        Distinguishes server reachability (see `is_database_reachable`) from schema readiness
        (see `is_schema_ready`). Prevents queries against uninitialized databases.
        """
        return self.is_schema_ready

    def test_database_connection(self) -> dict[str, Any]:
        """Test database connection and return detailed diagnostic status."""
        if not self.settings.database_url:
            return {
                "connected": False,
                "schema_ready": False,
                "status": "unconfigured",
                "message": "BEACON_DATABASE_URL is not set. Running in-memory mode.",
                "database_url_masked": "None",
                "engine_type": "In-Memory",
                "tables": {},
                "total_tables": len(REQUIRED_TABLES),
                "ready_tables": 0,
            }

        masked_url = "[REDACTED]"

        try:
            factory = self._ensure_session_factory()
            if factory is None:
                return {
                    "connected": False,
                    "schema_ready": False,
                    "status": "error",
                    "message": f"Connection initialization failed for {masked_url}",
                    "engine_type": "PostgreSQL",
                    "database_url_masked": masked_url,
                    "tables": {},
                    "total_tables": len(REQUIRED_TABLES),
                    "ready_tables": 0,
                }
            tables_count: dict[str, int] = {}
            with get_session(factory) as session:
                bind = session.get_bind()
                inspector = inspect(bind)
                current_schema = (
                    session.execute(text("SELECT current_schema()")).scalar()
                    if bind.dialect.name == "postgresql"
                    else None
                )
                existing_tables = set(inspector.get_table_names(schema=current_schema))
                for model, name in REQUIRED_TABLE_MODELS:
                    if name in existing_tables:
                        try:
                            count = session.execute(select(func.count()).select_from(model)).scalar() or 0
                            tables_count[name] = count
                        except Exception:
                            tables_count[name] = -1
                    else:
                        tables_count[name] = -1  # Table does not exist

            total_required = len(REQUIRED_TABLES)
            ready_count = sum(1 for c in tables_count.values() if c >= 0)
            missing = [name for name, count in tables_count.items() if count == -1]
            if missing:
                if missing == ["pipeline_runs"]:
                    migration_hint = "Run 'python scripts/init_db.py' to initialize the schema (or 'python scripts/migrate_v1_0.py' for additive v1.0 upgrade)."
                else:
                    migration_hint = "Run 'python scripts/init_db.py' to initialize the schema."
                return {
                    "connected": True,
                    "schema_ready": False,
                    "status": "uninitialized",
                    "message": (
                        f"Connected to database, but {len(missing)} required table(s) are missing: "
                        f"{', '.join(missing)}. {migration_hint}"
                    ),
                    "database_url_masked": masked_url,
                    "engine_type": "PostgreSQL",
                    "tables": tables_count,
                    "total_tables": total_required,
                    "ready_tables": ready_count,
                }

            return {
                "connected": True,
                "schema_ready": True,
                "status": "healthy",
                "message": "Connected to database successfully.",
                "database_url_masked": masked_url,
                "engine_type": "PostgreSQL",
                "tables": tables_count,
                "total_tables": total_required,
                "ready_tables": ready_count,
            }
        except Exception as err:
            return {
                "connected": False,
                "schema_ready": False,
                "status": "error",
                "message": f"Connection failed: {redact_sensitive_text(str(err))}",
                "engine_type": "PostgreSQL",
                "database_url_masked": masked_url,
                "tables": {},
                "total_tables": len(REQUIRED_TABLES),
                "ready_tables": 0,
            }

    def get_system_overview(self) -> dict[str, Any]:
        """Collect aggregated overview metrics for the executive dashboard overview."""
        if self.is_database_connected:
            assert self.session_factory is not None
            try:
                with get_session(self.session_factory) as session:
                    summary = self.analytics_service.summarize_db(session)
                    sources_count = session.execute(
                        select(func.count(distinct(SourceModel.source_id)))
                    ).scalar() or 0
                    return {
                        "sources_count": sources_count,
                        "observations_count": summary.total_observations,
                        "events_count": summary.total_events,
                        "multi_source_events_count": summary.multi_source_events,
                        "multi_source_ratio": summary.multi_source_ratio,
                        "compression_ratio": summary.compression_ratio,
                        "coverage_gaps": summary.coverage_gaps,
                        "conflicting_claims_count": summary.conflicting_claims_count,
                        "ambiguous_count": summary.ambiguous_observations_count,
                        "merge_candidates_count": summary.merge_candidates_count,
                        "events_by_type": summary.events_by_type,
                        "events_by_severity": summary.events_by_severity,
                        "is_live_db": True,
                        "storage_mode": "postgresql",
                        "db_connected": True,
                        "schema_ready": True,
                        "uninitialized_db": False,
                    }
            except Exception as err:
                logger.warning(
                    "Database query failed during overview aggregation; falling back to in-memory: %s",
                    err,
                )

        # Fallback to in-memory state if DB is not connected / schema uninitialized
        in_mem_ambig = sum(
            1 for out in self._in_memory_resolution_outcomes
            if getattr(out, "decision", None) and getattr(out.decision, "value", str(out.decision)) == "ambiguous_candidate"
        )
        summary = self.analytics_service.summarize(
            events=self._in_memory_events,
            observations=self._in_memory_observations,
            ambiguous_count=in_mem_ambig,
        )
        sources = {o.source_id for o in self._in_memory_observations}
        db_reachable = self.is_database_reachable
        schema_ready = self.is_schema_ready
        return {
            "sources_count": len(sources),
            "observations_count": summary.total_observations,
            "events_count": summary.total_events,
            "multi_source_events_count": summary.multi_source_events,
            "multi_source_ratio": summary.multi_source_ratio,
            "compression_ratio": summary.compression_ratio,
            "coverage_gaps": summary.coverage_gaps,
            "conflicting_claims_count": summary.conflicting_claims_count,
            "ambiguous_count": summary.ambiguous_observations_count,
            "merge_candidates_count": summary.merge_candidates_count,
            "events_by_type": summary.events_by_type,
            "events_by_severity": summary.events_by_severity,
            "is_live_db": False,
            "storage_mode": "in_memory",
            "db_connected": db_reachable,
            "schema_ready": schema_ready,
            "uninitialized_db": db_reachable and not schema_ready,
        }

    def ingest_and_process_uploads(
        self,
        uploads: Sequence[tuple[str, bytes]],
        enable_resolution: bool = True,
        persist_db: bool = True,
    ) -> dict[str, Any]:
        """Execute complete structured multi-stage ingestion pipeline for uploaded files.

        Implements all 9 operational stages:
        1. File received
        2. File type and integrity validation
        3. Format-specific extraction
        4. Normalization and field mapping
        5. Data-quality validation
        6. Observation creation
        7. Event matching and resolution
        8. Persistence (transactional PostgreSQL with rollback, or stateless in-memory)
        9. Final summary

        Guarantees idempotent persistence, partial-failure tolerance, and zero-fabrication.
        """
        start_ts = time.time()
        stages: list[dict[str, Any]] = []
        errors: list[str] = []
        warnings: list[str] = []

        temp_dir = Path(tempfile.mkdtemp(prefix="beacon_upload_"))
        try:
            # Stage 1: File Received
            t0 = time.time()
            total_files = len(uploads)
            stages.append({
                "stage_id": "files_received",
                "name": "1. File Received",
                "status": "completed" if total_files > 0 else "failed",
                "message": f"Received {total_files} file payload(s) for multi-format processing.",
                "counts": {"files_received": total_files},
                "elapsed": round(time.time() - t0, 3),
            })
            if total_files == 0:
                return {
                    "stages": stages,
                    "ingestion_results": [],
                    "observations": [],
                    "events": [],
                    "resolution_outcomes": [],
                    "persistence_mode": "none",
                    "errors": ["No files uploaded."],
                    "warnings": [],
                }

            # Stage 2: Format Recognition & Preflight
            t0 = time.time()
            valid_files: list[tuple[str, bytes, Path]] = []
            invalid_files: list[tuple[str, str]] = []
            from beacon.ingestion.dispatcher import EXTENSION_INGESTOR_MAP

            for filename, content in uploads:
                safe_name = Path(filename).name
                ext = Path(safe_name).suffix.lower()
                if ext not in EXTENSION_INGESTOR_MAP:
                    invalid_files.append((safe_name, f"Unsupported format '{ext}'"))
                elif len(content) == 0:
                    invalid_files.append((safe_name, "Zero-byte empty file payload"))
                else:
                    file_path = temp_dir / safe_name
                    file_path.write_bytes(content)
                    valid_files.append((safe_name, content, file_path))

            stage2_status = "completed"
            if invalid_files and valid_files:
                stage2_status = "warning"
                warnings.extend([f"[{fn}]: {reason}" for fn, reason in invalid_files])
            elif invalid_files and not valid_files:
                stage2_status = "failed"
                errors.extend([f"[{fn}]: {reason}" for fn, reason in invalid_files])

            stages.append({
                "stage_id": "validation_integrity",
                "name": "2. Format Recognition & Preflight",
                "status": stage2_status,
                "message": f"Recognized {len(valid_files)}/{total_files} file format signature(s) eligible for extraction.",
                "counts": {"files_valid": len(valid_files), "preflight_passed": len(valid_files), "files_invalid": len(invalid_files), "unsupported_or_empty": len(invalid_files)},
                "details": [f"{fn}: {r}" for fn, r in invalid_files] if invalid_files else None,
                "elapsed": round(time.time() - t0, 3),
            })

            # Stage 3: Format-Specific Extraction
            t0 = time.time()
            ingestion_results: list[IngestionResult] = []
            extracted_observations: list[DisasterObservation] = []
            for safe_name, _content, file_path in valid_files:
                res = ingest_file(file_path, raise_on_unsupported=False, raise_on_missing=False)
                ingestion_results.append(res)
                extracted_observations.extend(res.observations)
                if res.errors:
                    errors.extend([f"[{safe_name}]: {e}" for e in res.errors])
                if res.warnings:
                    warnings.extend([f"[{safe_name}]: {w}" for w in res.warnings])

            files_succeeded = sum(1 for r in ingestion_results if r.is_success)
            files_yielding_observations = sum(1 for r in ingestion_results if r.is_success and len(r.observations) > 0)
            files_yielding_metadata = sum(1 for r in ingestion_results if r.is_success and len(r.observations) == 0)
            files_partially_successful = sum(1 for r in ingestion_results if r.is_partial)
            files_failed = sum(1 for r in ingestion_results if r.is_failed) + len(invalid_files)
            if files_failed > 0 and (files_succeeded > 0 or files_partially_successful > 0):
                stage3_status = "warning"
            elif files_failed > 0 and files_succeeded == 0 and files_partially_successful == 0:
                stage3_status = "failed"
            elif files_partially_successful > 0:
                stage3_status = "warning"
            else:
                stage3_status = "completed"

            if files_failed > 0:
                stage3_msg = (
                    f"Extracted {len(extracted_observations)} raw observation(s) from "
                    f"{files_yielding_observations} file(s) ({files_yielding_metadata} file(s) yielded metadata/documentation); "
                    f"{files_failed} file(s) failed extraction."
                )
            elif files_partially_successful > 0:
                stage3_msg = (
                    f"Extracted {len(extracted_observations)} raw observation(s) from {files_yielding_observations} file(s); "
                    f"{files_partially_successful} file(s) partially extracted with warnings."
                )
            else:
                stage3_msg = (
                    f"Extracted {len(extracted_observations)} raw observation(s) across {files_yielding_observations} data file(s); "
                    f"{files_yielding_metadata} supporting file(s) yielded verified metadata."
                )

            stage3_counts = {
                "observations_extracted": len(extracted_observations),
                "files_succeeded": files_succeeded,
                "yielding_observations": files_yielding_observations,
                "yielding_metadata": files_yielding_metadata,
                "files_failed": files_failed,
            }
            if files_partially_successful > 0:
                stage3_counts["files_partial"] = files_partially_successful

            stages.append({
                "stage_id": "extraction",
                "name": "3. Format-Specific Extraction",
                "status": stage3_status,
                "message": stage3_msg,
                "counts": stage3_counts,
                "elapsed": round(time.time() - t0, 3),
            })

            # Stage 4: Normalization & Field Mapping
            t0 = time.time()
            from beacon.pipeline.transform import ObservationTransformer
            transformer = ObservationTransformer()
            transformed_observations = transformer.transform_batch(extracted_observations)
            stages.append({
                "stage_id": "normalization",
                "name": "4. Normalization & Field Mapping",
                "status": "completed",
                "message": f"Normalized spatial bounds, UTC temporal anchors, and severity levels for {len(transformed_observations)} record(s).",
                "counts": {"observations_normalized": len(transformed_observations)},
                "elapsed": round(time.time() - t0, 3),
            })

            # Stage 5: Data-Quality Validation
            t0 = time.time()
            from beacon.quality import DataQualityValidator
            validator = DataQualityValidator()
            quality_report = validator.validate(transformed_observations)
            stage5_status = "completed"
            if quality_report.rejected_record_ids:
                stage5_status = "warning" if quality_report.accepted_records else "failed"

            stages.append({
                "stage_id": "data_quality",
                "name": "5. Data-Quality Validation",
                "status": stage5_status,
                "message": f"Quality audit: {quality_report.passed_count} checks passed, {len(quality_report.accepted_records)} accepted, {len(quality_report.rejected_record_ids)} rejected.",
                "counts": {
                    "passed_checks": quality_report.passed_count,
                    "records_accepted": len(quality_report.accepted_records),
                    "records_rejected": len(quality_report.rejected_record_ids),
                },
                "elapsed": round(time.time() - t0, 3),
            })

            # Stage 6: Observation Creation
            t0 = time.time()
            accepted_observations = quality_report.accepted_records
            incident_obs_count = sum(1 for o in accepted_observations if getattr(o, "is_incident", True))
            context_obs_count = sum(1 for o in accepted_observations if not getattr(o, "is_incident", True))
            stages.append({
                "stage_id": "observation_creation",
                "name": "6. Observation Creation",
                "status": "completed",
                "message": (
                    f"Formed {len(accepted_observations)} canonical observation(s) "
                    f"({incident_obs_count} incident, {context_obs_count} context) with deterministic IDs."
                ),
                "counts": {
                    "observations_ready": len(accepted_observations),
                    "incident_observations": incident_obs_count,
                    "context_observations": context_obs_count,
                },
                "elapsed": round(time.time() - t0, 3),
            })

            # Stage 7: Event Matching & Resolution
            t0 = time.time()
            from beacon.resolution.resolver import EventResolver
            resolver = EventResolver(settings=self.settings)
            resolved_events: list[UnifiedDisasterEvent] = []
            resolution_outcomes: list[Any] = []
            resolution_result = None

            if enable_resolution and accepted_observations:
                if self.is_database_connected:
                    assert self.session_factory is not None
                    with get_session(self.session_factory) as session:
                        existing_events, existing_event_obs = self.repository.get_all_events_with_observations(session)
                else:
                    existing_events = list(self._in_memory_events)
                    existing_event_obs = {
                        ev.event_id: [o for o in self._in_memory_observations if o.observation_id in ev.observation_ids]
                        for ev in self._in_memory_events
                    }

                incident_accepted = [o for o in accepted_observations if getattr(o, "is_incident", True)]
                resolution_result = resolver.resolve_observations(
                    incident_accepted,
                    existing_events=existing_events,
                    existing_event_observations=existing_event_obs,
                )
                resolved_events = resolution_result.resolved_events
                resolution_outcomes = resolution_result.outcomes

                events_created = resolution_result.events_created
                events_updated = resolution_result.events_updated
                active_portfolio = len(resolved_events)
                ambiguous_count = resolution_result.ambiguous_count

                stages.append({
                    "stage_id": "event_resolution",
                    "name": "7. Event Matching & Resolution",
                    "status": "completed",
                    "message": (
                        f"Resolution complete: {events_created} new event(s) created, "
                        f"{events_updated} existing event(s) updated "
                        f"({active_portfolio} total active event(s) in portfolio)."
                    ),
                    "counts": {
                        "created_this_run": events_created,
                        "updated_this_run": events_updated,
                        "active_portfolio": active_portfolio,
                        "ambiguous_count": ambiguous_count,
                    },
                    "elapsed": round(time.time() - t0, 3),
                })
            else:
                stages.append({
                    "stage_id": "event_resolution",
                    "name": "7. Event Matching & Resolution",
                    "status": "skipped",
                    "message": "Event resolution skipped per user configuration." if not enable_resolution else "No accepted observations to resolve.",
                    "counts": {
                        "created_this_run": 0,
                        "updated_this_run": 0,
                        "active_portfolio": len(self._in_memory_events),
                    },
                    "elapsed": round(time.time() - t0, 3),
                })

            # Stage 8: Persistence
            t0 = time.time()
            records_persisted = 0
            events_persisted = 0
            active_events_persisted = 0
            persistence_mode = "in_memory"

            can_persist_db = self.is_database_connected and persist_db
            if can_persist_db:
                assert self.session_factory is not None
                try:
                    with get_session(self.session_factory) as session:
                        for file_res in ingestion_results:
                            src_rec = file_res.source_record
                            src_accepted = [o for o in accepted_observations if o.source_id == src_rec.source_id]
                            src_outcomes = [o for o in quality_report.outcomes if o.source_id == src_rec.source_id]
                            counts = self.repository.save_batch(
                                source_record=src_rec,
                                accepted_observations=src_accepted,
                                quality_outcomes=src_outcomes,
                                session=session,
                            )
                            records_persisted += counts["observations_persisted"]

                        if resolution_result:
                            res_counts = self.repository.save_resolution_result(resolution_result, session)
                            events_persisted = res_counts.get(
                                "events_persisted_this_run",
                                len({out.assigned_event_id for out in resolution_result.outcomes if out.assigned_event_id}),
                            )
                            active_events_persisted = res_counts.get("active_portfolio", res_counts.get("events_persisted", 0))

                    persistence_mode = "postgresql"
                    stages.append({
                        "stage_id": "persistence",
                        "name": "8. Persistence",
                        "status": "completed",
                        "message": (
                            f"Persisted {records_persisted} observation(s) and {events_persisted} event(s) this run "
                            f"({active_events_persisted} total active event(s) in portfolio) to PostgreSQL."
                        ),
                        "counts": {
                            "records_persisted": records_persisted,
                            "events_persisted_this_run": events_persisted,
                            "active_portfolio": active_events_persisted,
                        },
                        "elapsed": round(time.time() - t0, 3),
                    })
                except Exception as db_err:
                    persistence_mode = "failed"
                    err_msg = f"Database transaction failed: {redact_sensitive_text(str(db_err))}"
                    errors.append(err_msg)
                    stages.append({
                        "stage_id": "persistence",
                        "name": "8. Persistence",
                        "status": "failed",
                        "message": err_msg,
                        "counts": {"records_persisted": 0, "events_persisted_this_run": 0},
                        "elapsed": round(time.time() - t0, 3),
                    })
            else:
                # In-memory persistence / caching
                obs_map = {obs.observation_id: obs for obs in self._in_memory_observations}
                for obs in accepted_observations:
                    obs_map[obs.observation_id] = obs
                self._in_memory_observations = list(obs_map.values())
                if resolved_events:
                    self._in_memory_events = resolved_events
                if resolution_outcomes:
                    self._in_memory_resolution_outcomes = resolution_outcomes

                diag = self.test_database_connection()
                missing_tables = [k for k, v in diag.get("tables", {}).items() if v == -1]
                if diag.get("connected") and not diag.get("schema_ready"):
                    msg = (
                        f"PostgreSQL connected but schema is incomplete ({len(missing_tables)} missing table(s)): "
                        f"cached {len(accepted_observations)} observation(s) in-memory without persistent DB writes."
                    )
                elif not self.settings.database_url:
                    msg = f"In-memory mode (stateless): cached {len(accepted_observations)} observation(s) in application session."
                else:
                    msg = f"Persistence disabled by user: cached {len(accepted_observations)} observation(s) in-memory without persistent DB writes."

                stages.append({
                    "stage_id": "persistence",
                    "name": "8. Persistence",
                    "status": "warning",
                    "message": msg,
                    "counts": {
                        "in_memory_cached": len(accepted_observations),
                        "events_persisted_this_run": 0,
                        "active_portfolio": len(resolved_events) if resolved_events else len(self._in_memory_events),
                        "db_writes": 0,
                    },
                    "elapsed": round(time.time() - t0, 3),
                })

            # Stage 9: Final Summary
            total_elapsed = round(time.time() - start_ts, 3)
            final_status = "completed"
            if errors:
                final_status = "failed" if (files_succeeded == 0 and files_partially_successful == 0) else "warning"
            elif warnings or stage2_status == "warning" or stage3_status == "warning":
                final_status = "warning"

            summary_reasons = []
            if files_failed > 0:
                summary_reasons.append(f"{files_failed}/{total_files} file(s) failed extraction")
            if files_partially_successful > 0:
                summary_reasons.append(f"{files_partially_successful} file(s) partially extracted")
            if quality_report.rejected_record_ids:
                summary_reasons.append(f"{len(quality_report.rejected_record_ids)} observation(s) rejected by DQ audit")
            if persistence_mode != "postgresql" and self.settings.database_url:
                summary_reasons.append("PostgreSQL schema incomplete (cached in-memory)")

            events_created_val = resolution_result.events_created if resolution_result else 0
            events_updated_val = resolution_result.events_updated if resolution_result else 0
            active_events_val = len(resolved_events) if resolved_events else len(self._in_memory_events)

            if final_status == "completed":
                inc_cnt = sum(1 for o in accepted_observations if getattr(o, "is_incident", True))
                ctx_cnt = sum(1 for o in accepted_observations if not getattr(o, "is_incident", True))
                summary_msg = (
                    f"Batch completed successfully in {total_elapsed}s: All {total_files} file(s) successfully processed "
                    f"({files_yielding_observations} yielding incident observations, {files_yielding_metadata} yielding context observations). "
                    f"Formed {len(accepted_observations)} total observation(s) ({inc_cnt} incident, {ctx_cnt} context); "
                    f"{events_created_val} event(s) created, {events_updated_val} event(s) updated; "
                    f"{active_events_val} total active disaster event(s) in portfolio."
                )
            elif final_status == "failed":
                summary_msg = (
                    f"Batch failed in {total_elapsed}s: All {total_files} file(s) failed processing "
                    f"({'; '.join(summary_reasons)})."
                )
            else:
                reason_suffix = f" [{'; '.join(summary_reasons)}]" if summary_reasons else ""
                if files_failed > 0:
                    summary_msg = (
                        f"Partial batch completed with warnings in {total_elapsed}s{reason_suffix}: "
                        f"The batch was partial because {files_failed} of {total_files} file(s) failed extraction, "
                        f"even though {len(accepted_observations)} valid observation(s) were accepted "
                        f"({events_created_val} created this run, {events_updated_val} updated this run); "
                        f"{active_events_val} total active event(s) in portfolio."
                    )
                else:
                    summary_msg = (
                        f"Batch completed with {len(warnings)} notice(s) in {total_elapsed}s{reason_suffix}: "
                        f"All {total_files} file(s) successfully processed ({files_yielding_observations} yielding observations, "
                        f"{files_yielding_metadata} yielding metadata/documentation); "
                        f"{len(accepted_observations)} observation(s) accepted "
                        f"({events_created_val} created this run, {events_updated_val} updated this run); "
                        f"{active_events_val} total active event(s) in portfolio."
                    )

            stages.append({
                "stage_id": "final_summary",
                "name": "9. Final Summary",
                "status": final_status,
                "message": summary_msg,
                "counts": {
                    "total_files": total_files,
                    "files_succeeded": files_succeeded,
                    "files_failed": files_failed,
                    "observations_accepted": len(accepted_observations),
                    "created_this_run": events_created_val,
                    "updated_this_run": events_updated_val,
                    "active_portfolio": active_events_val,
                },
                "elapsed": total_elapsed,
            })

            return {
                "stages": stages,
                "ingestion_results": ingestion_results,
                "observations": accepted_observations,
                "events": resolved_events,
                "resolution_outcomes": resolution_outcomes,
                "quality_report": quality_report,
                "persistence_mode": persistence_mode,
                "errors": errors,
                "warnings": warnings,
            }
        finally:
            if temp_dir.exists() and temp_dir.name.startswith("beacon_upload_"):
                shutil.rmtree(temp_dir, ignore_errors=True)

    def process_uploaded_files(
        self,
        uploads: Sequence[tuple[str, bytes]],
        cleanup: bool = True,
    ) -> tuple[list[IngestionResult], Path]:
        """Write uploaded bytes to a managed temporary directory and ingest each file.

        Guarantees temporary directory cleanup in a finally block while preserving
        full extraction results and diagnostics.

        Args:
            uploads: Sequence of (filename, file_bytes) tuples.
            cleanup: Whether to remove the temporary directory after processing (default True).

        Returns:
            Tuple of (list of IngestionResult, temporary directory Path).
        """
        temp_dir = Path(tempfile.mkdtemp(prefix="beacon_upload_"))
        results: list[IngestionResult] = []

        try:
            for filename, content in uploads:
                safe_name = Path(filename).name
                file_path = temp_dir / safe_name
                file_path.write_bytes(content)

                res = ingest_file(file_path, raise_on_unsupported=False, raise_on_missing=False)
                results.append(res)

            return results, temp_dir
        finally:
            if cleanup and temp_dir.exists() and temp_dir.name.startswith("beacon_upload_"):
                shutil.rmtree(temp_dir, ignore_errors=True)

    def run_pipeline_on_paths(
        self,
        file_paths: Sequence[Union[str, Path]],
        enable_resolution: bool = True,
        persist_db: bool = True,
    ) -> ETLPipelineResult:
        """Execute ETL and resolution on the given file paths."""
        effective_factory = self.session_factory if persist_db else None
        effective_settings = self.settings
        if not persist_db and self.settings.database_url:
            effective_settings = self.settings.model_copy(update=dict(database_url=None))
        runner = ETLRunner(
            settings=effective_settings,
            session_factory=effective_factory,
            repository=self.repository,
        )

        result = runner.run(file_paths, enable_resolution=enable_resolution)
        self._last_pipeline_result = result
        self._in_memory_pipeline_runs.insert(0, result.to_run_record())

        # Update in-memory state if DB was not used
        if not persist_db or effective_factory is None:
            # Deduplicate observations by observation_id to prevent inflation on reruns,
            # while preserving distinct observations and updating existing ones.
            obs_map = {obs.observation_id: obs for obs in self._in_memory_observations}
            for obs in result.quality_report.accepted_records:
                obs_map[obs.observation_id] = obs
            self._in_memory_observations = list(obs_map.values())

            if enable_resolution and result.quality_report.accepted_records:
                in_memory_obs_by_event: dict[str, list[DisasterObservation]] = {}
                for ev in self._in_memory_events:
                    in_memory_obs_by_event[ev.event_id] = [
                        o for o in self._in_memory_observations if o.observation_id in ev.observation_ids
                    ]
                res_out = runner.resolver.resolve_observations(
                    result.quality_report.accepted_records,
                    existing_events=self._in_memory_events,
                    existing_event_observations=in_memory_obs_by_event,
                )
                self._in_memory_events = res_out.resolved_events
                self._in_memory_resolution_outcomes = res_out.outcomes

        return result

    def list_pipeline_runs(self, limit: int = 20) -> list[PipelineRunRecord]:
        """Fetch recent pipeline run records from database or in-memory history."""
        if self.is_database_connected:
            assert self.session_factory is not None
            try:
                with get_session(self.session_factory) as session:
                    return self.repository.list_pipeline_runs(session, limit=limit)
            except Exception as err:
                logger.warning("Could not query pipeline runs from database: %s", err)
        return self._in_memory_pipeline_runs[:limit]

    def get_pipeline_run(self, run_id: str) -> Optional[PipelineRunRecord]:
        """Fetch a pipeline run record by its run_id."""
        if self.is_database_connected:
            assert self.session_factory is not None
            try:
                with get_session(self.session_factory) as session:
                    return self.repository.get_pipeline_run(run_id, session)
            except Exception as err:
                logger.warning("Could not query pipeline run '%s' from database: %s", run_id, err)
        return next((r for r in self._in_memory_pipeline_runs if r.run_id == run_id), None)

    def retry_pipeline_run(
        self,
        run_id: str,
        failed_inputs_only: bool = False,
        persist_db: bool = True,
    ) -> ETLPipelineResult:
        """Retry a previously executed pipeline run safely.

        Discovers original inputs from the run record, preserves retry lineage,
        and ensures transactional idempotency.
        """
        record = self.get_pipeline_run(run_id)
        target_paths: list[Path] = []
        if record:
            candidate_files = (
                record.get_failed_input_files()
                if failed_inputs_only
                else record.get_all_input_files()
            )
            for f in candidate_files:
                p = Path(f)
                if p.exists():
                    target_paths.append(p)

        if not target_paths and self._last_pipeline_result:
            if self._last_pipeline_result.execution_id == run_id:
                for fr in self._last_pipeline_result.file_results:
                    if failed_inputs_only and not fr.is_failed:
                        continue
                    p = Path(fr.source_record.source_file)
                    if p.exists():
                        target_paths.append(p)

        if not target_paths:
            sample_dir = Path("data/sample")
            target_paths = [
                sample_dir / "events.json",
                sample_dir / "events.csv",
                sample_dir / "events.txt",
                sample_dir / "sample_report.pdf",
                sample_dir / "sample_disaster.png",
            ]
            target_paths = [p for p in target_paths if p.exists()]

        effective_factory = self.session_factory if persist_db else None
        effective_settings = self.settings
        if not persist_db and self.settings.database_url:
            effective_settings = self.settings.model_copy(update=dict(database_url=None))

        runner = ETLRunner(
            settings=effective_settings,
            session_factory=effective_factory,
            repository=self.repository,
        )

        result = runner.run(
            target_paths,
            enable_resolution=True,
            trigger_type="retry",
            retry_of_run_id=run_id,
            persist_run_record=persist_db,
        )
        self._last_pipeline_result = result
        self._in_memory_pipeline_runs.insert(0, result.to_run_record())

        if not persist_db or effective_factory is None:
            obs_map = {obs.observation_id: obs for obs in self._in_memory_observations}
            for obs in result.quality_report.accepted_records:
                obs_map[obs.observation_id] = obs
            self._in_memory_observations = list(obs_map.values())

            if result.quality_report.accepted_records:
                in_memory_obs_by_event: dict[str, list[DisasterObservation]] = {}
                for ev in self._in_memory_events:
                    in_memory_obs_by_event[ev.event_id] = [
                        o for o in self._in_memory_observations if o.observation_id in ev.observation_ids
                    ]
                res_out = runner.resolver.resolve_observations(
                    result.quality_report.accepted_records,
                    existing_events=self._in_memory_events,
                    existing_event_observations=in_memory_obs_by_event,
                )
                self._in_memory_events = res_out.resolved_events
                self._in_memory_resolution_outcomes = res_out.outcomes

        return result

    def query_events(
        self, filter_criteria: Optional[AnalyticsFilter] = None
    ) -> list[UnifiedDisasterEvent]:
        """Query unified disaster events from database or in-memory store."""
        if self.is_database_connected:
            assert self.session_factory is not None
            with get_session(self.session_factory) as session:
                stmt = select(EventModel)
                if filter_criteria:
                    if filter_criteria.event_type:
                        stmt = stmt.where(EventModel.event_type == filter_criteria.event_type.lower())
                    if filter_criteria.severity:
                        stmt = stmt.where(EventModel.severity == filter_criteria.severity.lower())
                    if filter_criteria.location:
                        stmt = stmt.where(EventModel.location.ilike(f"%{filter_criteria.location}%"))
                    if filter_criteria.start_time_after:
                        stmt = stmt.where(EventModel.start_time >= filter_criteria.start_time_after)
                    if filter_criteria.end_time_before:
                        stmt = stmt.where(EventModel.start_time <= filter_criteria.end_time_before)

                models = session.execute(stmt).scalars().all()
                events = [self.repository.get_event(m.event_id, session) for m in models]
                valid = [e for e in events if e is not None]
                if filter_criteria and filter_criteria.min_sources:
                    valid = [e for e in valid if len(e.source_ids) >= filter_criteria.min_sources]
                return valid

        # In-memory query
        return self.analytics_service.filter_events(
            self._in_memory_events, filter_criteria
        )

    def get_event_detail(self, event_id: str) -> Optional[dict[str, Any]]:
        """Retrieve complete event profile with constituent observations and audit decisions."""
        if self.is_database_connected:
            assert self.session_factory is not None
            with get_session(self.session_factory) as session:
                event = self.repository.get_event(event_id, session)
                if not event:
                    return None

                # Fetch constituent observations via junction table
                obs_stmt = (
                    select(ObservationModel)
                    .join(
                        EventObservationModel,
                        ObservationModel.observation_id == EventObservationModel.observation_id,
                    )
                    .where(EventObservationModel.event_id == event_id)
                )
                obs_models = session.execute(obs_stmt).scalars().all()
                obs_list: list[DisasterObservation] = []
                for o in obs_models:
                    ev_time = o.event_time
                    if ev_time is not None and ev_time.tzinfo is None:
                        ev_time = ev_time.replace(tzinfo=timezone.utc)
                    obs_list.append(
                        DisasterObservation(
                            observation_id=o.observation_id,
                            source_id=o.source_id,
                            event_type=o.event_type,
                            location=o.location,
                            event_time=ev_time,
                            description=o.description,
                            severity=o.severity,
                            latitude=o.latitude,
                            longitude=o.longitude,
                            extraction_confidence=o.extraction_confidence,
                            page_number=o.page_number,
                            source_reference=o.source_reference,
                            raw_claim=o.raw_claim,
                            validation_warnings=o.validation_warnings or [],
                        )
                    )
                observations = obs_list

                # Fetch resolution decisions ordered by creation time descending (latest audit event first)
                dec_stmt = (
                    select(ResolutionDecisionModel)
                    .where(ResolutionDecisionModel.event_id == event_id)
                    .order_by(
                        ResolutionDecisionModel.created_at.desc(),
                        ResolutionDecisionModel.id.desc(),
                    )
                )
                dec_models = session.execute(dec_stmt).scalars().all()
                decisions = [
                    {
                        "decision_type": d.decision_type,
                        "composite_score": d.composite_score,
                        "rationale": d.rationale,
                        "feature_scores": d.feature_scores,
                        "observation_id": d.observation_id,
                    }
                    for d in dec_models
                ]

                return {
                    "event": event,
                    "observations": observations,
                    "decisions": decisions,
                }

        # In-memory lookup
        event_match = next((e for e in self._in_memory_events if e.event_id == event_id), None)
        if not event_match:
            return None

        constituent_obs = [
            o for o in self._in_memory_observations
            if o.observation_id in event_match.observation_ids
        ]
        return {
            "event": event_match,
            "observations": constituent_obs,
            "decisions": [],
        }

    def get_unassigned_observations(self) -> list[dict[str, Any]]:
        """Fetch observations not assigned to any unified event profile."""
        if self.is_database_connected:
            assert self.session_factory is not None
            try:
                with get_session(self.session_factory) as session:
                    return self.repository.get_unassigned_observations_with_decisions(session)
            except Exception as err:
                logger.warning("Could not fetch unassigned observations from database: %s", err)

        # In-memory fallback
        assigned_obs_ids = {
            oid for ev in self._in_memory_events for oid in ev.observation_ids
        }
        unassigned_obs = [
            o for o in self._in_memory_observations if o.observation_id not in assigned_obs_ids
        ]
        decision_map = {
            out.observation_id: {
                "decision_type": getattr(out.decision, "value", str(out.decision)),
                "composite_score": out.score,
                "rationale": out.rationale,
                "feature_scores": out.feature_scores.model_dump() if out.feature_scores else {},
                "created_at": None,
            }
            for out in self._in_memory_resolution_outcomes
        }
        results = []
        for o in unassigned_obs:
            dec = decision_map.get(o.observation_id)
            results.append({
                "observation": o,
                "decision": dec,
            })
        return results

    def get_analytics_summary(
        self, filter_criteria: Optional[AnalyticsFilter] = None
    ) -> AnalyticsSummary:
        """Compute AnalyticsSummary from database or in-memory state."""
        if self.is_database_connected:
            assert self.session_factory is not None
            with get_session(self.session_factory) as session:
                return self.analytics_service.summarize_db(session, filter_criteria=filter_criteria)

        return self.analytics_service.summarize(
            events=self._in_memory_events,
            observations=self._in_memory_observations,
            filter_criteria=filter_criteria,
        )
