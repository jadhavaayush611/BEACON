"""Service layer for the BEACON Streamlit dashboard.

Encapsulates data retrieval, pipeline execution, file ingestion,
and analytics aggregation so UI components remain presentation-only.
"""

from datetime import timezone
import logging
from pathlib import Path
import shutil
import tempfile
from typing import Any, Optional, Sequence, Union

from sqlalchemy import distinct, func, select, text
from sqlalchemy.orm import Session, sessionmaker

from beacon.analytics.models import AnalyticsFilter, AnalyticsSummary
from beacon.analytics.service import AnalyticsService
from beacon.config import Settings, get_settings
from beacon.ingestion import IngestionResult, ingest_file
from beacon.models import DisasterObservation, UnifiedDisasterEvent
from beacon.pipeline.result import ETLPipelineResult
from beacon.pipeline.runner import ETLRunner
from beacon.storage import (
    EventModel,
    EventObservationModel,
    MergeCandidateModel,
    ObservationModel,
    QualityResultModel,
    ResolutionDecisionModel,
    SourceModel,
    StorageRepository,
    create_db_engine,
    get_session,
    get_session_factory,
)

logger = logging.getLogger("beacon.dashboard.services")


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

        # Initialize session factory lazily if database_url is configured
        if self.session_factory is None and self.settings.database_url:
            try:
                engine = create_db_engine(settings=self.settings)
                self.session_factory = get_session_factory(engine)
            except Exception as err:
                logger.warning("Dashboard could not connect to configured database: %s", err)

        # In-memory storage for file-based runs when database is not connected
        self._in_memory_events: list[UnifiedDisasterEvent] = []
        self._in_memory_observations: list[DisasterObservation] = []
        self._last_pipeline_result: Optional[ETLPipelineResult] = None

    @property
    def is_database_connected(self) -> bool:
        """Return True if a database session factory is available and operational."""
        if self.session_factory is None:
            return False
        try:
            with get_session(self.session_factory) as session:
                session.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    def test_database_connection(self) -> dict[str, Any]:
        """Test database connection and return detailed diagnostic status."""
        if not self.settings.database_url:
            return {
                "connected": False,
                "status": "unconfigured",
                "message": "BEACON_DATABASE_URL is not set. Running in-memory mode.",
                "database_url_masked": "None",
                "tables": {},
            }

        # Mask password in URL for display safety
        raw_url = str(self.settings.database_url)
        masked_url = raw_url
        if "@" in raw_url and "://" in raw_url:
            scheme, rest = raw_url.split("://", 1)
            creds, host_part = rest.split("@", 1)
            if ":" in creds:
                user, _ = creds.split(":", 1)
                masked_url = f"{scheme}://{user}:****@{host_part}"

        try:
            engine = create_db_engine(settings=self.settings)
            factory = get_session_factory(engine)
            tables_count: dict[str, int] = {}
            with get_session(factory) as session:
                for model, name in [
                    (SourceModel, "sources"),
                    (ObservationModel, "observations"),
                    (QualityResultModel, "data_quality_results"),
                    (EventModel, "events"),
                    (EventObservationModel, "event_observations"),
                    (ResolutionDecisionModel, "resolution_decisions"),
                    (MergeCandidateModel, "merge_candidates"),
                ]:
                    try:
                        count = session.execute(select(func.count()).select_from(model)).scalar() or 0
                        tables_count[name] = count
                    except Exception:
                        tables_count[name] = -1  # Table does not exist

            return {
                "connected": True,
                "status": "healthy",
                "message": "Connected to database successfully.",
                "database_url_masked": masked_url,
                "tables": tables_count,
            }
        except Exception as err:
            return {
                "connected": False,
                "status": "error",
                "message": f"Connection failed: {err}",
                "database_url_masked": masked_url,
                "tables": {},
            }

    def get_system_overview(self) -> dict[str, Any]:
        """Collect aggregated overview metrics for the executive dashboard overview."""
        if self.is_database_connected:
            assert self.session_factory is not None
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
                }

        # Fallback to in-memory state if DB is not connected
        summary = self.analytics_service.summarize(
            events=self._in_memory_events,
            observations=self._in_memory_observations,
        )
        sources = {o.source_id for o in self._in_memory_observations}
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
        }

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

                # Fetch resolution decisions
                dec_stmt = select(ResolutionDecisionModel).where(
                    ResolutionDecisionModel.event_id == event_id
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
