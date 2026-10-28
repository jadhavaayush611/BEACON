"""BEACON: Multi-Format Disaster Intelligence Data Engineering Dashboard.

Command Center Edition: "One event, many sources, one trusted data profile."
"""

from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import streamlit as st

from beacon.analytics.models import AnalyticsFilter
from beacon.config import get_settings
from beacon.dashboard.components import (
    extract_geographic_dataframe,
    get_system_status_pill_html,
    inject_custom_styles,
    render_empty_state,
)
from beacon.dashboard.formatters import (
    explain_compression_ratio,
    format_compression_ratio,
    format_conflict_claim_markdown,
    format_conflict_claim_row,
    format_decision_row,
    format_observation_row,
    format_score,
    format_timestamp,
)
from beacon.dashboard.services import REQUIRED_TABLES, DashboardService
from beacon.logging_config import redact_sensitive_text, setup_logging
from beacon.models import STANDARD_EVENT_TYPES, SeverityLevel

setup_logging()

# Page configuration
st.set_page_config(
    page_title="BEACON — Disaster Intelligence Command Center",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Apply BEACON Design System CSS
inject_custom_styles()


@st.cache_resource
def get_service() -> DashboardService:
    """Initialize cached DashboardService singleton."""
    return DashboardService()


service = get_service()
db_status = service.test_database_connection()

# Sidebar: System Status & Architecture
st.sidebar.title("🚨 BEACON System")
st.sidebar.caption("v1.0.0 — Multi-Format Disaster Intelligence")

if db_status.get("schema_ready"):
    st.sidebar.success("🟢 Storage: PostgreSQL Online")
    total_tbls = db_status.get("total_tables") or len(db_status.get("tables", {})) or len(REQUIRED_TABLES)
    ready_tbls = db_status.get("ready_tables") or sum(1 for c in db_status.get("tables", {}).values() if c >= 0)
    st.sidebar.caption(f"Relational schemas initialized and verified ({ready_tbls}/{total_tbls} tables online).")
elif db_status.get("connected"):
    st.sidebar.warning("🟡 Storage: PostgreSQL Connected (Uninitialized)")
    st.sidebar.caption(redact_sensitive_text(db_status.get("message", "Schema uninitialized.")))
else:
    st.sidebar.warning("🟡 Storage: In-Memory Mode (Stateless)")
    st.sidebar.caption(
        redact_sensitive_text(
            db_status.get("message", "BEACON_DATABASE_URL is not set. Running in-memory mode.")
        )
    )

st.sidebar.caption(
    "🔒 **Security Posture:** Connection endpoints and credentials are redacted in compliance with the Command Center security specification."
)

st.sidebar.markdown("---")
st.sidebar.markdown(
    """
    **Architecture Principles:**
    - Heterogeneous Ingestion
    - Non-Fabrication Guarantee
    - Explicit Conflict Tracking
    - Spatiotemporal Clustered Matching
    - Explainable Resolution Lineage
    """
)

# Command Center Header
col_hdr_title, col_hdr_status = st.columns([3, 1])
with col_hdr_title:
    st.markdown(
        '<div class="beacon-title">🚨 BEACON COMMAND CENTER</div>'
        '<div class="beacon-subtitle">Operational Disaster Intelligence & Canonical Entity Resolution Platform</div>',
        unsafe_allow_html=True,
    )
    st.caption("> *\"One event, many sources, one trusted data profile.\"*")

with col_hdr_status:
    pill_html = get_system_status_pill_html(
        db_status.get("connected", False),
        db_status.get("schema_ready", False),
    )
    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    st.markdown(
        f'<div style="text-align: right; padding-top: 0.25rem;">{pill_html}</div>'
        f'<div style="text-align: right; font-size: 0.75rem; color: #64748b; margin-top: 0.25rem;">{utc_now}</div>',
        unsafe_allow_html=True,
    )

# Navigation Tabs
tab_overview, tab_ingest, tab_pipeline, tab_events, tab_analytics, tab_diagnostics = st.tabs([
    "📊 Overview",
    "📥 Ingestion",
    "⚡ Pipeline Execution",
    "🔍 Event Explorer",
    "📈 Analytics",
    "⚙️ Database & Health",
])

# ==============================================================================
# TAB 1: OVERVIEW (COMMAND CENTER HOME)
# ==============================================================================
with tab_overview:
    col_ov_title, col_ov_btn = st.columns([4, 1])
    with col_ov_title:
        st.header("Operational Overview")
    with col_ov_btn:
        if st.button("🔄 Refresh Overview", key="btn_refresh_overview"):
            st.rerun()

    st.caption(
        "ℹ️ **Standard Sample Dataset:** Synthetic incident telemetry generated for demonstration and validation "
        "adhering to the Non-Fabrication Guarantee."
    )

    overview = service.get_system_overview()
    total_ev = overview["events_count"]
    total_obs = overview["observations_count"]
    total_src = overview["sources_count"]
    comp_ratio = overview.get("compression_ratio")

    # Four Command Center KPI Cards
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric(
            label="Unified Events",
            value=total_ev,
            help="Total canonical disaster profiles synthesized across all ingested observations",
        )
    with c2:
        st.metric(
            label="Constituent Observations",
            value=total_obs,
            help="Total normalized observations extracted across all ingested reporting channels",
        )
    with c3:
        st.metric(
            label="Contributing Sources",
            value=total_src,
            help="Distinct files or sensor streams successfully ingested into the pipeline",
        )
    with c4:
        st.metric(
            label="Compression Ratio",
            value=format_compression_ratio(comp_ratio),
            help=(
                "Consolidation Metric: Total Accepted Observations ÷ Total Unified Events (average observations per unified event).\n\n"
                "Descriptive ratio indicating the degree of data consolidation (e.g., 10 observations consolidated into 7 events = 1.43x). "
                "If total events is 0, the ratio evaluates safely to 0.00x without fabricating synthetic values."
            ),
        )

    st.caption(
        f"ℹ️ **Compression Ratio:** "
        f"{explain_compression_ratio(total_obs, total_ev, comp_ratio)}. "
        "Descriptive consolidation metric (observations per unified event): $\\text{Compression Ratio} = \\frac{\\text{Total Observations}}{\\text{Total Events}}$."
    )

    st.markdown("---")

    # Geospatial Context & Recent Incident Feed
    events_for_overview = service.query_events()
    geo_df = extract_geographic_dataframe(events_for_overview)

    if total_ev == 0:
        render_empty_state(
            title="Command Center Idle",
            message="No disaster events are currently loaded in system memory or database storage.",
            icon="🚨",
            guidance="Navigate to the 'Pipeline Execution' tab to run the automated ETL and resolution pipeline.",
        )
    else:
        g_col1, g_col2 = st.columns([1, 1])
        with g_col1:
            st.subheader("🗺️ Operational Geospatial Distribution")
            if len(geo_df) > 0:
                st.map(geo_df[["latitude", "longitude"]])
                st.caption(f"Mapped **{len(geo_df)}** event centroid(s) across affected territories.")
            else:
                st.caption("No geospatial coordinate centroids available in current events.")

        with g_col2:
            st.subheader("📡 Recent Disaster Intelligence Feed")
            feed_rows = [
                {
                    "Event ID": ev.event_id,
                    "Type": ev.event_type.upper(),
                    "Location": ev.location,
                    "Severity": (ev.severity or "unspecified").upper(),
                    "Start Time": format_timestamp(ev.start_time),
                    "Sources": len(ev.source_ids),
                    "Corroboration": "Multi-Source" if len(ev.source_ids) > 1 else "Single-Source",
                }
                for ev in events_for_overview[:8]
            ]
            st.dataframe(feed_rows, width="stretch")

    st.markdown("---")

    # Secondary Metrics Row: Corroboration & Coverage Gaps
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Data Validation & Multi-Source Trust")
        multi_count = overview["multi_source_events_count"]
        multi_ratio = overview.get("multi_source_ratio") or 0.0
        multi_pct = multi_ratio * 100

        st.progress(multi_ratio, text=f"Multi-Source Corroboration: {multi_pct:.1f}%")
        st.write(
            f"**{multi_count}** of **{total_ev}** events are corroborated by "
            f"two or more independent reporting channels."
        )

        st.markdown("#### Audit & Review Flags")
        f1, f2, f3 = st.columns(3)
        with f1:
            st.metric("Conflicting Claims", overview["conflicting_claims_count"])
        with f2:
            st.metric("Ambiguous Matches", overview["ambiguous_count"])
        with f3:
            st.metric("Merge Candidates", overview["merge_candidates_count"])

    with col_right:
        st.subheader("Coverage Gaps Audit")
        gaps = overview["coverage_gaps"]
        st.write("Identifies records lacking critical spatiotemporal coordinates:")
        st.table({
            "Metric": [
                "Events Missing Coordinate Centroid",
                "Events with Unanchored Timestamps",
                "Observations Missing Coordinates",
                "Observations Missing Timestamps",
            ],
            "Count": [
                gaps.events_missing_coordinates,
                gaps.events_unanchored_timestamps,
                gaps.observations_missing_coordinates,
                gaps.observations_missing_timestamps,
            ],
        })

# ==============================================================================
# TAB 2: INGESTION (MULTI-FORMAT SUBSYSTEM)
# ==============================================================================
with tab_ingest:
    st.header("Multi-Format Ingestion Subsystem")
    st.write(
        "Upload raw disaster files across supported formats: **JSON**, **CSV**, **TXT**, **PDF**, and **PNG/Raster Images**."
    )

    st.warning(
        "ℹ️ **Raster Image Notice:** The image ingestor validates file integrity, dimensions, and image format metadata. "
        "Optical Character Recognition (OCR) is explicitly not claimed or executed in v1.0.0 MVP."
    )

    uploaded_files = st.file_uploader(
        "Select disaster intelligence files:",
        type=["json", "csv", "txt", "pdf", "png", "jpg", "jpeg"],
        accept_multiple_files=True,
    )

    if uploaded_files:
        st.write(f"Selected **{len(uploaded_files)}** file(s).")
        if st.button("Ingest Uploaded Files", type="primary"):
            file_tuples = [(f.name, f.getvalue()) for f in uploaded_files]
            with st.spinner("Ingesting and validating files..."):
                results, _ = service.process_uploaded_files(file_tuples, cleanup=True)

            st.success(f"Successfully processed {len(results)} file(s).")

            rows = []
            for r in results:
                rows.append({
                    "Filename": Path(r.source_record.source_file).name,
                    "Type": r.source_record.source_type.value,
                    "Status": r.status.value,
                    "Observations Extracted": len(r.observations),
                    "Errors": len(r.errors),
                    "Warnings": len(r.warnings),
                })
            st.dataframe(rows, width="stretch")

            with st.expander("View Extracted Observations"):
                all_obs = [o.model_dump(mode="json") for r in results for o in r.observations]
                if all_obs:
                    st.json(all_obs[:10])
                else:
                    st.write("No observations extracted from uploaded files.")

    st.markdown("---")

    # Command Center Supported Formats Panel
    st.subheader("Supported Extraction Formats & Extractor Matrix")
    st.write(
        "BEACON handles heterogeneous, multi-modal disaster inputs through dedicated format-specific extractors:"
    )

    fmt_col1, fmt_col2 = st.columns(2)
    with fmt_col1:
        st.markdown(
            """
            <div class="beacon-format-card">
                <div class="beacon-format-title">📄 JSON Extractor (Structured Telemetry)</div>
                <div style="font-size: 0.85rem; color: #475569;">
                    Parses structured event records with strict schema validation. Supports native coordinate pairs,
                    ISO-8601 timestamps, nested casualties, and source confidence scores.
                </div>
            </div>
            <div class="beacon-format-card">
                <div class="beacon-format-title">📊 CSV Extractor (Tabular Incident Logs)</div>
                <div style="font-size: 0.85rem; color: #475569;">
                    Extracts multi-row sensor logs and agency incident reports. Implements dynamic column mapping,
                    null-value normalization, and per-row validation.
                </div>
            </div>
            <div class="beacon-format-card">
                <div class="beacon-format-title">📝 TXT Extractor (Unstructured Dispatch Dispatches)</div>
                <div style="font-size: 0.85rem; color: #475569;">
                    Processes free-form situational reports using regex entity extractors for spatial landmarks,
                    temporal anchors, hazard keywords, and severity indicators.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with fmt_col2:
        st.markdown(
            """
            <div class="beacon-format-card">
                <div class="beacon-format-title">📑 PDF Extractor (Agency SITREPs & Bulletins)</div>
                <div style="font-size: 0.85rem; color: #475569;">
                    Extracts digital text streams from multi-page bulletins via <code>pypdf</code>. Scans header sections,
                    coordinate coordinates, and situation narratives.
                </div>
            </div>
            <div class="beacon-format-card">
                <div class="beacon-format-title">🖼️ Raster Image Extractor (PNG / JPEG)</div>
                <div style="font-size: 0.85rem; color: #475569;">
                    Validates image format, dimension bounds, and file integrity.
                    <br><strong>OCR Limitation:</strong> Text extraction from raster imagery is deferred in v1.0.0;
                    images are cataloged as verified binary evidence without synthetic transcription.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("---")
    st.subheader("Default Sample Observation Files (Pipeline Inputs)")
    sample_dir = Path("data/sample")
    if sample_dir.exists():
        # Separate the 5 observation data files from any metadata
        observation_samples = [
            s for s in sorted(sample_dir.glob("*.*")) if s.name.lower() != "manifest.json"
        ]
        st.write(
            f"Found **{len(observation_samples)}** standard disaster observation input files in `{sample_dir}` "
            f"used as target dataset for pipeline execution:"
        )
        sample_rows = [
            {"File": s.name, "Format": s.suffix.upper(), "Size (bytes)": s.stat().st_size}
            for s in observation_samples
        ]
        st.dataframe(sample_rows, width="stretch")

# ==============================================================================
# TAB 3: PIPELINE EXECUTION
# ==============================================================================
with tab_pipeline:
    st.header("End-to-End Pipeline Execution")
    st.write(
        "Execute the full pipeline: **Extract ➔ Transform ➔ Validate (Data Quality) ➔ Resolve ➔ Load (PostgreSQL)**."
    )

    sample_paths = [
        Path("data/sample/events.json"),
        Path("data/sample/events.csv"),
        Path("data/sample/events.txt"),
        Path("data/sample/sample_report.pdf"),
        Path("data/sample/sample_disaster.png"),
    ]
    existing_samples = [p for p in sample_paths if p.exists()]

    col_p1, col_p2 = st.columns([2, 1])
    with col_p1:
        st.write(f"Target dataset: **{len(existing_samples)}** standard sample files.")
        persist_option = st.checkbox(
            "Persist results to database (if configured)",
            value=service.is_database_connected,
            disabled=not service.is_database_connected,
        )
        resolution_option = st.checkbox("Execute Event Resolution & Synthesis", value=True)

    with col_p2:
        run_btn = st.button("🚀 Run Complete Pipeline", type="primary")

    if run_btn:
        # Clear prior execution result and errors to prevent stale metrics on failure
        st.session_state["last_pipeline_result"] = None
        st.session_state["last_pipeline_error"] = None

        with st.spinner("Executing BEACON ETL and Resolution Pipeline..."):
            try:
                result = service.run_pipeline_on_paths(
                    existing_samples,
                    enable_resolution=resolution_option,
                    persist_db=persist_option,
                )
                st.session_state["last_pipeline_result"] = result
                st.rerun()
            except Exception as ex:
                st.session_state["last_pipeline_error"] = redact_sensitive_text(str(ex))
                st.rerun()

    # Display error from latest run if execution failed
    last_error = st.session_state.get("last_pipeline_error")
    if last_error:
        st.error(f"Pipeline execution failed: {redact_sensitive_text(last_error)}")

    # Display success results ONLY if the latest run succeeded
    last_result = st.session_state.get("last_pipeline_result")
    if last_result is not None:
        st.success(f"Pipeline Run `{last_result.execution_id}` Complete!")

        # Results metrics
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Files Ingested", f"{last_result.files_succeeded}/{last_result.files_discovered}")
        m2.metric("Raw Extracted", last_result.observations_extracted)
        m3.metric("Quality Accepted", last_result.observations_accepted)
        m4.metric("Quality Rejected", last_result.observations_rejected)
        m5.metric("Events Synthesized", last_result.events_synthesized)

        if last_result.errors:
            st.error(f"Encountered {len(last_result.errors)} pipeline error(s):")
            for err in last_result.errors:
                st.write(f"- {redact_sensitive_text(str(err))}")

        with st.expander("Data Quality Validation Findings"):
            qr = last_result.quality_report
            st.write(f"Summary: **{qr.passed_count}** passed, **{qr.failed_count}** failed checks.")
            findings_data = [
                {
                    "Rule": o.rule_name,
                    "Record ID": o.record_id or "Source Batch",
                    "Severity": o.severity.value,
                    "Passed": o.passed,
                    "Message": redact_sensitive_text(str(o.message)),
                }
                for o in qr.outcomes
            ]
            st.dataframe(findings_data, width="stretch")

    # Section: Run History & Safe Retries
    st.markdown("---")
    st.subheader("📜 Pipeline Run History & Audit Trail")
    recent_runs = service.list_pipeline_runs(limit=10)

    if not recent_runs:
        st.info(
            "ℹ️ No pipeline execution records found yet. "
            "Trigger the pipeline above to record execution metrics, file statuses, and audit lineage."
        )
    else:
        run_rows = []
        for r in recent_runs:
            inputs_meta = r.inputs_summary or {}
            counts_meta = r.counts_summary or {}
            files_str = f"{inputs_meta.get('succeeded', 0)}/{inputs_meta.get('total_inputs', 0)} ok"
            if inputs_meta.get("failed", 0) > 0:
                files_str += f" ({inputs_meta.get('failed')} failed)"
            dur_str = f"{r.duration_seconds:.2f}s" if r.duration_seconds is not None else "—"
            run_rows.append({
                "Run ID": r.run_id,
                "Status": r.status.value.upper(),
                "Trigger": r.trigger_type.upper(),
                "Retry Of": r.retry_of_run_id or "—",
                "Started (UTC)": format_timestamp(r.started_at),
                "Duration": dur_str,
                "Inputs": files_str,
                "Extracted": counts_meta.get("observations_extracted", 0),
                "Accepted": counts_meta.get("observations_accepted", 0),
                "Events": counts_meta.get("events_synthesized", 0),
                "Failures": len(r.failure_details),
            })
        st.dataframe(run_rows, width="stretch")

        # Retry controls for failed or partial runs
        failed_or_partial_runs = [
            r for r in recent_runs if r.status.value in ("failed", "partial")
        ]
        if failed_or_partial_runs:
            st.markdown("##### 🔄 Safe Execution Retries")
            col_r1, col_r2, col_r3 = st.columns([2, 1, 1])
            with col_r1:
                selected_retry_id = st.selectbox(
                    "Select execution run to retry:",
                    options=[r.run_id for r in failed_or_partial_runs],
                    key="select_retry_run_id",
                )
            with col_r2:
                failed_only = st.checkbox("Retry failed inputs only", value=True, key="retry_failed_only")
            with col_r3:
                if st.button("🔁 Trigger Retry Run", type="secondary", key="btn_trigger_retry"):
                    with st.spinner(f"Retrying run '{selected_retry_id}'..."):
                        try:
                            retry_res = service.retry_pipeline_run(
                                selected_retry_id,
                                failed_inputs_only=failed_only,
                                persist_db=persist_option,
                            )
                            st.session_state["last_pipeline_result"] = retry_res
                            st.session_state["last_pipeline_error"] = None
                            st.rerun()
                        except Exception as retry_err:
                            sanitized_retry_err = redact_sensitive_text(str(retry_err))
                            st.error(f"Retry execution failed: {sanitized_retry_err}")

    st.caption(
        "🕒 **Automated Scheduling Status:** Recurring daemon/cron execution is deferred in BEACON v1.0.0 MVP. "
        "Batch orchestration operates via explicit deterministic execution triggers with full audit history."
    )

# ==============================================================================
# TAB 4: UNIFIED EVENT EXPLORER
# ==============================================================================
with tab_events:
    st.header("Unified Disaster Event Explorer")
    st.write("Browse and audit canonical unified disaster profiles and their constituent observations.")

    # Filters
    f_col1, f_col2, f_col3, f_col4 = st.columns(4)
    with f_col1:
        type_filter = st.selectbox(
            "Filter by Event Type:",
            options=["All"] + list(STANDARD_EVENT_TYPES),
        )
    with f_col2:
        sev_filter = st.selectbox(
            "Filter by Severity:",
            options=["All"] + [s.value for s in SeverityLevel],
        )
    with f_col3:
        loc_search = st.text_input("Search Location:", placeholder="e.g. Riverside")
    with f_col4:
        corrob_filter = st.selectbox(
            "Filter Corroboration:",
            options=["All", "Multi-Source Only", "Single-Source Only"],
        )

    criteria = AnalyticsFilter(
        event_type=type_filter if type_filter != "All" else None,
        severity=sev_filter if sev_filter != "All" else None,
        location=loc_search if loc_search.strip() else None,
    )

    events = service.query_events(criteria)
    if corrob_filter == "Multi-Source Only":
        events = [e for e in events if len(e.source_ids) > 1]
    elif corrob_filter == "Single-Source Only":
        events = [e for e in events if len(e.source_ids) <= 1]

    st.write(f"Found **{len(events)}** matching unified event(s):")

    if events:
        for ev in events:
            status_tag = "MULTI-SOURCE" if len(ev.source_ids) > 1 else "SINGLE-SOURCE"
            conflict_tag = f"⚠️ {len(ev.conflicting_claims)} CONFLICT(S)" if ev.conflicting_claims else "CLEAN"
            with st.expander(f"📍 {ev.event_type.upper()} in {ev.location} ({ev.event_id}) — [{status_tag}] [{conflict_tag}]"):
                ec1, ec2 = st.columns([2, 1])
                with ec1:
                    st.write(f"**Summary:** {ev.summary}")
                    st.write(f"**Severity:** `{ev.severity or 'unspecified'}`")
                    start_disp = ev.start_time.isoformat() if ev.start_time else "N/A"
                    st.write(f"**Start Time:** `{start_disp}`")
                    if ev.end_time:
                        st.write(f"**End Time:** `{ev.end_time.isoformat()}`")
                    st.write(f"**Sources Linked:** `{', '.join(ev.source_ids)}`")
                    st.write(f"**Resolution Rationale:** {ev.resolution_rationale or 'N/A'}")

                with ec2:
                    centroid = (ev.metadata or {}).get("centroid")
                    bbox = (ev.metadata or {}).get("bounding_box")
                    if centroid and centroid.get("latitude") is not None and centroid.get("longitude") is not None:
                        st.write(f"**Centroid:** `({centroid['latitude']}, {centroid['longitude']})`")
                        st.map(
                            pd.DataFrame([{"latitude": float(centroid["latitude"]), "longitude": float(centroid["longitude"])}]),
                            zoom=6,
                        )
                    if (
                        bbox
                        and bbox.get("min_latitude") is not None
                        and bbox.get("max_latitude") is not None
                        and bbox.get("min_longitude") is not None
                        and bbox.get("max_longitude") is not None
                    ):
                        st.write("**Bounding Box:**")
                        st.caption(f"Lat: [{bbox['min_latitude']:.2f}, {bbox['max_latitude']:.2f}]")
                        st.caption(f"Lon: [{bbox['min_longitude']:.2f}, {bbox['max_longitude']:.2f}]")

                # Conflicting claims audit
                if ev.conflicting_claims:
                    st.markdown("##### ⚠️ Conflicting Claims Preserved Across Sources")
                    c_rows = [format_conflict_claim_row(c) for c in ev.conflicting_claims]
                    st.table(c_rows)

                    with st.expander("Accessible Text View (Screen Readers)"):
                        for c in ev.conflicting_claims:
                            st.markdown(format_conflict_claim_markdown(c))

                # Fetch full event detail including constituent observations and decisions
                detail = service.get_event_detail(ev.event_id)
                if detail and detail.get("observations"):
                    st.markdown("##### Constituent Observations")
                    obs_rows = [format_observation_row(o) for o in detail["observations"]]
                    st.dataframe(obs_rows, width="stretch")

                if detail and detail.get("decisions"):
                    st.markdown("##### Resolution Decisions & Feature Scoring")
                    dec_rows = [format_decision_row(d) for d in detail["decisions"]]
                    st.dataframe(dec_rows, width="stretch")
    else:
        st.info("No unified events match the current filter criteria.")

    # Unassigned and ambiguous observations audit section
    unassigned = service.get_unassigned_observations()
    if unassigned:
        st.markdown("---")
        with st.expander(
            f"⚠️ Unassigned & Ambiguous Observations ({len(unassigned)}) — Quarantined for Audit",
            expanded=False,
        ):
            st.caption(
                "These observations have been validated and persisted, but could not be confidently linked to a single "
                "unified event profile due to score ambiguity or near-tie candidates. They remain unassigned and "
                "quarantined pending further corroborating evidence or manual review."
            )
            u_rows = []
            for item in unassigned:
                obs = item["observation"]
                dec = item.get("decision") or {}
                u_rows.append({
                    "Obs ID": obs.observation_id,
                    "Source ID": obs.source_id,
                    "Type": obs.event_type,
                    "Location": obs.location or "N/A",
                    "Timestamp": format_timestamp(obs.event_time),
                    "Decision": dec.get("decision_type", "unassigned"),
                    "Score": format_score(dec.get("composite_score")),
                    "Rationale": dec.get("rationale", "No resolution decision recorded"),
                })
            st.dataframe(u_rows, width="stretch")

# ==============================================================================
# TAB 5: ANALYTICS (COMMAND CENTER DISASTER INTELLIGENCE)
# ==============================================================================
with tab_analytics:
    st.header("Disaster Intelligence Analytics")
    st.write("Aggregated data quality, compression efficiency, hazard distributions, and cross-source verification.")

    # Interactive Analytics Filter Bar
    st.markdown("##### 🔍 Slice Analytics by Dimensions")
    af_col1, af_col2, af_col3, af_col4 = st.columns(4)
    with af_col1:
        an_type_filter = st.selectbox(
            "Event Type Filter:",
            options=["All"] + list(STANDARD_EVENT_TYPES),
            key="an_type_filter",
        )
    with af_col2:
        an_sev_filter = st.selectbox(
            "Severity Rank Filter:",
            options=["All"] + [s.value for s in SeverityLevel],
            key="an_sev_filter",
        )
    with af_col3:
        an_loc_filter = st.text_input("Location Search:", placeholder="e.g. Metro", key="an_loc_filter")
    with af_col4:
        an_corrob_filter = st.selectbox(
            "Corroboration Threshold:",
            options=["All", "Multi-Source (2+ Sources)", "Single-Source Only"],
            key="an_corrob_filter",
        )

    min_src = 2 if an_corrob_filter == "Multi-Source (2+ Sources)" else None
    analytics_criteria = AnalyticsFilter(
        event_type=an_type_filter if an_type_filter != "All" else None,
        severity=an_sev_filter if an_sev_filter != "All" else None,
        location=an_loc_filter if an_loc_filter.strip() else None,
        min_sources=min_src,
    )

    summary = service.get_analytics_summary(filter_criteria=analytics_criteria)

    # If single-source only requested, adjust event count display
    if an_corrob_filter == "Single-Source Only":
        matching_events = service.query_events(analytics_criteria)
        single_events = [e for e in matching_events if len(e.source_ids) <= 1]
        display_events_count = len(single_events)
    else:
        display_events_count = summary.total_events

    a1, a2, a3 = st.columns(3)
    a1.metric("Total Events", display_events_count, help="Total canonical disaster profiles synthesized in filter scope")
    a2.metric("Total Observations", summary.total_observations, help="Total constituent observations in scope")
    a3.metric(
        "Compression Ratio",
        format_compression_ratio(summary.compression_ratio),
        help=(
            "Consolidation Metric: Total Observations ÷ Total Events (average observations per unified event).\n\n"
            "Descriptive ratio indicating consolidation degree (e.g., 10 observations consolidated into 7 events = 1.43x). "
            "Evaluates safely to 0.00x when total events is zero."
        ),
    )

    st.info(
        f"💡 **Compression Ratio:** "
        f"{explain_compression_ratio(summary.total_observations, summary.total_events, summary.compression_ratio)}. "
        "Descriptive consolidation metric representing the average number of constituent observations per unified disaster event."
    )

    st.markdown("---")

    # Row 1: Hazard Distribution by Type and Severity Hierarchy
    d1, d2 = st.columns(2)

    with d1:
        st.subheader("Events by Disaster Type")
        if summary.events_by_type:
            df_types = pd.DataFrame(
                list(summary.events_by_type.items()),
                columns=["Disaster Type", "Event Count"],
            )
            st.bar_chart(
                df_types,
                x="Disaster Type",
                y="Event Count",
                horizontal=True,
                x_label="Event Count",
                y_label="Disaster Type",
            )
        else:
            st.write("No event type distribution data available for selected filter.")

    with d2:
        st.subheader("Events by Severity Hierarchy")
        if summary.events_by_severity:
            sev_order = ["critical", "severe", "moderate", "minor", "unspecified"]
            sorted_sev = [
                (s, summary.events_by_severity.get(s, 0))
                for s in sev_order
                if s in summary.events_by_severity
            ]
            df_sev = pd.DataFrame(
                sorted_sev,
                columns=["Severity Level", "Event Count"],
            )
            st.bar_chart(
                df_sev,
                x="Severity Level",
                y="Event Count",
                horizontal=True,
                x_label="Event Count",
                y_label="Severity Level",
            )
        else:
            st.write("No severity distribution data available for selected filter.")

    st.markdown("---")

    # Row 2: Corroboration Breakdown and Conflict Frequency
    corrob_col, conf_col = st.columns(2)

    with corrob_col:
        st.subheader("Source Corroboration Breakdown")
        corrob_df = pd.DataFrame([
            {"Classification": "Multi-Source Corroborated", "Count": summary.multi_source_events},
            {"Classification": "Single-Source", "Count": summary.single_source_events},
        ])
        st.bar_chart(
            corrob_df,
            x="Classification",
            y="Count",
            horizontal=True,
            x_label="Event Count",
            y_label="Classification",
        )
        st.caption(
            f"Multi-Source Corroboration Rate: **{summary.multi_source_ratio * 100:.1f}%** "
            f"({summary.multi_source_events} of {summary.total_events} events verified by 2+ sources)."
        )

    with conf_col:
        st.subheader("Field Conflict Frequency")
        if summary.conflicting_claims_by_field:
            df_conf = pd.DataFrame(
                list(summary.conflicting_claims_by_field.items()),
                columns=["Field Name", "Conflicting Claims"],
            )
            st.bar_chart(
                df_conf,
                x="Field Name",
                y="Conflicting Claims",
                horizontal=True,
                x_label="Conflicting Claims",
                y_label="Field Name",
            )
        else:
            st.write("No attribute conflict frequency recorded.")

    st.markdown("---")

    # Row 3: Temporal Incident Activity Timeline & Spatiotemporal Coverage Gaps
    time_col, gaps_col = st.columns(2)

    with time_col:
        st.subheader("Temporal Incident Timeline")
        matching_events = service.query_events(analytics_criteria)
        events_with_time = [
            e for e in matching_events if e.start_time is not None
        ]
        if events_with_time:
            date_counts: dict[str, int] = {}
            for e in events_with_time:
                assert e.start_time is not None
                d_str = e.start_time.strftime("%Y-%m-%d")
                date_counts[d_str] = date_counts.get(d_str, 0) + 1

            timeline_df = pd.DataFrame(
                sorted(date_counts.items()),
                columns=["Incident Date", "Events Active"],
            )
            st.bar_chart(
                timeline_df,
                x="Incident Date",
                y="Events Active",
                x_label="Incident Date",
                y_label="Events Active",
            )
            st.caption(f"Timeline reflects **{len(events_with_time)}** anchored events with valid UTC timestamps.")
        else:
            st.caption(
                "ℹ️ **Temporal Anchoring Notice:** No events in the active filter scope contain verified datetime anchors. "
                "Adhering to the Non-Fabrication Guarantee, synthetic timeline curves are not rendered."
            )

    with gaps_col:
        st.subheader("Data Quality & Coverage Gaps Audit")
        cg = summary.coverage_gaps
        gaps_df = pd.DataFrame([
            {"Data Quality Dimension": "Events Missing Centroid Coordinates", "Count": cg.events_missing_coordinates},
            {"Data Quality Dimension": "Events with Unanchored Timestamps", "Count": cg.events_unanchored_timestamps},
            {"Data Quality Dimension": "Observations Missing Coordinates", "Count": cg.observations_missing_coordinates},
            {"Data Quality Dimension": "Observations Missing Timestamps", "Count": cg.observations_missing_timestamps},
        ])
        st.table(gaps_df)

# ==============================================================================
# TAB 6: DATABASE & HEALTH (SAFE DIAGNOSTICS)
# ==============================================================================
with tab_diagnostics:
    st.header("Persistence & Subsystem Health Diagnostics")
    st.write(
        "Inspect relational storage status, schema completeness, and engine health. "
        "Connection credentials and host topologies are redacted in compliance with Command Center security rules."
    )

    diag = service.test_database_connection()
    is_ready = diag.get("schema_ready", False)
    is_conn = diag.get("connected", False)
    status_str = diag.get("status", "unknown").upper()
    engine_name = diag.get("engine_type", "PostgreSQL" if is_conn else "In-Memory")

    # Subsystem Health Cards
    h_col1, h_col2, h_col3 = st.columns(3)
    with h_col1:
        st.metric("Storage Engine", engine_name)
    with h_col2:
        st.metric("Connection Health", status_str)
    with h_col3:
        diag_total = diag.get("total_tables") or len(diag.get("tables", {})) or len(REQUIRED_TABLES)
        diag_ready = diag.get("ready_tables") or sum(1 for c in diag.get("tables", {}).values() if c >= 0)
        readiness_label = (
            f"Ready ({diag_ready}/{diag_total} Tables)"
            if is_ready
            else ("Uninitialized" if is_conn else "Stateless")
        )
        st.metric("Schema Integrity", readiness_label)

    st.markdown("---")

    if is_ready:
        st.success(f"Database Engine Status: {status_str} — Relational Persistence Online")
        st.subheader("Relational Tables Integrity & Row Counts")
        t_data = [
            {"Table": k, "Status": "Ready", "Row Count": v if v >= 0 else 0}
            for k, v in diag.get("tables", {}).items()
        ]
        st.table(t_data)

    elif is_conn:
        st.warning(f"Database Engine Status: {status_str} — Schema Incomplete")
        st.info(redact_sensitive_text(diag.get("message", "Database connected, but schema is uninitialized.")))
        st.subheader("Relational Tables Integrity")
        t_data = [
            {
                "Table": k,
                "Status": "Ready" if v >= 0 else "Missing",
                "Row Count": v if v >= 0 else "—",
            }
            for k, v in diag.get("tables", {}).items()
        ]
        st.table(t_data)
        st.info(
            "To initialize the schema, execute the initialization script in your terminal:\n\n"
            "```bash\n"
            "python scripts/init_db.py\n"
            "```"
        )

    else:
        st.warning(f"Database Status: {status_str} — Running In-Memory Mode")
        st.write(redact_sensitive_text(diag.get("message", "Database not configured.")))
        st.info(
            "Persistent PostgreSQL storage is currently disabled. To connect a database instance:\n\n"
            "1. Set the `BEACON_DATABASE_URL` variable in your `.env` file or execution environment.\n"
            "2. Initialize core schemas with `python scripts/init_db.py`."
        )

    st.markdown("---")
    st.subheader("Configuration Inspector")
    st.caption("Inspect active runtime settings. All sensitive parameters are scrubbed.")
    cfg = get_settings()
    st.json({
        "environment": cfg.environment,
        "enable_strict_validation": cfg.enable_strict_validation,
        "default_confidence_threshold": cfg.default_confidence_threshold,
        "resolution_match_threshold": cfg.resolution_match_threshold,
        "resolution_ambiguity_threshold": cfg.resolution_ambiguity_threshold,
        "resolution_max_spatial_distance_km": cfg.resolution_max_spatial_distance_km,
        "resolution_max_temporal_distance_hours": cfg.resolution_max_temporal_distance_hours,
        "storage_mode": engine_name,
    })
