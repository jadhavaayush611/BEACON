"""BEACON: Multi-Format Disaster Intelligence Data Engineering Dashboard.

"One event, many sources, one trusted data profile."
"""

import json
from pathlib import Path
import pandas as pd
import streamlit as st

from beacon.analytics.models import AnalyticsFilter
from beacon.config import get_settings
from beacon.dashboard.formatters import (
    explain_compression_ratio,
    format_compression_ratio,
    format_conflict_claim_markdown,
    format_conflict_claim_row,
    format_decision_row,
    format_observation_row,
)
from beacon.dashboard.services import DashboardService
from beacon.models import STANDARD_EVENT_TYPES, SeverityLevel

# Page configuration
st.set_page_config(
    page_title="BEACON — Disaster Intelligence Dashboard",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def get_service() -> DashboardService:
    """Initialize cached DashboardService singleton."""
    return DashboardService()


service = get_service()
db_status = service.test_database_connection()

# Sidebar: System Status & Configuration
st.sidebar.title("🚨 BEACON System")
st.sidebar.caption("v1.0.0 — Multi-Format Disaster Intelligence")

if db_status.get("schema_ready"):
    st.sidebar.success("🟢 PostgreSQL Connected & Ready")
    st.sidebar.caption(f"Host: `{db_status['database_url_masked']}`")
elif db_status.get("connected"):
    st.sidebar.warning("🟡 PostgreSQL Connected (Uninitialized)")
    st.sidebar.caption(db_status["message"])
else:
    st.sidebar.warning("🟡 Storage: In-Memory / Unconnected")
    st.sidebar.caption(db_status["message"])

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

# Header
st.title("BEACON — Disaster Intelligence Pipeline")
st.markdown("> *\"One event, many sources, one trusted data profile.\"*")

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
# TAB 1: OVERVIEW
# ==============================================================================
with tab_overview:
    col_hdr1, col_hdr2 = st.columns([4, 1])
    with col_hdr1:
        st.header("Executive Pipeline Overview")
    with col_hdr2:
        if st.button("🔄 Refresh Overview"):
            st.rerun()

    overview = service.get_system_overview()

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric(
            label="Unified Events",
            value=overview["events_count"],
            help="Total canonical disaster profiles synthesized",
        )
    with c2:
        st.metric(
            label="Constituent Observations",
            value=overview["observations_count"],
            help="Total normalized observations extracted across sources",
        )
    with c3:
        st.metric(
            label="Contributing Sources",
            value=overview["sources_count"],
            help="Distinct files or sensor streams ingested",
        )
    with c4:
        st.metric(
            label="Compression Ratio",
            value=format_compression_ratio(overview.get("compression_ratio")),
            help=(
                "Consolidation Metric: Total Accepted Observations ÷ Total Unified Events (average observations per unified event).\n\n"
                "Descriptive ratio indicating the degree of data consolidation (e.g., 10 observations consolidated into 7 events = 1.43x). "
                "If total events is 0, the ratio evaluates safely to 0.00x without fabricating synthetic values."
            ),
        )

    st.caption(
        f"ℹ️ **Compression Ratio:** "
        f"{explain_compression_ratio(overview['observations_count'], overview['events_count'], overview.get('compression_ratio'))}. "
        "Descriptive consolidation metric (observations per unified event): $\\text{Compression Ratio} = \\frac{\\text{Total Observations}}{\\text{Total Events}}$."
    )

    st.markdown("---")

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Data Validation & Multi-Source Trust")
        multi_count = overview["multi_source_events_count"]
        total_ev = overview["events_count"]
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

    if total_ev == 0:
        st.info("💡 **Empty State:** No disaster events currently loaded. Navigate to **Ingestion** or **Pipeline Execution** to process sample files.")

# ==============================================================================
# TAB 2: INGESTION
# ==============================================================================
with tab_ingest:
    st.header("Multi-Format Ingestion Subsystem")
    st.write(
        "Upload raw disaster files across supported formats: **JSON**, **CSV**, **TXT**, **PDF**, and **PNG/Raster Images**."
    )

    st.warning(
        "ℹ️ **Raster Image Notice:** The image ingestor validates file integrity, dimensions, and image format metadata. "
        "Optical Character Recognition (OCR) is explicitly not claimed or executed."
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
    st.subheader("Default Sample Observation Files (Pipeline Inputs)")
    sample_dir = Path("data/sample")
    if sample_dir.exists():
        # Separate the 5 observation data files from the metadata manifest
        observation_samples = [
            s for s in sorted(sample_dir.glob("*.*")) if s.name.lower() != "manifest.json"
        ]
        manifest_path = sample_dir / "manifest.json"

        st.write(
            f"Found **{len(observation_samples)}** standard disaster observation input files in `{sample_dir}` "
            f"used as target dataset for pipeline execution:"
        )
        sample_rows = [
            {"File": s.name, "Format": s.suffix.upper(), "Size (bytes)": s.stat().st_size}
            for s in observation_samples
        ]
        st.dataframe(sample_rows, width="stretch")

        if manifest_path.exists():
            st.markdown("#### 📋 Dataset Specification & Manifest Metadata")
            st.caption(
                f"`{manifest_path.name}` ({manifest_path.stat().st_size} bytes) is the dataset manifest and specification. "
                "It defines the schema version, ground-truth co-references, and synthetic data disclaimer for the 5 sample inputs. "
                "It serves as test metadata and is not ingested as an observation feed."
            )
            with st.expander("View Sample Dataset Manifest Specification"):
                try:
                    manifest_content = json.loads(manifest_path.read_text(encoding="utf-8"))
                    st.json(manifest_content)
                except Exception as m_err:
                    st.warning(f"Could not read manifest metadata: {m_err}")

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
                st.session_state["last_pipeline_error"] = str(ex)
                st.rerun()

    # Display error from latest run if execution failed
    last_error = st.session_state.get("last_pipeline_error")
    if last_error:
        st.error(f"Pipeline execution failed: {last_error}")

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
                st.write(f"- {err}")

        with st.expander("Data Quality Validation Findings"):
            qr = last_result.quality_report
            st.write(f"Summary: **{qr.passed_count}** passed, **{qr.failed_count}** failed checks.")
            findings_data = [
                {
                    "Rule": o.rule_name,
                    "Record ID": o.record_id or "Source Batch",
                    "Severity": o.severity.value,
                    "Passed": o.passed,
                    "Message": o.message,
                }
                for o in qr.outcomes
            ]
            st.dataframe(findings_data, width="stretch")

# ==============================================================================
# TAB 4: UNIFIED EVENT EXPLORER
# ==============================================================================
with tab_events:
    st.header("Unified Disaster Event Explorer")
    st.write("Browse and audit canonical unified disaster profiles and their constituent observations.")

    # Filters
    f_col1, f_col2, f_col3 = st.columns(3)
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

    criteria = AnalyticsFilter(
        event_type=type_filter if type_filter != "All" else None,
        severity=sev_filter if sev_filter != "All" else None,
        location=loc_search if loc_search.strip() else None,
    )

    events = service.query_events(criteria)
    st.write(f"Found **{len(events)}** matching unified event(s):")

    if events:
        for ev in events:
            with st.expander(f"📍 {ev.event_type.upper()} in {ev.location} ({ev.event_id})"):
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
                        st.write(f"**Centroid:** `{centroid['latitude']}, {centroid['longitude']}`")
                    if (
                        bbox
                        and bbox.get("min_latitude") is not None
                        and bbox.get("max_latitude") is not None
                        and bbox.get("min_longitude") is not None
                        and bbox.get("max_longitude") is not None
                    ):
                        st.write("**Bounding Box:**")
                        st.caption(f"Lat: [{bbox['min_latitude']}, {bbox['max_latitude']}]")
                        st.caption(f"Lon: [{bbox['min_longitude']}, {bbox['max_longitude']}]")

                # Conflicting claims audit
                if ev.conflicting_claims:
                    st.markdown("##### ⚠️ Conflicting Claims Preserved Across Sources")
                    c_rows = [format_conflict_claim_row(c) for c in ev.conflicting_claims]
                    claims_column_config = {
                        "Field": st.column_config.TextColumn("Field", width="small"),
                        "Claimed Value": st.column_config.TextColumn("Claimed Value", width="small"),
                        "Observation ID": st.column_config.TextColumn("Observation ID", width="medium"),
                        "Source ID": st.column_config.TextColumn("Source ID", width="medium"),
                        "Status": st.column_config.TextColumn("Status", width="small"),
                        "Resolution Notes": st.column_config.TextColumn(
                            "Resolution Notes",
                            width="large",
                            help="Complete explainable consensus resolution rationale.",
                        ),
                    }
                    st.dataframe(c_rows, column_config=claims_column_config, width="stretch")

                    with st.expander("🔍 Complete Resolution Notes & Claim Audit Details", expanded=True):
                        for claim in ev.conflicting_claims:
                            st.markdown(format_conflict_claim_markdown(claim))

                # Fetch full event detail
                detail = service.get_event_detail(ev.event_id)
                if detail and detail.get("observations"):
                    st.markdown("##### 📄 Constituent Observations")
                    obs_rows = [format_observation_row(o) for o in detail["observations"]]
                    st.dataframe(obs_rows, width="stretch")

                if detail and detail.get("decisions"):
                    st.markdown("##### ⚖️ Resolution Match Decisions")
                    dec_rows = [format_decision_row(d) for d in detail["decisions"]]
                    st.dataframe(dec_rows, width="stretch")
    else:
        st.info("No unified events match the current filter criteria.")

# ==============================================================================
# TAB 5: ANALYTICS
# ==============================================================================
with tab_analytics:
    st.header("Disaster Intelligence Analytics")
    st.write("Aggregated data quality, compression efficiency, and hazard distributions.")

    summary = service.get_analytics_summary()

    a1, a2, a3 = st.columns(3)
    a1.metric("Total Events", summary.total_events, help="Total canonical disaster profiles synthesized")
    a2.metric("Total Observations", summary.total_observations, help="Total observations extracted across all input sources")
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
            st.write("No event type distribution data available.")

    with d2:
        st.subheader("Events by Severity Rank")
        if summary.events_by_severity:
            df_sev = pd.DataFrame(
                list(summary.events_by_severity.items()),
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
            st.write("No severity distribution data available.")

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

# ==============================================================================
# TAB 6: DATABASE & HEALTH
# ==============================================================================
with tab_diagnostics:
    st.header("Persistence & System Health Diagnostics")

    diag = service.test_database_connection()

    if diag.get("schema_ready"):
        st.success(f"Database Status: {diag['status'].upper()}")
        st.write(f"Connected Endpoint: `{diag['database_url_masked']}`")

        st.subheader("Relational Tables Row Counts")
        t_data = [{"Table": k, "Row Count": v} for k, v in diag["tables"].items()]
        st.table(t_data)
    elif diag.get("connected"):
        st.warning(f"Database Status: {diag['status'].upper()}")
        st.write(f"Connected Endpoint: `{diag['database_url_masked']}`")
        st.info(diag["message"])

        st.subheader("Relational Tables Row Counts")
        t_data = [{"Table": k, "Row Count": v} for k, v in diag["tables"].items()]
        st.table(t_data)
    else:
        st.error(f"Database Status: {diag['status'].upper()}")
        st.write(diag["message"])
        st.info(
            "To connect PostgreSQL, set the `BEACON_DATABASE_URL` environment variable:\n\n"
            "```bash\n"
            "export BEACON_DATABASE_URL=postgresql+psycopg://user:pass@localhost:5432/beacon\n"
            "```"
        )

    st.markdown("---")
    st.subheader("Configuration Inspector")
    cfg = get_settings()
    st.json({
        "environment": cfg.environment,
        "enable_strict_validation": cfg.enable_strict_validation,
        "default_confidence_threshold": cfg.default_confidence_threshold,
        "resolution_match_threshold": cfg.resolution_match_threshold,
        "resolution_ambiguity_threshold": cfg.resolution_ambiguity_threshold,
        "resolution_max_spatial_distance_km": cfg.resolution_max_spatial_distance_km,
        "resolution_max_temporal_distance_hours": cfg.resolution_max_temporal_distance_hours,
    })
