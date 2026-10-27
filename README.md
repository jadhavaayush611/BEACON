# BEACON — Multi-Format Disaster Intelligence Data Engineering Pipeline

> **"One event, many sources, one trusted data profile."**

BEACON is a production-grade data engineering pipeline engineered to ingest heterogeneous disaster-related information from unstructured and semi-structured sources: PDF situation reports, JSON telemetry streams, CSV field sensors, text dispatches, and aerial raster images. It normalizes atomic observations, preserves end-to-end source provenance, retains conflicting claims, and consolidates co-referent reports into unified disaster event profiles.

---

## Current Status: v1.0.0 MVP (Release Hardening & Dashboard Completed)

BEACON delivers an end-to-end multi-format disaster intelligence engineering pipeline with:
- **Heterogeneous Ingestion:** Structured JSON, sensor CSV, structured situation reports (TXT), multi-page PDF (`pypdf`), and raster image validation (`Pillow`).
- **ETL & Data Quality Framework:** Normalized observation attributes, explicit data quality rule audits (`RuleSeverity.ERROR`, `WARNING`, `INFO`), and atomic error isolation.
- **Explainable Event Resolution:** Multi-attribute candidate blocking (`CandidateGenerator`), spatiotemporal proximity scoring (`ObservationMatcher`), deterministic policy (`MatchPolicy`), and canonical profile synthesis (`EventProfileAggregator`) with preserved conflicting claims.
- **Relational PostgreSQL Storage & Additive Migrations:** Eight relational tables with composite unique constraints, cascade rules, transaction safety, and pure additive idempotent migration scripts (sources, observations, quality_results, events, event_observations, merge_candidates, resolution_decisions, pipeline_runs).
- **Disaster Intelligence Analytics (`AnalyticsService`):** Compression ratios, multi-source validation percentages, coverage gaps, and conflict frequency.
- **Interactive Streamlit Dashboard:** Complete redesigned dashboard (`src/beacon/dashboard/`) featuring Live Overview with maps, Multi-Format Ingestion, Pipeline Execution with Run History and Retries, Unified Event Explorer, Resolution Lineage and Ambiguity Review, Disaster Analytics, and Database Health diagnostics.
- **Continuous Integration (CI):** Fully automated GitHub Actions workflow verifying Python 3.10-3.12 with a live PostgreSQL 15 service container.

---

## Architectural Decision: Centralized Configuration

In accordance with Python packaging best practices (`src`-layout), all application configuration is centralized exclusively in `src/beacon/config.py` using `pydantic-settings`. The redundant root-level `config/settings.py` was intentionally omitted to prevent competing settings implementations, eliminate module drift, and ensure all application code imports from `beacon.config`.

---

## Project Structure

```text
beacon/
├── README.md                          # Project documentation and roadmap
├── pyproject.toml                     # Package build configuration & pytest options
├── requirements.txt                   # Runtime dependencies (SQLAlchemy, psycopg, etc.)
├── .gitignore                         # Python, venv, secret, and raw data exclusions
├── .env.example                       # Non-sensitive configuration template
├── src/
│   └── beacon/
│       ├── __init__.py                # Package exports & public API
│       ├── config.py                  # Single authoritative typed Settings
│       ├── models.py                  # Pydantic models (SourceRecord, DisasterObservation, etc.)
│       ├── logging_config.py          # Secure logging setup with credential redaction
│       ├── ingestion/                 # Multi-format ingestion subsystem
│       │   ├── __init__.py            # Ingestion public interface
│       │   ├── base.py                # BaseIngestor abstract class & SHA-256 helpers
│       │   ├── dispatcher.py          # Central ingest_file entry point & routing
│       │   ├── exceptions.py          # Typed ingestion exceptions
│       │   ├── result.py              # IngestionResult typed model
│       │   ├── json_ingestor.py       # JSON telemetry ingestor
│       │   ├── csv_ingestor.py        # CSV sensor log ingestor
│       │   ├── txt_ingestor.py        # Structured text situation report ingestor
│       │   ├── pdf_ingestor.py        # pypdf multi-page situation report ingestor
│       │   └── image_ingestor.py      # Pillow image validator & metadata reader
│       ├── quality/                   # Data Quality Framework
│       │   ├── __init__.py            # Quality exports
│       │   ├── models.py              # RuleSeverity, ValidationOutcome, QualityReport
│       │   ├── rules.py               # Explicit data quality check functions
│       │   └── validator.py           # DataQualityValidator engine
│       ├── pipeline/                  # ETL Pipeline Orchestration
│       │   ├── __init__.py            # Pipeline exports
│       │   ├── transform.py           # ObservationTransformer normalization engine
│       │   ├── result.py              # ETLPipelineResult execution metrics
│       │   └── runner.py              # ETLRunner (Extract -> Transform -> Validate -> Load -> Report)
│       ├── resolution/                # Event Resolution & Entity Linking
│       │   ├── __init__.py            # Resolution public API exports
│       │   ├── distance.py            # Haversine, temporal hours, and token similarity
│       │   ├── models.py              # FeatureScores, CandidateMatch, MergeCandidate, ResolutionResult
│       │   ├── matcher.py             # Multi-feature ObservationMatcher & dynamic reweighting
│       │   ├── candidate_generator.py # Multi-attribute blocking (type, time, space)
│       │   ├── policy.py              # MatchPolicy classification & bridge detection
│       │   ├── aggregator.py          # EventProfileAggregator profile synthesis & conflict tracking
│       │   └── resolver.py            # EventResolver batch orchestrator
│       ├── analytics/                 # Disaster Intelligence Analytics
│       │   ├── __init__.py            # Analytics exports
│       │   ├── models.py              # AnalyticsFilter, AnalyticsSummary, CoverageGaps
│       │   └── service.py             # AnalyticsService aggregations & database metrics
│       └── storage/                   # Relational Persistence Layer
│           ├── __init__.py            # Storage exports
│           ├── schema.py              # SQLAlchemy ORM models (Source, Observation, Event, Links, Audits)
│           ├── schema.sql             # Portable pure PostgreSQL DDL script
│           ├── connection.py          # Engine creation, pooling, session factory & context manager
│           ├── repository.py          # StorageRepository idempotent CRUD & queries
│           └── migrations/            # Additive migration scripts
│               └── v0_4_event_resolution.sql # Phase 4 additive DDL script
├── data/
│   ├── raw/
│   │   └── .gitkeep                   # Local directory for uncommitted raw ingestion files
│   └── sample/
│       ├── manifest.json              # Sample format and extraction path manifest
│       ├── events.json                # Synthetic telemetry feed (JSON)
│       ├── events.csv                 # Synthetic sensor logs (CSV)
│       ├── events.txt                 # Synthetic situation report dispatches (TXT)
│       ├── sample_report.pdf          # Synthetic aerial recon report (PDF)
│       └── sample_disaster.png        # Synthetic raster hazard map (PNG)
├── scripts/
│   ├── generate_sample_binaries.py    # Deterministic pure-Python sample PDF/PNG generator
│   ├── init_db.py                     # CLI database schema initialization & reset script
│   └── migrate_v0_4.py                # CLI database Phase 4 additive migration script
├── tests/
│   ├── test_models.py                 # Data contract validations & constraints
│   ├── test_config.py                 # Configuration and logging filter tests
│   ├── test_sample_data.py            # Sample dataset integrity & co-reference tests
│   ├── test_ingestion.py              # Multi-format ingestion, routing, and error tests
│   ├── test_quality.py                # Data quality rules and validator tests
│   ├── test_transform.py              # Transformation, alias mapping, and precision tests
│   ├── test_pipeline.py               # ETLRunner flow, idempotency, and failure isolation
│   ├── test_storage_unit.py           # In-memory database schema, repository, and rollback tests
│   ├── test_init_db.py                # Database CLI safety and confirmation workflow tests
│   ├── test_storage_integration.py    # Live PostgreSQL integration test (skipped if DB unset)
│   ├── test_distance.py               # Haversine, temporal hours, and text similarity unit tests
│   ├── test_matcher.py                # Multi-feature ObservationMatcher tests
│   ├── test_candidate_generator.py    # Multi-attribute blocking tests
│   ├── test_policy.py                 # MatchPolicy decision and bridge detection tests
│   ├── test_aggregator.py             # Event profile aggregation & conflicting claim tests
│   ├── test_resolver.py               # EventResolver orchestration & cross-source co-reference
│   ├── test_analytics.py              # Disaster intelligence analytics tests
│   ├── test_resolution_storage.py     # Event storage, linkage, and audit persistence tests
│   ├── test_migration_v0_4.py         # Phase 4 migration CLI and safety tests
│   ├── test_dashboard.py              # Streamlit dashboard services and UI integration tests
│   ├── test_data_operations.py        # Pipeline run records, safe retries, and batch operations tests
│   └── test_end_to_end.py             # Full multi-format end-to-end integration and idempotency tests
└── docs/
    ├── data_contract.md               # Canonical data contracts specification
    ├── ingestion.md                   # Ingestion & extraction architecture guide
    ├── etl_and_storage.md             # ETL pipeline, data quality & PostgreSQL storage guide
    └── event_resolution.md            # Event resolution, bridge detection & analytics guide
```

---

## Windows PowerShell Setup & Quickstart

### Prerequisites
- Python 3.10+ (tested on Python 3.14)
- Windows PowerShell

### 1. Create and Activate Virtual Environment
```powershell
# From the repository root:
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 2. Install Dependencies
```powershell
pip install -r requirements.txt
```

### 3. Install BEACON in Editable Mode
```powershell
pip install -e .
```

### 4. Configure Local Environment (Optional)
```powershell
Copy-Item .env.example .env
```

### 5. Run the Test Suite
```powershell
pytest -v
```

The 301 unit, contract, end-to-end, and dashboard tests run completely locally without requiring external APIs, PostgreSQL, or OCR binaries. The 6 PostgreSQL integration tests require a configured isolated test database and are skipped when `BEACON_TEST_DATABASE_URL` is unset to prevent modifying development or shared database instances.

To run the complete 305-test suite against a live PostgreSQL database:
```powershell
$env:BEACON_TEST_DATABASE_URL = "postgresql+psycopg://username:password@localhost:5432/beacon_test"
$env:BEACON_REQUIRE_POSTGRES = "1"
pytest -v
```

---

## Launching the Streamlit Dashboard

Launch the interactive web dashboard with a single command:

```powershell
# Using Streamlit directly
streamlit run src/beacon/dashboard/app.py

# Or using the package CLI entrypoint
beacon-dashboard
```

The dashboard opens at `http://localhost:8501` featuring:
1. **Live Overview:** Real-time system KPIs, interactive geospatial disaster map, recent event feed, and data coverage indicators.
2. **Event Explorer:** Faceted search and filters (type, severity, location, dates, min sources), canonical disaster event profiles, constituent observations with source lineage, and conflicting claims.
3. **Pipeline Execution:** In-memory or database-backed batch processing, execution duration, Pipeline Run History audit trail, and safe execution retries with `failed_inputs_only` filtering.
4. **Resolution & Lineage:** Ambiguous observation review with manual event assignment, and merge candidate bridge detection.
5. **Analytics & Quality:** Multi-source validation ratios, compression statistics, conflict frequency distributions, and Data Quality rule evaluation audits.
6. **Database & Diagnostics:** Relational schema readiness checks (all 8 tables), table row counts, connection status, and configuration inspection.






---

## Database Initialization & Migration

```powershell
# 1. Fresh Database Initialization (Initializes all 8 tables)
python scripts/init_db.py --url postgresql+psycopg://username:password@localhost:5432/beacon

# 2. Additive Migration from Phase 3 to Phase 4 / v1.0
python scripts/migrate_v0_4.py --url postgresql+psycopg://username:password@localhost:5432/beacon

# Inspect migration SQL without applying changes
python scripts/migrate_v0_4.py --dry-run
```

For detailed migration, isolation, and rollback guidance, see [docs/database_migration.md](docs/database_migration.md).

---

## Known System Boundaries & Explicit Limitations

> [!NOTE]

> **Supported Formats & OCR Limitations:**
> BEACON natively ingests JSON telemetry streams, CSV sensor logs, structured TXT dispatches, multi-page PDF documents (`pypdf`), and raster imagery (`Pillow`). For raster imagery, image dimensions and format integrity are verified; OCR via `pytesseract` is evaluated only if optional OCR binary dependencies are installed, otherwise OCR is gracefully deferred/skipped without fabricating data.

> [!NOTE]
> **Resolution Thresholds & Ambiguity Management:**
> - **Match Threshold ($\ge 0.75$):** Automatically links observations into a unified canonical event profile.
> - **Candidate Threshold ($0.45 \le \text{score} < 0.75$):** Flags observations as ambiguous, persisting an immutable `ResolutionDecisionModel(decision_type="ambiguous")` and leaving the observation unassigned to prevent spurious merges. Ambiguous observations can be audited and manually assigned in Tab 4 ("Resolution & Lineage") without rewriting history.
> - **New Event ($\text{score} < 0.45$):** Creates a distinct canonical disaster event profile.
> - **Conflict Preservation:** Conflicting field values across corroborating sources (e.g. conflicting severities or timestamps) are preserved in `conflicting_claims` rather than overwritten.

> [!NOTE]
> **Pipeline Run History, Safe Retries & Idempotency:**
> - Every pipeline execution receives a unique stable identifier (`run_...` or `retry_...`) capturing UTC timestamps, duration, status, and input summaries.
> - Passwords, tokens, API keys, and connection strings are automatically redacted from error traces and run records.
> - Retries establish parent-child execution lineage (`retry_of_run_id`). Reprocessing identical inputs preserves strict entity idempotency (no duplicate sources, observations, events, or merge candidates), while append-only resolution decisions maintain full auditability.
> - Filtering by `failed_inputs_only` leverages structured per-input execution records (`InputFileExecutionRecord`) and stable identifiers, avoiding fragile error text parsing.
> - Recurring/cron scheduling is deferred in MVP v1.0.0.

> [!NOTE]

> **Antimeridian Bounding-Box Boundary:**
> The synthesized event bounding box computes standard Cartesian planar extrema (`min` and `max` longitude). It is not antimeridian-aware across the $\pm 180^\circ$ International Date Line. Individual constituent observation coordinates remain intact for spherical GIS calculations.

---

## Fresh Developer Walkthrough & Demo Steps

A fresh developer can clone the repository, run tests, and explore the complete application without configuring external databases or Docker containers:

### 1. Setup and Verify Tests
```powershell
# Activate virtual environment and run the test suite (301 passed, 6 live PostgreSQL-only tests safely skipped)
.\.venv\Scripts\Activate.ps1
pytest -rs tests
```

### 2. Launch the Web Dashboard
```powershell
# In-memory SQLite is used automatically when no PostgreSQL database URL is configured
streamlit run src/beacon/dashboard/app.py
```

### 3. End-to-End Demo Workflow
1. **Pipeline Execution (Tab 3):** Click **"⚡ Run Complete Pipeline"** to process the 5 heterogeneous sample files in `data/sample/`. Inspect real-time execution duration and the new entry in the **"Pipeline Run History & Audit Trail"** table.
2. **Safe Execution Retries (Tab 3):** Under **"🔄 Safe Execution Retries"**, select the executed run ID, choose **"Full execution retry"** or **"Retry failed inputs only"**, and click **"Retry Run"**. Note that entity records do not duplicate, while run history maintains parent-child lineage.
3. **Live Overview (Tab 1):** View overall system metrics, disaster distribution by severity, and interactive geospatial mapping of disaster events.
4. **Event Explorer (Tab 2):** Filter events by type (e.g., `flood`) or severity (e.g., `severe`). Drill down into canonical profiles, constituent observations with source references, and preserved conflicting claims.
5. **Resolution & Lineage (Tab 4):** Inspect candidate pairs and review unassigned ambiguous observations with their exact feature similarity scores.
6. **Analytics & Quality (Tab 5):** Review compression ratios, multi-source corroboration metrics, and data quality rule evaluation charts.
7. **Database Health (Tab 6):** Verify connection status and schema readiness checks across all 8 relational tables.

---

## Five-Phase Project Roadmap

| Phase | Title | Focus Area | Status |
|---|---|---|---|
| **Phase 1** | **Foundation & Data Contracts** | Canonical Pydantic schemas, deterministic IDs, config, sample datasets, unit test suite. | **Completed (`v0.1-foundation`)** |
| **Phase 2** | **Multi-Format Ingestion & Extraction** | Extractors for JSON, CSV, structured TXT, multi-page PDF (`pypdf`), and image validation (`Pillow`). | **Completed (`v0.2-ingestion`)** |
| **Phase 3** | **ETL, Data Quality & PostgreSQL Storage** | Decoupled ETL pipeline, explicit DQ rules/audit reports, SQLAlchemy 2.0 ORM & PostgreSQL persistence. | **Completed (`v0.3-storage`)** |
| **Phase 4** | **Entity Resolution & Unified Event Synthesis** | Spatiotemporal clustering, cross-source co-reference, confidence weighting, conflict tracking, and analytics. | **Completed (`v0.4-event-resolution`)** |
| **Phase 5** | **Release Hardening & Streamlit Dashboard** | Multi-screen dashboard, live PostgreSQL CI, E2E validation, and v1.0.0 MVP release. | **Completed (`v1.0.0`)** |
