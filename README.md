# BEACON — Multi-Format Disaster Intelligence Data Engineering Pipeline

> **"One event, many sources, one trusted data profile."**

BEACON is a production-grade data engineering pipeline engineered to ingest heterogeneous disaster-related information from unstructured and semi-structured sources: PDF situation reports, JSON telemetry streams, CSV field sensors, text dispatches, and aerial raster images. It normalizes atomic observations, preserves end-to-end source provenance, retains conflicting claims, and consolidates co-referent reports into unified disaster event profiles.

---

## Current Status: Phase 4 (Event Resolution & Analytics Completed)

Phase 4 implements an explainable, deterministic event-resolution layer and analytics engine:
- **Core Proposition:** "One event, many sources, one trusted data profile."
- **Multi-Attribute Blocking (`CandidateGenerator`):** Category compatibility, temporal bounds, and spatial bounding boxes to evaluate co-referent events without quadratic comparisons.
- **Feature Comparison & Scoring Engine (`ObservationMatcher`):** Spatiotemporal proximity via Haversine distance, temporal hours delta, textual token Jaccard similarity, dynamic weight rebalancing, and calibration safeguards.
- **Decision Policy & Bridge Detection (`MatchPolicy`):** Classifies candidate matches into `confident_match`, `ambiguous_candidate`, `no_match`, and flags multi-event bridges as `merge_candidate` for human audit without silent destructive merges.
- **Unified Profile Synthesis (`EventProfileAggregator`):** Derives canonical `UnifiedDisasterEvent` profiles, spatiotemporal extents, bounding boxes, consensus values, and tracks divergent values in `conflicting_claims`.
- **Relational Storage & Additive Migration:** Extends schema with `events`, `event_observations`, `resolution_decisions`, and `merge_candidates`. Includes pure SQL migration `v0_4_event_resolution.sql` and CLI `scripts/migrate_v0_4.py`.
- **Disaster Intelligence Analytics (`AnalyticsService`):** Aggregates compression ratios, multi-source validation metrics, spatial/temporal coverage gaps, and conflicting claim breakdowns.

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
│   └── test_migration_v0_4.py         # Phase 4 migration CLI and safety tests
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

All 198 unit tests run completely locally without requiring external APIs, PostgreSQL, or OCR binaries.

To optionally run integration tests against a live PostgreSQL database:
```powershell
$env:BEACON_TEST_DATABASE_URL = "postgresql+psycopg://postgres:postgres@localhost:5432/beacon_test"
pytest -v tests/test_storage_integration.py
```

---

## Event Resolution & Analytics Usage Examples

### End-to-End Event Resolution
```python
from pathlib import Path
from beacon.ingestion import ingest_file
from beacon.pipeline import ObservationTransformer
from beacon.resolution import EventResolver
from beacon.analytics import AnalyticsService

# 1. Ingest heterogeneous files
sample_files = [
    Path("data/sample/events.json"),
    Path("data/sample/events.csv"),
    Path("data/sample/events.txt"),
    Path("data/sample/sample_report.pdf"),
]
transformer = ObservationTransformer()
observations = []
for p in sample_files:
    res = ingest_file(p)
    for raw in res.observations:
        observations.append(transformer.transform(raw))

# 2. Resolve co-referent observations into unified events
resolver = EventResolver()
res_result = resolver.resolve_observations(observations)

print(f"Total Observations: {res_result.total_observations}")
print(f"Events Created: {res_result.events_created}")
for ev in res_result.resolved_events:
    print(f"Unified Event [{ev.event_id}]: {ev.summary}")
    print(f"  Contributing Sources ({len(ev.source_ids)}): {ev.source_ids}")
    print(f"  Conflicting Claims ({len(ev.conflicting_claims)}): {ev.conflicting_claims}")

# 3. Compute disaster intelligence analytics
analytics = AnalyticsService()
summary = analytics.summarize(res_result.resolved_events, observations=observations)
print(f"Compression Ratio: {summary.compression_ratio}")
print(f"Multi-Source Ratio: {summary.multi_source_ratio * 100:.1f}%")
```

### Database Migration CLI
```powershell
# Inspect migration DDL without applying changes
python scripts/migrate_v0_4.py --dry-run

# Apply additive migration to target database
python scripts/migrate_v0_4.py --url postgresql+psycopg://postgres:postgres@localhost:5432/beacon
```

---

## Five-Phase Project Roadmap

| Phase | Title | Focus Area | Status |
|---|---|---|---|
| **Phase 1** | **Foundation & Data Contracts** | Canonical Pydantic schemas, deterministic IDs, config, sample datasets, unit test suite. | **Completed** |
| **Phase 2** | **Multi-Format Ingestion & Extraction** | Extractors for JSON, CSV, structured TXT, multi-page PDF (`pypdf`), and image validation (`Pillow`). | **Completed** |
| **Phase 3** | **ETL, Data Quality & PostgreSQL Storage** | Decoupled ETL pipeline, explicit DQ rules/audit reports, SQLAlchemy 2.0 ORM & PostgreSQL persistence. | **Completed** |
| **Phase 4** | **Entity Resolution & Unified Event Synthesis** | Spatiotemporal clustering, cross-source co-reference, confidence weighting, conflict tracking, and analytics. | **Completed** |
| **Phase 5** | **Disaster Intelligence Dashboard** | Interactive Streamlit/FastAPI UI for situation awareness, audit trails, and data profiling. | Upcoming |
