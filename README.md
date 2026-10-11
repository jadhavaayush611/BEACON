# BEACON — Multi-Format Disaster Intelligence Data Engineering Pipeline

> **"One event, many sources, one trusted data profile."**

BEACON is a production-grade data engineering pipeline engineered to ingest heterogeneous disaster-related information from unstructured and semi-structured sources: PDF situation reports, JSON telemetry streams, CSV field sensors, text dispatches, and aerial raster images. It normalizes atomic observations, preserves end-to-end source provenance, retains conflicting claims, and consolidates co-referent reports into unified disaster event profiles.

---

## Current Status: Phase 3 (ETL, Data Quality & PostgreSQL Storage Completed)

Phase 3 implements the complete, idempotent, auditable data engineering pipeline:
- **Observation Transformation Layer (`ObservationTransformer`):** Deterministic normalization of whitespace, geographic coordinate precision (6 decimal places), severity scale, and event category synonyms without fabricating missing facts.
- **Data Quality Framework (`beacon.quality`):** Machine-readable validation rules with explicit severity tiers (`ERROR`, `WARNING`, `INFO`), generating fine-grained `ValidationOutcome` records and `QualityReport` summaries. Records with fatal errors are rejected from persistence while audit records are preserved.
- **Relational PostgreSQL Storage (`beacon.storage`):** Production-grade SQLAlchemy 2.0 schemas (`sources`, `observations`, `data_quality_results`), pure SQL DDL (`schema.sql`), connection pooling, and atomic transaction handling with rollback protection.
- **ETL Pipeline Orchestrator (`ETLRunner`):** End-to-end Extract $\rightarrow$ Transform $\rightarrow$ Validate $\rightarrow$ Load $\rightarrow$ Report execution with partial-failure isolation and deterministic rerun idempotency.
- **Database CLI Initializer (`scripts/init_db.py`):** Schema creation and reset utility.

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
│       └── storage/                   # Relational Persistence Layer
│           ├── __init__.py            # Storage exports
│           ├── schema.py              # SQLAlchemy ORM models (SourceModel, ObservationModel, QualityResultModel)
│           ├── schema.sql             # Portable pure PostgreSQL DDL script
│           ├── connection.py          # Engine creation, pooling, session factory & context manager
│           └── repository.py          # StorageRepository idempotent CRUD & queries
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
│   └── init_db.py                     # CLI database schema initialization & reset script
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
│   └── test_storage_integration.py    # Live PostgreSQL integration test (skipped if DB unset)
└── docs/
    ├── data_contract.md               # Canonical data contracts specification
    ├── ingestion.md                   # Ingestion & extraction architecture guide
    └── etl_and_storage.md             # ETL pipeline, data quality & PostgreSQL storage guide
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

All 141 unit tests run completely locally without requiring external APIs, PostgreSQL, or OCR binaries.

To optionally run integration tests against a live PostgreSQL database:
```powershell
$env:BEACON_TEST_DATABASE_URL = "postgresql+psycopg://postgres:postgres@localhost:5432/beacon_test"
pytest -v tests/test_storage_integration.py
```

---

## ETL Pipeline Usage Examples

### Running the End-to-End Pipeline
```python
from pathlib import Path
from beacon.pipeline import ETLRunner
from beacon.storage import create_db_engine, get_session_factory

# 1. Create session factory (or pass None to run in storage-free in-memory mode)
engine = create_db_engine()
session_factory = get_session_factory(engine)

# 2. Initialize ETL runner
runner = ETLRunner(session_factory=session_factory)

# 3. Execute across input files
input_files = [
    Path("data/sample/events.json"),
    Path("data/sample/events.csv"),
    Path("data/sample/events.txt"),
    Path("data/sample/sample_report.pdf"),
]
result = runner.run(input_files)

# 4. View results
print(result.summary())
print(f"Accepted observations: {result.observations_accepted}")
print(f"Persisted records: {result.records_persisted}")
```

### Database Initialization CLI
```powershell
# Create tables non-destructively using settings from .env
python scripts/init_db.py

# Drop and recreate tables interactively (prompts for confirmation)
python scripts/init_db.py --drop-first

# Drop and recreate tables non-interactively / in CI (requires explicit --yes)
python scripts/init_db.py --drop-first --yes
```

---

## Five-Phase Project Roadmap

| Phase | Title | Focus Area | Status |
|---|---|---|---|
| **Phase 1** | **Foundation & Data Contracts** | Canonical Pydantic schemas, deterministic IDs, config, sample datasets, unit test suite. | **Completed** |
| **Phase 2** | **Multi-Format Ingestion & Extraction** | Extractors for JSON, CSV, structured TXT, multi-page PDF (`pypdf`), and image validation (`Pillow`). | **Completed** |
| **Phase 3** | **ETL, Data Quality & PostgreSQL Storage** | Decoupled ETL pipeline, explicit DQ rules/audit reports, SQLAlchemy 2.0 ORM & PostgreSQL persistence. | **Completed** |
| **Phase 4** | **Entity Resolution & Unified Event Synthesis** | Spatiotemporal clustering, cross-source co-reference, confidence weighting, and conflict tracking. | Upcoming |
| **Phase 5** | **Disaster Intelligence Dashboard** | Interactive Streamlit/FastAPI UI for situation awareness, audit trails, and data profiling. | Upcoming |
