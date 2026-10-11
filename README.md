# BEACON — Multi-Format Disaster Intelligence Data Engineering Pipeline

> **"One event, many sources, one trusted data profile."**

BEACON is a production-grade data engineering pipeline engineered to ingest heterogeneous disaster-related information from unstructured and semi-structured sources: PDF situation reports, JSON telemetry streams, CSV field sensors, text dispatches, and aerial raster images. It normalizes atomic observations, preserves end-to-end source provenance, retains conflicting claims, and consolidates co-referent reports into unified disaster event profiles.

---

## Current Status: Phase 1 (Foundation & Data Contracts)

Phase 1 establishes the bedrock of BEACON without prematurely introducing heavy database, OCR, or orchestrator dependencies:
- **Canonical Pydantic Data Contracts:** Strongly typed `SourceRecord`, `DisasterObservation`, and `UnifiedDisasterEvent` models with strict validation rules.
- **Typed Configuration & Security:** Single-source-of-truth configuration via `pydantic-settings` (`src/beacon/config.py`) and log filters that scrub credentials.
- **Deterministic Identity Schemes:** Reproducible, rerun-stable identifier generation for sources, observations, and events.
- **Multi-Format Sample Datasets:** Synthetic JSON, CSV, TXT, PDF, and PNG sample assets with co-referent disaster incidents and missing field demonstrations.
- **Automated Test Suite:** Comprehensive pytest verification covering model validations, configuration overrides, and sample data integrity.

---

## Architectural Decision: Centralized Configuration

In accordance with Python packaging best practices (`src`-layout), all application configuration is centralized exclusively in `src/beacon/config.py` using `pydantic-settings`. The redundant root-level `config/settings.py` was intentionally omitted to prevent competing settings implementations, eliminate module drift, and ensure all application code imports from `beacon.config`.

---

## Project Structure

```text
beacon/
├── README.md                          # Project documentation and roadmap
├── pyproject.toml                     # Package build configuration & pytest options
├── requirements.txt                   # Minimal Phase 1 runtime dependencies
├── .gitignore                         # Python, venv, secret, and raw data exclusions
├── .env.example                       # Non-sensitive configuration template
├── src/
│   └── beacon/
│       ├── __init__.py                # Package exports & public API
│       ├── config.py                  # Single authoritative typed Settings
│       ├── models.py                  # Pydantic models (SourceRecord, DisasterObservation, etc.)
│       └── logging_config.py          # Secure logging setup with credential redaction
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
│   └── generate_sample_binaries.py    # Deterministic pure-Python sample PDF/PNG generator
├── tests/
│   ├── test_models.py                 # Data contract validations & constraints
│   ├── test_config.py                 # Configuration and logging filter tests
│   └── test_sample_data.py            # Sample dataset integrity & co-reference tests
└── docs/
    └── data_contract.md               # Full specification of models & schemas
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

### 2. Install Phase 1 Dependencies
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

All 54 tests run completely locally without requiring external APIs, PostgreSQL, or OCR binaries.

---

## Five-Phase Project Roadmap

| Phase | Title | Focus Area | Status |
|---|---|---|---|
| **Phase 1** | **Foundation & Data Contracts** | Canonical Pydantic schemas, deterministic IDs, config, sample datasets, unit test suite. | **Completed** |
| **Phase 2** | **Multi-Format Ingestion & Extraction** | Extractors for PDF (pypdf/pdfplumber), JSON, CSV, TXT (regex/NLP), and raster Image OCR (Tesseract/Vision). | Upcoming |
| **Phase 3** | **Entity Resolution & Provenance Engine** | Spatiotemporal clustering, co-reference resolution, confidence weighting, and conflict tracking. | Upcoming |
| **Phase 4** | **Storage & Transformation Pipeline** | Relational storage (PostgreSQL), data lake ingestion, SQL transformations, and dbt models. | Upcoming |
| **Phase 5** | **Disaster Intelligence Dashboard** | Interactive Streamlit/FastAPI UI for situation awareness, audit trails, and data profiling. | Upcoming |
