# BEACON — Multi-Format Disaster Intelligence Data Engineering Pipeline

> **"One event, many sources, one trusted data profile."**

BEACON is a production-grade data engineering pipeline engineered to ingest heterogeneous disaster-related information from unstructured and semi-structured sources: PDF situation reports, JSON telemetry streams, CSV field sensors, text dispatches, and aerial raster images. It normalizes atomic observations, preserves end-to-end source provenance, retains conflicting claims, and consolidates co-referent reports into unified disaster event profiles.

---

## Current Status: Phase 2 (Multi-Format Ingestion Completed)

Phase 2 implements the ingestion and extraction engine for all five input formats:
- **Central Dispatcher (`ingest_file`):** Case-insensitive extension resolution (`.json`, `.csv`, `.txt`, `.pdf`, `.png`, etc.) producing a unified `IngestionResult`.
- **JSON Ingestor (`JsonIngestor`):** Safely deserializes structured JSON feeds, validating individual records into `DisasterObservation` and handling partial extractions.
- **CSV Ingestor (`CsvIngestor`):** Standard library tabular parser mapping sensor rows, preserving row locators, and handling missing values without fact fabrication.
- **TXT Ingestor (`TxtIngestor`):** Grammar-based situational text parser for delimited `[REPORT ...]` blocks with multi-line narrative extraction.
- **PDF Ingestor (`PdfIngestor`):** Lightweight `pypdf` extractor for multi-page situation reports with page-level reference tracking.
- **Image Ingestor (`ImageIngestor`):** `Pillow`-based raster container validation and metadata extraction, with OCR deferred to later phases.
- **Cryptographic Provenance:** Automatic SHA-256 calculation over raw bytes and rerun-stable deterministic identifier generation.

---

## Architectural Decision: Centralized Configuration

In accordance with Python packaging best practices (`src`-layout), all application configuration is centralized exclusively in `src/beacon/config.py` using `pydantic-settings`. The redundant root-level `config/settings.py` was intentionally omitted to prevent competing settings implementations, eliminate module drift, and ensure all application code imports from `beacon.config`.

---

## Project Structure

```text
beacon/
├── README.md                          # Project documentation and roadmap
├── pyproject.toml                     # Package build configuration & pytest options
├── requirements.txt                   # Phase 2 runtime dependencies
├── .gitignore                         # Python, venv, secret, and raw data exclusions
├── .env.example                       # Non-sensitive configuration template
├── src/
│   └── beacon/
│       ├── __init__.py                # Package exports & public API
│       ├── config.py                  # Single authoritative typed Settings
│       ├── models.py                  # Pydantic models (SourceRecord, DisasterObservation, etc.)
│       ├── logging_config.py          # Secure logging setup with credential redaction
│       └── ingestion/                 # Multi-format ingestion subsystem
│           ├── __init__.py            # Ingestion public interface
│           ├── base.py                # BaseIngestor abstract class & SHA-256 helpers
│           ├── dispatcher.py          # Central ingest_file entry point & routing
│           ├── exceptions.py          # Typed ingestion exceptions
│           ├── result.py              # IngestionResult typed model
│           ├── json_ingestor.py       # JSON telemetry ingestor
│           ├── csv_ingestor.py        # CSV sensor log ingestor
│           ├── txt_ingestor.py        # Structured text situation report ingestor
│           ├── pdf_ingestor.py        # pypdf multi-page situation report ingestor
│           └── image_ingestor.py      # Pillow image validator & metadata reader
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
│   ├── test_sample_data.py            # Sample dataset integrity & co-reference tests
│   └── test_ingestion.py              # Multi-format ingestion, routing, and error tests
└── docs/
    ├── data_contract.md               # Canonical data contracts specification
    └── ingestion.md                   # Ingestion & extraction architecture guide
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

All 92 tests run completely locally without requiring external APIs, PostgreSQL, or OCR binaries.

---

## Ingestion Usage Example

```python
from pathlib import Path
from beacon import ingest_file

# Ingest any supported format via the unified API
result = ingest_file("data/sample/events.json")

print(f"Status: {result.status.value}")
print(f"Source ID: {result.source_record.source_id}")
print(f"Content Hash: {result.source_record.content_hash}")
print(f"Accepted Observations: {result.accepted_count}")

for obs in result.observations:
    print(f"  - [{obs.event_type}] {obs.location} @ {obs.event_time} (severity={obs.severity})")
```

---

## Five-Phase Project Roadmap

| Phase | Title | Focus Area | Status |
|---|---|---|---|
| **Phase 1** | **Foundation & Data Contracts** | Canonical Pydantic schemas, deterministic IDs, config, sample datasets, unit test suite. | **Completed** |
| **Phase 2** | **Multi-Format Ingestion & Extraction** | Extractors for JSON, CSV, structured TXT, multi-page PDF (`pypdf`), and image validation (`Pillow`). | **Completed** |
| **Phase 3** | **Entity Resolution & Provenance Engine** | Spatiotemporal clustering, co-reference resolution, confidence weighting, and conflict tracking. | Upcoming |
| **Phase 4** | **Storage & Transformation Pipeline** | Relational storage (PostgreSQL), data lake ingestion, SQL transformations, and dbt models. | Upcoming |
| **Phase 5** | **Disaster Intelligence Dashboard** | Interactive Streamlit/FastAPI UI for situation awareness, audit trails, and data profiling. | Upcoming |
