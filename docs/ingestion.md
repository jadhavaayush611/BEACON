# BEACON Ingestion & Multi-Format Extraction Guide

**Version:** 0.2.0 (Phase 2 Implementation)  
**Status:** Ingestion Reference Documentation

---

## 1. Overview & Architectural Role

Phase 2 implements the ingestion and extraction subsystem for **BEACON**. The ingestion layer ingests heterogeneous raw files across five primary formats (JSON, CSV, TXT, PDF, and PNG/raster image), computes cryptographically stable content provenance, extracts atomic claims, standardizes timestamps, and maps data into canonical Phase 1 data contracts without fabricating facts.

```
+-------------------------------------------------------------------------------+
|                             Raw Source File                                   |
|             (.json, .csv, .txt, .pdf, .png, .jpg, .tiff, etc.)               |
+---------------------------------------+---------------------------------------+
                                        |
                                        v
+-------------------------------------------------------------------------------+
|                       Dispatcher: ingest_file(path)                           |
|       - Validates filesystem existence                                         |
|       - Case-insensitive extension resolution                                 |
|       - Calculates SHA-256 byte digest and file size                          |
+---------------------------------------+---------------------------------------+
                                        |
      +-------------------+-------------+-------------+-------------------+
      |                   |                           |                   |
      v                   v                           v                   v
+------------+     +------------+              +------------+      +------------+
|JsonIngestor|     |CsvIngestor |              |TxtIngestor |      |PdfIngestor |
| (json_parse|     | (csv_parse)|              | (text_read)|      | (pdf_text) |
+------------+     +------------+              +------------+      +------------+
      |                   |                           |                   |
      +-------------------+-------------+-------------+-------------------+
                                        |
                                        +-------------------> +-----------------+
                                        |                     | ImageIngestor   |
                                        |                     | (metadata/verify|
                                        v                     +-----------------+
+-------------------------------------------------------------------------------+
|                              IngestionResult                                  |
|   - source_record: SourceRecord (provenance, hash, status, metadata)          |
|   - observations: list[DisasterObservation] (zero or more atomic claims)     |
|   - status: ExtractionStatus (SUCCESS, PARTIAL, FAILED)                       |
|   - counts: processed_count, accepted_count, rejected_count                   |
|   - diagnostics: errors, warnings                                             |
+-------------------------------------------------------------------------------+
```

---

## 2. Public Ingestion API

The central public entrypoint is `ingest_file`:

```python
from pathlib import Path
from beacon import ingest_file

result = ingest_file("data/sample/events.json")

print(f"Status: {result.status.value}")
print(f"Accepted observations: {result.accepted_count} of {result.processed_count}")
for obs in result.observations:
    print(f"  - [{obs.event_type}] {obs.location} @ {obs.event_time} (conf={obs.extraction_confidence})")
```

### Parameters
- `path: Union[str, Path]`: Path to the source file to ingest.
- `raise_on_unsupported: bool = False`: If `True`, raises `UnsupportedFormatError` when given an unsupported extension. When `False`, returns an `IngestionResult` with `status=ExtractionStatus.FAILED`.
- `raise_on_missing: bool = False`: If `True`, raises `SourceFileNotFoundError` when the file does not exist. When `False`, returns an `IngestionResult` with `status=ExtractionStatus.FAILED`.

---

## 3. Ingestion Result Structure & Status Taxonomy

Every call to `ingest_file` produces an `IngestionResult` model:

| Attribute | Type | Description |
|---|---|---|
| `source_record` | `SourceRecord` | Immutable ingestion record with `source_id`, `content_hash`, and format metadata. |
| `observations` | `list[DisasterObservation]` | Extracted, validated claims conforming to canonical Phase 1 schemas. |
| `status` | `ExtractionStatus` | `SUCCESS`, `PARTIAL`, or `FAILED`. |
| `errors` | `list[str]` | Non-secret actionable error descriptions for failed parsing or validation. |
| `warnings` | `list[str]` | Non-fatal diagnostics (e.g., OCR deferral notice, blank pages, custom categories). |
| `processed_count` | `int` | Total candidate records/blocks parsed in the document. |
| `accepted_count` | `int` | Number of records successfully mapped to `DisasterObservation`. |
| `rejected_count` | `int` | Number of records rejected due to validation or schema errors. |

### Status Distinctions:
1. **`SUCCESS` with observations:** All processed candidate records passed validation (`accepted_count > 0, rejected_count == 0`).
2. **`SUCCESS` with zero observations:** Successfully validated file containing no extractable claims (e.g. empty CSV/JSON array, or raster image containing no machine-readable text).
3. **`PARTIAL` extraction:** Some records were valid while malformed records were rejected (`accepted_count > 0, rejected_count > 0`). Valid records are preserved.
4. **`FAILED` extraction:** File corrupt, all records invalid, or unreadable container (`accepted_count == 0, rejected_count > 0` or read error).

---

## 4. Supported Formats & Field Mapping Rules

### 4.1 JSON (`JsonIngestor`)
- **Extensions:** `.json`
- **Method:** `ExtractionMethod.JSON_PARSE`
- **Mapping:**
  - `event_type`: Direct mapping from `event_type` key.
  - `location`: Mapped from `location` (or `None`).
  - `event_time`: Parsed from ISO-8601 string (e.g., `"2026-10-12T08:00:00Z"`) to UTC datetime. Unset or null remains `None`.
  - `latitude` / `longitude`: Direct mapping with `[-90, 90]` and `[-180, 180]` range enforcement.
  - `confidence` / `extraction_confidence`: Direct mapping with `[0, 1]` constraint.
  - `source_reference`: Prefers `source_reference` key, fallback to `record_id` or `items[index]`.
  - `raw_claim`: Compact JSON serialization of the originating object.

### 4.2 CSV (`CsvIngestor`)
- **Extensions:** `.csv`
- **Method:** `ExtractionMethod.CSV_PARSE`
- **Mapping:**
  - Header support: `record_id`, `event_type`, `location`, `event_time`, `latitude`, `longitude`, `severity`, `description`, `confidence`, `disclaimer`.
  - Empty strings (`""`) automatically map to `None` without fabricating values.
  - `source_reference`: Formatted as `row_{line_number}:{record_id}`.
  - Non-numeric coordinate or confidence strings are flagged as row errors without aborting other rows.

### 4.3 Structured TXT (`TxtIngestor`)
- **Extensions:** `.txt`
- **Method:** `ExtractionMethod.TEXT_READ`
- **Grammar & Scope:**
  - Parses delimited incident blocks starting with `[REPORT <identifier>]`.
  - Extracts key-value lines: `TIMESTAMP:`, `LOCATION:`, `EVENT:`, `SEVERITY:`, `NARRATIVE:`, and `SOURCE_ID:`.
  - Continuation lines without a key prefix append to multi-line narratives.
  - **Boundary Limitation:** BEACON does **not** perform open-ended natural-language extraction in this phase; only structured sitrep blocks adhering to the documented grammar are extracted.

### 4.4 PDF (`PdfIngestor`)
- **Extensions:** `.pdf`
- **Method:** `ExtractionMethod.PDF_TEXT`
- **Engine:** `pypdf`
- **Multipage & Page References:**
  - Iterates through all document pages with 1-based page numbers (`page_number = page_idx + 1`).
  - Extracts key-value incident fields: `Incident ID:`, `Event Category:`, `Timestamp:`, `Location:`, `Estimated Coordinates:`, `Severity:`, and narrative excerpts.
  - Page references are embedded directly in `source_reference` (e.g. `page_1:PDF-SIM-301`).
  - **Empty/Scanned Pages:** If a page contains no machine-readable text stream, a clear warning is recorded. Scanned pages are **not** falsely claimed to be OCR-processed.

### 4.5 Image / Raster (`ImageIngestor`)
- **Extensions:** `.png`, `.jpg`, `.jpeg`, `.tiff`, `.bmp`, `.webp`
- **Method:** `ExtractionMethod.OCR`
- **Engine:** `Pillow`
- **Validation & Metadata:**
  - Validates image container integrity using `Image.verify()`. Corrupted image files produce `ExtractionStatus.FAILED`.
  - Extracts image metadata: dimensions (`width`, `height`), `image_format`, and `color_mode`.
  - **OCR Deferral Policy:** Phase 2 strictly validates image containers without running OCR or computer vision. Ingesting visual images returns `ExtractionStatus.SUCCESS` with zero observations and a clear explanatory warning that semantic extraction requires subsequent OCR phases.

---

## 5. Provenance and Deterministic Identifiers

Every ingestion execution guarantees complete auditability:

1. **SHA-256 Content Hash:** Calculated from raw file bytes on disk before parsing.
2. **Deterministic Source ID:**
   `source_id = "src_" + SHA256(source_type + "::" + normalized_path + "::" + content_hash)[:16]`
3. **Deterministic Observation ID:**
   `observation_id = "obs_" + SHA256(source_id + "::" + source_reference + "::" + raw_claim + "::" + index)[:16]`
4. **Traceability:** Every `DisasterObservation` possesses an immutable foreign key `obs.source_id == source_rec.source_id`.

---

## 6. Testing

Run the ingestion test suite:

```powershell
pytest -v tests/test_ingestion.py
```

Run all project tests (Phase 1 + Phase 2):

```powershell
pytest -v
```
