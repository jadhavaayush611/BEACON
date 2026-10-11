# BEACON Canonical Data Contracts Specification

**Version:** 1.0.0 (Phase 1 Baseline)  
**Status:** Canonical Reference

---

## 1. Architectural Purpose and Core USP

BEACON is a Multi-Format Disaster Intelligence Data Engineering Pipeline engineered around a foundational principle:

> **"One event, many sources, one trusted data profile."**

In crisis response and disaster analytics, ingested data arrives asynchronously from heterogeneous channels (structured sensor JSON streams, CSV field logs, emergency dispatch text sitreps, formal aerial PDF reports, and drone raster imagery). Sources routinely contradict one another on timing, magnitude, casualties, and localized severity.

BEACON **never** assumes that repeated claims are automatically true, nor does it silently discard contradictory evidence. The data contracts defined in this specification provide the immutable data foundation and typed schema contracts required to preserve granular provenance, audit trails, and evidence lineage.

---

## 2. Entity Hierarchy and Relationships

The pipeline architecture enforces a three-tier model hierarchy:

```
+-----------------------------------------------------------------------+
|                             SourceRecord                              |
|           (Ingested raw document / payload provenance metadata)        |
+-----------------------------------+-----------------------------------+
                                    | 1
                                    |
                                    | produces
                                    | n
+-----------------------------------v-----------------------------------+
|                          DisasterObservation                          |
|         (Atomic extracted claim; unverified, retains raw claim)        |
+-----------------------------------+-----------------------------------+
                                    | n
                                    |
                                    | resolved into
                                    | 1
+-----------------------------------v-----------------------------------+
|                         UnifiedDisasterEvent                          |
|          (Consolidated profile; preserves conflicting claims)         |
+-----------------------------------------------------------------------+
```

### Source-to-Observation Cardinality
- A single **`SourceRecord`** produces zero, one, or multiple **`DisasterObservation`** records (e.g., a PDF report may contain 15 distinct incident observations; a tabular CSV row maps to one observation).
- Each observation references its originating parent via `source_id` and pinpoints its within-document location via `source_reference` (e.g., page number, JSON path, row index, or bounding region).

### Observation-to-Event Cardinality
- Many **`DisasterObservation`** records from distinct sources (e.g., JSON telemetry, CSV field logs, text sitreps, and PDF aerial reconnaissance) are synthesized into a single consolidated **`UnifiedDisasterEvent`**.
- The unified event retains foreign keys to all supporting observations (`observation_ids`) and parent sources (`source_ids`).
- If observations conflict on severity, magnitude, or coordinates, the differing values are retained in `conflicting_claims` rather than being overwritten.

---

## 3. Canonical Model Specifications

### 3.1 `SourceRecord`

Represents an ingested source artifact. Tracks byte integrity, ingestion timestamps, and format-specific metadata.

| Field Name | Type | Constraints / Format | Description |
|---|---|---|---|
| `source_id` | `str` | Format: `src_<sha256[:16]>` | Stable, deterministic identifier derived from source type and normalized file reference. |
| `source_file` | `str` | Non-empty string | Filename, URI, or storage pointer to the ingested source file. |
| `source_type` | `SourceType` (Enum) | `pdf`, `json`, `csv`, `txt`, `image`, `api` | Format classification of the input channel. |
| `extraction_method` | `ExtractionMethod` (Enum) | `json_parse`, `csv_parse`, `text_read`, `pdf_text`, `ocr`, `api_fetch` | Extraction algorithm scheduled or executed. |
| `ingested_at` | `datetime` | Timezone-aware UTC | Timestamp recording ingestion into the pipeline. Naive datetimes are rejected. |
| `extraction_status` | `ExtractionStatus` (Enum) | `pending`, `success`, `partial`, `failed` | Current status of the ingestion pipeline step. Default: `pending`. |
| `content_hash` | `Optional[str]` | 64-char lowercase hexadecimal | SHA-256 digest of the raw source file bytes. Validated against regex `^[a-f0-9]{64}$`. |
| `raw_content` | `Optional[str]` | Optional | Extracted raw text, payload preview, or payload reference. |
| `metadata` | `dict[str, Any]` | Credential-scrubbed dictionary | Format metadata (byte size, headers, MIME type). **Secret keys (passwords, API tokens) are strictly forbidden.** |

#### Validation Constraints:
1. **Timezone Awareness:** Naive datetimes raise a `ValidationError`. Aware datetimes in non-UTC offsets are normalized to UTC.
2. **Metadata Security Policy:** Keys matching `password`, `secret`, `token`, `api_key`, `auth`, `authorization`, or `private_key` are rejected during model validation.
3. **Strict Extra Fields:** `extra="forbid"` prevents arbitrary undeclared fields.

---

### 3.2 `DisasterObservation`

Represents an atomic, extracted claim or observation reported by an individual source document. It is **not** a confirmed fact.

| Field Name | Type | Constraints / Format | Description |
|---|---|---|---|
| `observation_id` | `str` | Format: `obs_<sha256[:16]>` | Stable, deterministic identifier derived from `source_id`, `source_reference`, and claim content. |
| `source_id` | `str` | Foreign key to `SourceRecord` | Reference to the originating source document. |
| `event_type` | `str` | Non-empty; normalized lowercase | Extensible category. Standard types include `flood`, `wildfire`, `earthquake`, etc. |
| `location` | `Optional[str]` | Free text | Human-readable location reported by the source. Missing represented as `None`. |
| `event_time` | `Optional[datetime]` | Timezone-aware UTC | Incident timestamp reported in source text. **Never defaulted to ingestion time.** |
| `description` | `Optional[str]` | Extracted string | Textual narrative. Blank/whitespace-only input is automatically converted to `None`. |
| `severity` | `Optional[str]` | `minor`, `moderate`, `severe`, `critical`, `unknown` | Normalized severity level, if stated. |
| `latitude` | `Optional[float]` | Range: `[-90.0, 90.0]` | Geographic latitude. Values outside range are rejected with `ValidationError`. |
| `longitude` | `Optional[float]` | Range: `[-180.0, 180.0]` | Geographic longitude. Values outside range are rejected with `ValidationError`. |
| `extraction_confidence` | `Optional[float]` | Range: `[0.0, 1.0]` | Model or parser confidence score. |
| `page_number` | `Optional[int]` | Integer `> 0` | 1-based page number in multi-page documents (PDFs). Values `0` or negative are rejected. |
| `source_reference` | `Optional[str]` | Free text | Precise locator within source (e.g. `row_14`, `$.items[0]`, `bbox:[40,50,120,80]`). |
| `raw_claim` | `Optional[str]` | Original string | Verbatim unparsed claim extracted from source. |
| `validation_warnings` | `list[str]` | List of strings | Non-fatal warnings (e.g., blank description converted to None, custom event category). |

#### Validation Constraints & Decisions:
1. **Extensible Categories:** Known standard categories are validated without warnings. Custom categories are normalized (stripped and lowercased), accepted, and flagged in `validation_warnings` to allow unexpected disaster phenomena.
2. **Missing Information & Non-Fabrication:** Missing attributes are represented strictly as `None`. The system **never** substitutes the current execution time for an unstated `event_time`.
3. **Blank Description Handling:** Whitespace-only strings (`"   "`) are converted to `None`, and `"Blank or whitespace-only description normalized to None."` is appended to `validation_warnings`.
4. **Coordinate Ranges:** Strict boundary validation ensures invalid GPS coordinates are rejected immediately.

---

### 3.3 `UnifiedDisasterEvent` & `ConflictingClaim`

Represents the synthesized disaster event profile produced by event resolution engines.

#### `ConflictingClaim`
Preserves contradictory claims across sources without discarding minority or divergent reports.

| Field Name | Type | Description |
|---|---|---|
| `field_name` | `str` | Attribute name in conflict (e.g., `severity`, `casualty_count`, `event_time`). |
| `observation_id` | `str` | Identifier of observation asserting this claim. |
| `source_id` | `str` | Source identifier where the claim originated. |
| `claimed_value` | `Any` | Value asserted by this source. |
| `confidence` | `Optional[float]` | Extraction confidence of the claim. |
| `resolution_status` | `str` | Status: `unresolved`, `accepted`, `superseded`, `flagged_for_review`. |
| `notes` | `Optional[str]` | Detailed reasoning for accepting or superseding this claim. |

#### `UnifiedDisasterEvent`
The consolidated disaster event profile.

| Field Name | Type | Constraints / Format | Description |
|---|---|---|---|
| `event_id` | `str` | Format: `evt_<sha256[:16]>` | Stable, deterministic identifier derived from event type, location, and start time. |
| `event_type` | `str` | Normalized string | Consolidated disaster type. |
| `location` | `str` | Normalized string | Consolidated geographic location name. |
| `start_time` | `datetime` | Timezone-aware UTC | Consolidated incident start timestamp. |
| `end_time` | `Optional[datetime]` | Timezone-aware UTC | Incident conclusion timestamp. Must be `>= start_time`. |
| `summary` | `str` | Synthesized text | Comprehensive narrative summary of the unified event. |
| `severity` | `Optional[str]` | Normalized rating | Consolidated incident severity rating. |
| `observation_ids` | `list[str]` | Minimum length: `1` | Supporting observation IDs contributing evidence. |
| `created_at` | `datetime` | Timezone-aware UTC | Timestamp when event profile was created. |
| `updated_at` | `datetime` | Timezone-aware UTC | Timestamp when event profile was last modified. |
| `conflicting_claims` | `list[ConflictingClaim]` | List of claim objects | Preserved contradictory claims with resolution rationale. |
| `resolution_rationale`| `Optional[str]` | Audit trail | Explanation of evidence policies used for consolidation. |
| `source_ids` | `list[str]` | List of strings | Contributing distinct source document IDs. |
| `metadata` | `dict[str, Any]` | Extensible metadata | Bounding boxes, tags, sensor aggregates. |

---

## 4. Deterministic Identifier Conventions

To support pipeline idempotency, replayability, and rerun stability, all primary keys are deterministic rather than random UUIDs:

1. **Source Identifiers (`source_id`):**
   - **Origin:** Source-derived.
   - **Formula:** `src_` + `SHA256(source_type + "::" + normalized_file_path + ["::" + content_hash])[:16]`
   - **Guarantee:** Ingesting the same file path and content on consecutive pipeline runs generates an identical `source_id`.

2. **Observation Identifiers (`observation_id`):**
   - **Origin:** Observation-derived.
   - **Formula:** `obs_` + `SHA256(source_id + "::" + source_reference + "::" + raw_claim + ["::" + index])[:16]`
   - **Guarantee:** Parsing the identical claim at the identical line/record within the source yields the same `observation_id`.

3. **Event Identifiers (`event_id`):**
   - **Origin:** Cluster/Event-derived.
   - **Formula:** `evt_` + `SHA256(event_type + "::" + location + "::" + start_time_iso_utc)[:16]`
   - **Guarantee:** Re-clustering the same temporal and spatial incident creates the identical canonical event entity.

---

## 5. Support for All Five Input Formats

| Format | Source Type | Extraction Method | Locator Mapping (`source_reference`) | Extraction Path Overview |
|---|---|---|---|---|
| **JSON** | `SourceType.JSON` | `ExtractionMethod.JSON_PARSE` | JSONPath (e.g. `items[0]`, `$.data.incidents[2]`) | Ingested via native JSON parser directly deserialized into observation records. |
| **CSV** | `SourceType.CSV` | `ExtractionMethod.CSV_PARSE` | Row index / record key (e.g. `row_14`, `CSV-SIM-101`) | Parsed line-by-line via `csv.DictReader` or pandas; empty strings map to `None`. |
| **TXT** | `SourceType.TXT` | `ExtractionMethod.TEXT_READ` | Incident header / line span (e.g. `line_20-45`, `[REPORT 2026-10-12-001]`) | Unstructured sitrep parsed via regex tokenization and entity extraction patterns. |
| **PDF** | `SourceType.PDF` | `ExtractionMethod.PDF_TEXT` | Page number and section (e.g. `page_1::section_3`) | Multi-page text streams extracted with page numbering preserved; text hashed for verification. |
| **Image** | `SourceType.IMAGE` | `ExtractionMethod.OCR` | Pixel bounding box / crop coordinates (e.g. `bbox:[50,60,110,180]`) | Raster maps processed via OCR engine; visual metadata preserved in observation. |

---

## 6. Provenance and Conflict Resolution Strategy

BEACON's fundamental principle is that truth is not established by popularity or duplication:

1. **Retaining Contradictions:** When Source A (a local CSV sensor log) claims "moderate" flooding with water height 0.8m, and Source B (an aerial PDF reconnaissance report) claims "severe" flooding with water height 1.45m, BEACON does **not** discard the CSV report.
2. **Explicit Lineage:** Both observations remain linked in `observation_ids`. The consolidated event adopts the higher confidence observation for its primary `severity` field, while recording the divergent claim in `conflicting_claims` along with an explicit `notes` rationale and `resolution_rationale` explanation.
3. **Audit Readiness:** Emergency coordinators can drill down into any consolidated field to view every supporting observation, raw excerpt, and originating source file.
