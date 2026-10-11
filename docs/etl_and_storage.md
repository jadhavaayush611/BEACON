# BEACON — ETL Pipeline, Data Quality Framework & Storage Architecture

> **"One event, many sources, one trusted data profile."**

This document describes the design, execution flow, data quality framework, and database persistence architecture implemented in **Phase 3** of the BEACON pipeline.

---

## 1. Architectural Overview & Pipeline Flow

The BEACON pipeline operates on heterogeneous disaster inputs (JSON, CSV, TXT, PDF, raster images), transforming extracted observations into validated, auditable records and persisting them into PostgreSQL.

The execution flow follows five decoupled stages:

```
[ Raw Files ]
      │
      ▼
 1. EXTRACT   ──►  Multi-format Ingestors read files into IngestionResult instances.
      │
      ▼
 2. TRANSFORM ──►  ObservationTransformer normalizes categories, whitespace & precision.
      │
      ▼
 3. VALIDATE  ──►  DataQualityValidator evaluates explicit rules (Error/Warning/Info).
      │
      ▼
 4. LOAD      ──►  StorageRepository atomically persists sources, observations & DQ audit logs.
      │
      ▼
 5. REPORT    ──►  ETLPipelineResult aggregates metrics, audit logs, and status summaries.
```

### Decoupling & Storage Independence
The pipeline can execute in two modes:
1. **With Database Configured:** Persists source records, observations, and data quality evaluation outcomes into PostgreSQL (or SQLite for local development/testing).
2. **Without Database (`session_factory=None`):** Operates purely in-memory, extracting, transforming, and validating records while producing the complete `ETLPipelineResult` and `QualityReport` without requiring database connectivity.

---

## 2. Transformation Layer (`ObservationTransformer`)

The transformation layer applies deterministic, justified normalizations without fabricating missing information:

- **Whitespace & Formatting Normalization:** Strips leading/trailing whitespace and collapses inner multiple spaces across locations and descriptions. Trailing punctuation (`,`, `;`, `:`, `-`) is cleaned from location strings.
- **Event Category Standardization:** Converts category names to lowercase and standardizes common domain synonyms to canonical event types:
  - `"flash flood"`, `"riverine flood"`, `"urban flood"` $\rightarrow$ `"flood"`
  - `"brushfire"`, `"bushfire"`, `"forest fire"`, `"bush fire"` $\rightarrow$ `"wildfire"`
  - `"cyclonic storm"`, `"tropical storm"` $\rightarrow$ `"cyclone"`
  - `"tremor"` $\rightarrow$ `"earthquake"`
  - `"mudslide"`, `"rockfall"` $\rightarrow$ `"landslide"`
- **Severity Scale Normalization:** Lowercases severity values and converts sentinel tokens (`"unknown"`, `"none"`, `"n/a"`, `""`) to `None`.
- **Coordinate Precision Standard:** Rounds latitude and longitude to 6 decimal places ($\approx 0.1\text{m}$ precision at the equator) to eliminate floating-point representation artifacts.
- **Non-Fabrication Guarantee:** When optional attributes (`location`, `event_time`, `description`, `severity`, `latitude`, `longitude`, `extraction_confidence`, `page_number`) are unstated in the source data, they remain `None`. They are **never** populated with synthetic placeholders.

---

## 3. Data Quality Framework (`beacon.quality`)

BEACON evaluates extracted observations against explicit data quality rules before persistence.

### Severity Tiers

| Severity | Code | Meaning & Pipeline Action |
|---|---|---|
| **ERROR** | `RuleSeverity.ERROR` | Fatal rule breach. The observation is **rejected** from persistence into `observations`. The breach is recorded in `data_quality_results`. |
| **WARNING** | `RuleSeverity.WARNING` | Non-fatal anomaly. The observation is **accepted** and persisted, but flagged in `validation_warnings` and recorded in `data_quality_results`. |
| **INFO** | `RuleSeverity.INFO` | Audit/telemetry notification (e.g. unstated timestamp). Record is accepted. |

### Implemented Quality Rules

| Rule Name | Target Field | Severity | Logic & Criteria |
|---|---|---|---|
| `REQUIRED_OBSERVATION_ID` | `observation_id` | `ERROR` | Must be a non-empty, non-whitespace string. |
| `REQUIRED_SOURCE_ID` | `source_id` | `ERROR` | Must be a non-empty string referencing the parent source document. |
| `REQUIRED_EVENT_TYPE` | `event_type` | `ERROR` | Must be a non-empty string. |
| `COORDINATES_PAIRED` | `latitude`/`longitude` | `WARNING` | Latitude and longitude must both be present or both absent. Unpaired coordinates raise a warning. |
| `LATITUDE_BOUNDS` | `latitude` | `ERROR` | Valid range $[-90.0, 90.0]$. Values outside raise a fatal error. |
| `LONGITUDE_BOUNDS` | `longitude` | `ERROR` | Valid range $[-180.0, 180.0]$. Values outside raise a fatal error. |
| `TIMESTAMP_MISSING` | `event_time` | `INFO` | Noted when event timestamp is unstated in source report. |
| `TIMESTAMP_TIMEZONE_AWARE`| `event_time` | `ERROR` | Must have `tzinfo` and be timezone-aware (UTC normalized). |
| `TIMESTAMP_PLAUSIBLE_RANGE`| `event_time` | `ERROR` | Year must be within plausible domain range $[1900, 2100]$. |
| `CONFIDENCE_BOUNDS` | `extraction_confidence` | `ERROR` | Valid range $[0.0, 1.0]$. Values outside raise a fatal error. |
| `CONFIDENCE_LOW` | `extraction_confidence` | `WARNING` | Extraction confidence $< 0.2$ is flagged as low reliability. |
| `PAGE_NUMBER_POSITIVE` | `page_number` | `ERROR` | Must be a positive integer ($\ge 1$). Zero or negative values raise a fatal error. |
| `VOCABULARY_EVENT_CATEGORY`| `event_type` | `WARNING` | Flagged if event category is outside the standard reference vocabulary (`STANDARD_EVENT_TYPES`), but accepted as a custom category. |
| `VOCABULARY_SEVERITY` | `severity` | `WARNING` | Flagged if severity is not in (`minor`, `moderate`, `severe`, `critical`, `unknown`). |
| `BATCH_DUPLICATE_ID` | `observation_id` | `ERROR` | Colliding observation IDs within the current processing batch are rejected. |
| `BATCH_DUPLICATE_CLAIM` | Claim signature | `WARNING` | Flagged when identical `(source_id, event_type, location, event_time)` claims appear repeatedly within the same source. |

### Machine-Readable Outcomes
Validation produces a `QualityReport` consisting of granular `ValidationOutcome` objects containing:
- `record_id`: Evaluated observation identifier
- `source_id`: Originating source identifier
- `rule_name`: Standardized rule identifier
- `field_name`: Affected attribute
- `severity`: `ERROR`, `WARNING`, or `INFO`
- `message`: Human-readable explanation
- `passed`: Boolean pass/fail indicator
- `details`: Contextual diagnostic dictionary

---

## 4. PostgreSQL Storage Architecture (`beacon.storage`)

Persistence uses SQLAlchemy 2.0 with Psycopg 3 (`psycopg[binary]`), supporting full connection pooling and transaction management.

### Database Tables

```
                    ┌───────────────────────────┐
                    │          sources          │
                    ├───────────────────────────┤
         ┌─────────►│ PK  source_id             │◄─────────┐
         │          │     source_file           │          │
         │          │     source_type           │          │
         │          │     extraction_method     │          │
         │          │     ingested_at           │          │
         │          │     extraction_status     │          │
         │          │     content_hash          │          │
         │          │     raw_content           │          │
         │          │     metadata              │          │
         │          │     created_at            │          │
         │          │     updated_at            │          │
         │          └───────────────────────────┘          │
         │ 1:N                                             │ 1:N
         │                                                 │
┌───────────────────────────┐             ┌───────────────────────────────┐
│       observations        │             │     data_quality_results      │
├───────────────────────────┤             ├───────────────────────────────┤
│ PK  observation_id        │             │ PK  result_id (BIGSERIAL)     │
│ FK  source_id             │             │     record_id (nullable)      │
│     event_type            │             │ FK  source_id                 │
│     location              │             │     rule_name                 │
│     event_time            │             │     field_name                │
│     description           │             │     severity                  │
│     severity              │             │     message                   │
│     latitude              │             │     passed                    │
│     longitude             │             │     details (JSON)            │
│     extraction_confidence │             │     checked_at                │
│     page_number           │             └───────────────────────────────┘
│     source_reference      │
│     raw_claim             │
│     validation_warnings   │
│     created_at            │
│     updated_at            │
└───────────────────────────┘
```

### Table Specifications

#### 1. `sources`
Tracks originating document provenance, cryptographic verification, and extraction status.
- Primary Key: `source_id VARCHAR(64)`
- Indices:
  - `idx_sources_content_hash` on `content_hash`
  - `idx_sources_status` on `extraction_status`
  - `idx_sources_type` on `source_type`
- Attributes: `source_file`, `source_type`, `extraction_method`, `ingested_at`, `extraction_status`, `content_hash`, `raw_content`, `metadata` (JSON), `created_at`, `updated_at`.

#### 2. `observations`
Stores canonical atomic disaster claims extracted from sources.
- Primary Key: `observation_id VARCHAR(64)`
- Foreign Key: `source_id REFERENCES sources(source_id) ON DELETE CASCADE`
- Indices:
  - `idx_obs_source_id` on `source_id`
  - `idx_obs_event_type` on `event_type`
  - `idx_obs_event_time` on `event_time`
  - `idx_obs_coordinates` on `(latitude, longitude)`
- Attributes: `event_type`, `location`, `event_time`, `description`, `severity`, `latitude`, `longitude`, `extraction_confidence`, `page_number`, `source_reference`, `raw_claim`, `validation_warnings` (JSON array), `created_at`, `updated_at`.

#### 3. `data_quality_results`
Provides complete machine-readable audit lineage for every data quality check evaluated.
- Primary Key: `id BIGSERIAL` (ORM `BigInteger` with SQLite `Integer` variant)
- Foreign Key: `source_id REFERENCES sources(source_id) ON DELETE CASCADE`
- Foreign Key: `record_id REFERENCES observations(observation_id) ON DELETE SET NULL`
- Indices:
  - `idx_dq_source_id` on `source_id`
  - `idx_dq_record_id` on `record_id`
  - `idx_dq_rule` on `rule_name`
  - `idx_dq_severity` on `severity`
- Attributes: `record_id`, `rule_name`, `field_name`, `severity`, `message`, `passed`, `details` (JSON), `checked_at`.

### Pure SQL DDL Reference
A portable, standalone PostgreSQL DDL script is maintained at `src/beacon/storage/schema.sql`. It can be inspected or executed directly using `psql`:

```powershell
psql -h localhost -U beacon_user -d beacon_db -f src/beacon/storage/schema.sql
```

---

## 5. Idempotency, Concurrency & Transactions

The persistence layer is engineered for deterministic, repeatable execution:

1. **Source Identification & Idempotency Model:**
   - **`source_id` Generation:** Deterministically derived via `generate_source_id(source_type, source_file, content_hash)`. The hashing preimage is constructed as `"{source_type}::{normalized_file_path}::{content_hash}"` and hashed using SHA-256 (truncated to 16 hex characters, formatted as `src_{digest}`).
   - **Unchanged Input Reruns:** Processing the same file repeatedly with identical contents computes the exact same `source_id`. In `StorageRepository.save_source`, an existing row matching `source_id` is updated in-place (refreshing mutable metadata, extraction status, and `updated_at`) rather than inserted anew, preventing duplicate rows without requiring database-level composite unique constraints.
   - **Changed Contents at Same Path:** Modifying a file at an existing path alters its SHA-256 `content_hash`. Because `content_hash` forms part of the hash preimage, a new `source_id` is computed. The repository inserts a new source version record, preserving historical provenance for both the prior snapshot and the updated version.
   - **Identical Content Across Different Paths:** Two distinct file paths with identical file content compute distinct `source_id` values because the normalized file path is included in the hash preimage. Both files are tracked as distinct source documents with independent audit lineage.
   - **Observation UPSERT / In-Place Merge:** Deterministic `observation_id`s enable `save_observations` to update existing observation attributes in-place on repeated ingestion runs.
2. **Atomic Batch Transactions:**
   - Persistence occurs within an atomic database transaction context (`get_session`).
   - `save_batch` flushes source records, observations, and quality outcomes together.
   - If an error occurs midway, the transaction automatically rolls back, leaving no orphan rows or corrupt state.
3. **Partial Failure Isolation:**
   - If one file fails extraction (e.g. corrupt format or missing file), remaining files continue to be processed and persisted.
   - If an observation fails data quality with an `ERROR`, it is rejected from `observations`, but valid observations from the same source are safely loaded.
   - All outcomes (accepted, rejected, and errors) are audited in `data_quality_results` and `ETLPipelineResult`.

---

## 6. CLI Schema Management (`scripts/init_db.py`)

A command-line script is provided to initialize or reset database tables without manual SQL authoring:

```powershell
# Initialize tables using default settings from .env (non-destructive):
python scripts/init_db.py

# Initialize tables on an explicit database connection:
python scripts/init_db.py --url "postgresql+psycopg://beacon_user:secret@localhost:5432/beacon_db"

# Drop and recreate all tables interactively (prompts for confirmation before dropping):
python scripts/init_db.py --drop-first

# Drop and recreate tables non-interactively / in CI pipelines (explicit --yes required):
python scripts/init_db.py --drop-first --yes
```

> [!CAUTION]
> `--drop-first` permanently destroys all application tables (`sources`, `observations`, `data_quality_results`). In an interactive terminal, an explicit affirmative response (`y` or `yes`) is required. In automated or headless environments where `stdin` is not a TTY, `--yes` (`-y`) is mandatory; omitting it will immediately fail safe with an error and exit code `1` before executing any database operations.

---

## 7. Programmatic Pipeline Execution

```python
from pathlib import Path
from beacon.pipeline import ETLRunner
from beacon.storage import create_db_engine, get_session_factory

# 1. Setup session factory (or omit for in-memory mode)
engine = create_db_engine()
session_factory = get_session_factory(engine)

# 2. Instantiate runner
runner = ETLRunner(session_factory=session_factory)

# 3. Execute pipeline over candidate files
input_files = [
    Path("data/sample/events.json"),
    Path("data/sample/events.csv"),
    Path("data/sample/events.txt"),
    Path("data/sample/sample_report.pdf"),
]

result = runner.run(input_files)

# 4. Inspect outcomes
print(result.summary())
print(f"Discovered: {result.files_discovered}")
print(f"Observations accepted: {result.observations_accepted}")
print(f"Persisted to DB: {result.records_persisted}")
```

---

## 8. Testing Strategy

The BEACON test suite verifies Phase 3 through two complementary strategies:

1. **In-Memory SQLite Unit Tests (Default):**
   - All unit tests (`tests/test_storage_unit.py`, `tests/test_quality.py`, `tests/test_transform.py`, `tests/test_pipeline.py`) run against in-memory SQLite (`sqlite:///:memory:`).
   - Verifies transactions, commits, rollbacks, constraints, and data quality without requiring a PostgreSQL server or external dependencies.
2. **Opt-in Live PostgreSQL Integration Tests:**
   - `tests/test_storage_integration.py` runs only when the `BEACON_TEST_DATABASE_URL` environment variable is explicitly set:
   ```powershell
   $env:BEACON_TEST_DATABASE_URL = "postgresql+psycopg://postgres:postgres@localhost:5432/beacon_test"
   pytest -v tests/test_storage_integration.py
   ```
   - When the variable is unset, the integration test skips cleanly without failing test runs.
