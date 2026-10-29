# BEACON Database Schema & Migration Guide

## 1. Overview & Architecture

BEACON uses a relational PostgreSQL storage architecture with eight relational tables:

| Table | Phase Introduced | Purpose | Constraints / Indexes |
| :--- | :--- | :--- | :--- |
| `sources` | Phase 3 (`v0.3-storage`) | Tracks raw file ingestion provenance, hashes, and extraction methods | `PRIMARY KEY (source_id)` |
| `observations` | Phase 3 (`v0.3-storage`) | Stores normalized atomic disaster claims extracted from raw files | `PRIMARY KEY (observation_id)`, FK to `sources` (`ON DELETE CASCADE`) |
| `data_quality_results` | Phase 3 (`v0.3-storage`) | Machine-readable audit log of rules evaluated against records | `PRIMARY KEY (id)`, FK to `observations` (`ON DELETE SET NULL`), FK to `sources` |
| `events` | Phase 4 (`v0.4-event-resolution`) | Canonical consolidated disaster event profiles with conflict tracking | `PRIMARY KEY (event_id)`, indexed by `event_type`, `start_time`, `severity` |
| `event_observations` | Phase 4 (`v0.4-event-resolution`) | Junction table linking unified events to constituent observations | `PRIMARY KEY (id)`, FK to `events`, FK to `observations`, `CONSTRAINT uq_event_observation UNIQUE (event_id, observation_id)` |
| `resolution_decisions` | Phase 4 (`v0.4-event-resolution`) | Explainable audit lineage recording matching scores and decision rationale | `PRIMARY KEY (id)`, FK to `observations`, FK to `events` |
| `merge_candidates` | Phase 4 (`v0.4-event-resolution`) | Human review queue for potential multi-event bridge observations | `PRIMARY KEY (id)`, FK to `primary_event_id`, FK to `secondary_event_id` |
| `pipeline_runs` | Phase 2B (`v1.0-dashboard`) | Execution telemetry, batch duration metrics, and operational audit trail | `PRIMARY KEY (run_id)`, indexed by `status`, `started_at`, `retry_of_run_id` |

---

## 2. Fresh Database Initialization

For new deployments or fresh test environments:

```bash
# Initialize all 8 tables in PostgreSQL
python scripts/init_db.py --url postgresql+psycopg://username:password@localhost:5432/beacon_db

# Re-initialize (drop all existing tables first and recreate)
python scripts/init_db.py --url postgresql+psycopg://username:password@localhost:5432/beacon_db --drop-first --yes
```

> [!CAUTION]
> `--drop-first` permanently destroys all existing application tables. In non-interactive environments (e.g. CI/CD), `--yes` is mandatory to confirm destructive deletion.

---

## 3. Upgrading from Phase 3 (`v0.3-storage`) to Phase 4 / v1.0

### Phase 2B / v1.0 Migration (Pipeline Execution Telemetry)
If an existing database has the 7 Phase 4 tables and requires upgrading to v1.0, apply the additive `pipeline_runs` migration:

```bash
python scripts/migrate_v1_0.py --dry-run
python scripts/migrate_v1_0.py --url postgresql+psycopg://username:password@localhost:5432/beacon_db
```

---

### Phase 4 Migration (Event Resolution)
If an existing database is already running with Phase 3 tables (`sources`, `observations`, `data_quality_results`), apply the additive Phase 4 migration without dropping or altering existing tables:

### Dry-Run Inspection
```bash
python scripts/migrate_v0_4.py --dry-run
```

### Applying Migration
```bash
python scripts/migrate_v0_4.py --url postgresql+psycopg://username:password@localhost:5432/beacon_db
```

### Migration Safety Guarantees
1. **Pure Additive DDL:** The migration uses `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS`. No `DROP`, `TRUNCATE`, or `ALTER TABLE ... DROP COLUMN` commands exist in the migration script.
2. **Transactional Execution:** All DDL statements execute inside a single transactional block (`engine.begin()`). If any statement encounters an error, the transaction rolls back completely without partial state changes.
3. **Idempotency:** Executing `scripts/migrate_v0_4.py` repeatedly produces no errors and does not corrupt existing data or duplicate constraints.

---

## 4. Rollback & Disaster Recovery Guidance

In the event of an operational need to roll back Phase 4 tables while preserving Phase 3 observations and source records:

```sql
-- Safe Phase 4 Rollback SQL (Preserves Phase 3 sources and observations)
BEGIN;

DROP TABLE IF EXISTS merge_candidates CASCADE;
DROP TABLE IF EXISTS resolution_decisions CASCADE;
DROP TABLE IF EXISTS event_observations CASCADE;
DROP TABLE IF EXISTS events CASCADE;

COMMIT;
```

> [!NOTE]
> Phase 3 tables (`sources`, `observations`, `data_quality_results`) remain completely unaffected by dropping Phase 4 junction and resolution tables.

---

## 5. Automated CI Verification

Automated CI executes tests against a real PostgreSQL service container in GitHub Actions:
- Fresh schema initialization verified via `test_postgres_fresh_initialization_and_batch_persistence`.
- Phase 3 to Phase 4 migration verified via `test_postgres_phase_3_to_phase_4_migration_upgrade`.
- Constraint enforcement and rollback verified via `test_postgres_composite_uniqueness_and_foreign_keys` and `test_postgres_transactional_rollback_on_error`.
