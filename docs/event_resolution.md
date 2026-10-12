# BEACON Event Resolution & Entity Linking Subsystem

## Core Proposition

> **"One event, many sources, one trusted data profile."**

In disaster intelligence data engineering, separate agencies, sensor feeds, news outlets, and situation reports often describe the same physical event using disparate naming conventions, distinct temporal windows, and varying levels of spatial precision.

BEACON's **Event Resolution Layer** (Phase 4) groups co-referent observations describing the same real-world disaster into canonical, unified event profiles (`UnifiedDisasterEvent`). It operates deterministically, maintains strict provenance lineage, and preserves contradictory claims across sources rather than silently discarding them.

---

## Architectural Architecture

```
DisasterObservation(s) (from Ingestion & ETL)
                     │
                     ▼
       ┌───────────────────────────┐
       │   CandidateGenerator      │  Multi-attribute blocking (type, time, space)
       └─────────────┬─────────────┘
                     │ Candidate Event Profiles
                     ▼
       ┌───────────────────────────┐
       │   ObservationMatcher      │  Multi-feature scoring (Haversine, time delta, Jaccard)
       └─────────────┬─────────────┘
                     │ FeatureScores & Composite Score
                     ▼
       ┌───────────────────────────┐
       │       MatchPolicy         │  Deterministic classification
       └──────┬──────────────┬─────┘
              │              │
      Match / No Match   Bridge Detected
              │              │
              ▼              ▼
 ┌──────────────────────┐  ┌──────────────────────┐
 │EventProfileAggregator│  │ MergeCandidate Audit │
 └──────────┬───────────┘  └──────────────────────┘
            │
            ▼
   UnifiedDisasterEvent (Canonical Profile + ConflictingClaims)
            │
            ▼
   Relational Persistence & Analytics (PostgreSQL / SQLite)
```

---

## 1. Multi-Attribute Blocking (`CandidateGenerator`)

To evaluate thousands of incoming observations without quadratic $O(N^2)$ all-pairs comparisons, `CandidateGenerator` filters existing event profiles down to plausible candidates using indexed multi-attribute blocking:

1. **Disaster Category Filter:** Hard veto on incompatible disaster types (e.g. `flood` will not evaluate against `earthquake`). Standardizes aliases (`flash flood` -> `flood`).
2. **Temporal Window Filter:** Limits candidate comparison to events within `resolution_max_temporal_distance_hours` (default: 48.0 hours) of the observation timestamp.
3. **Spatial Bounding Box Filter:** Limits candidate comparison to events within `1.5 * resolution_max_spatial_distance_km` (default: 75.0 km) of the observation coordinates.
4. **Missing Feature Gracefulness:** Observations lacking coordinates or timestamps are not prematurely dropped by blocking filters.

---

## 2. Feature Scoring Engine (`ObservationMatcher`)

`ObservationMatcher` evaluates candidate pairs across multiple independent feature axes:

- **Spatial Proximity:** Great-circle distance calculated via the Haversine formula on spherical coordinates:
  $$\text{score}_{\text{spatial}} = \max\left(0, 1 - \frac{d_{\text{km}}}{d_{\text{max}}}\right)$$
  Enforces strict coordinate boundaries ($[-90, 90]$ for latitude, $[-180, 180]$ for longitude) and clamps antipodal numerical precision.
- **Temporal Proximity:** Linear decay score across timestamp differences:
  $$\text{score}_{\text{temporal}} = \max\left(0, 1 - \frac{\Delta t_{\text{hours}}}{\Delta t_{\text{max}}}\right)$$
  Enforces UTC-aware datetime comparisons.
- **Textual Location Similarity:** Tokenizes location strings, strips non-distinctive geographic stop-words (`area`, `district`, `zone`, `metro`), and computes combined Jaccard and containment scores.
- **Dynamic Weight Rebalancing:** When coordinates or timestamps are absent in raw sources, active feature weights dynamically rebalance over available evidence.
- **Calibration Safeguard:** When both spatial coordinates and temporal anchors are absent, match scores are capped at `0.40` to prevent false-positive merges based solely on generic text.

---

## 3. Decision Policy & Bridge Detection (`MatchPolicy`)

Classification rules:

| Decision | Condition | Action |
| :--- | :--- | :--- |
| **Confident Match** (`confident_match`) | `composite_score >= 0.75` and top candidate clear winner | Links observation to event; updates profile |
| **Ambiguous Candidate** (`ambiguous_candidate`) | `0.50 <= composite_score < 0.75` OR candidate tie margin $< 0.08$ | Flagged for audit review; quarantined from automatic merge |
| **No Match** (`no_match`) | `composite_score < 0.50` across all candidates | Observation anchors a new canonical `UnifiedDisasterEvent` |
| **Bridge Candidate** (`merge_candidate`) | Observation matches $\ge 2$ distinct existing events with score $\ge 0.75$ | Observation assigned to highest match; `MergeCandidate` logged for human review |

---

## 4. Profile Synthesis & Conflict Tracking (`EventProfileAggregator`)

When observations are merged into an event:

- **Deterministic Event Identifier:** Uses `generate_event_id(event_type, location, start_time)` for rerun-stable reproducibility.
- **Spatiotemporal Extent:**
  - `start_time`: earliest known constituent observation timestamp.
  - `end_time`: latest known constituent observation timestamp.
  - `metadata.centroid`: geographic center of mass across valid coordinates.
  - `metadata.bounding_box`: rectangular coordinate envelope `[min_lat, max_lat, min_lon, max_lon]`.

> [!WARNING]
> **Antimeridian Bounding-Box Limitation:**
> The current bounding box calculation uses planar Cartesian extrema (`min(longitudes)` and `max(longitudes)`). It is **not antimeridian-aware**. When constituent observations cross the $180^\circ / -180^\circ$ antimeridian (International Date Line, e.g., points at $+179.5^\circ\text{E}$ and $-179.5^\circ\text{W}$), ordinary minimum/maximum calculations yield an artificial envelope spanning almost the entire circumference of the Earth ($[-179.5^\circ, +179.5^\circ]$, or $359^\circ$ of longitude) rather than a localized $1^\circ$ cross-meridian extent. For cross-meridian events, this bounding box should not be interpreted as an accurate localized extent. Individual observation coordinates and source documents remain fully preserved and available in contributing records for exact spherical queries.

- **Attribute Consensus:**
  - `event_type`: Majority vote / highest extraction confidence.
  - `location`: Best-supported location name, preferring descriptive length on ties.
  - `severity`: Highest reported domain severity or plurality consensus.
- **Preservation of Conflicting Claims:**
  Any disagreement on severity, location, or event type is preserved in `conflicting_claims: list[ConflictingClaim]`:
  ```json
  {
    "field_name": "severity",
    "observation_id": "obs_csv_101",
    "source_id": "src_csv",
    "claimed_value": "moderate",
    "resolution_status": "superseded",
    "notes": "Resolved to 'severe' based on highest reported severity/plurality."
  }
  ```

---

## 5. Storage Schema & Migrations

Phase 4 introduces four relational tables:

1. `events`: Stores canonical unified disaster profiles with JSON-encoded conflicting claims and metadata.
2. `event_observations`: Junction table linking `events` to constituent `observations`.
3. `resolution_decisions`: Immutable audit log recording scoring breakdown, candidate rank, and policy rationale.
4. `merge_candidates`: Audit log tracking potential multi-event bridges for human review.

### Running Migrations

Apply the additive migration to PostgreSQL:

```bash
python scripts/migrate_v0_4.py --url postgresql+psycopg://user:pass@localhost:5432/beacon
```

Inspect DDL without executing:

```bash
python scripts/migrate_v0_4.py --dry-run
```

---

## 6. Disaster Intelligence Analytics (`AnalyticsService`)

`AnalyticsService` generates aggregated summaries:

- **Compression Ratio:** Total observations divided by total unified events.
- **Multi-Source Validation:** Counts events verified across $\ge 2$ distinct sources vs single-source claims.
- **Distributions:** Breakdowns by disaster category, severity level, and geographic location.
- **Conflict Metrics:** Frequency of contradictory claims by field name.
- **Coverage Audits:** Identifies unanchored timestamps and missing coordinate envelopes.
- **Audit Logs:** Counts ambiguous candidates and pending merge candidates.
