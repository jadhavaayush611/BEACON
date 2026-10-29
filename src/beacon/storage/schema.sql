-- ==============================================================================
-- BEACON Database Schema (PostgreSQL)
-- Phase 3: Storage, Provenance & Data Quality
-- ==============================================================================

-- 1. Ingested Sources & Document Provenance
CREATE TABLE IF NOT EXISTS sources (
    source_id VARCHAR(64) PRIMARY KEY,
    source_file VARCHAR(1024) NOT NULL,
    source_type VARCHAR(32) NOT NULL,
    extraction_method VARCHAR(32) NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE NOT NULL,
    extraction_status VARCHAR(32) NOT NULL,
    content_hash VARCHAR(64),
    raw_content TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sources_content_hash ON sources (content_hash);
CREATE INDEX IF NOT EXISTS idx_sources_status ON sources (extraction_status);
CREATE INDEX IF NOT EXISTS idx_sources_type ON sources (source_type);

-- 2. Canonical Disaster Observations
CREATE TABLE IF NOT EXISTS observations (
    observation_id VARCHAR(64) PRIMARY KEY,
    source_id VARCHAR(64) NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
    event_type VARCHAR(128) NOT NULL,
    location TEXT,
    event_time TIMESTAMP WITH TIME ZONE,
    description TEXT,
    severity VARCHAR(32),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    extraction_confidence DOUBLE PRECISION,
    page_number INTEGER,
    source_reference VARCHAR(512),
    raw_claim TEXT,
    validation_warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_obs_source_id ON observations (source_id);
CREATE INDEX IF NOT EXISTS idx_obs_event_type ON observations (event_type);
CREATE INDEX IF NOT EXISTS idx_obs_event_time ON observations (event_time);
CREATE INDEX IF NOT EXISTS idx_obs_coordinates ON observations (latitude, longitude);

-- 3. Data Quality Audit Outcomes
CREATE TABLE IF NOT EXISTS data_quality_results (
    id BIGSERIAL PRIMARY KEY,
    record_id VARCHAR(64) REFERENCES observations(observation_id) ON DELETE SET NULL,
    source_id VARCHAR(64) NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
    rule_name VARCHAR(128) NOT NULL,
    field_name VARCHAR(128),
    severity VARCHAR(32) NOT NULL,
    message TEXT NOT NULL,
    passed BOOLEAN NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    checked_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dq_source_id ON data_quality_results (source_id);
CREATE INDEX IF NOT EXISTS idx_dq_record_id ON data_quality_results (record_id);
CREATE INDEX IF NOT EXISTS idx_dq_rule ON data_quality_results (rule_name);
CREATE INDEX IF NOT EXISTS idx_dq_severity ON data_quality_results (severity);

-- 4. Unified Disaster Events
CREATE TABLE IF NOT EXISTS events (
    event_id VARCHAR(64) PRIMARY KEY,
    event_type VARCHAR(128) NOT NULL,
    location TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE,
    summary TEXT NOT NULL,
    severity VARCHAR(32),
    resolution_rationale TEXT,
    conflicting_claims JSONB NOT NULL DEFAULT '[]'::jsonb,
    source_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_events_event_type ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_start_time ON events (start_time);
CREATE INDEX IF NOT EXISTS idx_events_severity ON events (severity);

-- 5. Event-Observation Junction Table
CREATE TABLE IF NOT EXISTS event_observations (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    observation_id VARCHAR(64) NOT NULL REFERENCES observations(observation_id) ON DELETE CASCADE,
    confidence_score DOUBLE PRECISION,
    assigned_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_event_observation UNIQUE (event_id, observation_id)
);

CREATE INDEX IF NOT EXISTS idx_event_obs_event_id ON event_observations (event_id);
CREATE INDEX IF NOT EXISTS idx_event_obs_observation_id ON event_observations (observation_id);

-- 6. Resolution Decision Audit Table
CREATE TABLE IF NOT EXISTS resolution_decisions (
    id BIGSERIAL PRIMARY KEY,
    observation_id VARCHAR(64) REFERENCES observations(observation_id) ON DELETE CASCADE,
    event_id VARCHAR(64) REFERENCES events(event_id) ON DELETE SET NULL,
    decision_type VARCHAR(32) NOT NULL,
    composite_score DOUBLE PRECISION NOT NULL,
    feature_scores JSONB NOT NULL DEFAULT '{}'::jsonb,
    rationale TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_res_dec_obs_id ON resolution_decisions (observation_id);
CREATE INDEX IF NOT EXISTS idx_res_dec_event_id ON resolution_decisions (event_id);
CREATE INDEX IF NOT EXISTS idx_res_dec_decision_type ON resolution_decisions (decision_type);

-- 7. Multi-Event Bridge / Merge Candidate Audit Table
CREATE TABLE IF NOT EXISTS merge_candidates (
    id BIGSERIAL PRIMARY KEY,
    primary_event_id VARCHAR(64) NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    secondary_event_id VARCHAR(64) NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    bridging_observation_id VARCHAR(64) REFERENCES observations(observation_id) ON DELETE SET NULL,
    similarity_score DOUBLE PRECISION NOT NULL,
    rationale TEXT NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'pending_review',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_merge_primary_event ON merge_candidates (primary_event_id);
CREATE INDEX IF NOT EXISTS idx_merge_secondary_event ON merge_candidates (secondary_event_id);
CREATE INDEX IF NOT EXISTS idx_merge_status ON merge_candidates (status);
-- 8. Pipeline Run Execution History & Telemetry Table
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR(64) PRIMARY KEY,
    status VARCHAR(32) NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_seconds DOUBLE PRECISION,
    trigger_type VARCHAR(32) NOT NULL DEFAULT 'manual',
    retry_of_run_id VARCHAR(64) REFERENCES pipeline_runs(run_id) ON DELETE SET NULL,
    inputs_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    counts_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    failure_details JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs (status);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started ON pipeline_runs (started_at);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_retry ON pipeline_runs (retry_of_run_id);
