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
