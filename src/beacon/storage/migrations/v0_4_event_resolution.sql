-- ==============================================================================
-- BEACON Database Migration: Phase 4 (v0.4)
-- Event Resolution, Entity Linking & Merge Audit
-- ==============================================================================

-- 1. Unified Disaster Events
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

-- 2. Event-Observation Junction Table
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

-- 3. Resolution Decision Audit Table
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

-- 4. Multi-Event Bridge / Merge Candidate Audit Table
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
