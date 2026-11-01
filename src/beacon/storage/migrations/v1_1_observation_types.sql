-- ==============================================================================
-- BEACON Additive Migration: v1.1 Universal Observation Architecture
-- Adds observation_type, is_event_eligible, and context_payload to observations table.
-- Nullable/defaulted columns: non-breaking, backward-compatible with v1.0.
-- ==============================================================================

ALTER TABLE observations ADD COLUMN IF NOT EXISTS observation_type VARCHAR(32) NOT NULL DEFAULT 'incident';
ALTER TABLE observations ADD COLUMN IF NOT EXISTS is_event_eligible BOOLEAN NOT NULL DEFAULT true;
ALTER TABLE observations ADD COLUMN IF NOT EXISTS context_payload JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE observations ALTER COLUMN event_type DROP NOT NULL;

CREATE INDEX IF NOT EXISTS ix_observations_observation_type ON observations(observation_type);
CREATE INDEX IF NOT EXISTS ix_observations_is_event_eligible ON observations(is_event_eligible);
