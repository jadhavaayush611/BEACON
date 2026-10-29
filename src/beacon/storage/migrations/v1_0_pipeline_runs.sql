-- ==============================================================================
-- BEACON Additive Migration: v1.0 Pipeline Runs Execution Telemetry
-- Introduces pipeline_runs table for tracking batch runs, execution durations,
-- retry lineages, and operational audit metadata.
-- ==============================================================================

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR(64) PRIMARY KEY,
    status VARCHAR(32) NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_seconds DOUBLE PRECISION,
    trigger_type VARCHAR(32) NOT NULL DEFAULT 'manual',
    retry_of_run_id VARCHAR(64) REFERENCES pipeline_runs(run_id) ON DELETE SET NULL,
    inputs_summary JSONB NOT NULL DEFAULT '{}',
    counts_summary JSONB NOT NULL DEFAULT '{}',
    failure_details JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_pipeline_runs_status ON pipeline_runs(status);
CREATE INDEX IF NOT EXISTS ix_pipeline_runs_started_at ON pipeline_runs(started_at);
CREATE INDEX IF NOT EXISTS ix_pipeline_runs_retry_of_run_id ON pipeline_runs(retry_of_run_id);
