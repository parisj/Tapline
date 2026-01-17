-- s/persistence/schema.sql
-- Postgres-only schema for VisioEval

BEGIN;

-- Optional: keep everything in a dedicated schema
-- CREATE SCHEMA IF NOT EXISTS visioeval;
-- SET search_path TO visioeval, public;

-- =========================
-- Jobs
-- =========================
CREATE TABLE IF NOT EXISTS jobs (
    job_id           TEXT PRIMARY KEY,
    directory_key    TEXT NOT NULL,
    path             TEXT NOT NULL,
    fingerprint      TEXT NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ('created', 'started', 'completed', 'failed')),
    created_at_unix  DOUBLE PRECISION NOT NULL,
    started_at_unix  DOUBLE PRECISION,
    finished_at_unix DOUBLE PRECISION,
    error            TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at_unix);

-- =========================
-- Results (raw per job+algo)
-- =========================
CREATE TABLE IF NOT EXISTS results (
    id              BIGSERIAL PRIMARY KEY,
    job_id          TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    algo_name       TEXT NOT NULL,
    algo_version    TEXT NOT NULL,
    metrics_json    JSONB NOT NULL,
    created_at_unix DOUBLE PRECISION NOT NULL,
    UNIQUE(job_id, algo_name, algo_version)
);

CREATE INDEX IF NOT EXISTS idx_results_created_at ON results(created_at_unix);
CREATE INDEX IF NOT EXISTS idx_results_algo_time ON results(algo_name, algo_version, created_at_unix);
CREATE INDEX IF NOT EXISTS idx_results_job ON results(job_id);
-- Optional but often useful if you query inside metrics_json
CREATE INDEX IF NOT EXISTS idx_results_metrics_gin ON results USING GIN (metrics_json);

-- =========================
-- Artifacts per result (binary)
-- =========================
CREATE TABLE IF NOT EXISTS artifacts (
    id              BIGSERIAL PRIMARY KEY,
    result_id       BIGINT NOT NULL REFERENCES results(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    mime            TEXT NOT NULL,
    data            BYTEA NOT NULL,
    created_at_unix DOUBLE PRECISION NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())),
    UNIQUE(result_id, name)
);

CREATE INDEX IF NOT EXISTS idx_artifacts_result ON artifacts(result_id);

-- =========================
-- Aggregates (rollups per time window, per metric, per analysis kind)
-- aggregates.summary_json is small JSON (mean/std/count/etc)
-- aggregates.artifact_hash links to a binary artifact in aggregate_artifacts (optional)
-- =========================
CREATE TABLE IF NOT EXISTS aggregates (
    id               BIGSERIAL PRIMARY KEY,
    algo_name         TEXT NOT NULL,
    algo_version      TEXT NOT NULL,
    metric_name       TEXT NOT NULL,
    analysis_kind     TEXT NOT NULL,
    window_start_unix DOUBLE PRECISION NOT NULL,
    window_end_unix   DOUBLE PRECISION NOT NULL,
    summary_json      JSONB NOT NULL,
    artifact_hash     TEXT,
    created_at_unix   DOUBLE PRECISION NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())),
    UNIQUE(algo_name, algo_version, metric_name, analysis_kind, window_start_unix, window_end_unix)
);

CREATE INDEX IF NOT EXISTS idx_agg_algo_kind_time
ON aggregates(algo_name, algo_version, analysis_kind, window_start_unix, window_end_unix);

CREATE INDEX IF NOT EXISTS idx_agg_metric_kind_time
ON aggregates(metric_name, analysis_kind, window_start_unix, window_end_unix);

CREATE INDEX IF NOT EXISTS idx_agg_artifact_hash
ON aggregates(artifact_hash);

CREATE INDEX IF NOT EXISTS idx_agg_summary_gin ON aggregates USING GIN (summary_json);

-- =========================
-- Aggregate artifacts (binary payloads for Bokeh-ready plotting)
-- artifact_hash is the stable join key referenced by aggregates.artifact_hash
-- =========================
CREATE TABLE IF NOT EXISTS aggregate_artifacts (
    id               BIGSERIAL PRIMARY KEY,
    artifact_hash     TEXT NOT NULL UNIQUE,
    algo_name         TEXT NOT NULL,
    algo_version      TEXT NOT NULL,
    metric_name       TEXT NOT NULL,
    analysis_kind     TEXT NOT NULL,
    window_start_unix DOUBLE PRECISION NOT NULL,
    window_end_unix   DOUBLE PRECISION NOT NULL,
    name              TEXT NOT NULL,
    mime              TEXT NOT NULL,
    data              BYTEA NOT NULL,
    created_at_unix   DOUBLE PRECISION NOT NULL DEFAULT (EXTRACT(EPOCH FROM now()))
);

CREATE INDEX IF NOT EXISTS idx_agg_art_algo_kind_time
ON aggregate_artifacts(algo_name, algo_version, analysis_kind, window_start_unix, window_end_unix);

CREATE INDEX IF NOT EXISTS idx_agg_art_metric_kind_time
ON aggregate_artifacts(metric_name, analysis_kind, window_start_unix, window_end_unix);

COMMIT;
