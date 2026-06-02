-- V1: core schema. jobs (definition) vs job_runs (execution) split.
-- One Job spawns many JobRuns (one per cron fire, or one for a one-off).
-- Keeping definition separate from execution means the definition is immutable
-- history and retry/recurrence policy lives in one place.

CREATE TABLE jobs (
    id              UUID        PRIMARY KEY,
    job_type        TEXT        NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key TEXT        NOT NULL,
    delivery_policy TEXT        NOT NULL DEFAULT 'AT_LEAST_ONCE',
    -- recurrence (Phase 2 cron; present now to avoid migrating a hot table later)
    cron_expr       TEXT,
    timezone        TEXT        NOT NULL DEFAULT 'UTC',
    misfire_policy  TEXT        NOT NULL DEFAULT 'FIRE_ONCE',
    -- retry policy
    max_attempts    INT         NOT NULL DEFAULT 3,
    backoff_base_ms BIGINT      NOT NULL DEFAULT 1000,
    backoff_factor  NUMERIC     NOT NULL DEFAULT 2.0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_jobs_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT ck_jobs_delivery_policy CHECK (delivery_policy IN ('AT_LEAST_ONCE', 'AT_MOST_ONCE')),
    CONSTRAINT ck_jobs_misfire_policy  CHECK (misfire_policy  IN ('SKIP', 'FIRE_ONCE', 'CATCH_UP')),
    CONSTRAINT ck_jobs_max_attempts    CHECK (max_attempts >= 1)
);

CREATE TABLE job_runs (
    id                UUID        PRIMARY KEY,
    job_id            UUID        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    state             TEXT        NOT NULL DEFAULT 'SCHEDULED',
    attempt           INT         NOT NULL DEFAULT 0,
    scheduled_for     TIMESTAMPTZ NOT NULL,
    -- leasing: which node owns this run right now
    lease_owner       TEXT,
    lease_expires_at  TIMESTAMPTZ,
    -- fencing token: monotonically bumped on every lease grant.
    -- workers must echo this value on every state-mutating write;
    -- a stale (zombie) worker's lower token is rejected at the DB.
    lease_token       BIGINT      NOT NULL DEFAULT 0,
    -- effect deduplication key (job_key + schedule_slot)
    idempotency_key   TEXT        NOT NULL,
    -- bookkeeping
    last_heartbeat_at TIMESTAMPTZ,
    last_error        TEXT,
    enqueued_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at        TIMESTAMPTZ,
    finished_at       TIMESTAMPTZ,

    CONSTRAINT ck_job_runs_state CHECK (
        state IN ('SCHEDULED', 'LEASED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'DEAD_LETTER', 'CANCELLED')
    )
);
