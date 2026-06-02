-- V6: priority queues, multi-tenancy, and rate limiting.
--
-- Priority: higher = more urgent. Default 0 = normal. Negative allowed for background work.
-- Tenant:   optional grouping key for rate limiting and fair-share scheduling.

ALTER TABLE jobs     ADD COLUMN priority  SMALLINT NOT NULL DEFAULT 0;
ALTER TABLE jobs     ADD COLUMN tenant_id TEXT;

ALTER TABLE job_runs ADD COLUMN priority  SMALLINT NOT NULL DEFAULT 0;
ALTER TABLE job_runs ADD COLUMN tenant_id TEXT;

-- Backfill priority/tenant_id on existing runs from their parent job.
UPDATE job_runs SET priority = j.priority, tenant_id = j.tenant_id
FROM jobs j WHERE job_runs.job_id = j.id;

-- Rebuild the primary hot-path index with priority as the leading sort key so the scheduler
-- claims highest-priority, oldest-first without a separate sort step.
DROP INDEX IF EXISTS idx_due;
CREATE INDEX idx_due ON job_runs (shard_id, priority DESC, scheduled_for ASC)
    WHERE state = 'SCHEDULED';

-- Token bucket per tenant. Refilled at `refill_rate` tokens/sec up to `max_tokens`.
-- When tokens <= 0 the tenant is rate-limited and its runs are returned to the queue.
CREATE TABLE rate_limits (
    tenant_id   TEXT             PRIMARY KEY,
    tokens      DOUBLE PRECISION NOT NULL,
    max_tokens  DOUBLE PRECISION NOT NULL DEFAULT 100.0,
    refill_rate DOUBLE PRECISION NOT NULL DEFAULT 10.0,  -- tokens per second
    last_refill TIMESTAMPTZ      NOT NULL DEFAULT now(),
    weight      INT              NOT NULL DEFAULT 1       -- for weighted fair-share
);
