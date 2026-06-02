-- V4: add next_fire_time to jobs for recurring (cron) scheduling.
-- CronScheduler fires a job when next_fire_time <= now(), then atomically advances
-- next_fire_time to the next occurrence in the same transaction — so no fire is ever
-- skipped or doubled even under concurrent scheduler nodes.
-- NULL means one-off job (not recurring).

ALTER TABLE jobs
    ADD COLUMN next_fire_time TIMESTAMPTZ;

-- Partial index: only rows with a cron expression and a pending fire time.
-- Keeps the index tiny — terminal/cancelled jobs never appear here.
CREATE INDEX idx_jobs_cron_due ON jobs (next_fire_time)
    WHERE next_fire_time IS NOT NULL AND cron_expr IS NOT NULL;
