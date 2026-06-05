-- V2: partial indexes for the hot query paths.
-- Indexing only rows in the relevant states keeps each index small and
-- fast even with millions of terminal (SUCCEEDED/DEAD_LETTER) rows that
-- never need to be scanned by the scheduler or reaper.

-- Scheduler claim query:
--   SELECT ... FROM job_runs WHERE state = 'SCHEDULED' AND scheduled_for <= now()
--   ORDER BY scheduled_for ASC FOR UPDATE SKIP LOCKED
-- Note: V6 rebuilds this index with (shard_id, priority DESC, scheduled_for) once the
-- priority column is added in that migration.
CREATE INDEX idx_due ON job_runs (scheduled_for ASC)
    WHERE state = 'SCHEDULED';

-- Reaper query: find runs whose lease has expired
--   SELECT ... FROM job_runs WHERE lease_expires_at <= now() AND state IN ('LEASED', 'RUNNING')
CREATE INDEX idx_expired ON job_runs (lease_expires_at)
    WHERE state IN ('LEASED', 'RUNNING');

-- Lookups of all runs for a given job (job detail / cancel / history)
CREATE INDEX idx_job_runs_job_id ON job_runs (job_id);
