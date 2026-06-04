-- V12: run archival + scheduler settings.
-- job_runs_archive mirrors job_runs exactly. Runs older than the retention window are moved
-- here by ArchivalService, keeping the hot job_runs table small and fast.

CREATE TABLE job_runs_archive (LIKE job_runs INCLUDING ALL);

-- Scheduler-wide settings (key-value). Used for maintenance mode flag.
CREATE TABLE scheduler_settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Default: maintenance mode off
INSERT INTO scheduler_settings (key, value) VALUES ('maintenance_mode', 'false');
