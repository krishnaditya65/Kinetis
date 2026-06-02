-- V3: transactional outbox.
-- Writing a state change and the event that triggers the next step in the SAME transaction
-- is what makes internal fan-out effectively-once. Used in Phase 2 for atomic cron next-fire
-- enqueue and Phase 5 for DAG fan-out.

CREATE TABLE outbox (
    id            UUID        PRIMARY KEY,
    aggregate_id  UUID        NOT NULL,
    event_type    TEXT        NOT NULL,
    payload       JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    dispatched_at TIMESTAMPTZ
);

-- Dispatcher polls for undispatched events; partial index keeps it small.
CREATE INDEX idx_outbox_undispatched ON outbox (created_at)
    WHERE dispatched_at IS NULL;
