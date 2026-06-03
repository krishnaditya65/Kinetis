-- V9: audit log for write operations.
-- Records every state-changing API call for compliance and debugging.

CREATE TABLE audit_events (
    id          BIGSERIAL   PRIMARY KEY,
    actor       TEXT        NOT NULL DEFAULT 'anonymous',  -- API key description or 'anonymous'
    action      TEXT        NOT NULL,                      -- e.g. 'submit_job', 'cancel_job', 'submit_workflow'
    resource_id TEXT,                                      -- UUID of the affected entity
    detail      JSONB       NOT NULL DEFAULT '{}'::jsonb,  -- extra context (jobType, nodeCount, etc.)
    at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Support last-N events per resource queries and time-range scans
CREATE INDEX idx_audit_events_resource ON audit_events (resource_id, at DESC)
    WHERE resource_id IS NOT NULL;
CREATE INDEX idx_audit_events_at ON audit_events (at DESC);
