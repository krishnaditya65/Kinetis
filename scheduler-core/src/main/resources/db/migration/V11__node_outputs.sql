-- V11: node output passing for DAG workflows.
-- Handlers can store a JSON result via JobContext.complete(output). When a downstream node
-- executes, its JobContext.upstreamOutputs() contains all outputs from its direct upstreams,
-- keyed by the upstream node's stable nodeId.

CREATE TABLE job_run_outputs (
    run_id     UUID        PRIMARY KEY REFERENCES job_runs(id) ON DELETE CASCADE,
    output     JSONB       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
