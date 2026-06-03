-- V7: DAG workflows.
-- A workflow groups a set of job_runs into a directed acyclic graph. Nodes are
-- ordinary job_runs; edges express "run B only after A succeeds". The scheduler
-- moves a PENDING_DEPS run to SCHEDULED the moment its last upstream completes.

-- Partial-failure policy for the whole DAG.
-- FAIL_FAST      — cancel all non-terminal nodes on any single failure.
-- WAIT           — let other branches continue; failed branch's dependents stay PENDING_DEPS.
-- SKIP_DOWNSTREAM — mark failed node's direct dependents SKIPPED; rest of DAG continues.
CREATE TABLE workflows (
    id          UUID        PRIMARY KEY,
    on_failure  TEXT        NOT NULL DEFAULT 'FAIL_FAST',
    state       TEXT        NOT NULL DEFAULT 'RUNNING',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ck_workflows_on_failure CHECK (on_failure IN ('FAIL_FAST', 'WAIT', 'SKIP_DOWNSTREAM')),
    CONSTRAINT ck_workflows_state      CHECK (state      IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED'))
);

-- Each job_run that is part of a workflow carries the workflow id and a logical
-- node id (stable across retries — the same node retries under the same node_id).
ALTER TABLE job_runs
    ADD COLUMN workflow_id UUID REFERENCES workflows(id) ON DELETE CASCADE,
    ADD COLUMN node_id     TEXT;          -- caller-assigned stable node label within the DAG

-- Dependency edges: run `run_id` may not start until `depends_on_run_id` is SUCCEEDED.
-- One row per directed edge (A→B means B depends on A).
CREATE TABLE job_dependencies (
    run_id            UUID NOT NULL REFERENCES job_runs(id) ON DELETE CASCADE,
    depends_on_run_id UUID NOT NULL REFERENCES job_runs(id) ON DELETE CASCADE,
    PRIMARY KEY (run_id, depends_on_run_id)
);

-- Fast lookup: "all dependencies of a given downstream run"
CREATE INDEX idx_job_dependencies_run ON job_dependencies (run_id);
-- Fast lookup: "all downstream runs that depend on a given upstream"
CREATE INDEX idx_job_dependencies_upstream ON job_dependencies (depends_on_run_id);

-- Workflow-scoped run lookup (for GET /workflows/{id} and cancellation walk)
CREATE INDEX idx_job_runs_workflow ON job_runs (workflow_id)
    WHERE workflow_id IS NOT NULL;

-- Extend the state constraint to include the two new DAG states.
ALTER TABLE job_runs DROP CONSTRAINT ck_job_runs_state;
ALTER TABLE job_runs ADD CONSTRAINT ck_job_runs_state CHECK (
    state IN (
        'SCHEDULED', 'LEASED', 'RUNNING', 'SUCCEEDED',
        'FAILED', 'DEAD_LETTER', 'CANCELLED',
        'PENDING_DEPS',   -- waiting for upstream nodes to complete
        'SKIPPED'         -- upstream failed + SKIP_DOWNSTREAM policy
    )
);
