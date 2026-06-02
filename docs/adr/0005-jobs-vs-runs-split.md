# ADR-0005: Split `jobs` (definition) from `job_runs` (execution)

- **Status:** Accepted
- **Date:** 2026-06-02

## Context

A recurring (cron) job fires many times; a one-off fires once; a failed run is retried. We need a
data model that supports all of these plus DAGs without rework.

## Decision

Model two tables: **`jobs`** (immutable definition — type, payload, idempotency key, delivery/retry/
recurrence policy) and **`job_runs`** (one row per execution — state, attempt, `scheduled_for`, lease
fields, token). One `jobs` row → many `job_runs` rows.

## Options considered

- **Two tables (definition vs. execution)** — chosen.
- **Single table** — store everything on one row and mutate it per fire. Rejected: loses run history,
  forces mutation of the definition on every execution, and can't represent concurrent/queued runs of
  the same job.

## Consequences

- (+) Cron, retries, the queue, and DAGs all reuse the same run lifecycle and lease/fence machinery.
- (+) Full execution history and auditability; definition stays immutable.
- (+) Partial indexes on `job_runs.state` keep the hot lease/reaper paths fast as terminal rows grow.
- (−) A join (or second lookup) is needed to get a run's policy; acceptable, and batched where it
  matters.
