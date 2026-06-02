# Architecture Decision Records

Each ADR captures one significant, hard-to-reverse decision: its context, the options weighed, the
choice, and its consequences. Format is a lightweight [MADR](https://adr.github.io/madr/) variant.

Status values: `Proposed` · `Accepted` · `Superseded by ADR-XXXX` · `Deprecated`.

| # | Title | Status |
|---|---|---|
| [0001](0001-language-and-framework.md) | Java 21 + Spring Boot (thin) | Accepted |
| [0002](0002-postgres-skip-locked-leasing.md) | Postgres as source of truth; SKIP LOCKED leasing | Accepted |
| [0003](0003-delivery-semantics.md) | At-least-once delivery + idempotency keys | Accepted |
| [0004](0004-fencing-tokens.md) | Fencing tokens enforced from Phase 1 | Accepted |
| [0005](0005-jobs-vs-runs-split.md) | Split `jobs` (definition) from `job_runs` (execution) | Accepted |
| [0006](0006-in-process-worker.md) | In-process, DB-mediated worker for Phase 1 | Accepted |
| [0007](0007-backoff-with-jitter.md) | Exponential backoff with full jitter | Accepted |
| [0008](0008-transactional-outbox.md) | Transactional outbox for atomic fan-out | Accepted |
| [0009](0009-cron-parser-from-scratch.md) | Cron expression parser written from scratch | Accepted |
| [0010](0010-sharding-strategy.md) | Sharding — hash-partition job_runs by job_id MSB | Accepted |
| [0011](0011-raft-implementation.md) | Raft consensus implemented from scratch | Accepted |
| [0012](0012-leader-election-comparison.md) | Leader election mechanism comparison (pg advisory / etcd / Raft) | Accepted |

## Writing a new ADR

Copy the structure of an existing record. Number sequentially. Once `Accepted`, treat an ADR as
immutable — to change a decision, write a new ADR that supersedes it and update the old one's status.
| [0013](0013-remote-grpc-workers.md) | Remote gRPC workers — transport swap, protocol unchanged | Accepted |
