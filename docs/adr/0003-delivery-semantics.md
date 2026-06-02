# ADR-0003: At-least-once delivery + idempotency keys

- **Status:** Accepted
- **Date:** 2026-06-02

## Context

Across a process boundary that can fail, **exactly-once side effects are impossible** (a worker can
crash after performing the effect but before recording it — the Two Generals problem). We must choose
which uncertainty to accept and where to resolve it.

## Decision

Default to **at-least-once delivery**: the system retries until a worker positively acknowledges
success. Push effect-deduplication to the only party that can resolve it via **idempotency keys**:
a required job key (dedups submission) and a run-level key (`job key + schedule slot`, dedups a single
execution's effect). Provide `AT_MOST_ONCE` as a per-job opt-out for cases where a duplicate is worse
than a miss.

## Options considered

- **At-most-once** — never retry; simplest, but jobs silently vanish on crash. Unacceptable default.
- **At-least-once + idempotency** — chosen. The system guarantees *progress*; the job author (or an
  internal dedup layer) guarantees *effect safety*. This is what SQS, Kafka, Temporal, Step Functions
  all do.
- **"Exactly-once"** — only achievable as at-least-once + a dedup layer (idempotency keys / outbox).
  Not a separate guarantee; folded into the chosen approach.

## Consequences

- (+) No lost work; honest, well-understood semantics; good company/precedent.
- (+) Required idempotency key creates a "pit of success" — effectively-once by default.
- (−) Handlers must be written to tolerate re-execution; documented as a first-class expectation.
- (−) A healthy job can still run twice (slow worker + reaper); mitigated by [ADR-0004](0004-fencing-tokens.md)
  for internal state and by idempotency keys for external effects.
