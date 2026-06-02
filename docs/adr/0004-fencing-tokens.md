# ADR-0004: Fencing tokens enforced from Phase 1

- **Status:** Accepted
- **Date:** 2026-06-02

## Context

Under at-least-once delivery ([ADR-0003](0003-delivery-semantics.md)), a worker can stall (e.g. a
40s GC pause), have its lease expire and reassigned, then wake as a **zombie** and write stale
results — corrupting state. A lease that is only *checked* cannot prevent this, because the check can
be followed by a pause before the write lands.

## Decision

Enforce **fencing tokens** from Phase 1. `job_runs.lease_token` is a monotonic counter bumped on
every (re)lease. Every state-mutating write is guarded by `WHERE … AND lease_token = :expected`. A
zombie's stale token matches zero rows and is rejected **at the database** — the actor need not even
know it failed.

## Options considered

- **Full enforcement now** — chosen. Correctness is a property of the data, not the actor.
- **"Column now, enforce later"** — defer guards to Phase 3. Rejected: double-runs are a *correctness*
  bug triggerable by a single GC pause on a single node, not a scale concern; deferring would make
  Phases 1–2 silently incorrect and retrofitting guards into every query is error-prone.
- **Heartbeats / lock-checking only** — insufficient; the classic distributed-locks failure mode.

## Consequences

- (+) At-least-once delivery is *honestly* safe under pauses/partitions from day one.
- (+) Each mutator returns a boolean "did I win the row", giving callers a clean lost-race signal.
- (−) ~15% more code in transitions and a token threaded through worker writes — paid once.
- (−) Fencing protects only **internal** state. External effects need idempotency keys; the two are
  complementary, not redundant.
