# ADR-0006: In-process, DB-mediated worker for Phase 1

- **Status:** Accepted
- **Date:** 2026-06-02

## Context

The system will eventually run workers as separate processes (Phase 3, behind gRPC). For Phase 1 we
must decide whether to build that distribution now or defer it, without compromising the realism of
the distributed protocol.

## Decision

Run the worker **in-process** within the `api` app for Phase 1, but have it communicate with the
scheduler **only through the database** (lease → heartbeat → result), exactly as a remote worker
would. The `worker` module stays framework-light and depends only on `scheduler-core`.

## Options considered

- **In-process, DB-mediated** — chosen. The leasing/fencing/reaping semantics are real and fully
  testable now; Phase 3 swaps the *transport* (DB → gRPC) without touching the protocol.
- **Separate worker container now** — more "distributed" feel, but adds RPC/serialization/infra before
  the core correctness is proven, slowing Phase 1 with no semantic gain.

## Consequences

- (+) One deployable, fast iteration, and the hard semantics are exercised immediately.
- (+) Clean seam (`RunDispatcher` interface) for the future remote worker.
- (−) Phase 1 does not exercise network partitions between scheduler and worker (the DB-mediated
  protocol is designed to tolerate them; validated for real in Phase 3).
