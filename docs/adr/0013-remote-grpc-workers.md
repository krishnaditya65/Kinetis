# ADR-0013: Remote gRPC workers — transport swap, protocol unchanged

- **Status:** Accepted
- **Date:** 2026-06-03

## Context

Phase 3e splits the worker into a separate process/container. The core question is: what changes
and what stays exactly the same when the worker is no longer in-process?

## Decision

Use gRPC as the **dispatch transport only** — the distributed protocol (leasing, fencing, heartbeats,
retries) remains DB-mediated and unchanged. Specifically:

1. Scheduler → Worker: `Dispatch(RunAssignment)` — fire-and-forget; the worker Acks receipt
   immediately and executes asynchronously.
2. Worker → DB: `LeaseManager.markRunning`, `heartbeat`, `markSucceeded`, `rescheduleForRetry` —
   exactly as the in-process `WorkerPool` does. The fencing token travels in `RunAssignment` and is
   used by the worker on every DB write.
3. Worker → Scheduler: `Register(WorkerRegistration)` — the worker announces its gRPC address so
   the scheduler's `WorkerRegistry` can route dispatch to it.

## Why fire-and-forget dispatch

The alternative (scheduler waits for the RunResult RPC) creates a synchronous coupling that turns
network latency into scheduler throughput. Fire-and-forget preserves the same "outcome tracked via
DB" property as the in-process case. If the worker crashes before starting, the lease expires and
the reaper reclaims the run — same crash-recovery path, no new protocol needed.

## What changed (the transport)

- `RunDispatcher` interface: the seam designed in ADR-0006. In `standalone` role, `WorkerPool`
  implements it; in `scheduler` role, `GrpcRunDispatcher` is `@Primary` and wins injection.
- Worker is its own Spring Boot context (`app.role=worker`) with `WorkerGrpcServer` but no loops.

## What stayed the same (the protocol)

- Fencing tokens on every state transition.
- At-least-once delivery semantics.
- Reaper reclaims expired leases (works unchanged — worker is a separate process, but its lease
  is in the same Postgres with the same expiry logic).
- Backoff / dead-letter via `RetryHandler` — called in `WorkerGrpcServer.failOrRetry`.

## Consequences

- (+) ADR-0006 ("swap transport, not protocol") validated — the change was a clean seam swap.
- (+) Workers scale independently; adding more workers requires no scheduler change.
- (+) `WorkerRegistry` round-robins across registered workers; Phase 4 can replace this with
  least-load routing without touching the protocol.
- (−) Worker discovery is manual registration (Register RPC + TTL in `WorkerRegistry`). A proper
  service-discovery system (Kubernetes Services, Consul) replaces this in Phase 6.
- (−) TLS not yet wired (plaintext gRPC). Phase 6 hardening adds mTLS between scheduler and workers.
- (−) `WorkerGrpcServer.registerWithScheduler` is best-effort (non-fatal on failure) — a worker
  that can't reach the scheduler still runs, but gets no dispatch until it successfully registers.
  Retry loop deferred to Phase 6.
