# ADR-0008: Transactional outbox for atomic fan-out

- **Status:** Accepted (table defined in Phase 1; consumed from Phase 2)
- **Date:** 2026-06-02

## Context

When a run completes and must trigger the *next* unit of work — a cron job's next occurrence, or a
DAG's downstream tasks — doing the state change and the enqueue as two separate writes risks a crash
between them: either the next work is lost, or (on retry) double-enqueued.

## Decision

Adopt the **transactional outbox** pattern. The state change and an `outbox` row recording the
follow-up event are written in the **same database transaction** (atomic: both or neither). A separate
dispatcher reads undispatched outbox rows and performs the follow-up at-least-once, marking them done.
The `outbox` table ships in Phase 1 (migration `V3`); it is first consumed in Phase 2 for cron
next-fire, and in Phase 5 for DAG fan-out.

## Options considered

- **Transactional outbox** — chosen. Makes internal fan-out effectively-once using only the DB's own
  transactionality; no distributed transaction needed.
- **Dual writes (state, then enqueue)** — simplest, but the crash window loses or duplicates work.
- **Listen/notify or CDC streaming** — viable later for lower latency; the outbox is the robust,
  store-agnostic baseline and can be drained by CDC if needed.

## Consequences

- (+) Atomic, crash-safe triggering of follow-up work; the cornerstone of correct cron and DAGs.
- (+) Defined now → no hot-table migration later.
- (−) A dispatcher/poller and the outbox table add moving parts and a little latency; justified for
  the control-flow correctness they buy.
