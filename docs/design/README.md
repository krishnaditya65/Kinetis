# Design Documentation

This folder holds the *why* behind the system. Start here, then drill into:

- [architecture.md](architecture.md) — components, the three correctness mechanisms, data model, roadmap.
- [state-machine.md](state-machine.md) — the run lifecycle and every transition's SQL guard.
- ADRs in [../adr/](../adr/) — individual decisions with their trade-offs.

---

## 1. Problem statement

Build a distributed job scheduler that can run **delayed/one-off tasks, cron, a high-throughput
queue, and DAGs**, operating reliably under failure and scaling horizontally. The design strategy is
to reduce all of these to **one durable primitive** and build it correctly first.

## 2. The primitive

> Run `F(args)` **at-least-once** at-or-after time `T`, survive crashes, and never double-run
> concurrently for the same logical job.

Every higher feature is a thin layer:

| Feature | Layering |
|---|---|
| One-off / delayed | the primitive with `scheduled_for = T` |
| Cron | on each fire, atomically enqueue the next occurrence (outbox) |
| Queue | the primitive with `T = now`, many workers draining |
| DAG | completing A enqueues A's ready successors |

## 3. The fundamental constraint

Exactly-once *execution* is impossible across a failable process boundary: a worker can perform a side
effect and crash before recording it (Two Generals). So the design question is never "how many times
does it run" but **"who absorbs the uncertainty in the crash window."** We answer that with three
mechanisms, each closing a different gap:

| Uncertainty | Resolved by | Mechanism |
|---|---|---|
| Did the run make progress? | the **system** (it can retry) | at-least-once leasing |
| Did a zombie worker corrupt state? | the **database** (knows latest token) | fencing tokens |
| Did the external effect already happen? | the **effect boundary** | idempotency keys |
| Was the follow-up work enqueued atomically? | the **transaction** | transactional outbox |

No single mechanism suffices; together they make the system *honestly* correct.

### 3.1 At-least-once delivery
A run stays non-terminal until a worker acknowledges success. Crash, timeout, or lease expiry returns
it to `SCHEDULED`. See [ADR-0003](../adr/0003-delivery-semantics.md). `AT_MOST_ONCE` is a per-job
opt-out for "a duplicate is worse than a miss."

### 3.2 Fencing tokens
`lease_token` is bumped on every (re)lease; every state write is guarded by it, so a stalled worker's
stale-token write is rejected at the row. This is what makes at-least-once *safe* under GC pauses and
partitions. See [ADR-0004](../adr/0004-fencing-tokens.md) and
[state-machine.md](state-machine.md).

### 3.3 Idempotency keys
Two levels: the **job key** dedups submission (UNIQUE constraint); the **run key**
(`job key + schedule slot`) dedups a single execution's effect while still distinguishing each cron
occurrence. See [ADR-0003](../adr/0003-delivery-semantics.md).

### 3.4 Transactional outbox
The state change and the event that triggers the next job are written in one transaction, making
internal fan-out (cron next-fire, DAG successors) effectively-once. Defined in Phase 1, used from
Phase 2. See [ADR-0008](../adr/0008-transactional-outbox.md).

## 4. Leasing strategy

`SELECT … FOR UPDATE SKIP LOCKED` lets N schedulers poll the same table and claim disjoint batches
with no coordinator — the basis for coordinator-free horizontal scaling before Phase 3 adds sharding
and leader election. See [ADR-0002](../adr/0002-postgres-skip-locked-leasing.md).

## 5. Key tunable: lease TTL vs. heartbeat

The core safety relationship is **heartbeat interval ≪ lease TTL**:
- heartbeats must fire several times within a lease so a healthy long-running job never loses it;
- the TTL must be short enough that a dead worker's runs are reclaimed promptly.

Defaults: lease 30s, heartbeat 10s, poll 500ms, reaper 5s. Tunable per deployment via
`SchedulerProperties`. Shorter TTL = faster recovery but more heartbeat traffic and a wider
double-run window (always fenced, so safe — just more wasted work).

## 6. Non-goals (Phase 1)

Horizontal multi-node scheduling, sharding, leader election, remote workers, cron, priorities, and
DAGs are explicitly out of scope for Phase 1 and addressed in later phases (see
[architecture.md](architecture.md#roadmap)). Phase 1's job is to make the primitive bulletproof.
