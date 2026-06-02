# Architecture

## The one primitive

The whole system reduces to a single durable primitive:

> Run `F(args)` **at-least-once** at-or-after time `T`, survive crashes, and never double-run
> concurrently for the same logical job.

The advanced features are layers on top of it:

| Feature | How it reuses the primitive |
|---|---|
| One-off / delayed | the primitive directly (`scheduled_for = T`) |
| Cron / recurring | on each fire, atomically enqueue the next occurrence (Phase 2) |
| High-throughput queue | the primitive with `T = now` and many workers draining it (Phase 4) |
| Workflows / DAGs | completing task A enqueues its ready downstream tasks (Phase 5) |

Building the primitive well is what makes the rest cheap.

## Components (Phase 1)

```
        POST /jobs                         ┌──────────────────────────────┐
   client ─────────▶ JobController ──────▶ │ JobService.submit            │
                                           │  - dedup on idempotency key  │
                                           │  - insert job + initial run  │
                                           └──────────────┬───────────────┘
                                                          │ writes
                                                ┌─────────▼─────────┐
                                                │   PostgreSQL       │  source of truth
                                                │  jobs / job_runs   │
                                                └─────────┬─────────┘
              LoopRunner (fixed-delay)                    │
        ┌──────────────────────┬───────────────┐         │
        ▼                      ▼                          │
 ┌─────────────┐       ┌──────────────┐                  │
 │ SchedulerLoop│      │  ReaperLoop  │                   │
 │ claimDue     │      │ reclaim      │  ◀── reads/writes ┘
 │ (SKIP LOCKED)│      │ expired      │
 └──────┬───────┘      └──────────────┘
        │ dispatch(run)
 ┌──────▼───────────────────────────────────┐
 │ WorkerPool (virtual threads)              │
 │  markRunning → handler → markSucceeded    │
 │  on error → RetryHandler (backoff / DLQ)  │
 │  heartbeat extends lease (fencing-aware)  │
 └───────────────────────────────────────────┘
```

- **`scheduler-core`** — framework-light. Model records, JDBC stores, `LeaseManager` (all fenced
  transitions), `RetryHandler`, `BackoffCalculator`, `SchedulerLoop`, `ReaperLoop`, `JobService`.
- **`worker`** — `JobHandler`, `HandlerRegistry`, `WorkerPool` (Loom), demo handlers. Talks to the
  scheduler **only through the database**, so Phase 3 can move it behind gRPC by swapping transport.
- **`api`** — Spring Boot deployable: REST, metrics, bean wiring, the `LoopRunner`.

## The three correctness mechanisms

Each closes a *different* failure gap — they are complementary, not redundant.

1. **`SELECT … FOR UPDATE SKIP LOCKED` leasing.** N schedulers poll the same table and each claims a
   disjoint set of due runs. No coordinator, no double-dispatch. This is how we scale horizontally
   before adding any consensus machinery.
2. **Fencing tokens.** `lease_token` is bumped on every (re)lease; every state write is guarded by
   it, so a stalled/zombie worker's stale-token write is rejected at the row. This is what makes
   at-least-once *safe* under GC pauses and partitions. See [state-machine.md](state-machine.md).
3. **Idempotency keys.** Delivery is at-least-once, so a job can run more than once; the *run key*
   (job key + schedule slot) lets handlers dedup external **effects** for effectively-once outcomes.
   The job key also makes *submission* idempotent (a `UNIQUE` constraint).

Delivery defaults to `AT_LEAST_ONCE`; `AT_MOST_ONCE` is a per-job opt-out (never retried).

## Data model

- `jobs` — the **definition** (type, payload, idempotency key, delivery/retry/recurrence policy).
- `job_runs` — each **execution** (state, attempt, `scheduled_for`, lease fields, **`lease_token`**).
- `outbox` — present now, used from Phase 2 for atomic cron next-fire / DAG fan-out.

Hot paths use **partial indexes** (`WHERE state='SCHEDULED'`, `WHERE state IN ('LEASED','RUNNING')`)
so lease/reaper queries stay fast as terminal rows accumulate. Definition vs. execution is split so
one cron job → thousands of runs without mutating the definition, and so retries/DAGs reuse the run
table.

## Tunables (`SchedulerProperties`)

The key safety relationship is **heartbeat ≪ lease TTL**: heartbeats must fire several times within
a lease so a healthy long job never loses it, while the TTL stays short enough for timely crash
recovery. Defaults: lease 30s, heartbeat 10s, poll 500ms, reaper 5s.

## Verification

`./gradlew build` runs the suite against a real Postgres (Testcontainers, no mocks):

- `LeaseManagerTest` — SKIP LOCKED disjointness; fencing of a stale-token zombie write.
- `ReaperLoopTest` — expired lease → retry; exhausted attempts → dead-letter.
- `JobServiceTest` — idempotent submission; run-key per schedule slot; cancellation.
- `SchedulerEndToEndTest` — full app: success, retry-then-succeed, dead-letter, at-most-once.

All of these were also exercised live via `docker compose up`, including killing the app mid-run and
watching the reaper reclaim and complete the job.

## Roadmap

| Phase | Scope |
|---|---|
| 2 | Cron parsing (tz/DST aware), misfire policies, outbox-driven next-fire |
| 3 | Sharding, leader election, remote gRPC workers, Kafka dispatch at scale |
| 4 | Priority, rate limiting, tenant fairness, backpressure (high-throughput queue) |
| 5 | DAG dependencies, fan-out/fan-in, partial-failure handling |
| 6 | Chaos/fault-injection tests, web UI, benchmarks, exactly-once-effect analysis |
