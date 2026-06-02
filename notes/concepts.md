# Kinetis — Concepts & Discussions

---

## Data Model

Records are typed containers — no logic, just fields. Like a struct.
```java
record Job(UUID id, String jobType, ZoneId timezone, ...)
enum JobState { SCHEDULED, LEASED, RUNNING, SUCCEEDED }
```

**Jobs vs Runs split**
- `Job` = definition (what to run, retry policy, cron). Never changes.
- `JobRun` = one execution. One job → many runs (retries, cron fires).

**Why ZoneId not String for timezone**
`ZoneId` rejects invalid values at construction. `String` only fails at runtime.
Rule: push validation as early as possible.

**Why jobType stays a String**
Remote workers send handler names over gRPC. No type-safe cross-process alternative.
Mitigation: validate against the handler registry at job submission time.

---

## Build System

```
api  →  worker  →  scheduler-core  →  raft
```
Each arrow = "depends on". Only `api` becomes a runnable app. The rest are libraries.

```bash
./gradlew :api:bootJar                  # one fat JAR with everything
./gradlew :scheduler-core:compileJava   # compile just one module
```

`raft` has zero Spring deps — pure Java, easy to test in isolation.
`api` is the only module that knows about Spring Boot, REST, gRPC.

---

## Spring Wiring

You never call `new LeaseManager(...)` yourself. Spring creates all objects once at startup
and injects dependencies automatically. These objects are called **beans**.

```java
public class LeaseManager {
    private final JdbcTemplate jdbc;  // Spring passes this in

    public LeaseManager(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }
}

// In config — you declare how to build it once
@Bean
LeaseManager leaseManager(JdbcTemplate jdbc) {
    return new LeaseManager(jdbc);
}
```

Swapping implementations (e.g. real DB vs in-memory for tests) = change one config line.

---

## Callbacks and Lambdas

Java passes callbacks as lambdas `(args) -> { body }` — inline implementations of a one-method interface.

```java
jdbc.query(
    (Connection conn) -> {       // callback 1: build the SQL statement
        var ps = conn.prepareStatement("SELECT ...");
        ps.setString(1, owner);
        return ps;
    },
    Mappers.JOB_RUN              // callback 2: map each DB row → JobRun
);
```

Spring owns the connection lifecycle. You own the query logic and the mapping.
Still synchronous — your thread waits for Postgres to respond.

---

## Threads and Execution

```
JVM Process
│
├── Main thread          — starts Spring, wires beans, goes idle
├── SchedulerLoop        — one thread, polls DB every 500ms
│   └── claims due runs → hands to WorkerPool → loops again
├── ReaperLoop           — one thread, polls DB every 5s
│   └── finds expired leases → reschedules or dead-letters
├── CronScheduler        — one thread, polls DB every 1s
│   └── fires due cron jobs → inserts run rows
└── WorkerPool           — one virtual thread per job
    ├── virtual thread 1 → runs job handler → markSucceeded()
    └── ... up to maxConcurrentRuns
```

**Virtual threads (Java 21)**
~few KB each vs ~1MB for OS threads. Block on DB/network like normal code —
the JVM suspends them while waiting and runs others. You write simple blocking code; JVM makes it efficient.

**The fencing token — crash safety in one line**
Every state-mutating SQL includes `WHERE lease_token = ?`.
Two workers race → only one matches the row → the other gets 0 rows updated → drops silently.
No locks, no coordinator — the DB is the arbiter.

**Right now in the codebase**
No threads running yet. Just data shapes and LeaseManager sitting idle.
Threading starts in Step 12 (WorkerPool), wired together in Step 26 (LoopRunner).

---

## Retry Logic

**BackoffCalculator** — pure math, no DB, no Spring.
Delay for attempt `n` = random value in `[0, min(1hr, base * factor^n)]`.
Full jitter is intentional: if 1000 jobs fail at the same time (downstream outage),
fixed backoff makes them all retry together again. Jitter spreads them out.

**RetryHandler** — single decision point shared by worker and reaper.
```
run failed →
  AT_MOST_ONCE?       → markDeadLetter (never retry by design)
  attempts exhausted? → markDeadLetter
  otherwise           → backoff.nextDelay() → rescheduleForRetry
```
Both the worker (caught an exception) and the reaper (lease expired) call the same method.
Centralising means they can never diverge on retry rules.

---

## Data Stores (Step 8)

**Mappers** — translates DB rows into Java objects. Spring hands each result row to the mapper,
which reads columns by name and constructs the Java record. The `ZoneId.of()` call here is where
the DB string becomes a typed `ZoneId` on read; `job.timezone().getId()` converts it back on write.
```
DB row (columns) → mapper → Job / JobRun record
```

**JobStore** — all SQL for the `jobs` table. Key method is `insertIfAbsent`: tries to insert,
and if the idempotency key already exists (`ON CONFLICT DO NOTHING`), fetches and returns the
existing id. Client can retry submission safely — never creates two jobs.

**JobRunStore** — all SQL for `job_runs`, but **only** plain inserts and reads.
State transitions (`SCHEDULED → LEASED → RUNNING → SUCCEEDED`) live in `LeaseManager`.
Rule: if a SQL statement has `WHERE lease_token = ?`, it belongs in `LeaseManager`, not here.

---

## Job Service (Step 9)

**SubmitCommand** — data bag carrying everything a caller wants when creating a job. No logic.
Nulls are intentional — `JobService` applies defaults for anything not provided.

**JobSubmission** — the receipt after submitting: `jobId`, `runId`, `created` flag.
`created = false` means it was a duplicate submission — ids point at what already existed.

**JobService** — the orchestrator. `submit()` does five things in one `@Transactional` call:
```
1. Apply defaults (delivery, retry, timezone)
2. Derive idempotency key if not provided
3. insertIfAbsent → jobId (new or existing)
4. Duplicate → return existing ids, done
5. New recurring job → set next_fire_time, CronScheduler takes over
   New one-off job   → insert first JobRun as SCHEDULED, done
```
`@Transactional` = Spring wraps the whole method in one DB transaction.
Job insert + run insert either both succeed or both roll back.

---

## Scheduler Loop (Step 10)

**RunDispatcher** — a one-method interface (`@FunctionalInterface`). The scheduler hands it a
leased run and doesn't care what happens next — could be in-process pool or gRPC to a remote worker.
This is what lets Phase 3 swap in remote workers without touching scheduler-core.

**SchedulerLoop** — the main poll loop. `tick()` does two things:
1. Backpressure check: count LEASED+RUNNING rows in owned shards. If at capacity → skip this tick.
2. `claimDue()` → `dispatchBatch()`. Claims a batch, hands to FairShareDispatcher.

Backpressure prevents flooding the worker pool — without it, the scheduler would keep claiming
runs that can't execute, piling up lease expirations for the reaper.

---

## ReaperLoop (Step 11)

Crash recovery. Finds runs where `lease_expires_at < now()` — worker presumed dead.
Routes each through `RetryHandler.onFailure()` — same path as an explicit worker failure.

**Why the fencing token matters here:**
Reaper bumps the token when rescheduling. If the "dead" worker was just slow and wakes up,
it tries `markSucceeded(runId, oldToken)` → 0 rows updated → silently ignored.
The DB resolves the race. No locks, no coordinator.

**JVM crash causes:**
- `OutOfMemoryError` — heap exhausted (large payloads, memory leak)
- `StackOverflow` — infinite recursion in a handler
- Native crash — JIT/JNI bug, produces `hs_err_pid.log`
- `kill -9` / Linux OOM Killer — kernel kills the process, no cleanup chance
- Hardware failure — machine dies, power cut, disk full

**GC pause causes:**
JVM stops all threads to reclaim memory ("stop-the-world"). Heartbeats stop during this.
- Full GC on large heaps can pause for seconds or minutes
- Heap pressure from too many objects created too fast
- Java 21 + ZGC targets sub-millisecond pauses, but lease TTL protects you regardless

**Mental model:** the reaper is a patrol asking "is anyone supposed to be working on this
but hasn't checked in?" — lease-based failure detection. Same pattern as Kubernetes pod
leases, Zookeeper ephemeral nodes. Prove you're alive by refreshing a timestamp, or get
declared dead and your work reassigned.

---

## Worker Module (Step 12)

**JobHandler** — interface with one method: `handle(ctx)`. Your business logic goes here.
Must be idempotent — delivery is at-least-once, so it may run more than once for the same work.

**JobContext** — record passed to the handler: run/job ids, parsed JSON payload, attempt count.
`attempt` is 0-based — use it to skip already-sent notifications on retries.

**HandlerRegistry** — map of `jobType → handler`. Prevents duplicate registrations via
`ConcurrentHashMap.putIfAbsent`. A worker advertises what it can run by what's registered here.

**WorkerPool** — the core execution engine. Two thread pools inside:
- `newVirtualThreadPerTaskExecutor()` — one virtual thread per job. Thousands can run concurrently cheaply.
- `ScheduledExecutorService` (1 platform thread) — fires heartbeats on a reliable timer. Can't use virtual threads here because the JVM might suspend them, breaking the timing.

`execute()` flow inside WorkerPool:
```
markRunning(token)        → LEASED → RUNNING, or drop if already fenced
jobStore.findById()       → load job definition + retry policy
registry.get(jobType)     → find handler, or fail if missing
heartbeat starts          → pushes lease_expires_at forward every N seconds
handler.handle(ctx)       → your business logic runs here
markSucceeded(token)      → RUNNING → SUCCEEDED, or silently ignored if fenced
heartbeat.cancel()        → always in finally block
```

**The fencing token through full lifecycle:**
```
claimDue()      → token=5, LEASED
markRunning()   → WHERE token=5 → RUNNING
heartbeat()     → WHERE token=5 → extends lease
markSucceeded() → WHERE token=5 → SUCCEEDED

--- worker dies mid-execution ---
reaper finds it  → token=5, expired → reschedule → token becomes 6
new worker runs  → token=7
zombie wakes up  → markSucceeded(token=5) → 0 rows → ignored
```

**Three demo handlers:**
- `NoOpHandler` — does nothing, succeeds. Smoke tests the full pipeline.
- `SleepHandler` — blocks the thread. Proves heartbeat keeps lease alive past TTL.
- `FailNTimesHandler` — throws on attempts 0..N-1, succeeds on attempt N. Tests retry + dead-letter path deterministically.

**`AutoCloseable`** — Spring calls `close()` on shutdown. Stops heartbeats immediately,
stops accepting new work, lets in-flight jobs finish. Without it the JVM would hang.

---

## SchedulerMetrics (Step 13)

Thin wrapper over Micrometer counters. Each event in the lifecycle has a named counter:
```
kinetis.jobs.submitted    → job accepted by JobService
kinetis.runs.leased       → run claimed by SchedulerLoop
kinetis.runs.succeeded    → handler completed successfully
kinetis.runs.failed       → handler threw an exception
kinetis.runs.retried      → rescheduled for retry
kinetis.runs.dead_lettered → retries exhausted or AT_MOST_ONCE
kinetis.runs.reaped       → reclaimed by ReaperLoop
```
Micrometer pushes these to Prometheus. In Grafana:
- `rate(kinetis.runs.dead_lettered[5m])` → alert on dead-letter spikes
- `kinetis.runs.reaped` → watch for lease expiry patterns (indicates slow/crashing workers)
- `kinetis.runs.succeeded - kinetis.runs.failed` → throughput health

---

## Testing Checkpoints

**After Step 1 (Gradle scaffold)**
```bash
cd Kinetis
./gradlew :scheduler-core:dependencies   # resolves + downloads all deps
./gradlew projects                        # lists all 4 modules
```

**After Steps 3–4 (DB migrations) — requires Postgres**
```bash
createdb kinetis_test
psql kinetis_test -f scheduler-core/src/main/resources/db/migration/V1__core_schema.sql
psql kinetis_test -f scheduler-core/src/main/resources/db/migration/V2__indexes.sql
psql kinetis_test -c "\dt"   # should show: jobs, job_runs
psql kinetis_test -c "\di"   # should show: idx_due, idx_expired, idx_job_runs_job_id
```

**After Step 6 (IdempotencyKeys) — pure Java, no deps**
```bash
javac -d /tmp scheduler-core/src/main/java/io/kinetis/core/idempotency/IdempotencyKeys.java
```

**After Step 8 (Mappers) — first full compile checkpoint**
```bash
./gradlew :scheduler-core:compileJava    # all scheduler-core classes compile cleanly
```
