# Implementation Documentation

How the code is organised and how a request flows through it. Pairs with
[../design/](../design/) (the *why*) and [../specs/](../specs/) (the contracts).

## Module layout

```
scheduler-core/   framework-light core (plain Java + Spring JDBC)
  io.kinetis.core
    model/        Job, JobRun, JobState, DeliveryPolicy, MisfirePolicy, RetryPolicy
    store/        JobStore, JobRunStore, Mappers            (all SQL for plain reads/writes)
    lease/        LeaseManager                              (all FENCED state transitions)
    retry/        BackoffCalculator, RetryHandler
    idempotency/  IdempotencyKeys
    scheduler/    SchedulerLoop, RunDispatcher (interface)
    reaper/       ReaperLoop
    metrics/      SchedulerMetrics
    service/      JobService, SubmitCommand, JobSubmission

worker/           execution side (depends on scheduler-core)
  io.kinetis.worker
    JobHandler, JobContext, HandlerRegistry, WorkerPool
    handlers/     NoOpHandler, SleepHandler, FailNTimesHandler

api/              Spring Boot deployable (the only framework-coupled module)
  io.kinetis.api
    SchedulerApplication
    web/          JobController
    dto/          SubmitJobRequest/Response, JobView, RunView
    config/       SchedulerProperties, CoreConfig (bean wiring), LoopRunner
    error/        GlobalExceptionHandler (RFC 7807)
    metrics/      QueueDepthGauge
```

**Dependency direction:** `api → worker → scheduler-core`. The core has no knowledge of the web or of
how execution happens; `worker` implements the core's `RunDispatcher` interface. This is what lets the
worker become a separate gRPC process in Phase 3 without changing the core protocol
([ADR-0006](../adr/0006-in-process-worker.md)). A full dependency graph is in
[../specs/README.md](../specs/README.md#4-component--dependency-graph).

## Responsibility split: stores vs. LeaseManager

- **Stores** (`JobStore`, `JobRunStore`) hold only plain inserts/reads.
- **`LeaseManager`** holds every *state-mutating transition*, each guarded by the fencing token.

Keeping all fenced writes in one class makes the "every mutation is fenced" invariant auditable at a
glance. The full transition table is in [../design/state-machine.md](../design/state-machine.md).

## Request flow: submit → run

```
POST /jobs
  └─ JobController.submit
       ├─ parse scheduleAt ("+5s" | ISO instant), build SubmitCommand
       └─ JobService.submit            (@Transactional)
            ├─ derive job key (or use caller's)        IdempotencyKeys.deriveJobKey
            ├─ JobStore.insertIfAbsent  → dedups on UNIQUE(idempotency_key)
            └─ if created: JobRunStore.insert(initial SCHEDULED run, run key = job key + slot)

LoopRunner (fixed-delay, dedicated executor)
  ├─ SchedulerLoop.tick (every poll-interval)
  │    └─ LeaseManager.claimDue        UPDATE … FOR UPDATE SKIP LOCKED … RETURNING *  (token++)
  │         └─ WorkerPool.dispatch(run)  → virtual thread
  │              ├─ LeaseManager.markRunning(id, token)         LEASED → RUNNING
  │              ├─ heartbeat scheduled (every heartbeat-interval) → extends lease (fenced)
  │              ├─ HandlerRegistry.get(jobType).handle(JobContext)
  │              ├─ success → LeaseManager.markSucceeded(id, token)
  │              └─ failure → RetryHandler.onFailure
  │                   ├─ attempts left & AT_LEAST_ONCE → rescheduleForRetry (backoff, token++)
  │                   └─ else → markDeadLetter (token++)
  └─ ReaperLoop.tick (every reaper-interval)
       └─ LeaseManager.findExpiredLeases → RetryHandler.onFailure(... "lease expired")
            (reclaim bumps token, fencing the presumed-dead worker)
```

## Concurrency model

- **Virtual threads** (`Executors.newVirtualThreadPerTaskExecutor`) run handlers — one per in-flight
  run, so blocking handlers scale cheaply (Project Loom).
- **Heartbeats** run on a small daemon `ScheduledExecutorService`, extending the lease while a handler
  runs; all heartbeat writes carry the run's token.
- **Loops** run on a dedicated 2-thread scheduled executor (`LoopRunner`); each tick catches its own
  errors so a transient DB blip never cancels the recurring task.

## Configuration

`SchedulerProperties` (prefix `scheduler.*`) exposes `node-id`, `batch-size`, `lease-ttl`,
`heartbeat-interval`, `poll-interval`, `reaper-interval`, `reaper-batch-size`. All have env-var
overrides (see [application.yml](../../api/src/main/resources/application.yml)). The lease-TTL ↔
heartbeat relationship is the key safety knob — see [../design/README.md](../design/README.md#5-key-tunable-lease-ttl-vs-heartbeat).

## Adding a new job handler

1. Implement `io.kinetis.worker.JobHandler` — return your `type()` and implement `handle(ctx)`.
   Make it **idempotent** (delivery is at-least-once); read args from `ctx.payload()` and use
   `ctx.idempotencyKey()` / `ctx.attempt()` as needed.
2. Register it as a Spring `@Bean` (it is auto-collected into `HandlerRegistry` via the injected
   `List<JobHandler>` in `CoreConfig`).
3. Submit jobs with `"jobType": "<your type()>"`.

## Error handling

`GlobalExceptionHandler` maps `IllegalArgumentException` and validation failures to RFC 7807
`ProblemDetail` (400); missing resources surface as 404 via `ResponseStatusException`. Loop/worker
errors are logged and never escape to cancel a recurring task.
