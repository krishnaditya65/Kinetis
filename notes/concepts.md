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
