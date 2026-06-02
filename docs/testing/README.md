# Test Documentation

## Philosophy

The hard parts of this system — leasing, fencing, crash recovery — **only exist in the database**.
Mocks would test our assumptions, not the behaviour. So the suite runs against a **real PostgreSQL**
via [Testcontainers](https://java.testcontainers.org/); every test that touches semantics spins up an
actual `postgres:16` container and runs the real SQL.

## How to run

```bash
./gradlew build          # compile + full suite
./gradlew :scheduler-core:test
./gradlew :api:test
```

Requires a running Docker and JDK 21. The Gradle test task injects the Docker config automatically
(see the [environment quirks](#environment-quirks) below) — no manual env vars needed.

Reports: `*/build/reports/tests/test/index.html`; raw results: `*/build/test-results/test/*.xml`.

## Test inventory

### `scheduler-core` — Testcontainers, real Postgres

Base class [`AbstractPgTest`](../../scheduler-core/src/test/java/io/kinetis/core/AbstractPgTest.java)
starts one shared container, runs Flyway migrations, and truncates tables before each test.

| Test | Proves |
|---|---|
| `LeaseManagerTest.concurrentSchedulersClaimDisjointSets` | **SKIP LOCKED**: two schedulers claiming concurrently get disjoint sets that cover every run exactly once |
| `LeaseManagerTest.claimDueBumpsFencingTokenAndSkipsFutureRuns` | token bumps on lease; not-yet-due runs are not claimed |
| `LeaseManagerTest.staleTokenIsFencedOff` | **Fencing**: a zombie worker's stale-token write is rejected; state never flips to SUCCEEDED |
| `ReaperLoopTest.reaperReclaimsExpiredLeaseAndSchedulesRetry` | **Crash recovery**: expired lease → back to SCHEDULED, attempt++, token bumped |
| `ReaperLoopTest.reaperDeadLettersWhenAttemptsExhausted` | reaper dead-letters when no attempts remain |
| `JobServiceTest.resubmittingSameKeyDeduplicatesToOneJobAndOneRun` | idempotent submission: no duplicate job or run |
| `JobServiceTest.differentScheduleSlotsProduceDifferentRunKeys` | run key embeds the schedule slot (no false dedup across occurrences) |
| `JobServiceTest.cancelMarksRunsCancelled` | cancellation transitions non-terminal runs to CANCELLED |

### `api` — Spring Boot end-to-end

[`SchedulerEndToEndTest`](../../api/src/test/java/io/kinetis/api/SchedulerEndToEndTest.java) boots
the whole app (scheduler + reaper loops live) against a Testcontainers Postgres and drives jobs through
the real pipeline.

| Test | Proves |
|---|---|
| `noopJobRunsToSuccess` | full submit → lease → execute → SUCCEEDED |
| `failingJobRetriesThenSucceeds` | retries with backoff, succeeds at attempt 2 |
| `exhaustedRetriesEndInDeadLetter` | dead-letter after attempts exhausted |
| `atMostOnceJobIsNeverRetried` | `AT_MOST_ONCE` fails once, never retried (attempt stays 0) |

**Total: 12 tests, all green.**

## What each layer covers

- **Unit-ish (core, real DB):** the leasing/fencing/retry/reaper primitives in isolation.
- **Integration (api e2e):** the live loops, Spring wiring, and policy end-to-end.
- **Manual/live (`docker compose up`):** the full stack including a real container kill mid-run to
  watch the reaper reclaim and complete the job (the demo in the root [README](../../README.md)).

## Determinism notes

- Backoff uses jitter; tests seed the RNG (`new Random(1)`) or assert on **outcomes/attempt counts**,
  not exact delays.
- End-to-end tests shorten `poll-interval`/`reaper-interval` via `@DynamicPropertySource` and poll the
  run state with a timeout rather than sleeping a fixed amount.

## Environment quirks

The Gradle `Test` task (in the root [build.gradle.kts](../../build.gradle.kts)) sets, so Testcontainers
works on Docker Desktop without manual setup:

- `DOCKER_HOST` → `~/.docker/run/docker.sock` (desktop-linux context) if not already set;
- `systemProperty("api.version", "1.44")` — the local Docker Engine's `MinAPIVersion` (1.44) is newer
  than docker-java's default, which otherwise 400s on `/info`. The `DOCKER_API_VERSION` env var is
  **not** honored by docker-java; the system property is;
- `TESTCONTAINERS_RYUK_DISABLED=true` to avoid the reaper-sidecar in local runs.

## Gaps / future test work

- **Phase 2:** cron next-fire correctness across DST boundaries; misfire policies.
- **Phase 3:** partition tests between scheduler and remote worker; sharding/leader-election failover.
- **Load/soak:** throughput ceilings and lease-contention behaviour under high concurrency.
- **Chaos:** fault injection (DB blips, clock skew) as a CI gate.
