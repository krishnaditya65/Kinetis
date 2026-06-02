# System Specification & Operations

A single reference for the system's **boundaries, data, deployment, structure, and security**. Five
sections:

1. [API Contracts & Boundary Specs](#1-api-contracts--boundary-specs)
2. [Data Models & Schema Evolution](#2-data-models--schema-evolution)
3. [Deployment & Topology (Infrastructure as Code)](#3-deployment--topology-infrastructure-as-code)
4. [Component / Dependency Graph](#4-component--dependency-graph)
5. [Threat Model & Security Manifest](#5-threat-model--security-manifest)

---

## 1. API Contracts & Boundary Specs

### 1.1 External boundary — REST (Phase 1)

The synchronous control-plane API is specified in **[openapi.yaml](openapi.yaml)** (OpenAPI 3.1).
Summary:

| Method | Path | Purpose |
|---|---|---|
| POST | `/jobs` | Submit (idempotent on key); 201 created / 200 deduplicated |
| GET | `/jobs/{id}` | Job definition |
| GET | `/jobs/{id}/runs` | Runs for a job |
| DELETE | `/jobs/{id}` | Cancel non-terminal runs |
| GET | `/actuator/health`, `/actuator/prometheus` | Ops endpoints |

Errors use **RFC 7807** (`application/problem+json`). Contract source of truth is the controller
([JobController](../../api/src/main/java/io/kinetis/api/web/JobController.java)); keep
`openapi.yaml` in sync when endpoints change (a generator can be wired in later via springdoc).

### 1.2 Internal boundary — scheduler ↔ worker

- **Phase 1:** DB-mediated. The "contract" is the `job_runs` row protocol — `claimDue` →
  `markRunning` → `heartbeat` → `markSucceeded` / `rescheduleForRetry`, all fenced by `lease_token`
  (see [../design/state-machine.md](../design/state-machine.md)). The seam in code is the
  `RunDispatcher` interface.
- **Phase 3 (planned):** gRPC. Sketch of the forthcoming `worker.proto`:

  ```protobuf
  service Worker {
    rpc Dispatch(RunAssignment) returns (Ack);          // scheduler → worker
    rpc ReportResult(RunResult) returns (Ack);          // worker → scheduler
    rpc Heartbeat(stream HeartbeatPing) returns (stream HeartbeatPong);
  }
  message RunAssignment { string run_id = 1; string job_type = 2; bytes payload = 3;
                          int32 attempt = 4; int64 lease_token = 5; string idempotency_key = 6; }
  message RunResult     { string run_id = 1; int64 lease_token = 2;
                          enum Outcome { SUCCEEDED = 0; FAILED = 1; } Outcome outcome = 3; string error = 4; }
  ```
  Note the `lease_token` travels on every message — fencing survives the transport change unchanged.

### 1.3 Versioning

REST is unversioned pre-1.0; breaking changes are called out in [../../CHANGELOG.md](../../CHANGELOG.md).
Post-1.0, breaking REST changes go under `/v2`; proto evolves by field-number-stable additive changes.

---

## 2. Data Models & Schema Evolution

### 2.1 Tables (Phase 1)

Authoritative DDL: [scheduler-core/src/main/resources/db/migration](../../scheduler-core/src/main/resources/db/migration).

**`jobs`** — immutable definition.

| Column | Type | Notes |
|---|---|---|
| id | UUID PK | |
| job_type | TEXT | routes to a handler |
| payload | JSONB | handler args |
| idempotency_key | TEXT | **UNIQUE** — dedups submission |
| delivery_policy | TEXT | `AT_LEAST_ONCE` \| `AT_MOST_ONCE` (CHECK) |
| cron_expr | TEXT NULL | Phase 2 |
| timezone | TEXT | default UTC |
| misfire_policy | TEXT | `SKIP`\|`FIRE_ONCE`\|`CATCH_UP` (CHECK) |
| max_attempts / backoff_base_ms / backoff_factor | INT / BIGINT / NUMERIC | retry policy |
| created_at | TIMESTAMPTZ | |

**`job_runs`** — one execution.

| Column | Type | Notes |
|---|---|---|
| id | UUID PK | |
| job_id | UUID FK→jobs | ON DELETE CASCADE |
| state | TEXT | 7-value lifecycle (CHECK) |
| attempt | INT | 0-based |
| scheduled_for | TIMESTAMPTZ | due time |
| lease_owner / lease_expires_at | TEXT / TIMESTAMPTZ | lease |
| **lease_token** | BIGINT | **fencing token** |
| idempotency_key | TEXT | run key = job key + slot |
| last_heartbeat_at / last_error / enqueued_at / started_at / finished_at | TIMESTAMPTZ / TEXT / … | bookkeeping |

**`outbox`** — id, aggregate_id, event_type, payload (JSONB), created_at, dispatched_at. Defined now,
used from Phase 2.

**Indexes (partial, hot-path):**
`idx_due (scheduled_for) WHERE state='SCHEDULED'` ·
`idx_expired (lease_expires_at) WHERE state IN ('LEASED','RUNNING')` ·
`idx_job_runs_job_id (job_id)` · `idx_outbox_undispatched (created_at) WHERE dispatched_at IS NULL`.

Entity relationships and the rationale for the definition/execution split:
[../adr/0005-jobs-vs-runs-split.md](../adr/0005-jobs-vs-runs-split.md).

### 2.2 Evolution strategy

- **Tooling:** Flyway, versioned `V{n}__name.sql`, applied automatically on app startup and in tests.
  Migrations are **immutable once merged**; corrections go in a new migration.
- **Forward/backward compatibility:** additive, non-breaking changes preferred (new nullable columns,
  new tables, new indexes `CONCURRENTLY` in prod). The recurrence/policy columns were added in `V1`
  *ahead of need* specifically to avoid altering hot tables later.
- **Expand/contract for breaking changes:** (1) add new column/table; (2) dual-write & backfill;
  (3) switch reads; (4) drop the old in a later release — never in the same step.
- **Enum-as-TEXT + CHECK** (not native enums) so adding a state/policy value is a cheap CHECK update,
  not a type migration.
- **State machine compatibility:** new states must be added at the edges of the lifecycle; the fencing
  guard (`lease_token`) must remain on every mutating statement (invariant, not optional).

---

## 3. Deployment & Topology (Infrastructure as Code)

### 3.1 Current IaC

The reproducible stack is declared as code:

- [Dockerfile](../../Dockerfile) — multi-stage build (Gradle/JDK21 → `eclipse-temurin:21-jre`),
  non-root, ZGC.
- [docker-compose.yml](../../docker-compose.yml) — the topology below.
- [.env.example](../../.env.example) — parameters (ports, credentials, scheduler tunables).
- [docker/](../../docker/) — Prometheus scrape config + Grafana datasource/dashboard provisioning.

### 3.2 Topology (Phase 1)

```
                         ┌──────────────┐        ┌──────────────┐
        host:8080 ─────▶ │     app      │◀──────▶│   postgres   │  host:5440→5432
                         │ (Spring Boot │  JDBC  │   (volume:   │
                         │  + worker +  │        │    pgdata)   │
                         │   loops)     │        └──────────────┘
                         └──────┬───────┘
                          /actuator/prometheus
                                ▲ scrape
                         ┌──────┴───────┐        ┌──────────────┐
        host:9090 ─────▶ │  prometheus  │◀──────▶│   grafana    │  host:3000
                         └──────────────┘  query │ (provisioned │
                                                 │  dashboard)  │
                                                 └──────────────┘
```

| Service | Image | Host port | Healthcheck |
|---|---|---|---|
| postgres | postgres:16 | 5440→5432 | `pg_isready` |
| app | built from Dockerfile | 8080 | `/actuator/health` = UP |
| prometheus | prom/prometheus:v2.54.1 | 9090 | — |
| grafana | grafana/grafana:11.2.0 | 3000 | — |

Bring-up ordering is enforced via `depends_on: condition: service_healthy` (app waits for Postgres).

### 3.3 Configuration surface

All runtime config is env-driven (12-factor): DB connection (`DB_URL/USER/PASSWORD`), server port,
log level, and scheduler tunables (`SCHED_LEASE_TTL`, `SCHED_HEARTBEAT_INTERVAL`, `SCHED_POLL_INTERVAL`,
`SCHED_REAPER_INTERVAL`, `SCHED_BATCH_SIZE`, `NODE_ID`). Defaults live in
[application.yml](../../api/src/main/resources/application.yml).

### 3.4 Roadmap (later phases)

- **Phase 3:** Helm chart / Kubernetes manifests — Deployment for stateless scheduler nodes (HPA on
  queue depth), separate worker Deployment, managed Postgres, Kafka for hot-path dispatch. Multiple
  `app` replicas already work today thanks to SKIP LOCKED + fencing (single-DB).
- **Stateless scaling now:** the `app` is horizontally scalable against one Postgres in Phase 1
  (each replica polls independently); only cron/leadership concerns are deferred.

---

## 4. Component / Dependency Graph

### 4.1 Build modules

```
        ┌─────┐
        │ api │   (Spring Boot deployable)
        └──┬──┘
     ┌─────┴──────┐
     ▼            ▼
 ┌────────┐   ┌──────────────┐
 │ worker │──▶│ scheduler-core│   (framework-light; no web, no Spring Boot)
 └────────┘   └──────────────┘
```
Direction: `api → worker → scheduler-core`. `scheduler-core` depends on nothing internal. This
enforces that core logic is framework-agnostic and lets the worker split out in Phase 3.

### 4.2 Runtime components (within `app`)

```
JobController ─▶ JobService ─▶ JobStore / JobRunStore ─▶ JdbcTemplate ─▶ Postgres
LoopRunner ─▶ SchedulerLoop ─▶ LeaseManager ─▶ Postgres
          └─▶ ReaperLoop  ─▶ LeaseManager + JobStore + RetryHandler
SchedulerLoop ─▶ WorkerPool (RunDispatcher) ─▶ HandlerRegistry ─▶ JobHandler(s)
WorkerPool ─▶ LeaseManager (markRunning/heartbeat/markSucceeded) + RetryHandler
All ─▶ SchedulerMetrics ─▶ Micrometer ─▶ /actuator/prometheus
```

### 4.3 Key external dependencies

| Dependency | Used for | Notes |
|---|---|---|
| Spring Boot 3.3.5 | web, JDBC, Actuator, validation | `api` only |
| PostgreSQL 16 + JDBC | durable store, leasing | source of truth |
| Flyway | schema migrations | runs on startup |
| Micrometer + Prometheus | metrics | Grafana dashboard provisioned |
| HikariCP | connection pool | bundled |
| Jackson | JSON / payloads | bundled |
| Testcontainers 1.21.3 | real-DB tests | test scope |

A machine-readable dependency report is available via `./gradlew :api:dependencies`.

---

## 5. Threat Model & Security Manifest

> **Status:** Phase 1 is a **local/dev-trust deployment** — no authN/Z on the API yet. This section
> documents trust boundaries, current posture, and the hardening backlog so security is explicit, not
> assumed.

### 5.1 Assets

Job definitions & payloads (may contain sensitive args), execution history, the database, the metrics
endpoint, and the execution capability itself (a handler runs code with the app's privileges).

### 5.2 Trust boundaries

```
[ untrusted client ] ──HTTP──▶ [ app (trusted) ] ──JDBC──▶ [ postgres (trusted) ]
                                      │
                                      └─scrape─ [ prometheus/grafana (ops) ]
```
The REST boundary is the primary attack surface; the app↔DB and app↔ops boundaries are inside the
deployment trust zone (Docker network) in Phase 1.

### 5.3 STRIDE summary

| Threat | Vector | Current state | Mitigation / plan |
|---|---|---|---|
| **S**poofing | Unauthenticated API caller | **Open** in Phase 1 | Add API auth (API keys / OIDC / mTLS) before any non-local deploy |
| **T**ampering | Malicious payload; SQL injection | Partial | All SQL is **parameterized** (no string-built queries); JSONB validated. Add payload size/schema limits |
| **R**epudiation | Who submitted/cancelled? | Gap | Add audit log (submitter identity, action, timestamp) with auth |
| **I**nfo disclosure | Payloads/errors leak; metrics exposure | Partial | Errors are RFC 7807 (no stack traces to client). Plan: secret redaction, restrict `/actuator/*`, TLS |
| **D**oS | Unbounded submissions; huge payloads; retry storms | Partial | Backoff **with jitter** prevents retry stampedes; `batch-size` caps per-tick work. Add rate limiting, payload caps, per-tenant quotas |
| **E**levation | Handler runs arbitrary code | By design | Handlers are first-party code today. Plan: sandbox/isolate untrusted handlers; least-privilege DB role |

### 5.4 Current security posture (what *is* done)

- **Parameterized SQL everywhere** (`JdbcTemplate` with bind args) — no injection surface in queries.
- **Fencing tokens** prevent a compromised/zombie worker from corrupting other runs' state.
- **Idempotency** limits the blast radius of replayed submissions.
- **Container hardening:** app runs as a **non-root** user; minimal JRE base image.
- **No secrets in code:** DB credentials come from env; `.env` is git-ignored (`.env.example` only).
- **Problem responses** avoid leaking internals (no stack traces in API errors).

### 5.5 Hardening backlog (pre-production)

1. **AuthN/AuthZ** on the REST API (and later gRPC: mTLS between scheduler and workers).
2. **Transport security** (TLS termination) for API and DB connections.
3. **Secrets management** (Vault/SOPS/KMS) instead of plain env in non-local deploys.
4. **Rate limiting & payload limits**; per-tenant quotas (ties into Phase 4 fairness).
5. **Restrict/segment `/actuator`** (separate management port, auth, network policy).
6. **Audit trail** of submissions/cancellations with caller identity.
7. **Least-privilege DB role** for the app; separate migration role.
8. **Dependency & image scanning** in CI (SCA, Trivy) and SBOM generation.
9. **Handler sandboxing** if/when third-party job code is supported.
