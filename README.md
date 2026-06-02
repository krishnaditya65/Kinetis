# Kinetis

A distributed job scheduler built from scratch in **Java 21 + Spring Boot**. Grows from a single
durable primitive into delayed tasks, cron, a high-throughput queue, and DAGs.

> **Status:** Phase 3e complete — durable execution, cron, sharding, Raft consensus, and remote
> gRPC workers. See [docs/design/architecture.md](docs/design/architecture.md) for the full design.

## The core idea

Everything is built on **one primitive**:

> Run `F(args)` **at-least-once** at-or-after time `T`, survive crashes, and never double-run
> concurrently for the same logical job.

Three mechanisms provide correctness:

| Mechanism | Gap it closes |
|-----------|--------------|
| `SELECT … FOR UPDATE SKIP LOCKED` | Many schedulers, no double-dispatch, no coordinator |
| **Fencing tokens** | Stalled/zombie worker can't corrupt state |
| **Idempotency keys** | Duplicate effects on retry |

## Modules

```
api  →  worker  →  scheduler-core  →  raft
```

| Module | What it is |
|--------|-----------|
| `raft` | Raft consensus from scratch — zero Spring deps |
| `scheduler-core` | Framework-light core: model, stores, leasing, retry, cron, sharding |
| `worker` | Execution side: handler interface, virtual-thread pool, heartbeats |
| `api` | Spring Boot app: REST, gRPC, metrics, loop runner |

## Quick start

Requires Docker.

```bash
cp .env.example .env
docker compose up --build
```

Starts Postgres, the app (**:8080**), Prometheus (**:9090**), Grafana (**:3000**, admin/admin).

### Try it

```bash
# Submit a job due in 5 seconds
curl -s -XPOST localhost:8080/jobs -H 'content-type: application/json' \
  -d '{"jobType":"sleep","payload":{"ms":2000},"scheduleAt":"+5s"}'

# Watch it move SCHEDULED → LEASED → RUNNING → SUCCEEDED
curl -s localhost:8080/jobs/<jobId>/runs | jq

# Recurring every minute
curl -s -XPOST localhost:8080/jobs -H 'content-type: application/json' \
  -d '{"jobType":"noop","cronExpr":"* * * * *"}'

# Two-node demo (node-a shards 0-7, node-b shards 8-15)
docker compose -f docker-compose.yml -f docker-compose.multi-node.yml up --build
```

Built-in handlers: `noop`, `sleep` (`{ms}`), `failNTimes` (`{failTimes}`).

## API

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/jobs` | Submit a job (idempotent on key) |
| `GET`  | `/jobs/{id}` | Job definition |
| `GET`  | `/jobs/{id}/runs` | All runs |
| `GET`  | `/jobs/{id}/next-fire` | Next cron fire time |
| `DELETE` | `/jobs/{id}` | Cancel non-terminal runs |
| `GET`  | `/actuator/health` · `/actuator/prometheus` | Health & metrics |

## Build & test

Requires JDK 21 and Docker (Testcontainers).

```bash
./gradlew build          # compile + full test suite
./gradlew :api:bootJar   # fat JAR → api/build/libs/
```

## Deployment modes (`app.role`)

| Role | What runs |
|------|-----------|
| `standalone` (default) | Scheduler + in-process worker, no gRPC |
| `scheduler` | Scheduler loops + gRPC dispatch to remote workers |
| `worker` | gRPC server only — receives and executes assignments |

## Documentation

| Area | Path |
|------|------|
| Design & architecture | [docs/design/](docs/design/) |
| Implementation guide | [docs/implementation/](docs/implementation/) |
| Testing | [docs/testing/](docs/testing/) |
| Architecture Decision Records | [docs/adr/](docs/adr/) |
| OpenAPI spec | [docs/specs/openapi.yaml](docs/specs/openapi.yaml) |
| Changelog | [CHANGELOG.md](CHANGELOG.md) |
