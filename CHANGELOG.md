# Changelog

All notable changes to **Kinetis** are documented here.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning: [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Planned (Phase 4 — high-throughput queue)
- Priority lanes fully exercised, per-tenant rate limiting with Grafana visibility, fair-share
  scheduling, backpressure benchmarks, throughput characterisation.

---

## [0.3.1] — 2026-06-03

Phase 3e: **remote gRPC workers** — transport swap, protocol unchanged.

### Added
- `worker.proto` — `Dispatch(RunAssignment)→Ack`, `Register(WorkerRegistration)→Ack`. Fencing
  token travels in `RunAssignment`; the worker echoes it on every `LeaseManager` DB write.
- `GrpcRunDispatcher` — fire-and-forget dispatch to remote workers; `@Primary` when `app.role=scheduler`.
- `WorkerGrpcServer` — receives `Dispatch`, executes via `HandlerRegistry` on virtual threads,
  writes state through `LeaseManager`. Self-registers with the scheduler on startup.
- `WorkerRegistry` — round-robin pool of gRPC stubs; TTL-based stale pruning.
- `app.role` property: `standalone` (default), `scheduler`, `worker`.
- ADR-0013: validates the "swap transport, not protocol" promise from ADR-0006.

### No schema change — DB protocol identical to Phase 1.

---

## [0.3.0] — 2026-06-03

Phase 3: **sharding + Raft + three shard-ownership implementations**.

### Added
- V5 migration: `shard_id SMALLINT` on `jobs`/`job_runs`, `cluster_nodes` table, shard-aware indexes.
- `ShardingUtils`, `ShardOwnershipProvider`, `StaticShardOwnership`, `PostgresAdvisoryShardOwnership`,
  `EtcdShardOwnership`, `RaftShardStateMachine`.
- Full Raft module from scratch: leader election, log replication, log compaction, `InMemoryRaftRpc`.
- `LeaseManager.claimDue` + `findExpiredLeases` scoped to `ownedShards`.
- `docker-compose.multi-node.yml` — two-node demo.
- ADRs 0010–0012.

### Known gaps
Raft persistence and joint-consensus membership changes deferred to Phase 6.

---

## [0.2.0] — 2026-06-03

Phase 2: **cron / recurring jobs**.

### Added
- Custom cron parser from scratch — Unix + Quartz dialects, sealed `FieldConstraint` hierarchy,
  DST-correct `CronEvaluator`, misfire policies (SKIP / FIRE_ONCE / CATCH_UP).
- `CronScheduler` — polls + enqueues + advances `next_fire_time` atomically.
- V4 migration: `next_fire_time TIMESTAMPTZ` on `jobs`.
- `GET /jobs/{id}/next-fire` endpoint.
- ADR-0009.

---

## [0.1.0] — 2026-06-02

Phase 1: **the durable execution primitive**.

### Added
- Gradle multi-module build (`raft`, `scheduler-core`, `worker`, `api`), Java 21, Spring Boot 3.3.5.
- Submit → lease (SKIP LOCKED) → execute (virtual threads) → succeed / retry / dead-letter.
- Fencing tokens, idempotency keys, exponential backoff with full jitter, reaper crash recovery.
- REST API (`POST/GET/DELETE /jobs`), RFC 7807 errors, Micrometer/Prometheus/Grafana.
- V1–V3 migrations, multi-stage Dockerfile, docker-compose.
- 8 Testcontainers integration tests + 4 Spring Boot E2E tests.
- ADRs 0001–0008, architecture + state machine docs.
