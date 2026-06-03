# Distributed Job Scheduler — Full Phase Plan

## Baseline (complete)
Phase 0 + Phase 1 committed to `main`. Running via `docker compose up`.
All 12 tests green. Full documentation set in `docs/`.

---

## Phase 2 — Cron / Recurring

**Goal:** fire a job on a cron schedule, correctly, exactly once per occurrence, with
crash-safe next-fire enqueue and a configurable misfire policy.

### 2a. Cron expression parser (from scratch)
- Tokenizer + AST: fields (second/minute/hour/day/month/weekday), special values `*`, `?`,
  `/`, `,`, `-`, `L`, `W`, `#` (Quartz extensions)
- Evaluator: `nextFireTime(ZonedDateTime from, CronExpr) → ZonedDateTime`
- DST correctness: spring-forward skips a fire; fall-back may fire once or twice depending on policy
- Support Unix (`0 * * * *`) and Quartz (`0 0 12 * * ?`) dialects

### 2b. next_fire_time scheduling
- `jobs` table already has `cron_expr`, `timezone`, `misfire_policy`; add `next_fire_time TIMESTAMPTZ`
  in migration V4
- The scheduler adds `next_fire_time` to the due-run claim query (OR `scheduled_for <= now()`)
- On each cron fire: in the **same transaction** — create the `SCHEDULED` run for this occurrence
  AND update `next_fire_time` to the next occurrence (outbox not needed here; single-table update)

### 2c. Misfire policies
- `SKIP` — advance `next_fire_time` past now; skip all missed occurrences
- `FIRE_ONCE` — run one catch-up immediately, then resume from the next future occurrence
- `CATCH_UP` — create a `SCHEDULED` run for each missed occurrence (in order); dangerous, user-opt-in

### 2d. API additions
- `POST /jobs` already accepts `cronExpr`, `timezone`, `misfirePolicy` — wire them in
- Add `GET /jobs/{id}/next-fire` endpoint

### New ADR
- ADR-0009: cron parser from scratch vs. library (decision: from scratch, learning value, controlled)
- ADR-0010: DST misfire handling strategy

---

## Phase 3 — Scale-out & High Availability

Three parallel tracks, sequenced:

### Track A: Multi-node scheduling (first)

#### 3a. Sharding
- Add `shard_id SMALLINT` to `job_runs` (hash of `job_id % num_shards`); migration V5
- Scheduler nodes declare their shard range via `scheduler.shard-range` config property
- `claimDue` adds `AND shard_id = ANY(:ownedShards)` — each node only claims its shards
- SKIP LOCKED continues to protect within a shard for the case of two nodes sharing one

#### 3b. Shard ownership — Phase 1: Postgres advisory locks
- A `shard_assignments` table: `shard_id`, `owner_node_id`, `lease_expires_at`
- `pg_try_advisory_xact_lock(shard_id)` — a node holds a session-level advisory lock on each shard it owns
- On startup: claim unclaimed or expired shards; heartbeat to renew; detect departing nodes via expiry
- Rebalancing: on join, new node steals shards from overloaded owners; on leave, shards time out and are reclaimed

#### 3c. Shard ownership — Phase 2: etcd (production-grade distributed KV)
- `etcd` container added to compose
- Each node campaigns for `scheduler/shards/{id}` key with a TTL lease via `etcd` Java client
- Node death = TTL expiry = key released = another node picks it up
- Compares directly with the Postgres advisory lock approach in ADR-0011

#### 3d. Shard ownership — Phase 3: Raft from scratch (deep learning track)
- Standalone `raft/` module — no external dependency
- Leader election: randomised election timeout, `RequestVote` / `AppendEntries` RPCs (gRPC)
- Log replication: replicate shard assignment decisions to all nodes before committing
- Log compaction (snapshotting): prevent unbounded log growth
- Cluster membership changes: joint consensus (safe add/remove of nodes)
- The Raft module plugs into the same shard-assignment interface as the etcd / advisory lock implementations
- ADR-0012 documents Postgres advisory locks vs etcd vs Raft: correctness guarantees, failure modes, operational cost

#### 3e. Remote gRPC workers (after multi-node scheduling is proven)
- Generate `worker.proto` (sketch already in `docs/specs/README.md §1.2`)
- `worker` module becomes a separate Gradle module and Docker service
- Scheduler dispatches via gRPC instead of in-process `RunDispatcher`
- Fencing token travels on every gRPC message — protocol unchanged
- Heartbeat becomes a bidirectional gRPC stream

#### 3f. Kafka hot-path dispatch (Phase 3 tail, optional)
- Postgres → Kafka topic for dispatching leased runs to workers
- Worker commits Kafka offset AND updates `job_runs` state in the same logical transaction (outbox pattern applied to Kafka offsets)
- Postgres remains the source of truth; Kafka is the dispatch bus only
- ADR-0013: when Postgres polling saturates and Kafka is worth the added infra

### New ADRs
- ADR-0009: cron parser choice
- ADR-0010: DST misfire handling
- ADR-0011: sharding strategy
- ADR-0012: leader election — Postgres advisory locks vs. etcd vs. Raft from scratch (full comparison)
- ADR-0013: Kafka for hot-path dispatch

---

## Phase 4 — High-throughput Task Queue

### 4a. Priority queues
- `priority SMALLINT DEFAULT 0` on `job_runs` (migration V6)
- `claimDue` orders by `priority DESC, scheduled_for ASC`

### 4b. Per-tenant rate limiting
- `tenant_id TEXT` on `jobs` and `job_runs`
- Token bucket per tenant: `rate_limits(tenant_id, tokens, refill_rate, last_refill)` in Postgres;
  atomic `UPDATE ... RETURNING` to consume a token before leasing
- Redis as an alternative rate-limit store (faster, but an extra dependency) — documented and pluggable

### 4c. Fair-share scheduling across tenants
- Modified `claimDue`: window-based round-robin — take at most `quota` runs per tenant per batch
- Weighted fair share: tenants can have different weights (`rate_limits.weight`)

### 4d. Backpressure
- Worker pool exposes a `currentLoad()` gauge; scheduler throttles batch size when load is high
- Configurable max-concurrent-runs per job type

### 4e. Benchmarks
- JMH microbenchmarks on `claimDue`, `heartbeat`, `markSucceeded`
- End-to-end throughput test: target, measure, and publish `runs/sec` at various concurrency levels

---

## Phase 5 — Workflows / DAGs

### 5a. JSON API for DAG submission
- `POST /workflows` — submit a DAG: `{ nodes: [{id, jobType, payload}], edges: [{from, to}] }`
- Cycle detection at submission (DFS); reject cyclic DAGs with a clear error
- All nodes inserted atomically as `SCHEDULED` or `PENDING_DEPS` (new state)
- Only nodes with no upstream deps start as `SCHEDULED`; others start as `PENDING_DEPS`

### 5b. Dependency resolution
- `job_dependencies(run_id, depends_on_run_id)` table — migration V7
- On `markSucceeded`: query `job_dependencies` — if all upstreams of a downstream are `SUCCEEDED`,
  transition it to `SCHEDULED` (in the same transaction via outbox)
- Fan-out (A → B, C): both B and C evaluated independently
- Fan-in (D requires B AND C): D only moves to `SCHEDULED` when the last of B/C finishes

### 5c. Partial-failure policies
- Per-DAG: `on_failure: FAIL_FAST | WAIT | SKIP_DOWNSTREAM`
- `FAIL_FAST`: cancel all `PENDING_DEPS` and `SCHEDULED` nodes in the DAG on any failure
- `WAIT`: let other branches continue; the failed branch's dependents stay `PENDING_DEPS` forever (manual intervention)
- `SKIP_DOWNSTREAM`: mark failed node's direct dependents `SKIPPED`; rest of DAG continues

### 5d. Cancellation propagation
- `DELETE /workflows/{id}` — topological walk; cancel all non-terminal nodes

### 5e. Java DSL for programmatic DAGs
- `WorkflowBuilder` fluent API: `.job(type, payload).then(type2).fanOut(type3, type4).join().run()`
- Compiles to the same JSON structure submitted to `POST /workflows`

### 5f. DAG introspection endpoint
- `GET /workflows/{id}` — returns nodes + edges + current state of each (for rendering)

---

## Phase 6 — Production Hardening & Polish

### 6a. Chaos / fault-injection tests
- `ChaosRunner` test harness: randomly kill app mid-run, inject DB timeouts, simulate clock skew
- Assert: all runs eventually reach a terminal state, no phantom double-completions
- CI gate: chaos suite must pass before any release

### 6b. Security hardening (from the threat model in `docs/specs/README.md §5`)
- API authentication (API keys first; OIDC option documented)
- TLS for API and DB connections
- Secrets via environment / Vault; no plaintext in config
- Audit log: `audit_events(actor, action, resource_id, at)`
- Least-privilege DB role for the app; separate migration role
- Dependency + container scanning in CI (Trivy, Syft SBOM)

### 6c. Web UI
- Vite + React (or HTMX for minimal JS) served by the `api` module
- Job browser: filter by state/type/tenant, paginated
- Live run state (SSE / WebSocket poll)
- Retry / cancel controls
- DAG visualiser (uses `GET /workflows/{id}` adjacency data → rendered with D3 or Dagre)

### 6d. End-to-end benchmarks
- Throughput: runs/sec on a single node, 3-node cluster, 5-node cluster
- Latency: p50/p95/p99 submit-to-execution time
- Crash recovery time: median time from kill to reaper reclaim
- Published in `docs/benchmarks/`

### 6e. Portfolio write-up
- `docs/design/architecture.md` expanded to cover all phases
- Decision-trail narrative: what we built, why, what we learned
- Published to GitHub Pages or a static site

---

## Decisions locked (from prior discussion)

| Decision | Choice |
|---|---|
| Language / framework | Java 21, Spring Boot thin |
| Storage | Postgres, SKIP LOCKED leasing |
| Delivery semantics | At-least-once + idempotency keys; AT_MOST_ONCE opt-out |
| Fencing | Enforced Phase 1, every write guarded by lease_token |
| Cron parser | Written from scratch |
| Phase 3 order | Multi-node scheduling → remote gRPC workers |
| Leader election | All three implemented: Postgres advisory (Phase 3b), etcd (Phase 3c), Raft from scratch (Phase 3d); ADR compares all |
| DAG format | JSON API + Java DSL (both) |
