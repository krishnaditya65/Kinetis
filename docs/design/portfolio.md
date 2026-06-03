# Kinetis — Project Portfolio Write-up

## What it is

Kinetis is a distributed job scheduler built from scratch in Java 21 + Spring Boot. It implements
the full stack of concerns that production job schedulers must solve: crash safety, exactly-once
semantics, cron, multi-node scale-out, DAG workflows, observability, and hardening.

The project was built phase-by-phase with each phase adding a layer of real distributed-systems
correctness on top of the previous one.

---

## Why I built it

Job schedulers are deceptively hard. Every cloud platform has one, every backend team eventually
builds one, and they all hit the same failure modes: zombie workers, double-execution, missed cron
fires, no visibility into what's running. I wanted to understand all of those failure modes deeply
and build solutions that actually work — not wrappers around a framework that hides the complexity.

---

## What I learned / the hard parts

### 1. Leasing is the hardest problem to get right

`SELECT … FOR UPDATE SKIP LOCKED` sounds simple. The edge cases are not. What happens when
the worker holding the lease crashes? What if it's slow — GC pause, network partition, long
DB query? What if the scheduler node itself dies after claiming a row but before dispatching?

The answer is fencing tokens: every state-mutating write includes `WHERE lease_token = ?`.
A zombie worker carries a stale token, its write matches zero rows, and the DB silently
rejects it. Writing this correctly — and writing tests that *actually* exercise the race —
was the most satisfying part of Phase 1.

### 2. Cron is harder than it looks

A cron expression evaluator sounds like an interview problem. Then you add DST. Spring forward:
the 02:00–03:00 slot doesn't exist — a job scheduled for 02:30 must fire at 03:00, not be
silently dropped. Fall back: 01:00–02:00 occurs twice — which one fires? These edge cases are
only exposed when you build the evaluator field-by-field using `ZonedDateTime` and then write
specific DST tests.

### 3. Raft is a coherent story, but the details are endless

The paper is clear. Leader election is straightforward. Log replication is where the subtlety
lives: what happens when the leader crashes immediately after appending an entry but before
replicating it? The entry is in the leader's log but nowhere else. The new leader must not
commit it. The no-op entry on leader election (§8 of the paper) is the fix. Implementing this
correctly — and proving it with `InMemoryRaftRpc` tests that don't need a network — was
deeply educational.

### 4. DAG workflows require careful atomicity

Submitting a DAG seems simple: insert nodes, insert edges, done. The failure mode: you insert
three nodes, crash, and now you have orphan nodes with no workflow header. The fix is to
do all inserts in one transaction. Then: what if a node finishes while you're still inserting
the rest? The `PENDING_DEPS` state is the answer — no node can start until the full DAG is
committed and the dependency graph is resolved.

---

## Architecture decisions I'm most proud of

| Decision | Why |
|----------|-----|
| SKIP LOCKED over a coordinator | No external dependency, scales horizontally, crash-safe by construction |
| Fencing tokens on every write | Makes the correctness invariant trivially auditable — grep for `WHERE lease_token = ?` |
| `ZoneId` not `String` for timezone | Type system catches bad values at construction, not at cron evaluation time |
| `WorkflowAdvancer` as a separate polling loop | Keeps `WorkerPool` clean — it doesn't know about DAGs |
| Three shard-ownership implementations | Shows the same interface (simple, distributed, consensus-based) with different trade-offs |

---

## Phases summary

| Phase | What was built |
|-------|---------------|
| 1 | Durable primitive: SKIP LOCKED + fencing tokens + idempotency + retry + reaper |
| 2 | Cron: custom parser, DST-correct evaluator, misfire policies |
| 3 | Multi-node: sharding, Postgres advisory locks, etcd, Raft from scratch, remote gRPC workers |
| 4 | Queue: priority, per-tenant rate limiting, fair-share, backpressure |
| 5 | DAG workflows: DagValidator, DependencyResolver, WorkflowBuilder DSL |
| 6 | Hardening: chaos tests, API key auth, audit log, JMH benchmarks, HTMX web UI |

---

## Tech stack

Java 21 (virtual threads, records, sealed interfaces) · Spring Boot 3.3.5 · Postgres 14+
· gRPC 1.65 · etcd Java client · Micrometer → Prometheus → Grafana · Testcontainers
· HTMX + Thymeleaf · JMH

---

## What I'd do differently

- **Raft persistence** — `currentTerm` and `votedFor` should survive restarts. I deferred this
  to Phase 6 but never implemented it; real production use requires it.
- **Schema evolution** — the migration sequence grew organically. V6 rebuilds `idx_due` that V5
  just created. A cleaner approach is to design the final schema first and write a single migration.
- **Kafka integration** — I documented the decision point (ADR-0013) but didn't implement it.
  At high throughput, Postgres polling will saturate before Kafka would. For a next iteration,
  replacing `WorkflowAdvancer` polling with Kafka events would eliminate one polling loop.
