# ADR-0002: Postgres as source of truth; SKIP LOCKED leasing

- **Status:** Accepted
- **Date:** 2026-06-02

## Context

The scheduler must durably store jobs/runs and let one or more scheduler nodes claim due work
**without double-dispatching** the same run. Early phases should avoid heavy external coordination
(ZooKeeper/etcd/Raft) until genuinely needed.

## Decision

Use **PostgreSQL** as the single source of truth, and claim due runs with a single atomic
`UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED LIMIT n) RETURNING *`.

## Rationale

`FOR UPDATE SKIP LOCKED` lets N concurrent schedulers poll the *same* table and each receive a
**disjoint** batch — rows another transaction has locked are skipped rather than blocked. This
delivers safe, coordinator-free horizontal scaling for the polling hot path. Proven at scale by
db-scheduler, GoodJob, Sidekiq-style systems.

## Options considered

- **Postgres + SKIP LOCKED** — chosen. No new infra; transactional; great local dev story.
- **Redis-based queue** — fast, but weaker durability/transactional guarantees and a second store.
- **Kafka** — excellent for high-throughput dispatch, but overkill for Phase 1 and awkward for
  arbitrary delayed scheduling and per-job state transitions. Deferred to Phase 3 for the hot path.

## Consequences

- (+) One store, full transactions, easy reasoning, trivial local setup.
- (+) Partial indexes on `state` keep the lease query fast as terminal rows accumulate.
- (−) DB polling has a throughput ceiling; when reached, Phase 3 introduces Kafka for dispatch while
  keeping Postgres as the durable record.
- (−) Requires Postgres-specific SQL (`SKIP LOCKED`, `make_interval`, `jsonb`) — acceptable lock-in.
