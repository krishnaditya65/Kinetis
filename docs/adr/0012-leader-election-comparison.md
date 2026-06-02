# ADR-0012: Leader election mechanism comparison

- **Status:** Accepted (all three implemented; see deployment guidance)
- **Date:** 2026-06-03

## Context

Shard ownership requires a distributed coordination mechanism: exactly one node should own each
shard at any time, and ownership must transfer when a node dies. Three mechanisms are implemented
as pluggable `ShardOwnershipProvider` implementations.

## Comparison

| Dimension | Postgres advisory locks | etcd TTL leases | Raft from scratch |
|---|---|---|---|
| **Correctness guarantee** | Session lock released on crash (OS-guaranteed) | TTL expiry + linearisable CAS; no split-brain | Full consensus; proven safe under partitions |
| **Failure detection** | Immediate (connection close → lock release) | TTL bound (typically 3–10s) | Election timeout (configurable 150–300ms) |
| **New infra needed** | None — reuses Postgres | etcd cluster (3+ nodes) | Raft peers (same process or separate) |
| **Cross-datacenter** | No (single Postgres) | Yes (etcd is geo-replicated) | Yes (Raft peers can span DCs) |
| **Rebalancing** | Manual + background task | TTL expiry → re-claim on next tick | Leader proposes assignment; committed by quorum |
| **Operational complexity** | Low — one fewer moving part | Medium — etcd ops, lease renewal | High — own the implementation |
| **Learning value** | Low | Medium | Highest |

## When to use which

- **Single-node / small cluster (≤5 nodes), same DC:** Postgres advisory locks. Zero new infra,
  crash-release is automatic, simple mental model. The right default.
- **Multi-region / production at scale:** etcd. Proven at production scale by Kubernetes;
  linearisable; watch API gives instant ownership-change notification.
- **Deep understanding / research / custom requirements:** Raft from scratch. Own the protocol;
  no external dependency; full control over election timings and state machine semantics.

## Implementation status

All three are implemented:
- `StaticShardOwnership` — configured ranges, no coordination (single-node default)
- `PostgresAdvisoryShardOwnership` — dynamic, coordinator-free, production-viable for one-DC
- `EtcdShardOwnership` — etcd TTL leases; requires etcd in the stack
- `RaftShardOwnership` — Raft from scratch via `RaftNode`; `InMemoryRaftRpc` for tests

The active implementation is selected via Spring bean configuration (default: `StaticShardOwnership`
unless `scheduler.shard-ownership` property overrides it — planned for Phase 6 profile support).

## Consequences

- All implementations share the `ShardOwnershipProvider` interface — switching is a config change.
- The Raft implementation lacks persistence (currentTerm / votedFor are in-memory only) and
  joint-consensus membership changes. See ADR-0011 for the known gaps.
- `pg_try_advisory_lock` is a session lock and requires a dedicated (non-pooled) connection —
  documented in `PostgresAdvisoryShardOwnership`.
