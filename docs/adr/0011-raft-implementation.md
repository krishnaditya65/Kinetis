# ADR-0011: Raft consensus implemented from scratch

- **Status:** Accepted
- **Date:** 2026-06-03

## Context

The scheduler needs distributed leader election for shard ownership (Phase 3d). The goal is to
deeply understand consensus, not just consume a library. Three mechanisms are being implemented and
compared; this ADR covers the from-scratch Raft module.

## Decision

Implement the Raft consensus algorithm in a standalone `raft/` module (zero Spring/framework deps,
pure Java) covering:
- Leader election with randomised election timeouts (§5.2)
- Log replication with AppendEntries (§5.3)
- Safety: election restriction on log up-to-dateness (§5.4)
- Log compaction via snapshots (§7)
- The transport is abstracted behind `RaftRpc` (in-memory for tests; gRPC for production Phase 3e)

The `RaftShardStateMachine` applies committed shard-assignment commands, giving `RaftShardOwnership`
the same `ShardOwnershipProvider` interface as the Postgres and etcd implementations.

## Why from scratch

The primary goal is a deep, first-principles understanding of consensus. Using etcd or
Zookeeper as a black box skips the key learning. Writing Raft from scratch forces a precise
understanding of: why randomised timeouts prevent split votes; why the election restriction
(§5.4) is necessary for safety; why log matching (§5.3) gives linearisability; and why
snapshotting is needed to prevent unbounded log growth.

## Consequences

- (+) Complete ownership of the algorithm; every "why?" is answerable from the code.
- (+) The `RaftNode` is tested in isolation via `InMemoryRaftRpc` without any network.
- (+) `RaftShardOwnership` plugs into the same `ShardOwnershipProvider` interface — swapping
  between advisory locks / etcd / Raft is a bean configuration change, not an API change.
- (−) A production-grade Raft is weeks of careful work. Known gaps in this implementation:
  cluster membership changes (joint consensus §6), persistent state (currentTerm and votedFor
  must survive crashes), and the optimization from §5.4.2 (pre-vote). These are documented
  and tagged for Phase 6 hardening.
