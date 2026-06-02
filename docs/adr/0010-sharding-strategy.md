# ADR-0010: Sharding strategy — hash-partition job_runs by job_id MSB

- **Status:** Accepted
- **Date:** 2026-06-03

## Context

Multiple scheduler nodes must partition the `job_runs` table so each node claims only its own
subset of due runs. Without partitioning, every node contends for the same rows under SKIP LOCKED,
which works but wastes CPU on lock ping-pong at high node counts.

## Decision

Add `shard_id SMALLINT` to both `jobs` and `job_runs`. Compute as:

```java
shard_id = Math.floorMod(uuid.getMostSignificantBits(), totalShards)
```

The Postgres migration uses the equivalent expression on the UUID bytes. `totalShards = 16` is the
default (configurable). Claim queries filter `AND shard_id = ANY(?)` with the node's owned shards.
The `CronScheduler` scopes `jobs` the same way so only one node fires each cron occurrence.

## Options considered

| Option | Pro | Con |
|---|---|---|
| **MSB hash mod N** (chosen) | Zero contention between nodes; uniform distribution for UUID v4 | All nodes must agree on N; changing N requires re-sharding |
| Postgres table partitioning | Native; query planner can prune | DDL complexity; changing partition count is a migration |
| Range-based (submitted_at) | Time-locality; easy migration | Hot spotting on the most recent shard; skews if submission rate varies |

## Consequences

- (+) Each node reads and writes disjoint rows — zero inter-node lock contention in the hot path.
- (+) SKIP LOCKED still protects within a shard (two threads on the same node can't double-claim).
- (−) `totalShards` must be the same on every node and is non-trivial to change online.
- (−) A node with an empty `ownedShards` set claims nothing — the startup/configuration protocol
  must ensure every shard is owned by exactly one node (handled by ownership implementations).
