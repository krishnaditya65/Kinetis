-- V5: sharding infrastructure.
--
-- shard_id is computed from the job UUID's most-significant bits:
--   shard_id = floor_mod(msb_int64, total_shards)
-- where msb_int64 = first 8 bytes of the UUID interpreted as a signed int64.
-- This matches the Java formula in ShardingUtils.computeShardId().
--
-- Default total_shards = 16. A single-node deployment owns all 16 shards.
-- Multi-node: each node claims a disjoint subset via advisory locks, etcd, or Raft.

ALTER TABLE jobs     ADD COLUMN shard_id SMALLINT NOT NULL DEFAULT 0;
ALTER TABLE job_runs ADD COLUMN shard_id SMALLINT NOT NULL DEFAULT 0;

-- Backfill shard_id for existing rows using the same formula as the Java implementation.
DO $$
DECLARE total_shards SMALLINT := 16;
BEGIN
    UPDATE jobs SET shard_id = (
        ((('x' || substr(replace(id::text, '-', ''), 1, 16))::bit(64)::bigint % total_shards)
        + total_shards) % total_shards
    );
    UPDATE job_runs SET shard_id = (
        ((('x' || substr(replace(job_id::text, '-', ''), 1, 16))::bit(64)::bigint % total_shards)
        + total_shards) % total_shards
    );
END$$;

-- Rebuild hot-path indexes with shard_id as the leading column so shard-scoped
-- queries hit the index directly without a filter step.
DROP INDEX IF EXISTS idx_due;
CREATE INDEX idx_due ON job_runs (shard_id, scheduled_for)
    WHERE state = 'SCHEDULED';

DROP INDEX IF EXISTS idx_jobs_cron_due;
CREATE INDEX idx_jobs_cron_due ON jobs (shard_id, next_fire_time)
    WHERE next_fire_time IS NOT NULL AND cron_expr IS NOT NULL;

-- Cluster membership: each node heartbeats here.
-- Used by PostgresAdvisoryShardOwnership to estimate active node count
-- and by all implementations to advertise which shards they currently own.
CREATE TABLE cluster_nodes (
    node_id         TEXT        PRIMARY KEY,
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT now(),
    owned_shard_ids SMALLINT[]  NOT NULL DEFAULT '{}'
);

CREATE INDEX idx_cluster_nodes_heartbeat ON cluster_nodes (last_heartbeat);
