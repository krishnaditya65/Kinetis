package io.kinetis.core.shard;

import java.util.UUID;

/**
 * Shard-ID computation. Formula must be identical in Java and in SQL migration V5 so that
 * Java inserts produce the same shard_id the SQL backfill computed for existing rows.
 *
 * <p>Formula: {@code Math.floorMod(uuid.getMostSignificantBits(), totalShards)}
 * {@link Math#floorMod} always returns non-negative even for negative MSBs, matching
 * Postgres's {@code ((msb % n) + n) % n}.
 */
public final class ShardingUtils {

    private ShardingUtils() {}

    /**
     * @param jobId       the job's UUID
     * @param totalShards total number of shards (must be positive)
     * @return shard ID in [0, totalShards)
     */
    public static int computeShardId(UUID jobId, int totalShards) {
        return (int) Math.floorMod(jobId.getMostSignificantBits(), totalShards);
    }
}
