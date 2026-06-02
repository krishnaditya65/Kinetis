package io.kinetis.core.shard;

import java.util.Set;

/**
 * Tells the scheduler which shards this node owns and may claim work from.
 *
 * <p>Three implementations, progressively more sophisticated:
 * <ol>
 *   <li>{@link StaticShardOwnership} — configured at startup, never changes. Right for
 *       single-node or manually partitioned multi-node setups.</li>
 *   <li>{@link PostgresAdvisoryShardOwnership} — dynamically acquires shards via
 *       {@code pg_try_advisory_lock}. Coordinator-free, zero new infra.</li>
 *   <li>{@link EtcdShardOwnership} — etcd TTL leases. Proper distributed KV;
 *       requires etcd in the stack.</li>
 * </ol>
 *
 * All implementations must be thread-safe; {@link #ownedShards()} is called on the hot path
 * on every scheduler tick.
 */
public interface ShardOwnershipProvider {

    /**
     * Shard IDs this node currently owns. Must be an immutable/unmodifiable set —
     * callers convert it to a SQL array parameter without synchronization.
     */
    Set<Integer> ownedShards();

    /** Total shards in the cluster. All shard IDs are in {@code [0, totalShards)}. */
    int totalShards();
}
