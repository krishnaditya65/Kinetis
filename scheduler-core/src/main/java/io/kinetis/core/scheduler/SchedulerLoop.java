package io.kinetis.core.scheduler;

import io.kinetis.core.lease.LeaseManager;
import io.kinetis.core.metrics.SchedulerMetrics;
import io.kinetis.core.model.JobRun;
import io.kinetis.core.queue.FairShareDispatcher;
import io.kinetis.core.shard.ShardOwnershipProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Array;
import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.Set;

/**
 * One scheduler node's poll loop: claim due runs (disjoint from peers via SKIP LOCKED) and
 * hand each to {@link FairShareDispatcher#dispatchBatch}, which applies rate limiting and
 * per-tenant caps before forwarding to the real executor.
 *
 * <p><b>Backpressure:</b> before claiming, the loop counts how many runs are currently
 * LEASED or RUNNING in its owned shards. If that count reaches {@code maxConcurrentRuns}
 * the batch is reduced to zero — preventing the scheduler from flooding the worker pool
 * and causing a burst of lease expirations.
 */
public class SchedulerLoop {

    private static final Logger log = LoggerFactory.getLogger(SchedulerLoop.class);

    private final LeaseManager leases;
    private final FairShareDispatcher dispatcher;
    private final SchedulerMetrics metrics;
    private final ShardOwnershipProvider shardOwnership;
    private final JdbcTemplate jdbc;
    private final String nodeId;
    private final int batchSize;
    private final int maxConcurrentRuns;
    private final Duration leaseTtl;

    public SchedulerLoop(LeaseManager leases, FairShareDispatcher dispatcher,
                         SchedulerMetrics metrics, ShardOwnershipProvider shardOwnership,
                         JdbcTemplate jdbc, String nodeId, int batchSize,
                         int maxConcurrentRuns, Duration leaseTtl) {
        this.leases            = leases;
        this.dispatcher        = dispatcher;
        this.metrics           = metrics;
        this.shardOwnership    = shardOwnership;
        this.jdbc              = jdbc;
        this.nodeId            = nodeId;
        this.batchSize         = batchSize;
        this.maxConcurrentRuns = maxConcurrentRuns;
        this.leaseTtl          = leaseTtl;
    }

    /** Claim and dispatch one batch of due runs from this node's owned shards. */
    public int tick() {
        Set<Integer> ownedShards = shardOwnership.ownedShards();
        int effectiveBatch = backpressureBatch(ownedShards);
        if (effectiveBatch <= 0) {
            log.debug("backpressure: skipping tick (maxConcurrentRuns={} reached)", maxConcurrentRuns);
            return 0;
        }

        List<JobRun> claimed;
        try {
            claimed = leases.claimDue(nodeId, effectiveBatch, leaseTtl, ownedShards);
        } catch (RuntimeException e) {
            log.warn("claimDue failed", e);
            return 0;
        }
        if (claimed.isEmpty()) return 0;

        metrics.onLeased(claimed.size());
        try {
            dispatcher.dispatchBatch(claimed);
        } catch (RuntimeException e) {
            log.warn("dispatchBatch failed", e);
        }
        return claimed.size();
    }

    /**
     * Compute the effective batch size after backpressure.
     * Returns 0 if in-flight runs have reached {@code maxConcurrentRuns}.
     */
    private int backpressureBatch(Set<Integer> ownedShards) {
        if (maxConcurrentRuns <= 0) return batchSize; // unlimited
        if (ownedShards.isEmpty()) return 0;

        Integer[] shardArray = ownedShards.toArray(Integer[]::new);
        Long inFlight;
        try {
            inFlight = jdbc.query(
                    (Connection conn) -> {
                        Array pgShards = conn.createArrayOf("int2", shardArray);
                        var ps = conn.prepareStatement("""
                                SELECT count(*) FROM job_runs
                                WHERE state IN ('LEASED', 'RUNNING')
                                  AND shard_id = ANY(?)
                                """);
                        ps.setArray(1, pgShards);
                        return ps;
                    },
                    rs -> rs.next() ? rs.getLong(1) : 0L);
        } catch (RuntimeException e) {
            log.warn("backpressure check failed — proceeding with full batch", e);
            return batchSize;
        }

        long current = inFlight == null ? 0 : inFlight;
        if (current >= maxConcurrentRuns) return 0;
        return (int) Math.min(batchSize, maxConcurrentRuns - current);
    }
}
