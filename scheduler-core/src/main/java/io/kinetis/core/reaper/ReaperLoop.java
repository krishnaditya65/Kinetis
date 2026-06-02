package io.kinetis.core.reaper;

import io.kinetis.core.lease.LeaseManager;
import io.kinetis.core.metrics.SchedulerMetrics;
import io.kinetis.core.model.Job;
import io.kinetis.core.model.JobRun;
import io.kinetis.core.retry.RetryHandler;
import io.kinetis.core.shard.ShardOwnershipProvider;
import io.kinetis.core.store.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * Crash recovery. Finds runs whose lease has expired — meaning their worker is presumed dead —
 * and routes each back through {@link RetryHandler}, so a dead worker's job is retried (or
 * dead-lettered if attempts are exhausted) exactly as an explicit failure would be.
 *
 * <p>Reclaiming bumps the fencing token, so if the "dead" worker is merely slow it will be
 * fenced off when it finally tries to write — no double completion.
 */
public class ReaperLoop {

    private static final Logger log = LoggerFactory.getLogger(ReaperLoop.class);

    private final LeaseManager leases;
    private final JobStore jobStore;
    private final RetryHandler retryHandler;
    private final SchedulerMetrics metrics;
    private final ShardOwnershipProvider shardOwnership;
    private final int batchSize;

    public ReaperLoop(LeaseManager leases, JobStore jobStore, RetryHandler retryHandler,
                      SchedulerMetrics metrics, ShardOwnershipProvider shardOwnership,
                      int batchSize) {
        this.leases        = leases;
        this.jobStore      = jobStore;
        this.retryHandler  = retryHandler;
        this.metrics       = metrics;
        this.shardOwnership = shardOwnership;
        this.batchSize     = batchSize;
    }

    /** Reclaim expired leases in this node's owned shards. Returns how many were processed. */
    public int tick() {
        List<JobRun> expired;
        try {
            expired = leases.findExpiredLeases(batchSize, shardOwnership.ownedShards());
        } catch (RuntimeException e) {
            log.warn("findExpiredLeases failed", e);
            return 0;
        }

        int reclaimed = 0;
        for (JobRun run : expired) {
            Optional<Job> job = jobStore.findById(run.jobId());
            if (job.isEmpty()) continue;

            boolean retried = retryHandler.onFailure(
                    run, job.get(), run.leaseToken(),
                    "lease expired at " + run.leaseExpiresAt() + " (worker presumed dead)");
            // retried==false means dead-lettered OR fenced by a peer — either way it's handled.
            reclaimed++;
            log.debug("reaped run {} (retry={})", run.id(), retried);
        }

        if (reclaimed > 0) metrics.onReaped(reclaimed);
        return reclaimed;
    }
}
