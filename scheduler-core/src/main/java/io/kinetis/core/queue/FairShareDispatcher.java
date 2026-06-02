package io.kinetis.core.queue;

import io.kinetis.core.lease.LeaseManager;
import io.kinetis.core.model.JobRun;
import io.kinetis.core.scheduler.RunDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link RunDispatcher} decorator that enforces fair-share execution across tenants.
 *
 * <h2>Per-tick algorithm</h2>
 * <ol>
 *   <li><b>Rate-limit check</b> — for each claimed run, try to consume a token for its tenant.
 *       If the bucket is empty, call {@link LeaseManager#returnToScheduled} — the run goes back
 *       to SCHEDULED and will be claimed again once the bucket refills.</li>
 *   <li><b>Fair-share cap</b> — among runs that passed the rate check, cap each tenant to
 *       {@code floor(batchSize / activeTenants)} dispatches. Excess runs are returned to SCHEDULED.
 *       This prevents one busy tenant from consuming the entire batch.</li>
 * </ol>
 *
 * <p>Rate-limiting happens at dispatch time, not claim time, to keep the hot-path SQL
 * (SKIP LOCKED claim) simple. The fencing token guarantees no double-execution on returned runs.
 */
public class FairShareDispatcher implements RunDispatcher {

    private static final Logger log = LoggerFactory.getLogger(FairShareDispatcher.class);

    private final RunDispatcher delegate;
    private final RateLimiter rateLimiter;
    private final LeaseManager leaseManager;
    private final int batchSize;

    public FairShareDispatcher(RunDispatcher delegate, RateLimiter rateLimiter,
                                LeaseManager leaseManager, int batchSize) {
        this.delegate     = delegate;
        this.rateLimiter  = rateLimiter;
        this.leaseManager = leaseManager;
        this.batchSize    = batchSize;
    }

    /** Apply rate-limit and fair-share caps to a full claimed batch, then dispatch what passes. */
    public void dispatchBatch(List<JobRun> claimedRuns) {
        if (claimedRuns.isEmpty()) return;

        // Step 1: rate-limit check
        List<JobRun> ratePassed = new ArrayList<>(claimedRuns.size());
        for (JobRun run : claimedRuns) {
            if (rateLimiter.tryConsume(run.tenantId())) {
                ratePassed.add(run);
            } else {
                boolean returned = leaseManager.returnToScheduled(run.id(), run.leaseToken());
                log.debug("rate-limited run {} (tenant={}) returned={}", run.id(), run.tenantId(), returned);
            }
        }

        // Step 2: fair-share cap per tenant
        int numTenants = (int) ratePassed.stream().map(this::tenantKey).distinct().count();
        int quotaPerTenant = Math.max(1, batchSize / Math.max(1, numTenants));

        Map<String, Integer> dispatched = new HashMap<>();
        for (JobRun run : ratePassed) {
            String key = tenantKey(run);
            int count = dispatched.getOrDefault(key, 0);
            if (count >= quotaPerTenant) {
                boolean returned = leaseManager.returnToScheduled(run.id(), run.leaseToken());
                log.debug("fair-share: run {} over quota ({}/{}), returned={}", run.id(), count, quotaPerTenant, returned);
            } else {
                dispatched.put(key, count + 1);
                delegate.dispatch(run);
            }
        }
    }

    @Override
    public void dispatch(JobRun run) {
        if (rateLimiter.tryConsume(run.tenantId())) {
            delegate.dispatch(run);
        } else {
            leaseManager.returnToScheduled(run.id(), run.leaseToken());
        }
    }

    private String tenantKey(JobRun run) {
        return run.tenantId() == null ? "" : run.tenantId();
    }
}
