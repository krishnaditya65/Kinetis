package io.kinetis.core.retry;

import io.kinetis.core.lease.LeaseManager;
import io.kinetis.core.model.DeliveryPolicy;
import io.kinetis.core.model.Job;
import io.kinetis.core.model.JobRun;

import java.time.Clock;
import java.time.Instant;

/**
 * Single decision point for "a run failed — retry or give up?", shared by the worker (caught a
 * handler exception) and the reaper (lease expired, worker presumed dead). Centralising it means
 * both failure paths apply identical backoff and dead-letter rules.
 */
public class RetryHandler {

    private final LeaseManager leases;
    private final BackoffCalculator backoff;
    private final Clock clock;

    public RetryHandler(LeaseManager leases, BackoffCalculator backoff, Clock clock) {
        this.leases = leases;
        this.backoff = backoff;
        this.clock = clock;
    }

    /**
     * Apply the failure outcome for {@code run} using fencing token {@code token}.
     *
     * @return {@code true} if a retry was scheduled, {@code false} if dead-lettered
     *         (or the write was fenced — caller treats both as "no longer mine")
     */
    public boolean onFailure(JobRun run, Job job, long token, String error) {
        int attemptsMade = run.attempt() + 1;
        boolean exhausted = attemptsMade >= job.retryPolicy().maxAttempts();
        boolean atMostOnce = job.deliveryPolicy() == DeliveryPolicy.AT_MOST_ONCE;

        if (atMostOnce || exhausted) {
            leases.markDeadLetter(run.id(), token, error);
            return false;
        }
        Instant nextRunAt = clock.instant().plus(backoff.nextDelay(job.retryPolicy(), run.attempt()));
        return leases.rescheduleForRetry(run.id(), token, attemptsMade, nextRunAt, error);
    }
}
