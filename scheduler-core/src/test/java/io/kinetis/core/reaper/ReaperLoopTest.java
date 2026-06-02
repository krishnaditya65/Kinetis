package io.kinetis.core.reaper;

import io.kinetis.core.AbstractPgTest;
import io.kinetis.core.lease.LeaseManager;
import io.kinetis.core.metrics.SchedulerMetrics;
import io.kinetis.core.model.*;
import io.kinetis.core.retry.BackoffCalculator;
import io.kinetis.core.retry.RetryHandler;
import io.kinetis.core.shard.StaticShardOwnership;
import io.kinetis.core.store.JobRunStore;
import io.kinetis.core.store.JobStore;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ReaperLoopTest extends AbstractPgTest {

    private static final StaticShardOwnership ALL = new StaticShardOwnership(16, "all");

    @Test
    void reaperReclaimsExpiredLeaseAndSchedulesRetry() {
        LeaseManager leases  = new LeaseManager(jdbc);
        JobStore jobStore    = new JobStore(jdbc);
        JobRunStore runStore = new JobRunStore(jdbc);
        ReaperLoop reaper    = new ReaperLoop(leases, jobStore,
                new RetryHandler(leases, new BackoffCalculator(new Random(1)), Clock.systemUTC()),
                new SchedulerMetrics(new SimpleMeterRegistry()), ALL, 100);

        UUID jobId = seedJob(jobStore, 3);
        seedRun(runStore, jobId);

        JobRun leased = leases.claimDue("worker-A", 1, Duration.ofSeconds(30), ALL.ownedShards()).get(0);
        leases.markRunning(leased.id(), leased.leaseToken());
        jdbc.update("UPDATE job_runs SET lease_expires_at = now() - interval '1 minute' WHERE id = ?", leased.id());

        assertThat(reaper.tick()).isEqualTo(1);
        JobRun after = runStore.findById(leased.id()).orElseThrow();
        assertThat(after.state()).isEqualTo(JobState.SCHEDULED);
        assertThat(after.attempt()).isEqualTo(1);
        assertThat(after.leaseToken()).isGreaterThan(leased.leaseToken());
    }

    @Test
    void reaperDeadLettersWhenAttemptsExhausted() {
        LeaseManager leases  = new LeaseManager(jdbc);
        JobStore jobStore    = new JobStore(jdbc);
        JobRunStore runStore = new JobRunStore(jdbc);
        ReaperLoop reaper    = new ReaperLoop(leases, jobStore,
                new RetryHandler(leases, new BackoffCalculator(new Random(1)), Clock.systemUTC()),
                new SchedulerMetrics(new SimpleMeterRegistry()), ALL, 100);

        UUID jobId = seedJob(jobStore, 1);
        seedRun(runStore, jobId);

        JobRun leased = leases.claimDue("worker-A", 1, Duration.ofSeconds(30), ALL.ownedShards()).get(0);
        leases.markRunning(leased.id(), leased.leaseToken());
        jdbc.update("UPDATE job_runs SET lease_expires_at = now() - interval '1 minute' WHERE id = ?", leased.id());

        reaper.tick();
        assertThat(runStore.findById(leased.id()).orElseThrow().state()).isEqualTo(JobState.DEAD_LETTER);
    }

    private UUID seedJob(JobStore store, int maxAttempts) {
        UUID id = UUID.randomUUID();
        return store.insertIfAbsent(new Job(id, "noop", "{}", "k-" + UUID.randomUUID(),
                DeliveryPolicy.AT_LEAST_ONCE, null, ZoneId.of("UTC"), MisfirePolicy.FIRE_ONCE,
                new RetryPolicy(maxAttempts, 1L, 2.0), Instant.now(), 0, 0, null));
    }

    private void seedRun(JobRunStore store, UUID jobId) {
        store.insert(new JobRun(UUID.randomUUID(), jobId, JobState.SCHEDULED, 0,
                Instant.now().minusSeconds(1), null, null, 0L, "rk-" + UUID.randomUUID(),
                null, null, Instant.now(), null, null, 0, 0, null));
    }
}
