package io.kinetis.core.lease;

import io.kinetis.core.AbstractPgTest;
import io.kinetis.core.model.DeliveryPolicy;
import io.kinetis.core.model.Job;
import io.kinetis.core.model.JobRun;
import io.kinetis.core.model.JobState;
import io.kinetis.core.model.MisfirePolicy;
import io.kinetis.core.model.RetryPolicy;
import io.kinetis.core.store.JobRunStore;
import io.kinetis.core.store.JobStore;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class LeaseManagerTest extends AbstractPgTest {

    private LeaseManager leases;
    private JobStore jobStore;
    private JobRunStore runStore;

    private void init() {
        leases   = new LeaseManager(jdbc);
        jobStore = new JobStore(jdbc);
        runStore = new JobRunStore(jdbc);
    }

    @Test
    void concurrentSchedulersClaimDisjointSets() throws Exception {
        init();
        UUID jobId = seedJob();
        int total = 60;
        for (int i = 0; i < total; i++) seedScheduledRun(jobId);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        Set<Integer> all = Set.of(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15);
        Callable<List<JobRun>> claim = () ->
                leases.claimDue("node-" + Thread.currentThread().getId(), total, Duration.ofSeconds(30), all);
        Future<List<JobRun>> a = pool.submit(claim);
        Future<List<JobRun>> b = pool.submit(claim);
        List<JobRun> claimedA = a.get();
        List<JobRun> claimedB = b.get();
        pool.shutdown();

        Set<UUID> idsA = claimedA.stream().map(JobRun::id).collect(Collectors.toSet());
        Set<UUID> idsB = claimedB.stream().map(JobRun::id).collect(Collectors.toSet());
        idsA.retainAll(idsB);
        assertThat(idsA).as("SKIP LOCKED: no run claimed by both nodes").isEmpty();
        assertThat(claimedA.size() + claimedB.size()).isEqualTo(total);
        assertThat(claimedA).allSatisfy(r -> assertThat(r.state()).isEqualTo(JobState.LEASED));
    }

    @Test
    void claimDueBumpsFencingTokenAndSkipsFutureRuns() {
        init();
        UUID jobId = seedJob();
        seedScheduledRun(jobId);
        seedRunAt(jobId, Instant.now().plusSeconds(60));

        Set<Integer> all = Set.of(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15);
        List<JobRun> claimed = leases.claimDue("node-1", 10, Duration.ofSeconds(30), all);

        assertThat(claimed).hasSize(1);
        assertThat(claimed.get(0).leaseToken()).isEqualTo(1L);
        assertThat(claimed.get(0).leaseOwner()).isEqualTo("node-1");
    }

    @Test
    void staleTokenIsFencedOff() {
        init();
        UUID jobId = seedJob();
        seedScheduledRun(jobId);

        Set<Integer> all = Set.of(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15);
        JobRun leased = leases.claimDue("worker-A", 1, Duration.ofSeconds(30), all).get(0);
        long staleToken = leased.leaseToken();
        assertThat(leases.markRunning(leased.id(), staleToken)).isTrue();

        leases.rescheduleForRetry(leased.id(), staleToken, 1, Instant.now(), "lease expired");
        JobRun reclaimed = leases.claimDue("worker-B", 1, Duration.ofSeconds(30), all).get(0);
        assertThat(reclaimed.leaseToken()).isGreaterThan(staleToken);

        boolean zombieWrite = leases.markSucceeded(leased.id(), staleToken);
        assertThat(zombieWrite).as("zombie write must be fenced").isFalse();
        assertThat(runStore.findById(leased.id()).orElseThrow().state())
                .isNotEqualTo(JobState.SUCCEEDED);
    }

    private UUID seedJob() {
        UUID id = UUID.randomUUID();
        return jobStore.insertIfAbsent(new Job(id, "noop", "{}", "k-" + UUID.randomUUID(),
                DeliveryPolicy.AT_LEAST_ONCE, null, ZoneId.of("UTC"), MisfirePolicy.FIRE_ONCE,
                RetryPolicy.defaults(), Instant.now(), 0, 0, null));
    }

    private void seedScheduledRun(UUID jobId) { seedRunAt(jobId, Instant.now().minusSeconds(1)); }

    private void seedRunAt(UUID jobId, Instant scheduledFor) {
        runStore.insert(new JobRun(UUID.randomUUID(), jobId, JobState.SCHEDULED, 0, scheduledFor,
                null, null, 0L, "rk-" + UUID.randomUUID(), null, null, Instant.now(), null, null, 0, 0, null));
    }
}
