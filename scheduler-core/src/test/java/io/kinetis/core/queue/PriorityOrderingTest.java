package io.kinetis.core.queue;

import io.kinetis.core.AbstractPgTest;
import io.kinetis.core.lease.LeaseManager;
import io.kinetis.core.model.*;
import io.kinetis.core.store.JobRunStore;
import io.kinetis.core.store.JobStore;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class PriorityOrderingTest extends AbstractPgTest {

    private static final Set<Integer> ALL = Set.of(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15);

    @Test
    void higherPriorityRunsClaimedFirst() {
        LeaseManager leases  = new LeaseManager(jdbc);
        JobStore jobStore    = new JobStore(jdbc);
        JobRunStore runStore = new JobRunStore(jdbc);
        UUID jobId = seedJob(jobStore);

        IntStream.rangeClosed(0, 4).map(i -> 4 - i)
                .forEach(p -> seedRun(runStore, jobId, p, Instant.now().minusSeconds(1)));

        List<JobRun> claimed = leases.claimDue("node", 3, Duration.ofSeconds(30), ALL);
        assertThat(claimed).hasSize(3);
        assertThat(claimed.stream().map(JobRun::priority).toList())
                .containsExactlyInAnyOrder(4, 3, 2);
    }

    @Test
    void equalPriorityRunsOrderedByScheduledFor() {
        LeaseManager leases  = new LeaseManager(jdbc);
        JobStore jobStore    = new JobStore(jdbc);
        JobRunStore runStore = new JobRunStore(jdbc);
        UUID jobId = seedJob(jobStore);
        Instant base = Instant.now().minusSeconds(100);

        seedRun(runStore, jobId, 0, base.plusSeconds(10));
        seedRun(runStore, jobId, 0, base.plusSeconds(5));
        seedRun(runStore, jobId, 0, base.plusSeconds(20));

        List<JobRun> claimed = leases.claimDue("node", 1, Duration.ofSeconds(30), ALL);
        assertThat(claimed).hasSize(1);
        assertThat(claimed.get(0).scheduledFor()).isEqualTo(base.plusSeconds(5));
    }

    @Test
    void negativePriorityRunsClaimedLast() {
        LeaseManager leases  = new LeaseManager(jdbc);
        JobStore jobStore    = new JobStore(jdbc);
        JobRunStore runStore = new JobRunStore(jdbc);
        UUID jobId = seedJob(jobStore);
        Instant past = Instant.now().minusSeconds(1);

        seedRun(runStore, jobId, -1, past);
        seedRun(runStore, jobId,  0, past);
        seedRun(runStore, jobId,  5, past);

        List<JobRun> claimed = leases.claimDue("node", 2, Duration.ofSeconds(30), ALL);
        assertThat(claimed).hasSize(2);
        assertThat(claimed.stream().map(JobRun::priority).toList())
                .containsExactlyInAnyOrder(5, 0)
                .doesNotContain(-1);
    }

    private UUID seedJob(JobStore store) {
        UUID id = UUID.randomUUID();
        return store.insertIfAbsent(new Job(id, "noop", "{}", "k-" + UUID.randomUUID(),
                DeliveryPolicy.AT_LEAST_ONCE, null, ZoneId.of("UTC"), MisfirePolicy.FIRE_ONCE,
                RetryPolicy.defaults(), Instant.now(), 0, 0, null));
    }

    private void seedRun(JobRunStore store, UUID jobId, int priority, Instant scheduledFor) {
        store.insert(new JobRun(UUID.randomUUID(), jobId, JobState.SCHEDULED, 0,
                scheduledFor, null, null, 0L, "rk-" + UUID.randomUUID(),
                null, null, Instant.now(), null, null, 0, priority, null));
    }
}
