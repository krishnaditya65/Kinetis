package io.kinetis.core.shard;

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
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class ShardingTest extends AbstractPgTest {

    private static final int TOTAL = 16;

    @Test
    void computedShardIdIsWithinRange() {
        for (int i = 0; i < 1000; i++)
            assertThat(ShardingUtils.computeShardId(UUID.randomUUID(), TOTAL)).isBetween(0, TOTAL - 1);
    }

    @Test
    void staticOwnershipParsesAllShards() {
        StaticShardOwnership all = new StaticShardOwnership(TOTAL, "all");
        assertThat(all.ownedShards()).hasSize(TOTAL);
        assertThat(all.totalShards()).isEqualTo(TOTAL);
    }

    @Test
    void staticOwnershipParsesRange() {
        assertThat(new StaticShardOwnership(TOTAL, "0-7").ownedShards())
                .containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7);
    }

    @Test
    void twoNodesWithDisjointShardRangesClaimDisjointRuns() {
        LeaseManager leases  = new LeaseManager(jdbc);
        JobStore jobStore    = new JobStore(jdbc);
        JobRunStore runStore = new JobRunStore(jdbc);
        Set<Integer> shardsA = new StaticShardOwnership(TOTAL, "0-7").ownedShards();
        Set<Integer> shardsB = new StaticShardOwnership(TOTAL, "8-15").ownedShards();

        UUID jobA = seedJob(jobStore, 2);
        UUID jobB = seedJob(jobStore, 10);
        for (int i = 0; i < 4; i++) seedRun(runStore, jobA, 2);
        for (int i = 0; i < 4; i++) seedRun(runStore, jobB, 10);

        List<JobRun> claimedA = leases.claimDue("nodeA", 100, Duration.ofSeconds(30), shardsA);
        List<JobRun> claimedB = leases.claimDue("nodeB", 100, Duration.ofSeconds(30), shardsB);

        assertThat(claimedA).hasSize(4);
        assertThat(claimedA).allSatisfy(r -> assertThat(shardsA).contains(r.shardId()));
        assertThat(claimedB).hasSize(4);
        assertThat(claimedB).allSatisfy(r -> assertThat(shardsB).contains(r.shardId()));

        Set<UUID> intersection = claimedA.stream().map(JobRun::id).collect(Collectors.toSet());
        intersection.retainAll(claimedB.stream().map(JobRun::id).collect(Collectors.toSet()));
        assertThat(intersection).isEmpty();
    }

    @Test
    void emptyOwnedShardsClaimsNothing() {
        LeaseManager leases  = new LeaseManager(jdbc);
        JobStore jobStore    = new JobStore(jdbc);
        JobRunStore runStore = new JobRunStore(jdbc);
        UUID jobId = seedJob(jobStore, 0);
        seedRun(runStore, jobId, 0);
        assertThat(leases.claimDue("nodeX", 100, Duration.ofSeconds(30), Set.of())).isEmpty();
    }

    private UUID seedJob(JobStore store, int shardId) {
        UUID id = UUID.randomUUID();
        return store.insertIfAbsent(new Job(id, "noop", "{}", "k-" + UUID.randomUUID(),
                DeliveryPolicy.AT_LEAST_ONCE, null, ZoneId.of("UTC"), MisfirePolicy.FIRE_ONCE,
                RetryPolicy.defaults(), Instant.now(), shardId, 0, null));
    }

    private void seedRun(JobRunStore store, UUID jobId, int shardId) {
        store.insert(new JobRun(UUID.randomUUID(), jobId, JobState.SCHEDULED, 0,
                Instant.now().minusSeconds(1), null, null, 0L, "rk-" + UUID.randomUUID(),
                null, null, Instant.now(), null, null, shardId, 0, null));
    }
}
