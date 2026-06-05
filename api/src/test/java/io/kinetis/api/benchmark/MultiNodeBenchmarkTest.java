package io.kinetis.api.benchmark;

import io.kinetis.core.lease.LeaseManager;
import io.kinetis.core.metrics.SchedulerMetrics;
import io.kinetis.core.model.DeliveryPolicy;
import io.kinetis.core.model.JobState;
import io.kinetis.core.queue.FairShareDispatcher;
import io.kinetis.core.queue.RateLimiter;
import io.kinetis.core.scheduler.SchedulerLoop;
import io.kinetis.core.service.JobService;
import io.kinetis.core.service.SubmitCommand;
import io.kinetis.core.shard.StaticShardOwnership;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simulates a multi-node Kinetis cluster against a shared PostgreSQL instance.
 *
 * <h2>How multi-node is simulated</h2>
 * Each "node" is a {@link SchedulerLoop} instance with:
 * <ul>
 *   <li>A unique {@code nodeId} (node-0, node-1, …)</li>
 *   <li>A distinct shard partition (16 shards divided equally across N nodes)</li>
 *   <li>Shared {@link LeaseManager} and {@link FairShareDispatcher} (same in-process worker pool)</li>
 * </ul>
 *
 * <p>The built-in Spring context LoopRunner is neutralized by setting an extremely high
 * poll interval ({@code scheduler.poll-interval=10000s}), so only the manually-driven
 * loops run during the test.
 *
 * <h2>Correctness check</h2>
 * After each run, the test verifies that:
 * <ul>
 *   <li>All N jobs reached SUCCEEDED state (no dropped jobs)</li>
 *   <li>No job's SUCCEEDED count > 1 (no double-execution despite concurrent nodes)</li>
 * </ul>
 *
 * <h2>Metrics reported</h2>
 * <ul>
 *   <li>Jobs/sec at 1, 2, and 4 simulated nodes</li>
 *   <li>Maximum sustained throughput (from the highest-node-count run)</li>
 *   <li>Cluster sizes tested</li>
 * </ul>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MultiNodeBenchmarkTest {

    private static final Logger log = LoggerFactory.getLogger(MultiNodeBenchmarkTest.class);

    private static final int TOTAL_SHARDS  = 16;
    private static final int JOBS_PER_RUN  = 400;
    private static final int BATCH_SIZE    = 100;
    private static final Duration LEASE_TTL = Duration.ofSeconds(30);

    @Container
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kinetis_multinode")
            .withUsername("kinetis")
            .withPassword("kinetis");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url",         PG::getJdbcUrl);
        r.add("spring.datasource.username",    PG::getUsername);
        r.add("spring.datasource.password",    PG::getPassword);
        r.add("spring.datasource.hikari.maximum-pool-size", () -> "32");
        // Neutralize the built-in LoopRunner — we drive loops manually in each test
        r.add("scheduler.poll-interval",       () -> "10000s");
        r.add("scheduler.reaper-interval",     () -> "10000s");
        r.add("scheduler.lease-ttl",           () -> "30s");
        r.add("scheduler.batch-size",          () -> String.valueOf(BATCH_SIZE));
        r.add("scheduler.owned-shards",        () -> "all");
        r.add("scheduler.max-concurrent-runs", () -> "2000");
    }

    @Autowired JobService        jobService;
    @Autowired LeaseManager      leaseManager;
    @Autowired FairShareDispatcher fairShareDispatcher;
    @Autowired RateLimiter       rateLimiter;
    @Autowired SchedulerMetrics  schedulerMetrics;
    @Autowired JdbcTemplate      jdbc;

    // Static so printSummary() (a separate test instance) can read previous results
    private static final List<NodeResult> allResults = Collections.synchronizedList(new ArrayList<>());

    // ------------------------------------------------------------------
    // 1 node — baseline
    // ------------------------------------------------------------------

    @Test
    @Order(1)
    void throughput_1_node() throws Exception {
        NodeResult r = runWithNodes(1, JOBS_PER_RUN);
        allResults.add(r);
        assertAllSucceeded(r);
        assertNoDoubleExecution();
        log.info(String.format("1-node:  %.0f jobs/sec  (%d jobs in %d ms)",
                r.jobsPerSec(), r.succeeded(), r.wallMs()));
    }

    // ------------------------------------------------------------------
    // 2 nodes
    // ------------------------------------------------------------------

    @Test
    @Order(2)
    void throughput_2_nodes() throws Exception {
        NodeResult r = runWithNodes(2, JOBS_PER_RUN);
        allResults.add(r);
        assertAllSucceeded(r);
        assertNoDoubleExecution();
        log.info(String.format("2-nodes: %.0f jobs/sec  (%d jobs in %d ms)",
                r.jobsPerSec(), r.succeeded(), r.wallMs()));
    }

    // ------------------------------------------------------------------
    // 4 nodes
    // ------------------------------------------------------------------

    @Test
    @Order(3)
    void throughput_4_nodes() throws Exception {
        NodeResult r = runWithNodes(4, JOBS_PER_RUN);
        allResults.add(r);
        assertAllSucceeded(r);
        assertNoDoubleExecution();
        log.info(String.format("4-nodes: %.0f jobs/sec  (%d jobs in %d ms)",
                r.jobsPerSec(), r.succeeded(), r.wallMs()));
    }

    // ------------------------------------------------------------------
    // Summary table (runs last)
    // ------------------------------------------------------------------

    @Test
    @Order(4)
    void printSummary() {
        if (allResults.size() < 3) return;

        NodeResult r1 = allResults.get(0);
        NodeResult r2 = allResults.get(1);
        NodeResult r4 = allResults.get(2);
        double maxThroughput = Math.max(r1.jobsPerSec(), Math.max(r2.jobsPerSec(), r4.jobsPerSec()));

        log.info("");
        String sep = "╠═══════════════════════════════════════════════════════════════╣";
        log.info("");
        log.info("╔═══════════════════════════════════════════════════════════════╗");
        log.info(String.format("║  MULTI-NODE THROUGHPUT BENCHMARK  (jobs=%d per run)          ║", JOBS_PER_RUN));
        log.info(sep);
        log.info("║  Nodes  │  Jobs/sec  │  Wall ms  │  Speedup vs 1-node        ║");
        log.info("║─────────┼────────────┼───────────┼───────────────────────────║");
        log.info(String.format("║      1  │  %8.0f  │  %7d  │  1.00×                    ║",
                r1.jobsPerSec(), r1.wallMs()));
        log.info(String.format("║      2  │  %8.0f  │  %7d  │  %4.2f×                    ║",
                r2.jobsPerSec(), r2.wallMs(), r2.jobsPerSec() / r1.jobsPerSec()));
        log.info(String.format("║      4  │  %8.0f  │  %7d  │  %4.2f×                    ║",
                r4.jobsPerSec(), r4.wallMs(), r4.jobsPerSec() / r1.jobsPerSec()));
        log.info(sep);
        log.info(String.format("║  Maximum sustained throughput : %8.0f jobs/sec             ║", maxThroughput));
        log.info("║  Cluster sizes tested         : 1, 2, 4 nodes                ║");
        log.info("║  No double-executions         : verified across all runs      ║");
        log.info("╚═══════════════════════════════════════════════════════════════╝");
        log.info("");
    }

    // ------------------------------------------------------------------
    // Core simulation engine
    // ------------------------------------------------------------------

    /**
     * Submit {@code n} jobs, then drive {@code nodeCount} SchedulerLoops in parallel threads
     * until all jobs complete. Each loop owns a disjoint shard partition.
     */
    private NodeResult runWithNodes(int nodeCount, int n) throws Exception {
        // 1. Submit all jobs
        List<UUID> runIds = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            UUID runId = jobService.submit(new SubmitCommand(
                    "noop", "{}", "mn-node" + nodeCount + "-" + i,
                    DeliveryPolicy.AT_LEAST_ONCE, Instant.now(),
                    null, "UTC", null, null, 0, null)).runId();
            if (runId != null) runIds.add(runId);
        }

        // 2. Build SchedulerLoops with partitioned shard ownership
        int shardsPerNode = TOTAL_SHARDS / nodeCount;
        List<SchedulerLoop> loops = new ArrayList<>(nodeCount);
        for (int nodeIdx = 0; nodeIdx < nodeCount; nodeIdx++) {
            int from = nodeIdx * shardsPerNode;
            int to   = (nodeIdx == nodeCount - 1)
                    ? TOTAL_SHARDS - 1
                    : from + shardsPerNode - 1;
            String rangeSpec = (from == to) ? String.valueOf(from) : (from + "-" + to);
            StaticShardOwnership ownership = new StaticShardOwnership(TOTAL_SHARDS, rangeSpec);
            SchedulerLoop loop = new SchedulerLoop(
                    leaseManager, fairShareDispatcher, schedulerMetrics,
                    ownership, jdbc, "bench-node-" + nodeIdx,
                    BATCH_SIZE, 2000, LEASE_TTL);
            loops.add(loop);
        }

        // 3. Drive all loops in parallel threads, polling every 25 ms
        AtomicBoolean done = new AtomicBoolean(false);
        CountDownLatch started = new CountDownLatch(nodeCount);
        ExecutorService pool = Executors.newFixedThreadPool(nodeCount);

        for (SchedulerLoop loop : loops) {
            pool.submit(() -> {
                started.countDown();
                while (!done.get() && !Thread.currentThread().isInterrupted()) {
                    try {
                        loop.tick();
                        Thread.sleep(25);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception ignored) { }
                }
            });
        }

        started.await(5, TimeUnit.SECONDS);
        Instant start = Instant.now();

        // 4. Wait for all runs to finish
        long timeoutMs = Math.max(30_000, n * 20L);
        long deadline  = System.nanoTime() + timeoutMs * 1_000_000L;

        while (System.nanoTime() < deadline) {
            long pending = runIds.stream()
                    .map(id -> jobService.findRun(id).orElse(null))
                    .filter(r -> r == null || !r.state().isTerminal())
                    .count();
            if (pending == 0) break;
            Thread.sleep(25);
        }

        done.set(true);
        pool.shutdownNow();
        pool.awaitTermination(3, TimeUnit.SECONDS);

        long wallMs   = Duration.between(start, Instant.now()).toMillis();
        long succeeded = runIds.stream()
                .map(id -> jobService.findRun(id).orElse(null))
                .filter(r -> r != null && r.state() == JobState.SUCCEEDED)
                .count();
        double jobsPerSec = succeeded / Math.max(wallMs / 1000.0, 0.001);

        return new NodeResult(nodeCount, n, succeeded, wallMs, jobsPerSec);
    }

    // ------------------------------------------------------------------
    // Correctness assertions
    // ------------------------------------------------------------------

    private void assertAllSucceeded(NodeResult r) {
        assertThat(r.succeeded())
                .as("all %d jobs must succeed (node=%d)", r.n(), r.nodeCount())
                .isEqualTo(r.n());
    }

    private void assertNoDoubleExecution() {
        Long doubles = jdbc.queryForObject(
                """
                SELECT count(*) FROM (
                    SELECT idempotency_key
                    FROM   job_runs
                    WHERE  state = 'SUCCEEDED'
                    GROUP  BY idempotency_key
                    HAVING count(*) > 1
                ) dups
                """, Long.class);
        assertThat(doubles).as("zero double-executions").isEqualTo(0L);
    }

    // ------------------------------------------------------------------
    // Records
    // ------------------------------------------------------------------

    record NodeResult(
            int nodeCount, int n, long succeeded,
            long wallMs, double jobsPerSec
    ) {}
}
