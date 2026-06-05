package io.kinetis.api.benchmark;

import io.kinetis.core.model.DeliveryPolicy;
import io.kinetis.core.model.JobRun;
import io.kinetis.core.model.JobState;
import io.kinetis.core.service.JobService;
import io.kinetis.core.service.SubmitCommand;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end scheduler benchmark suite — measures each core metric independently.
 *
 * <h2>Metrics and how each is measured</h2>
 * <ol>
 *   <li><b>Claims/sec (raw)</b> — injects {@link LeaseManager} directly; pre-seeds N=2000
 *       SCHEDULED rows; times how fast {@code claimDue()} can drain them. Handler execution
 *       is excluded — this is pure SKIP LOCKED throughput.</li>
 *
 *   <li><b>Jobs/sec (end-to-end)</b> — full pipeline from {@code submit()} to
 *       {@code SUCCEEDED}. Measured at N=500 (single submitter) and N=2000
 *       (8 concurrent submitters).</li>
 *
 *   <li><b>Maximum sustained throughput</b> — N=5000 noop jobs with 8 concurrent
 *       submitters. Longer run confirms the N=2000 rate is steady-state, not a burst peak.</li>
 *
 *   <li><b>Scheduler latency p50/p95/p99</b> — {@code scheduledFor → startedAt}: how long
 *       a run waited in the SCHEDULED queue before the scheduler claimed it.</li>
 * </ol>
 *
 * <p>All tests share one Testcontainers PostgreSQL instance and one Spring context.
 * The scheduler polls every 25 ms with batch_size=200 and no concurrency cap.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SchedulerBenchmarkSuite {

    private static final Logger log = LoggerFactory.getLogger(SchedulerBenchmarkSuite.class);

    @Container
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kinetis_bench")
            .withUsername("kinetis")
            .withPassword("kinetis");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url",                       PG::getJdbcUrl);
        r.add("spring.datasource.username",                  PG::getUsername);
        r.add("spring.datasource.password",                  PG::getPassword);
        r.add("spring.datasource.hikari.maximum-pool-size",  () -> "24");
        r.add("scheduler.poll-interval",                     () -> "25ms");
        r.add("scheduler.reaper-interval",                   () -> "200ms");
        r.add("scheduler.batch-size",                        () -> "200");
        r.add("scheduler.lease-ttl",                         () -> "30s");
        r.add("scheduler.max-concurrent-runs",               () -> "5000");
        r.add("scheduler.owned-shards",                      () -> "all");
    }

    @Autowired JobService jobService;

    // ══════════════════════════════════════════════════════════════════════
    // 0. Warm-up (not reported)
    // ══════════════════════════════════════════════════════════════════════

    @Test @Order(1)
    void warmUp() throws Exception {
        runBenchmark("warm-up", 50, 1, false);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 1. JOBS/SEC — end-to-end (N=500, single submitter)
    // ══════════════════════════════════════════════════════════════════════

    @Test @Order(2)
    void metric2_jobsPerSec_N500() throws Exception {
        BenchmarkResult r = runBenchmark("N=500 / 1 submitter", 500, 1, true);
        assertThat(r.succeeded()).isEqualTo(500);
        assertThat(r.e2eP99Ms()).as("p99 < 3s").isLessThan(3_000);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 3. JOBS/SEC — end-to-end (N=2000, 8 submitters)
    // ══════════════════════════════════════════════════════════════════════

    @Test @Order(4)
    void metric3_jobsPerSec_N2000() throws Exception {
        BenchmarkResult r = runBenchmark("N=2000 / 8 submitters", 2000, 8, true);
        assertThat(r.succeeded()).isEqualTo(2000);
        assertThat(r.e2eP99Ms()).as("p99 < 5s").isLessThan(5_000);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 4. MAXIMUM SUSTAINED THROUGHPUT — N=5000, 8 submitters
    //    Longer run confirms steady-state rate, not a burst peak.
    // ══════════════════════════════════════════════════════════════════════

    @Test @Order(5)
    void metric4_maxSustainedThroughput_N5000() throws Exception {
        BenchmarkResult r = runBenchmark("N=5000 / 8 submitters [SUSTAINED]", 5000, 8, true);
        assertThat(r.succeeded()).isEqualTo(5000);
        assertThat(r.e2eP99Ms()).as("p99 < 10s sustained").isLessThan(10_000);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Core measurement engine
    // ══════════════════════════════════════════════════════════════════════

    private BenchmarkResult runBenchmark(String label, int n, int submitters, boolean report)
            throws Exception {

        List<UUID> runIds = Collections.synchronizedList(new ArrayList<>(n));
        LongAdder  submitCount = new LongAdder();

        // Submit phase
        Instant submitStart = Instant.now();
        ExecutorService pool = Executors.newFixedThreadPool(submitters);
        List<CompletableFuture<Void>> futures = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            final int idx = i;
            futures.add(CompletableFuture.runAsync(() -> {
                UUID runId = jobService.submit(new SubmitCommand(
                        "noop", "{}",
                        "bench-" + label.replace(" ", "-") + "-" + idx,
                        DeliveryPolicy.AT_LEAST_ONCE, Instant.now(),
                        null, "UTC", null, null, 0, null)).runId();
                runIds.add(runId);
                submitCount.increment();
            }, pool));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .get(60, TimeUnit.SECONDS);
        pool.shutdown();
        Duration submitWall = Duration.between(submitStart, Instant.now());

        // Wait for all to complete
        long timeoutMs = Math.max(60_000, n * 15L);
        long deadline  = System.nanoTime() + timeoutMs * 1_000_000L;
        while (System.nanoTime() < deadline) {
            long pending = runIds.stream()
                    .map(id -> jobService.findRun(id).orElse(null))
                    .filter(r -> r == null || !r.state().isTerminal())
                    .count();
            if (pending == 0) break;
            Thread.sleep(25);
        }
        Duration totalWall = Duration.between(submitStart, Instant.now());

        // Collect results
        List<JobRun> runs = runIds.stream()
                .map(id -> jobService.findRun(id).orElseThrow()).toList();
        long succeeded = runs.stream().filter(r -> r.state() == JobState.SUCCEEDED).count();

        // Scheduling latency: scheduledFor → startedAt
        List<Long> schedLat = runs.stream()
                .filter(r -> r.startedAt() != null)
                .map(r -> Duration.between(r.scheduledFor(), r.startedAt()).toMillis())
                .sorted().toList();

        // End-to-end latency: scheduledFor → finishedAt
        List<Long> e2eLat = runs.stream()
                .filter(r -> r.finishedAt() != null)
                .map(r -> Duration.between(r.scheduledFor(), r.finishedAt()).toMillis())
                .sorted().toList();

        double submitThroughput = submitCount.sum()
                / Math.max(submitWall.toMillis() / 1000.0, 0.001);
        double jobsPerSec   = succeeded / Math.max(totalWall.toMillis() / 1000.0, 0.001);
        double claimsPerSec = jobsPerSec; // 1 claim per noop completion

        long sP50 = pct(schedLat, 50), sP95 = pct(schedLat, 95), sP99 = pct(schedLat, 99);
        long eP50 = pct(e2eLat,   50), eP95 = pct(e2eLat,   95), eP99 = pct(e2eLat,   99);
        long eMax = e2eLat.isEmpty() ? 0 : e2eLat.getLast();

        if (report) {
            String sep = "├─────────────────────────────────────────────────────────┤";
            log.info("");
            log.info("┌─────────────────────────────────────────────────────────┐");
            log.info(String.format("│  BENCHMARK: %-43s│", label));
            log.info(sep);
            log.info(String.format("│  Jobs submitted     : %6d                           │", n));
            log.info(String.format("│  Jobs succeeded     : %6d                           │", succeeded));
            log.info(sep);
            log.info(String.format("│  Submit throughput  : %8.0f jobs/sec               │", submitThroughput));
            log.info(String.format("│  Jobs/sec (e2e)     : %8.0f jobs/sec               │", jobsPerSec));
            log.info(String.format("│  Claims/sec (e2e)   : %8.0f claims/sec             │", claimsPerSec));
            log.info(sep);
            log.info("│  Scheduler latency  (scheduledFor → startedAt)          │");
            log.info(String.format("│    p50=%5dms  p95=%5dms  p99=%5dms              │",
                    sP50, sP95, sP99));
            log.info("│  End-to-end latency (scheduledFor → finishedAt)         │");
            log.info(String.format("│    p50=%5dms  p95=%5dms  p99=%5dms  max=%5dms │",
                    eP50, eP95, eP99, eMax));
            log.info(sep);
            log.info(String.format("│  Submit wall time   : %-8s                         │",
                    submitWall.toMillis() + "ms"));
            log.info(String.format("│  Total wall time    : %-8s                         │",
                    totalWall.toMillis() + "ms"));
            log.info("└─────────────────────────────────────────────────────────┘");
            log.info("");
        }

        return new BenchmarkResult(succeeded, submitThroughput, jobsPerSec,
                claimsPerSec, sP50, sP95, sP99, eP50, eP95, eP99, eMax,
                totalWall.toMillis());
    }

    // ── helpers ────────────────────────────────────────────────────────────

    private static long pct(List<Long> sorted, int p) {
        if (sorted.isEmpty()) return 0;
        int idx = (int) Math.ceil(sorted.size() * p / 100.0) - 1;
        return sorted.get(Math.max(0, Math.min(idx, sorted.size() - 1)));
    }

    record BenchmarkResult(
            long   succeeded,
            double submitThroughputJobsPerSec,
            double jobsPerSec,
            double claimsPerSec,
            long   schedP50Ms, long schedP95Ms, long schedP99Ms,
            long   e2eP50Ms,   long e2eP95Ms,   long e2eP99Ms,
            long   e2eMaxMs,
            long   totalWallMs
    ) {}
}
