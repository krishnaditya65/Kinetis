package io.kinetis.api.benchmark;

import io.kinetis.core.idempotency.IdempotencyKeys;
import io.kinetis.core.lease.LeaseManager;
import io.kinetis.core.model.DeliveryPolicy;
import io.kinetis.core.model.Job;
import io.kinetis.core.model.JobRun;
import io.kinetis.core.model.JobState;
import io.kinetis.core.model.MisfirePolicy;
import io.kinetis.core.model.RetryPolicy;
import io.kinetis.core.store.JobRunStore;
import io.kinetis.core.store.JobStore;
import org.junit.jupiter.api.Test;
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
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * METRIC: Claims/sec — raw SKIP LOCKED throughput.
 *
 * <p>Measures how fast the {@code claimDue()} SQL can claim rows from the
 * {@code job_runs} table using {@code FOR UPDATE SKIP LOCKED}. Handler
 * execution is excluded from timing; this is pure database claim throughput.
 *
 * <h2>Protocol</h2>
 * <ol>
 *   <li>Disable the background scheduler loop (very high poll interval).</li>
 *   <li>Pre-seed N SCHEDULED rows directly via {@link JobRunStore#insert}.</li>
 *   <li>Time a series of {@code claimDue(batchSize=200)} calls until all
 *       N rows are claimed. Each claimed run is immediately marked succeeded
 *       so the table stays clean between iterations.</li>
 *   <li>Report: rows claimed / wall time = raw claims/sec.</li>
 * </ol>
 *
 * <p>Runs in a dedicated Spring context with its own Testcontainers PostgreSQL
 * instance so load from other test classes cannot interfere.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class ClaimsRawBenchmarkTest {

    private static final Logger log = LoggerFactory.getLogger(ClaimsRawBenchmarkTest.class);

    private static final int    BATCH   = 200;
    private static final int    ROUNDS  = 3;   // repeat for stable average
    private static final Set<Integer> ALL_SHARDS =
            Set.of(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15);

    @Container
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kinetis_claims")
            .withUsername("kinetis")
            .withPassword("kinetis");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url",                       PG::getJdbcUrl);
        r.add("spring.datasource.username",                  PG::getUsername);
        r.add("spring.datasource.password",                  PG::getPassword);
        r.add("spring.datasource.hikari.maximum-pool-size",  () -> "8");
        // Disable background loop — this test drives claimDue directly
        r.add("scheduler.poll-interval",                     () -> "3600s");
        r.add("scheduler.reaper-interval",                   () -> "3600s");
        r.add("scheduler.owned-shards",                      () -> "all");
    }

    @Autowired LeaseManager  leaseManager;
    @Autowired JobStore      jobStore;
    @Autowired JobRunStore   jobRunStore;

    // ── Benchmark: batch=200, N=1000 rows per round ──────────────────────

    @Test
    void claimsPerSec_batch200() throws Exception {
        int N = 1000;
        List<Double> rates = new ArrayList<>();

        // Warm-up round (not reported)
        seedAndDrain("warmup", N);

        for (int round = 0; round < ROUNDS; round++) {
            long wallMs = seedAndDrain("round-" + round, N);
            double rate  = N / Math.max(wallMs / 1000.0, 0.001);
            rates.add(rate);
            log.info(String.format("  round %d: %d rows in %d ms = %.0f claims/sec",
                    round + 1, N, wallMs, rate));
        }

        // Compute median across rounds
        rates.sort(Double::compareTo);
        double median = rates.get(rates.size() / 2);
        double min    = rates.get(0);
        double max    = rates.get(rates.size() - 1);

        log.info("");
        log.info("┌─────────────────────────────────────────────────────────┐");
        log.info("│  METRIC 1 — CLAIMS/SEC (raw SKIP LOCKED, batch=200)     │");
        log.info("├─────────────────────────────────────────────────────────┤");
        log.info(String.format("│  Rows per round       : %6d                           │", N));
        log.info(String.format("│  Rounds measured      : %6d                           │", ROUNDS));
        log.info(String.format("│  Median claims/sec    : %8.0f rows/sec               │", median));
        log.info(String.format("│  Min / Max            : %8.0f / %-8.0f rows/sec      │", min, max));
        log.info("└─────────────────────────────────────────────────────────┘");
        log.info("");

        assertThat(median).as("median raw claims/sec > 500").isGreaterThan(500);
    }

    // ── helpers ──────────────────────────────────────────────────────────

    /**
     * Inserts {@code n} SCHEDULED rows, times how long it takes to claim
     * and complete them all, then returns the wall-clock milliseconds.
     */
    private long seedAndDrain(String tag, int n) throws InterruptedException {
        UUID seedJobId = UUID.randomUUID();
        jobStore.insertIfAbsent(new Job(
                seedJobId, "bench", "{}",
                "claims-" + tag + "-" + seedJobId,
                DeliveryPolicy.AT_LEAST_ONCE, null, ZoneId.of("UTC"),
                MisfirePolicy.FIRE_ONCE, RetryPolicy.defaults(),
                Instant.now(), 0, 0, null));

        // Insert N due SCHEDULED rows
        Instant due = Instant.now().minusMillis(1);
        for (int i = 0; i < n; i++) {
            String key = IdempotencyKeys.deriveRunKey(
                    "claims-" + tag + "-" + seedJobId, due.plusNanos(i));
            jobRunStore.insert(new JobRun(
                    UUID.randomUUID(), seedJobId,
                    JobState.SCHEDULED, 0, due.plusNanos(i),
                    null, null, 0L, key,
                    null, null, Instant.now(), null, null,
                    i % 16, 0, null));
        }

        // Time the drain: claim → markRunning → markSucceeded, no handler
        Instant start = Instant.now();
        int drained = 0;
        while (drained < n) {
            List<JobRun> batch = leaseManager.claimDue(
                    "bench-raw", BATCH, Duration.ofSeconds(30), ALL_SHARDS);
            if (batch.isEmpty()) { Thread.sleep(1); continue; }
            for (JobRun r : batch) {
                leaseManager.markRunning(r.id(), r.leaseToken());
                leaseManager.markSucceeded(r.id(), r.leaseToken());
            }
            drained += batch.size();
        }
        return Duration.between(start, Instant.now()).toMillis();
    }
}
