package io.kinetis.api.benchmark;

import io.kinetis.core.model.DeliveryPolicy;
import io.kinetis.core.model.JobRun;
import io.kinetis.core.model.JobState;
import io.kinetis.core.service.JobService;
import io.kinetis.core.service.SubmitCommand;
import org.junit.jupiter.api.Test;
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
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures crash-recovery latency through the reaper cycle.
 *
 * <h2>Protocol</h2>
 * <ol>
 *   <li>Submit a long-running sleep job so it stays LEASED/RUNNING long enough to observe.</li>
 *   <li>Wait until the run reaches LEASED or RUNNING state.</li>
 *   <li>Record {@code t0} and force lease expiry (simulate worker crash).</li>
 *   <li>Poll at 25 ms intervals:
 *     <ul>
 *       <li>{@code t_reaper} = first tick where the run leaves LEASED/RUNNING
 *           (reaper reschedules it back to SCHEDULED)</li>
 *       <li>{@code t_redispatch} = first tick where the run is re-LEASED by the scheduler
 *           (picked up again after the reaper put it back)</li>
 *     </ul>
 *   </li>
 * </ol>
 *
 * <h2>Metrics reported</h2>
 * <ul>
 *   <li><b>Reaper reclaim latency</b> — t_reaper − t0: how long until the reaper notices
 *       the expired lease and reschedules the run.</li>
 *   <li><b>Re-dispatch latency</b>  — t_redispatch − t0: how long until the scheduler
 *       picks up the rescheduled run and re-executes it. This is the wall-clock cost of
 *       crash recovery visible to users: t_reaper + 1 scheduler poll.</li>
 *   <li>p50 / p95 / max across N=10 samples</li>
 * </ul>
 *
 * <p>The reaper runs every 200 ms in this test and the scheduler polls every 25 ms,
 * so reaper reclaim latency should be ~0–200 ms and re-dispatch should add ~25 ms.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class RecoveryBenchmarkTest {

    private static final Logger log = LoggerFactory.getLogger(RecoveryBenchmarkTest.class);

    private static final int SAMPLES = 10;
    private static final Duration POLL_INTERVAL = Duration.ofMillis(25);

    @Container
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kinetis_recovery")
            .withUsername("kinetis")
            .withPassword("kinetis");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url",         PG::getJdbcUrl);
        r.add("spring.datasource.username",    PG::getUsername);
        r.add("spring.datasource.password",    PG::getPassword);
        r.add("scheduler.poll-interval",       () -> "25ms");
        r.add("scheduler.reaper-interval",     () -> "200ms");
        r.add("scheduler.lease-ttl",           () -> "30s");
        r.add("scheduler.batch-size",          () -> "50");
        r.add("scheduler.max-concurrent-runs", () -> "200");
    }

    @Autowired JobService jobService;
    @Autowired JdbcTemplate jdbc;

    // ------------------------------------------------------------------
    // Reaper recovery latency — single crash
    // ------------------------------------------------------------------

    @Test
    void reaperRecoveryLatency_singleCrash() throws Exception {
        log.info("=== RECOVERY BENCHMARK: single crash ===");

        List<Long> reaperMs  = new ArrayList<>();
        List<Long> fullMs    = new ArrayList<>();

        for (int i = 0; i < SAMPLES; i++) {
            RecoverySample s = measureOneRecovery("single-crash-" + i);
            reaperMs.add(s.reaperReclaimMs());
            fullMs.add(s.fullRecoveryMs());
            log.info("  sample {}/{}: reaper={}ms  full={}ms",
                    i + 1, SAMPLES, s.reaperReclaimMs(), s.fullRecoveryMs());
        }

        Collections.sort(reaperMs);
        Collections.sort(fullMs);

        long rP50  = percentile(reaperMs, 50);
        long rP95  = percentile(reaperMs, 95);
        long rMax  = reaperMs.getLast();
        long fP50  = percentile(fullMs,   50);
        long fP95  = percentile(fullMs,   95);
        long fMax  = fullMs.getLast();

        String sep = "╠═══════════════════════════════════════════════════════════╣";
        log.info("");
        log.info("╔═══════════════════════════════════════════════════════════╗");
        log.info(String.format("║  RECOVERY BENCHMARK (reaper-interval=200ms, N=%d)       ║", SAMPLES));
        log.info(sep);
        log.info("║  Reaper reclaim latency  (crash → back to SCHEDULED)      ║");
        log.info(String.format("║    p50 = %5d ms   p95 = %5d ms   max = %5d ms      ║",
                rP50, rP95, rMax));
        log.info("║  Re-dispatch latency     (crash → re-LEASED by scheduler) ║");
        log.info(String.format("║    p50 = %5d ms   p95 = %5d ms   max = %5d ms      ║",
                fP50, fP95, fMax));
        log.info("╚═══════════════════════════════════════════════════════════╝");
        log.info("");

        assertThat(rP95).as("reaper reclaim p95 < 1s").isLessThan(1_000);
        assertThat(fP95).as("re-dispatch p95 < 2s").isLessThan(2_000);
    }

    // ------------------------------------------------------------------
    // Reaper recovery — 5 concurrent crashes
    // ------------------------------------------------------------------

    @Test
    void reaperRecoveryLatency_concurrentCrashes() throws Exception {
        log.info("=== RECOVERY BENCHMARK: 5 concurrent crashes ===");

        int CONCURRENT = 5;
        List<UUID> runIds = new ArrayList<>();

        // Submit CONCURRENT long-running sleep jobs
        List<Long> tokensAtCrash = new ArrayList<>();
        for (int i = 0; i < CONCURRENT; i++) {
            UUID runId = jobService.submit(new SubmitCommand(
                    "sleep", "{\"ms\":300000}", "conc-crash-" + i,
                    DeliveryPolicy.AT_LEAST_ONCE, Instant.now(),
                    null, "UTC", null, null, 0, null)).runId();
            runIds.add(runId);
        }

        // Wait for all to be claimed
        awaitAllAtLeastState(runIds, JobState.LEASED, Duration.ofSeconds(10));

        // Capture token before crash
        for (UUID runId : runIds) {
            JobRun r = jobService.findRun(runId).orElseThrow();
            tokensAtCrash.add(r.leaseToken());
        }

        // Record crash time and expire all leases simultaneously
        Instant t0 = Instant.now();
        for (UUID runId : runIds) forceLeaseExpiry(runId);
        log.info("  forced {} leases expired simultaneously at {}", CONCURRENT, t0);

        // Wait for all to be re-dispatched (re-LEASED with new token after reaper reschedule)
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            long redispatched = 0;
            for (int i = 0; i < CONCURRENT; i++) {
                JobRun r = jobService.findRun(runIds.get(i)).orElse(null);
                if (r != null
                        && (r.state() == JobState.LEASED || r.state() == JobState.RUNNING)
                        && r.leaseToken() > tokensAtCrash.get(i) + 1) {
                    redispatched++;
                }
            }
            if (redispatched == CONCURRENT) break;
            Thread.sleep(25);
        }

        Duration recoveryWall = Duration.between(t0, Instant.now());

        // Count how many were re-dispatched
        long recovered = 0;
        for (int i = 0; i < CONCURRENT; i++) {
            JobRun r = jobService.findRun(runIds.get(i)).orElse(null);
            if (r != null
                    && (r.state() == JobState.LEASED || r.state() == JobState.RUNNING
                        || r.state().isTerminal())
                    && r.leaseToken() > tokensAtCrash.get(i)) {
                recovered++;
            }
        }

        log.info("  concurrent recovery: {} / {} re-dispatched in {} ms",
                recovered, CONCURRENT, recoveryWall.toMillis());

        assertThat(recovered).as("all %d runs must be re-dispatched", CONCURRENT).isEqualTo(CONCURRENT);
        assertThat(recoveryWall.toMillis())
                .as("concurrent re-dispatch < 3s").isLessThan(3_000);
    }

    // ------------------------------------------------------------------
    // Core measurement
    // ------------------------------------------------------------------

    private RecoverySample measureOneRecovery(String key) throws Exception {
        // Submit a long sleep so it stays LEASED/RUNNING long enough to observe
        UUID runId = jobService.submit(new SubmitCommand(
                "sleep", "{\"ms\":300000}", key,
                DeliveryPolicy.AT_LEAST_ONCE, Instant.now(),
                null, "UTC", null, null, 0, null)).runId();

        // Wait until LEASED or RUNNING
        long acquireDeadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < acquireDeadline) {
            JobRun r = jobService.findRun(runId).orElse(null);
            if (r != null && (r.state() == JobState.LEASED || r.state() == JobState.RUNNING))
                break;
            Thread.sleep(POLL_INTERVAL.toMillis());
        }

        JobRun beforeCrash = jobService.findRun(runId).orElseThrow();
        assertThat(beforeCrash.state()).as("run must reach LEASED/RUNNING before crash")
                .isIn(JobState.LEASED, JobState.RUNNING);

        // Capture the lease token at crash time; the next claim will bump it
        long tokenAtCrash = beforeCrash.leaseToken();

        // --- CRASH ---
        Instant t0 = Instant.now();
        forceLeaseExpiry(runId);

        // Poll for:
        //   t_reaper     = state leaves LEASED/RUNNING (reaper put it back to SCHEDULED)
        //   t_redispatch = run's leaseToken > tokenAtCrash + 1 AND state is LEASED/RUNNING again
        //                  (scheduler re-claimed it after the reaper reschedule)
        Long reaperMs     = null;
        Long redispatchMs = null;
        long recoveryDeadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();

        while (System.nanoTime() < recoveryDeadline && redispatchMs == null) {
            Thread.sleep(POLL_INTERVAL.toMillis());
            JobRun r = jobService.findRun(runId).orElse(null);
            if (r == null) continue;

            if (reaperMs == null
                    && r.state() != JobState.LEASED
                    && r.state() != JobState.RUNNING) {
                reaperMs = Duration.between(t0, Instant.now()).toMillis();
            }
            // Re-dispatched: scheduler claimed it again (fresh token, back to LEASED/RUNNING)
            if (reaperMs != null
                    && (r.state() == JobState.LEASED || r.state() == JobState.RUNNING)
                    && r.leaseToken() > tokenAtCrash + 1) {
                redispatchMs = Duration.between(t0, Instant.now()).toMillis();
            }
        }

        if (reaperMs     == null) reaperMs     = Duration.between(t0, Instant.now()).toMillis();
        if (redispatchMs == null) redispatchMs = Duration.between(t0, Instant.now()).toMillis();

        return new RecoverySample(reaperMs, redispatchMs);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private void forceLeaseExpiry(UUID runId) {
        jdbc.update("""
                UPDATE job_runs
                SET lease_expires_at = now() - interval '10 minutes'
                WHERE id = ? AND state IN ('LEASED', 'RUNNING')
                """, runId);
    }

    private void awaitAllAtLeastState(List<UUID> runIds, JobState target, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            long ready = runIds.stream()
                    .map(id -> jobService.findRun(id).orElse(null))
                    .filter(r -> r != null && isAtLeast(r.state(), target))
                    .count();
            if (ready == runIds.size()) return;
            Thread.sleep(50);
        }
    }

    private static boolean isAtLeast(JobState actual, JobState target) {
        // SCHEDULED → LEASED → RUNNING → SUCCEEDED/DEAD_LETTER
        return switch (target) {
            case LEASED  -> actual == JobState.LEASED || actual == JobState.RUNNING || actual.isTerminal();
            case RUNNING -> actual == JobState.RUNNING || actual.isTerminal();
            default      -> actual == target;
        };
    }

    private static long percentile(List<Long> sorted, int pct) {
        if (sorted.isEmpty()) return 0;
        int idx = (int) Math.ceil(sorted.size() * pct / 100.0) - 1;
        return sorted.get(Math.max(0, Math.min(idx, sorted.size() - 1)));
    }

    record RecoverySample(long reaperReclaimMs, long fullRecoveryMs) {}
}
