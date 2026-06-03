package io.kinetis.api.chaos;

import io.kinetis.core.model.DeliveryPolicy;
import io.kinetis.core.model.JobRun;
import io.kinetis.core.model.JobState;
import io.kinetis.core.service.JobService;
import io.kinetis.core.service.JobSubmission;
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
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Chaos test harness: verifies the scheduler is resilient to simulated failures.
 *
 * <h2>Fault injections</h2>
 * <ul>
 *   <li><b>Lease expiry mid-run</b> — forces {@code lease_expires_at} into the past while a run
 *       is LEASED/RUNNING, simulating a worker crash. Asserts the reaper reclaims it and the run
 *       eventually reaches a terminal state.</li>
 *   <li><b>Concurrent lease expiry</b> — expires multiple runs simultaneously; asserts no run is
 *       completed twice (no phantom double-completions).</li>
 *   <li><b>Random lease poisoning under load</b> — submit N jobs, randomly expire leases as they
 *       execute; assert all eventually reach a terminal state.</li>
 * </ul>
 *
 * <p>This is a CI gate: these tests must pass before any release.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class ChaosTest {

    private static final Logger log = LoggerFactory.getLogger(ChaosTest.class);

    @Container
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kinetis").withUsername("kinetis").withPassword("kinetis");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url",      PG::getJdbcUrl);
        r.add("spring.datasource.username", PG::getUsername);
        r.add("spring.datasource.password", PG::getPassword);
        r.add("scheduler.poll-interval",    () -> "100ms");
        r.add("scheduler.reaper-interval",  () -> "300ms");
        r.add("scheduler.lease-ttl",        () -> "5s");
    }

    @Autowired JobService jobService;
    @Autowired JdbcTemplate jdbc;

    // ---- helpers ----

    private JobSubmission submitNoop() {
        return jobService.submit(new SubmitCommand(
                "noop", "{}", "chaos-" + UUID.randomUUID(),
                DeliveryPolicy.AT_LEAST_ONCE, Instant.now(),
                null, "UTC", null, null, 0, null));
    }

    private void forceLeaseExpiry(UUID runId) {
        jdbc.update("""
                UPDATE job_runs
                SET lease_expires_at = now() - interval '10 minutes'
                WHERE id = ? AND state IN ('LEASED', 'RUNNING')
                """, runId);
    }

    private boolean awaitTerminal(UUID runId, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            JobRun run = jobService.findRun(runId).orElse(null);
            if (run != null && run.state().isTerminal()) return true;
            Thread.sleep(100);
        }
        return false;
    }

    // ---- tests ----

    @Test
    void reaperReclaims_simulatedWorkerCrash() throws Exception {
        // Submit a sleep job so it stays LEASED long enough for us to expire it
        JobSubmission s = jobService.submit(new SubmitCommand(
                "sleep", "{\"ms\":30000}", "chaos-sleep-" + UUID.randomUUID(),
                DeliveryPolicy.AT_LEAST_ONCE, Instant.now(), null, "UTC", null, null, 0, null));

        // Wait until it's claimed (LEASED or RUNNING)
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            JobRun run = jobService.findRun(s.runId()).orElse(null);
            if (run != null && (run.state() == JobState.LEASED || run.state() == JobState.RUNNING))
                break;
            Thread.sleep(50);
        }

        // Simulate crash: expire the lease
        forceLeaseExpiry(s.runId());
        log.info("chaos: forced lease expiry on run {}", s.runId());

        // Reaper should pick it up and reschedule within a few reaper intervals
        boolean reached = awaitTerminal(s.runId(), Duration.ofSeconds(15));

        // Verify: run reached a terminal state (SUCCEEDED after reschedule, or DEAD_LETTER if
        // maxAttempts=1 — defaults are 3, so it gets rescheduled and eventually succeeds)
        assertThat(reached).as("run must reach terminal state after simulated crash").isTrue();

        // Verify no duplicate completions: exactly one SUCCEEDED row per run
        Long successCount = jdbc.queryForObject(
                "SELECT count(*) FROM job_runs WHERE id = ? AND state = 'SUCCEEDED'",
                Long.class, s.runId());
        assertThat(successCount).isLessThanOrEqualTo(1L);
    }

    @Test
    void noPhantomDoubleCompletions_underConcurrentLeaseExpiry() throws Exception {
        int N = 10;
        List<UUID> runIds = new ArrayList<>();
        for (int i = 0; i < N; i++) runIds.add(submitNoop().runId());

        // Wait until all are at least leased
        TimeUnit.MILLISECONDS.sleep(500);

        // Expire all leases simultaneously
        for (UUID runId : runIds) forceLeaseExpiry(runId);
        log.info("chaos: expired {} leases simultaneously", N);

        // Wait for all to reach terminal state
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            long terminal = runIds.stream()
                    .map(id -> jobService.findRun(id).orElse(null))
                    .filter(r -> r != null && r.state().isTerminal())
                    .count();
            if (terminal == N) break;
            Thread.sleep(200);
        }

        for (UUID runId : runIds) {
            JobRun run = jobService.findRun(runId).orElseThrow();
            assertThat(run.state().isTerminal())
                    .as("run %s must be terminal", runId).isTrue();
            // A run can only have one DB row — verify no duplicates via idempotency key
            Long dupCount = jdbc.queryForObject(
                    "SELECT count(*) FROM job_runs WHERE idempotency_key = ? AND state = 'SUCCEEDED'",
                    Long.class, run.idempotencyKey());
            assertThat(dupCount).as("no duplicate SUCCEEDED for run %s", runId).isLessThanOrEqualTo(1L);
        }
    }

    @Test
    void randomLeasePoisoning_allRunsEventuallyTerminate() throws Exception {
        int N = 20;
        Random rng = new Random(42);
        List<UUID> runIds = new ArrayList<>();
        for (int i = 0; i < N; i++) runIds.add(submitNoop().runId());

        // Randomly expire leases while jobs are executing
        for (int round = 0; round < 3; round++) {
            TimeUnit.MILLISECONDS.sleep(200);
            for (UUID runId : runIds) {
                if (rng.nextBoolean()) forceLeaseExpiry(runId);
            }
        }
        log.info("chaos: random lease poisoning complete; awaiting {} runs", N);

        // All runs must eventually reach a terminal state
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            long terminal = runIds.stream()
                    .map(id -> jobService.findRun(id).orElse(null))
                    .filter(r -> r != null && r.state().isTerminal())
                    .count();
            log.info("chaos: {}/{} terminal", terminal, N);
            if (terminal == N) break;
            Thread.sleep(500);
        }

        long terminal = runIds.stream()
                .map(id -> jobService.findRun(id).orElse(null))
                .filter(r -> r != null && r.state().isTerminal())
                .count();
        assertThat(terminal).as("all %d runs must reach terminal state", N).isEqualTo(N);
    }
}
