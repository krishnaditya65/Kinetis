package io.kinetis.api;

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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class ThroughputBenchmarkTest {

    private static final Logger log = LoggerFactory.getLogger(ThroughputBenchmarkTest.class);

    @Container
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kinetis").withUsername("kinetis").withPassword("kinetis");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url",     PG::getJdbcUrl);
        r.add("spring.datasource.username", PG::getUsername);
        r.add("spring.datasource.password", PG::getPassword);
        r.add("scheduler.poll-interval",   () -> "50ms");
        r.add("scheduler.reaper-interval", () -> "500ms");
        r.add("scheduler.batch-size",      () -> "100");
    }

    @Autowired JobService jobService;

    @Test
    void submitOneHundredNoopJobsAndMeasureThroughput() throws Exception {
        int N = 100;
        List<UUID> runIds = new ArrayList<>(N);
        Instant start = Instant.now();

        for (int i = 0; i < N; i++) {
            JobSubmission s = jobService.submit(new SubmitCommand(
                    "noop", "{}", "bench-" + UUID.randomUUID(),
                    DeliveryPolicy.AT_LEAST_ONCE, Instant.now(), null, "UTC", null, null, 0, null));
            runIds.add(s.runId());
        }

        Instant deadline = Instant.now().plusSeconds(25);
        while (Instant.now().isBefore(deadline)) {
            long pending = runIds.stream()
                    .map(id -> jobService.findRun(id).orElse(null))
                    .filter(r -> r != null && r.state() != JobState.SUCCEEDED)
                    .count();
            if (pending == 0) break;
            Thread.sleep(50);
        }

        List<JobRun> runs = runIds.stream().map(id -> jobService.findRun(id).orElseThrow()).toList();
        long succeeded = runs.stream().filter(r -> r.state() == JobState.SUCCEEDED).count();

        List<Long> latencies = runs.stream()
                .filter(r -> r.startedAt() != null && r.finishedAt() != null)
                .map(r -> Duration.between(r.scheduledFor(), r.finishedAt()).toMillis())
                .sorted().toList();

        LongSummaryStatistics stats = latencies.stream().mapToLong(Long::longValue).summaryStatistics();
        long p50 = latencies.isEmpty() ? 0 : latencies.get(latencies.size() / 2);
        long p95 = latencies.isEmpty() ? 0 : latencies.get((int)(latencies.size() * 0.95));
        long p99 = latencies.isEmpty() ? 0 : latencies.get((int)(latencies.size() * 0.99));

        log.info("=== THROUGHPUT N={}: succeeded={} p50={}ms p95={}ms p99={}ms min={}ms max={}ms",
                N, succeeded, p50, p95, p99, stats.getMin(), stats.getMax());

        assertThat(succeeded).as("all %d jobs should complete", N).isEqualTo(N);
        assertThat(p99).as("p99 latency < 5s").isLessThan(5000);
    }

    @Test
    void highPriorityJobsCompleteBeforeLowPriority() throws Exception {
        List<UUID> low = new ArrayList<>(), high = new ArrayList<>();
        Instant future = Instant.now().plusMillis(500);

        for (int i = 0; i < 5; i++) {
            low.add(jobService.submit(new SubmitCommand("sleep", "{\"ms\":200}", "low-" + UUID.randomUUID(),
                    DeliveryPolicy.AT_LEAST_ONCE, future, null, "UTC", null, null, -1, null)).runId());
        }
        for (int i = 0; i < 3; i++) {
            high.add(jobService.submit(new SubmitCommand("noop", "{}", "high-" + UUID.randomUUID(),
                    DeliveryPolicy.AT_LEAST_ONCE, future, null, "UTC", null, null, 10, null)).runId());
        }

        Instant deadline = Instant.now().plusSeconds(10);
        while (Instant.now().isBefore(deadline)) {
            long done = high.stream().map(id -> jobService.findRun(id).orElse(null))
                    .filter(r -> r != null && r.state() == JobState.SUCCEEDED).count();
            if (done == 3) break;
            Thread.sleep(100);
        }

        long highSucceeded = high.stream().map(id -> jobService.findRun(id).orElseThrow())
                .filter(r -> r.state() == JobState.SUCCEEDED).count();
        assertThat(highSucceeded).as("all 3 high-priority jobs succeed").isEqualTo(3);
    }
}
