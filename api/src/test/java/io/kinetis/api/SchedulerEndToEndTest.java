package io.kinetis.api;

import io.kinetis.core.model.DeliveryPolicy;
import io.kinetis.core.model.JobRun;
import io.kinetis.core.model.JobState;
import io.kinetis.core.model.RetryPolicy;
import io.kinetis.core.service.JobService;
import io.kinetis.core.service.JobSubmission;
import io.kinetis.core.service.SubmitCommand;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class SchedulerEndToEndTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kinetis").withUsername("kinetis").withPassword("kinetis");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url",      POSTGRES::getJdbcUrl);
        r.add("spring.datasource.username", POSTGRES::getUsername);
        r.add("spring.datasource.password", POSTGRES::getPassword);
        r.add("scheduler.poll-interval",    () -> "150ms");
        r.add("scheduler.reaper-interval",  () -> "500ms");
    }

    @Autowired JobService jobService;

    @Test
    void noopJobRunsToSuccess() throws Exception {
        JobSubmission s = jobService.submit(simple("noop", "{}"));
        assertThat(awaitState(s.runId(), JobState.SUCCEEDED, Duration.ofSeconds(15)).attempt()).isZero();
    }

    @Test
    void failingJobRetriesThenSucceeds() throws Exception {
        JobSubmission s = jobService.submit(new SubmitCommand(
                "failNTimes", "{\"failTimes\":2}", null, DeliveryPolicy.AT_LEAST_ONCE,
                Instant.now(), null, "UTC", null, new RetryPolicy(5, 50L, 2.0), 0, null));
        assertThat(awaitState(s.runId(), JobState.SUCCEEDED, Duration.ofSeconds(20)).attempt()).isEqualTo(2);
    }

    @Test
    void exhaustedRetriesEndInDeadLetter() throws Exception {
        JobSubmission s = jobService.submit(new SubmitCommand(
                "failNTimes", "{\"failTimes\":99}", null, DeliveryPolicy.AT_LEAST_ONCE,
                Instant.now(), null, "UTC", null, new RetryPolicy(2, 50L, 2.0), 0, null));
        assertThat(awaitState(s.runId(), JobState.DEAD_LETTER, Duration.ofSeconds(20)).lastError()).isNotBlank();
    }

    @Test
    void atMostOnceJobIsNeverRetried() throws Exception {
        JobSubmission s = jobService.submit(new SubmitCommand(
                "failNTimes", "{\"failTimes\":99}", null, DeliveryPolicy.AT_MOST_ONCE,
                Instant.now(), null, "UTC", null, new RetryPolicy(5, 50L, 2.0), 0, null));
        assertThat(awaitState(s.runId(), JobState.DEAD_LETTER, Duration.ofSeconds(15)).attempt()).isZero();
    }

    private SubmitCommand simple(String type, String payload) {
        return new SubmitCommand(type, payload, null, DeliveryPolicy.AT_LEAST_ONCE,
                Instant.now(), null, "UTC", null, null, 0, null);
    }

    private JobRun awaitState(UUID runId, JobState target, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            JobRun run = jobService.findRun(runId).orElseThrow();
            if (run.state() == target) return run;
            Thread.sleep(100);
        }
        JobRun last = jobService.findRun(runId).orElse(null);
        throw new TimeoutException("run " + runId + " did not reach " + target
                + " (last=" + (last == null ? "?" : last.state()) + ")");
    }
}
