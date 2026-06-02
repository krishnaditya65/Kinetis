package io.kinetis.api;

import io.kinetis.core.model.DeliveryPolicy;
import io.kinetis.core.model.JobRun;
import io.kinetis.core.model.JobState;
import io.kinetis.core.model.MisfirePolicy;
import io.kinetis.core.service.JobService;
import io.kinetis.core.service.JobSubmission;
import io.kinetis.core.service.SubmitCommand;
import org.junit.jupiter.api.Test;
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
import java.util.List;

import static org.assertj.core.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class CronEndToEndTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kinetis").withUsername("kinetis").withPassword("kinetis");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url",      POSTGRES::getJdbcUrl);
        r.add("spring.datasource.username", POSTGRES::getUsername);
        r.add("spring.datasource.password", POSTGRES::getPassword);
        r.add("scheduler.poll-interval",    () -> "100ms");
        r.add("scheduler.cron-interval",    () -> "200ms");
        r.add("scheduler.reaper-interval",  () -> "500ms");
    }

    @Autowired JobService jobService;
    @Autowired JdbcTemplate jdbc;

    @Test
    void recurringJobFiresMultipleTimes() throws Exception {
        JobSubmission s = jobService.submit(new SubmitCommand(
                "noop", "{}", "cron-every-second-" + System.nanoTime(),
                DeliveryPolicy.AT_LEAST_ONCE, null, "* * * * * ?", "UTC", null, null, 0, null));
        assertThat(s.created()).isTrue();
        assertThat(s.runId()).isNull();

        Instant deadline = Instant.now().plus(Duration.ofSeconds(15));
        while (Instant.now().isBefore(deadline)) {
            long succeeded = jobService.findRuns(s.jobId()).stream()
                    .filter(r -> r.state() == JobState.SUCCEEDED).count();
            if (succeeded >= 3) return;
            Thread.sleep(300);
        }
        long succeeded = jobService.findRuns(s.jobId()).stream()
                .filter(r -> r.state() == JobState.SUCCEEDED).count();
        fail("Expected >= 3 SUCCEEDED runs, got " + succeeded);
    }

    @Test
    void nextFireTimeIsSetOnSubmit() throws Exception {
        JobSubmission s = jobService.submit(new SubmitCommand(
                "noop", "{}", "cron-nft-" + System.nanoTime(),
                DeliveryPolicy.AT_LEAST_ONCE, null, "0 * * * *", "UTC", null, null, 0, null));
        assertThat(s.created()).isTrue();
        var nft = jobService.nextFireTime(s.jobId());
        assertThat(nft).isPresent();
        assertThat(nft.get()).isAfter(Instant.now());
    }

    @Test
    void misfireSkipAdvancesNextFireTimeWithoutFiring() throws Exception {
        JobSubmission s = jobService.submit(new SubmitCommand(
                "noop", "{}", "cron-misfire-" + System.nanoTime(),
                DeliveryPolicy.AT_LEAST_ONCE, null, "0 * * * *", "UTC",
                MisfirePolicy.SKIP, null, 0, null));
        jdbc.update("UPDATE jobs SET next_fire_time = now() - interval '10 minutes' WHERE id = ?", s.jobId());
        Thread.sleep(1000);

        long missedRuns = jobService.findRuns(s.jobId()).stream()
                .filter(r -> r.scheduledFor().isBefore(Instant.now().minus(Duration.ofMinutes(5))))
                .count();
        assertThat(missedRuns).as("SKIP: no run for missed occurrence").isZero();
        assertThat(jobService.nextFireTime(s.jobId())).isPresent();
    }

    @Test
    void cancelStopsCronFromFiring() throws Exception {
        JobSubmission s = jobService.submit(new SubmitCommand(
                "noop", "{}", "cron-cancel-" + System.nanoTime(),
                DeliveryPolicy.AT_LEAST_ONCE, null, "* * * * * ?", "UTC", null, null, 0, null));
        Thread.sleep(2500);
        assertThat(jobService.findRuns(s.jobId()).size()).isGreaterThanOrEqualTo(1);

        jobService.cancel(s.jobId());
        assertThat(jobService.nextFireTime(s.jobId())).isEmpty();

        int before = jobService.findRuns(s.jobId()).size();
        Thread.sleep(2000);
        assertThat(jobService.findRuns(s.jobId()).size()).isEqualTo(before);
    }
}
