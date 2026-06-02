package io.kinetis.core.service;

import io.kinetis.core.AbstractPgTest;
import io.kinetis.core.model.DeliveryPolicy;
import io.kinetis.core.model.JobRun;
import io.kinetis.core.store.JobRunStore;
import io.kinetis.core.store.JobStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class JobServiceTest extends AbstractPgTest {

    private JobService service() {
        return new JobService(new JobStore(jdbc), new JobRunStore(jdbc), jdbc, Clock.systemUTC(), 16);
    }

    @Test
    void resubmittingSameKeyDeduplicatesToOneJobAndOneRun() {
        JobService svc = service();
        SubmitCommand cmd = new SubmitCommand("noop", "{}", "fixed-key",
                DeliveryPolicy.AT_LEAST_ONCE, Instant.now(), null, "UTC", null, null, 0, null);

        JobSubmission first  = svc.submit(cmd);
        JobSubmission second = svc.submit(cmd);

        assertThat(first.created()).isTrue();
        assertThat(second.created()).isFalse();
        assertThat(second.jobId()).isEqualTo(first.jobId());
        assertThat(jdbc.queryForObject("SELECT count(*) FROM jobs", Long.class)).isEqualTo(1L);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_runs", Long.class)).isEqualTo(1L);
    }

    @Test
    void differentScheduleSlotsProduceDifferentRunKeys() {
        JobService svc = service();
        JobSubmission a = svc.submit(new SubmitCommand("noop", "{\"a\":1}", null,
                DeliveryPolicy.AT_LEAST_ONCE, Instant.parse("2026-06-02T10:00:00Z"), null, "UTC", null, null, 0, null));

        JobRun runA = new JobRunStore(jdbc).findById(a.runId()).orElseThrow();
        String jobKey = new JobStore(jdbc).findById(a.jobId()).orElseThrow().idempotencyKey();
        assertThat(runA.idempotencyKey()).startsWith(jobKey);
        assertThat(runA.idempotencyKey()).isNotEqualTo(jobKey);
    }

    @Test
    void cancelMarksRunsCancelled() {
        JobService svc = service();
        JobSubmission s = svc.submit(new SubmitCommand("noop", "{}", "to-cancel",
                DeliveryPolicy.AT_LEAST_ONCE, Instant.now().plusSeconds(3600), null, "UTC", null, null, 0, null));

        assertThat(svc.cancel(s.jobId())).isEqualTo(1);
        List<JobRun> runs = svc.findRuns(s.jobId());
        assertThat(runs).allSatisfy(r -> assertThat(r.state().name()).isEqualTo("CANCELLED"));
    }
}
