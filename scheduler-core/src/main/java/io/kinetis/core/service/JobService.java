package io.kinetis.core.service;

import io.kinetis.core.cron.CronEvaluator;
import io.kinetis.core.cron.CronExpression;
import io.kinetis.core.cron.CronParser;
import io.kinetis.core.idempotency.IdempotencyKeys;
import io.kinetis.core.model.DeliveryPolicy;
import io.kinetis.core.model.Job;
import io.kinetis.core.model.JobRun;
import io.kinetis.core.model.JobState;
import io.kinetis.core.model.MisfirePolicy;
import io.kinetis.core.model.RetryPolicy;
import io.kinetis.core.shard.ShardingUtils;
import io.kinetis.core.store.JobRunStore;
import io.kinetis.core.store.JobStore;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Application-facing entry point for creating and inspecting jobs. Submission is idempotent: the
 * job's UNIQUE idempotency key dedups re-submissions, and the initial run is only created when the
 * job is genuinely new — so a client retry can never spawn a second run.
 */
public class JobService {

    private final JobStore jobStore;
    private final JobRunStore runStore;
    private final JdbcTemplate jdbc;
    private final Clock clock;
    private final int totalShards;

    public JobService(JobStore jobStore, JobRunStore runStore, JdbcTemplate jdbc,
                      Clock clock, int totalShards) {
        this.jobStore = jobStore;
        this.runStore = runStore;
        this.jdbc = jdbc;
        this.clock = clock;
        this.totalShards = totalShards;
    }

    @Transactional
    public JobSubmission submit(SubmitCommand cmd) {
        Instant now = clock.instant();
        Instant scheduledFor = cmd.scheduledFor() != null ? cmd.scheduledFor() : now;
        String payload = cmd.payload() == null ? "{}" : cmd.payload();

        DeliveryPolicy delivery = cmd.deliveryPolicy() != null ? cmd.deliveryPolicy() : DeliveryPolicy.AT_LEAST_ONCE;
        MisfirePolicy  misfire  = cmd.misfirePolicy()  != null ? cmd.misfirePolicy()  : MisfirePolicy.FIRE_ONCE;
        RetryPolicy    retry    = cmd.retryPolicy()    != null ? cmd.retryPolicy()    : RetryPolicy.defaults();
        ZoneId         zone     = cmd.timezone()       != null ? ZoneId.of(cmd.timezone()) : ZoneId.of("UTC");

        String jobKey = cmd.idempotencyKey() != null && !cmd.idempotencyKey().isBlank()
                ? cmd.idempotencyKey()
                : IdempotencyKeys.deriveJobKey(cmd.jobType(), payload);

        UUID jobUuid  = UUID.randomUUID();
        int  shardId  = ShardingUtils.computeShardId(jobUuid, totalShards);

        Job job = new Job(jobUuid, cmd.jobType(), payload, jobKey, delivery,
                cmd.cronExpr(), zone, misfire, retry, now,
                shardId, cmd.priority(), cmd.tenantId());

        UUID jobId  = jobStore.insertIfAbsent(job);
        boolean created = jobId.equals(job.id());

        // Persist callback URL only for newly created jobs (not deduplicated ones)
        if (created && cmd.callbackUrl() != null && !cmd.callbackUrl().isBlank()) {
            jobStore.setCallbackUrl(jobId, cmd.callbackUrl());
        }

        if (!created) {
            // Deduplicated: return ids of the already-existing job and its first run.
            List<JobRun> existing = runStore.findByJobId(jobId);
            UUID existingRunId = existing.isEmpty() ? null : existing.get(0).id();
            return new JobSubmission(jobId, existingRunId, false);
        }

        // Recurring job: set next_fire_time; CronScheduler enqueues runs as occurrences come due.
        if (job.isRecurring()) {
            initNextFireTime(jobId, job);
            return new JobSubmission(jobId, null, true);
        }

        // One-off / delayed job: create the initial SCHEDULED run.
        JobRun run = new JobRun(
                UUID.randomUUID(), jobId, JobState.SCHEDULED, 0, scheduledFor,
                null, null, 0L,
                IdempotencyKeys.deriveRunKey(jobKey, scheduledFor),
                null, null, now, null, null, shardId, cmd.priority(), cmd.tenantId());
        runStore.insert(run);
        return new JobSubmission(jobId, run.id(), true);
    }

    public Optional<Job> findJob(UUID jobId) {
        return jobStore.findById(jobId);
    }

    public Optional<JobRun> findRun(UUID runId) {
        return runStore.findById(runId);
    }

    public List<JobRun> findRuns(UUID jobId) {
        return runStore.findByJobId(jobId);
    }

    /** Cancel a job: marks all non-terminal runs CANCELLED and stops future cron fires. */
    @Transactional
    public int cancel(UUID jobId) {
        jdbc.update("UPDATE jobs SET next_fire_time = NULL WHERE id = ?", jobId);
        return runStore.cancelByJobId(jobId);
    }

    /** Next scheduled fire time for a recurring job; empty for one-off jobs. */
    public Optional<Instant> nextFireTime(UUID jobId) {
        Timestamp ts = jdbc.queryForObject(
                "SELECT next_fire_time FROM jobs WHERE id = ?",
                (rs, r) -> rs.getTimestamp(1), jobId);
        return Optional.ofNullable(ts).map(Timestamp::toInstant);
    }

    private void initNextFireTime(UUID jobId, Job job) {
        try {
            CronExpression expr  = CronParser.parse(job.cronExpr());
            ZonedDateTime  first = CronEvaluator.next(expr, ZonedDateTime.ofInstant(clock.instant(), job.timezone()));
            jdbc.update("UPDATE jobs SET next_fire_time = ? WHERE id = ?",
                    Timestamp.from(first.toInstant()), jobId);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "invalid cron expression '" + job.cronExpr() + "': " + e.getMessage(), e);
        }
    }
}
