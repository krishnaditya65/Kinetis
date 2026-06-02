package io.kinetis.core.cron;

import io.kinetis.core.idempotency.IdempotencyKeys;
import io.kinetis.core.metrics.SchedulerMetrics;
import io.kinetis.core.model.DeliveryPolicy;
import io.kinetis.core.model.Job;
import io.kinetis.core.model.JobRun;
import io.kinetis.core.model.JobState;
import io.kinetis.core.model.MisfirePolicy;
import io.kinetis.core.model.RetryPolicy;
import io.kinetis.core.shard.ShardOwnershipProvider;
import io.kinetis.core.store.JobRunStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Array;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Polls for cron jobs whose {@code next_fire_time} is due and, for each one:
 * <ol>
 *   <li>Applies the misfire policy if the fire time is significantly in the past.</li>
 *   <li>Creates a {@code SCHEDULED} {@link JobRun} for this occurrence.</li>
 *   <li>Advances {@code next_fire_time} — all in one transaction.</li>
 * </ol>
 *
 * <p>The run insert and next_fire_time update are atomic. A crash between them is impossible;
 * the outbox pattern isn't needed because both writes go to the same Postgres transaction.
 * {@code FOR UPDATE SKIP LOCKED} ensures concurrent nodes don't double-fire the same job.
 */
public class CronScheduler {

    private static final Logger log = LoggerFactory.getLogger(CronScheduler.class);
    private static final long MISFIRE_THRESHOLD_SECONDS = 60;

    private final JdbcTemplate jdbc;
    private final JobRunStore runStore;
    private final SchedulerMetrics metrics;
    private final ShardOwnershipProvider shardOwnership;
    private final Clock clock;
    private final int batchSize;

    public CronScheduler(JdbcTemplate jdbc, JobRunStore runStore, SchedulerMetrics metrics,
                         ShardOwnershipProvider shardOwnership, Clock clock, int batchSize) {
        this.jdbc           = jdbc;
        this.runStore       = runStore;
        this.metrics        = metrics;
        this.shardOwnership = shardOwnership;
        this.clock          = clock;
        this.batchSize      = batchSize;
    }

    public int tick() {
        List<Job> due = findDueCronJobs();
        if (due.isEmpty()) return 0;
        int fired = 0;
        for (Job job : due) {
            try {
                if (fireAndAdvance(job)) fired++;
            } catch (Exception e) {
                log.warn("cron fire failed for job {}", job.id(), e);
            }
        }
        return fired;
    }

    private List<Job> findDueCronJobs() {
        var owned = shardOwnership.ownedShards();
        if (owned.isEmpty()) return List.of();
        Integer[] shardArray = owned.toArray(Integer[]::new);
        return jdbc.query(
                conn -> {
                    Array pgShards = conn.createArrayOf("int2", shardArray);
                    var ps = conn.prepareStatement("""
                            SELECT * FROM jobs
                            WHERE cron_expr IS NOT NULL
                              AND next_fire_time IS NOT NULL
                              AND next_fire_time <= now()
                              AND shard_id = ANY(?)
                            ORDER BY next_fire_time
                            LIMIT ?
                            FOR UPDATE SKIP LOCKED
                            """);
                    ps.setArray(1, pgShards);
                    ps.setInt(2, batchSize);
                    return ps;
                },
                (rs, row) -> new Job(
                        rs.getObject("id", UUID.class),
                        rs.getString("job_type"),
                        rs.getString("payload"),
                        rs.getString("idempotency_key"),
                        DeliveryPolicy.AT_LEAST_ONCE,
                        rs.getString("cron_expr"),
                        java.time.ZoneId.of(rs.getString("timezone")),
                        MisfirePolicy.valueOf(rs.getString("misfire_policy")),
                        new RetryPolicy(rs.getInt("max_attempts"), rs.getLong("backoff_base_ms"),
                                rs.getDouble("backoff_factor")),
                        rs.getTimestamp("created_at").toInstant(),
                        rs.getInt("shard_id"), rs.getInt("priority"), rs.getString("tenant_id")));
    }

    @Transactional
    boolean fireAndAdvance(Job job) {
        Instant now = clock.instant();
        ZonedDateTime nowZdt = ZonedDateTime.ofInstant(now, job.timezone());

        CronExpression expr;
        try {
            expr = CronParser.parse(job.cronExpr());
        } catch (CronParseException e) {
            log.error("invalid cron '{}' for job {}: {}", job.cronExpr(), job.id(), e.getMessage());
            jdbc.update("UPDATE jobs SET next_fire_time = NULL WHERE id = ?", job.id());
            return false;
        }

        Instant storedNextFire = jdbc.queryForObject(
                "SELECT next_fire_time FROM jobs WHERE id = ?",
                (rs, r) -> { Timestamp ts = rs.getTimestamp(1); return ts != null ? ts.toInstant() : null; },
                job.id());
        if (storedNextFire == null) return false;

        ZonedDateTime scheduledFire = ZonedDateTime.ofInstant(storedNextFire, job.timezone());
        long secondsLate = java.time.Duration.between(scheduledFire, nowZdt).toSeconds();
        ZonedDateTime nextFire = CronEvaluator.next(expr, nowZdt);
        boolean isMisfire = secondsLate > MISFIRE_THRESHOLD_SECONDS;

        if (isMisfire) {
            switch (job.misfirePolicy()) {
                case SKIP -> {
                    log.info("cron misfire SKIP for job {} ({}s late)", job.id(), secondsLate);
                    advanceNextFireTime(job.id(), nextFire);
                    return false;
                }
                case FIRE_ONCE -> {
                    log.info("cron misfire FIRE_ONCE for job {} ({}s late)", job.id(), secondsLate);
                    enqueueRun(job, storedNextFire);
                    advanceNextFireTime(job.id(), nextFire);
                    return true;
                }
                case CATCH_UP -> {
                    log.info("cron misfire CATCH_UP for job {} ({}s late)", job.id(), secondsLate);
                    List<ZonedDateTime> missed = CronEvaluator.allBetween(
                            expr, scheduledFire.minusSeconds(1), nowZdt);
                    missed.forEach(t -> enqueueRun(job, t.toInstant()));
                    advanceNextFireTime(job.id(), nextFire);
                    return !missed.isEmpty();
                }
            }
        }

        enqueueRun(job, storedNextFire);
        advanceNextFireTime(job.id(), nextFire);
        return true;
    }

    private void enqueueRun(Job job, Instant scheduledFor) {
        String runKey = IdempotencyKeys.deriveRunKey(job.idempotencyKey(), scheduledFor);
        Long existing = jdbc.queryForObject(
                "SELECT count(*) FROM job_runs WHERE idempotency_key = ?", Long.class, runKey);
        if (existing != null && existing > 0) return;

        runStore.insert(new JobRun(
                UUID.randomUUID(), job.id(), JobState.SCHEDULED, 0, scheduledFor,
                null, null, 0L, runKey,
                null, null, clock.instant(), null, null,
                job.shardId(), job.priority(), job.tenantId()));
        metrics.onSubmitted();
    }

    private void advanceNextFireTime(UUID jobId, ZonedDateTime next) {
        jdbc.update("UPDATE jobs SET next_fire_time = ? WHERE id = ?",
                Timestamp.from(next.toInstant()), jobId);
    }
}
