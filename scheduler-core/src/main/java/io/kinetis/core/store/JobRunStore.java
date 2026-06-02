package io.kinetis.core.store;

import io.kinetis.core.model.JobRun;
import io.kinetis.core.model.JobState;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * DAO for {@code job_runs} — plain inserts and reads only. State transitions (lease, heartbeat,
 * success, retry) live in {@link io.kinetis.core.lease.LeaseManager} because they carry fencing
 * guards. Keeping transitions out of here makes the "every mutation is fenced" rule easy to audit.
 */
public class JobRunStore {

    private final JdbcTemplate jdbc;

    public JobRunStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Insert the initial SCHEDULED run for a freshly-submitted job. */
    public void insert(JobRun run) {
        jdbc.update("""
                INSERT INTO job_runs (id, job_id, state, attempt, scheduled_for,
                                      lease_token, idempotency_key, enqueued_at,
                                      shard_id, priority, tenant_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                run.id(), run.jobId(), run.state().name(), run.attempt(),
                Timestamp.from(run.scheduledFor()), run.leaseToken(),
                run.idempotencyKey(), Timestamp.from(run.enqueuedAt()),
                (short) run.shardId(), (short) run.priority(), run.tenantId());
    }

    public Optional<JobRun> findById(UUID id) {
        List<JobRun> rows = jdbc.query("SELECT * FROM job_runs WHERE id = ?", Mappers.JOB_RUN, id);
        return rows.stream().findFirst();
    }

    public List<JobRun> findByJobId(UUID jobId) {
        return jdbc.query(
                "SELECT * FROM job_runs WHERE job_id = ? ORDER BY enqueued_at",
                Mappers.JOB_RUN, jobId);
    }

    /**
     * Cancel all non-terminal runs of a job. No fencing needed: CANCELLED is a terminal state
     * and a concurrently-running worker will be fenced off at its own next write.
     *
     * @return number of runs cancelled
     */
    public int cancelByJobId(UUID jobId) {
        return jdbc.update("""
                UPDATE job_runs SET state = ?, finished_at = now()
                WHERE job_id = ? AND state IN ('SCHEDULED', 'LEASED', 'RUNNING', 'FAILED')
                """, JobState.CANCELLED.name(), jobId);
    }
}
