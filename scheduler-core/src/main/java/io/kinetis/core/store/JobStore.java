package io.kinetis.core.store;

import io.kinetis.core.model.Job;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** DAO for {@code jobs} (definitions). All SQL for this table lives here. */
public class JobStore {

    private final JdbcTemplate jdbc;

    public JobStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Insert a job, deduplicating on its idempotency key. If a job with the same key already
     * exists the existing id is returned and nothing is inserted — making submission itself
     * idempotent (a client retrying "create job" never creates two).
     *
     * @return the id of the new or pre-existing job
     */
    public UUID insertIfAbsent(Job job) {
        // ON CONFLICT DO NOTHING does not RETURN the existing row, so we do it in two steps.
        int inserted = jdbc.update("""
                INSERT INTO jobs (id, job_type, payload, idempotency_key, delivery_policy,
                                  cron_expr, timezone, misfire_policy,
                                  max_attempts, backoff_base_ms, backoff_factor, created_at,
                                  shard_id, priority, tenant_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (idempotency_key) DO NOTHING
                """,
                job.id(), job.jobType(), jsonb(job.payload()), job.idempotencyKey(),
                job.deliveryPolicy().name(), job.cronExpr(), job.timezone().getId(),
                job.misfirePolicy().name(), job.retryPolicy().maxAttempts(),
                job.retryPolicy().backoffBaseMs(), job.retryPolicy().backoffFactor(),
                Timestamp.from(job.createdAt()), (short) job.shardId(),
                (short) job.priority(), job.tenantId());

        if (inserted > 0) return job.id();
        return jdbc.queryForObject(
                "SELECT id FROM jobs WHERE idempotency_key = ?", UUID.class, job.idempotencyKey());
    }

    public Optional<Job> findById(UUID id) {
        List<Job> rows = jdbc.query("SELECT * FROM jobs WHERE id = ?", Mappers.JOB, id);
        return rows.stream().findFirst();
    }

    /** Retrieve the callback URL for a job, or null if not set. */
    public String findCallbackUrl(UUID jobId) {
        List<String> rows = jdbc.query(
                "SELECT callback_url FROM jobs WHERE id = ?",
                (rs, r) -> rs.getString(1), jobId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public void setCallbackUrl(UUID jobId, String callbackUrl) {
        jdbc.update("UPDATE jobs SET callback_url = ? WHERE id = ?", callbackUrl, jobId);
    }

    static PGobject jsonb(String json) {
        PGobject obj = new PGobject();
        obj.setType("jsonb");
        try {
            obj.setValue(json == null || json.isBlank() ? "{}" : json);
        } catch (SQLException e) {
            throw new IllegalArgumentException("invalid jsonb payload", e);
        }
        return obj;
    }
}
