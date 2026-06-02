package io.kinetis.core.store;

import io.kinetis.core.model.DeliveryPolicy;
import io.kinetis.core.model.Job;
import io.kinetis.core.model.JobRun;
import io.kinetis.core.model.JobState;
import io.kinetis.core.model.MisfirePolicy;
import io.kinetis.core.model.RetryPolicy;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.UUID;

/** JDBC row mappers for the core entities. Column-to-field mapping lives here and nowhere else. */
public final class Mappers {

    public static final RowMapper<Job> JOB = Mappers::mapJob;
    public static final RowMapper<JobRun> JOB_RUN = Mappers::mapJobRun;

    private Mappers() {}

    private static Job mapJob(ResultSet rs, int rowNum) throws SQLException {
        return new Job(
                rs.getObject("id", UUID.class),
                rs.getString("job_type"),
                rs.getString("payload"),
                rs.getString("idempotency_key"),
                DeliveryPolicy.valueOf(rs.getString("delivery_policy")),
                rs.getString("cron_expr"),
                ZoneId.of(rs.getString("timezone")),
                MisfirePolicy.valueOf(rs.getString("misfire_policy")),
                new RetryPolicy(
                        rs.getInt("max_attempts"),
                        rs.getLong("backoff_base_ms"),
                        rs.getDouble("backoff_factor")),
                instant(rs, "created_at"),
                rs.getInt("shard_id"),
                rs.getInt("priority"),
                rs.getString("tenant_id"));
    }

    private static JobRun mapJobRun(ResultSet rs, int rowNum) throws SQLException {
        return new JobRun(
                rs.getObject("id", UUID.class),
                rs.getObject("job_id", UUID.class),
                JobState.valueOf(rs.getString("state")),
                rs.getInt("attempt"),
                instant(rs, "scheduled_for"),
                rs.getString("lease_owner"),
                instant(rs, "lease_expires_at"),
                rs.getLong("lease_token"),
                rs.getString("idempotency_key"),
                instant(rs, "last_heartbeat_at"),
                rs.getString("last_error"),
                instant(rs, "enqueued_at"),
                instant(rs, "started_at"),
                instant(rs, "finished_at"),
                rs.getInt("shard_id"),
                rs.getInt("priority"),
                rs.getString("tenant_id"));
    }

    /** Read a TIMESTAMPTZ column as an {@link Instant} (null-safe). */
    static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime odt = rs.getObject(column, OffsetDateTime.class);
        return odt == null ? null : odt.toInstant();
    }
}
