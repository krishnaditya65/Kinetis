package io.kinetis.core.lease;

import io.kinetis.core.model.JobRun;
import io.kinetis.core.store.Mappers;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Array;
import java.sql.Connection;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Owns every state-mutating transition on {@code job_runs}. Two invariants make the whole
 * distributed protocol correct:
 *
 * <ol>
 *   <li><b>SKIP LOCKED leasing</b> — many scheduler nodes can poll the same table concurrently
 *       and each claims a <em>disjoint</em> set of due runs with no external coordinator and no
 *       double-dispatch.</li>
 *   <li><b>Fencing on every write</b> — each mutation is guarded by
 *       {@code WHERE lease_token = :expectedToken}, and {@link #claimDue} bumps the token on
 *       every grant. A stalled (zombie) worker that wakes after its lease was reassigned carries
 *       a now-stale token, so its write matches zero rows and is harmlessly rejected. Each
 *       mutator returns {@code true} only if it actually won the row.</li>
 * </ol>
 */
public class LeaseManager {

    private final JdbcTemplate jdbc;

    public LeaseManager(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Atomically claim up to {@code batchSize} due runs for {@code owner}, leasing each for
     * {@code leaseTtl}. Uses {@code FOR UPDATE SKIP LOCKED} so concurrent schedulers on the
     * same shard never collide.
     *
     * <p>{@code ownedShards} scopes the claim to rows whose {@code shard_id} is in this node's
     * assigned partition. Each node polls only its own slice of the table, eliminating cross-node
     * SKIP LOCKED contention while still providing intra-shard deduplication.
     *
     * @param ownedShards shard IDs this node may claim from; empty set → nothing claimed
     * @return the runs now owned by this caller, each with a freshly-bumped {@code leaseToken}
     */
    public List<JobRun> claimDue(String owner, int batchSize, Duration leaseTtl,
                                  Set<Integer> ownedShards) {
        if (ownedShards == null || ownedShards.isEmpty()) {
            return List.of();
        }
        Integer[] shardArray = ownedShards.toArray(Integer[]::new);
        return jdbc.query(
                (Connection conn) -> {
                    Array pgShards = conn.createArrayOf("int2", shardArray);
                    var ps = conn.prepareStatement("""
                            UPDATE job_runs
                            SET state            = 'LEASED',
                                lease_owner      = ?,
                                lease_expires_at = now() + make_interval(secs => ?),
                                lease_token      = lease_token + 1
                            WHERE id IN (
                                SELECT id FROM job_runs
                                WHERE state         = 'SCHEDULED'
                                  AND scheduled_for <= now()
                                  AND shard_id      = ANY(?)
                                ORDER BY priority DESC, scheduled_for ASC
                                FOR UPDATE SKIP LOCKED
                                LIMIT ?
                            )
                            RETURNING *
                            """);
                    ps.setString(1, owner);
                    ps.setInt(2, (int) leaseTtl.toSeconds());
                    ps.setArray(3, pgShards);
                    ps.setInt(4, batchSize);
                    return ps;
                }, Mappers.JOB_RUN);
    }

    /**
     * LEASED → SCHEDULED: return a run to the queue without executing it.
     * Used by the fair-share dispatcher when a tenant's token bucket is exhausted.
     * Bumps the token so any worker that already received this run assignment is fenced off.
     */
    public boolean returnToScheduled(UUID runId, long token) {
        return jdbc.update("""
                UPDATE job_runs
                SET state            = 'SCHEDULED',
                    lease_owner      = NULL,
                    lease_expires_at = NULL,
                    lease_token      = lease_token + 1
                WHERE id = ? AND lease_token = ? AND state = 'LEASED'
                """, runId, token) == 1;
    }

    /** LEASED → RUNNING. Records {@code started_at} and first heartbeat. Fenced on {@code token}. */
    public boolean markRunning(UUID runId, long token) {
        return jdbc.update("""
                UPDATE job_runs
                SET state             = 'RUNNING',
                    started_at        = now(),
                    last_heartbeat_at = now()
                WHERE id = ? AND lease_token = ? AND state = 'LEASED'
                """, runId, token) == 1;
    }

    /** Extend the lease while a long-running job is still active. Fenced on {@code token}. */
    public boolean heartbeat(UUID runId, long token, Duration leaseTtl) {
        return jdbc.update("""
                UPDATE job_runs
                SET lease_expires_at  = now() + make_interval(secs => ?),
                    last_heartbeat_at = now()
                WHERE id = ? AND lease_token = ? AND state IN ('LEASED', 'RUNNING')
                """, (int) leaseTtl.toSeconds(), runId, token) == 1;
    }

    /** RUNNING → SUCCEEDED. Fenced on {@code token}. */
    public boolean markSucceeded(UUID runId, long token) {
        return jdbc.update("""
                UPDATE job_runs
                SET state       = 'SUCCEEDED',
                    finished_at = now()
                WHERE id = ? AND lease_token = ? AND state = 'RUNNING'
                """, runId, token) == 1;
    }

    /**
     * Return a failed run to SCHEDULED for the next attempt. Bumps the token so the failing
     * (possibly zombie) worker is fenced from any further writes. Called both by a worker that
     * caught an exception and by the reaper reclaiming an expired lease.
     */
    public boolean rescheduleForRetry(UUID runId, long token, int newAttempt,
                                       Instant nextRunAt, String error) {
        return jdbc.update("""
                UPDATE job_runs
                SET state            = 'SCHEDULED',
                    attempt          = ?,
                    scheduled_for    = ?,
                    lease_owner      = NULL,
                    lease_expires_at = NULL,
                    lease_token      = lease_token + 1,
                    last_error       = ?,
                    started_at       = NULL
                WHERE id = ? AND lease_token = ? AND state IN ('LEASED', 'RUNNING')
                """, newAttempt, Timestamp.from(nextRunAt), truncate(error), runId, token) == 1;
    }

    /** Retries exhausted (or AT_MOST_ONCE on first failure). Fenced on {@code token}. */
    public boolean markDeadLetter(UUID runId, long token, String error) {
        return jdbc.update("""
                UPDATE job_runs
                SET state       = 'DEAD_LETTER',
                    finished_at = now(),
                    last_error  = ?,
                    lease_token = lease_token + 1
                WHERE id = ? AND lease_token = ? AND state IN ('LEASED', 'RUNNING')
                """, truncate(error), runId, token) == 1;
    }

    /**
     * Extend leases for a batch of active runs in a single DB round-trip.
     *
     * <p>Unlike the per-run {@link #heartbeat}, this does <em>not</em> check the fencing token —
     * it updates all rows in LEASED or RUNNING state. This is safe because:
     * <ol>
     *   <li>We only touch rows in active states; SCHEDULED/terminal rows are unaffected.</li>
     *   <li>If a run was reclaimed by the reaper and re-claimed by a new worker, extending its
     *       {@code lease_expires_at} is harmless — the old worker still carries a stale
     *       {@code leaseToken} and will be fenced on its next state-mutating write.</li>
     * </ol>
     * Designed to replace per-run {@link ScheduledFuture} heartbeats in {@link WorkerPool},
     * reducing heartbeat DB calls from O(active runs) to O(1) per interval.
     *
     * @return number of rows updated
     */
    public int batchHeartbeat(Set<UUID> runIds, Duration leaseTtl) {
        if (runIds == null || runIds.isEmpty()) return 0;
        return jdbc.query(
                (Connection conn) -> {
                    Array pgIds = conn.createArrayOf("uuid", runIds.toArray(UUID[]::new));
                    var ps = conn.prepareStatement("""
                            UPDATE job_runs
                            SET lease_expires_at  = now() + make_interval(secs => ?),
                                last_heartbeat_at = now()
                            WHERE id = ANY(?)
                              AND state IN ('LEASED', 'RUNNING')
                            """);
                    ps.setInt(1, (int) leaseTtl.toSeconds());
                    ps.setArray(2, pgIds);
                    return ps;
                },
                rs -> rs.next() ? rs.getInt(1) : 0);
    }

    /**
     * Find runs whose lease has expired in shards owned by this node. Scoping to owned shards
     * means each node reaps only its own partition — no two nodes race to reclaim the same lease.
     */
    public List<JobRun> findExpiredLeases(int batchSize, Set<Integer> ownedShards) {
        if (ownedShards == null || ownedShards.isEmpty()) {
            return List.of();
        }
        Integer[] shardArray = ownedShards.toArray(Integer[]::new);
        return jdbc.query(
                (Connection conn) -> {
                    Array pgShards = conn.createArrayOf("int2", shardArray);
                    var ps = conn.prepareStatement("""
                            SELECT * FROM job_runs
                            WHERE state IN ('LEASED', 'RUNNING')
                              AND lease_expires_at < now()
                              AND shard_id = ANY(?)
                            ORDER BY lease_expires_at
                            LIMIT ?
                            """);
                    ps.setArray(1, pgShards);
                    ps.setInt(2, batchSize);
                    return ps;
                }, Mappers.JOB_RUN);
    }

    private static String truncate(String s) {
        return s == null || s.length() <= 4000 ? s : s.substring(0, 4000);
    }
}
