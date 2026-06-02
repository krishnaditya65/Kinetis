package io.kinetis.core.model;

import java.time.Instant;
import java.util.UUID;

/**
 * A single <em>execution</em> of a {@link Job}: the leased, fenced unit of work the scheduler
 * and workers move through the {@link JobState} lifecycle.
 *
 * @param leaseToken the fencing token — monotonically bumped on every lease grant. A worker must
 *                   present the token it received on every state-mutating write; a stale (zombie)
 *                   worker's lower token is rejected at the database. This is what makes
 *                   at-least-once delivery safe under GC pauses and network partitions.
 */
public record JobRun(
        UUID id,
        UUID jobId,
        JobState state,
        int attempt,
        Instant scheduledFor,
        String leaseOwner,
        Instant leaseExpiresAt,
        long leaseToken,
        String idempotencyKey,
        Instant lastHeartbeatAt,
        String lastError,
        Instant enqueuedAt,
        Instant startedAt,
        Instant finishedAt,
        int shardId,
        int priority,
        String tenantId
) {
}
