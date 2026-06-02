package io.kinetis.core.model;

import java.time.Instant;
import java.time.ZoneId;
import java.util.UUID;

/**
 * A job <em>definition</em> — what to run and (optionally) how to recur. One {@code Job} can
 * spawn many {@link JobRun}s (one per cron fire, or one for a one-off). Kept separate from the
 * run so the definition is immutable history and recurrence/retry policy live in one place.
 *
 * @param jobType        handler name used to look up the {@code JobHandler} in the registry
 * @param payload        handler arguments, stored as JSONB; the scheduler treats this as opaque
 * @param cronExpr       {@code null} for a one-off/delayed job; a cron expression string for recurring
 * @param timezone       time zone for cron evaluation; {@link ZoneId#systemDefault()} if omitted
 * @param priority       higher = more urgent; default 0; negative allowed for background tasks
 * @param tenantId       optional grouping key for rate limiting and fair-share scheduling
 */
public record Job(
        UUID id,
        String jobType,
        String payload,
        String idempotencyKey,
        DeliveryPolicy deliveryPolicy,
        String cronExpr,
        ZoneId timezone,
        MisfirePolicy misfirePolicy,
        RetryPolicy retryPolicy,
        Instant createdAt,
        int shardId,
        int priority,
        String tenantId
) {
    public boolean isRecurring() {
        return cronExpr != null && !cronExpr.isBlank();
    }
}
