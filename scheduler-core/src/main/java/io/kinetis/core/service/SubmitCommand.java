package io.kinetis.core.service;

import io.kinetis.core.model.DeliveryPolicy;
import io.kinetis.core.model.MisfirePolicy;
import io.kinetis.core.model.RetryPolicy;

import java.time.Instant;

/**
 * A request to submit a job. Null fields get defaults applied in {@link JobService#submit}.
 * {@code timezone} is a string here because it arrives from the API as a raw string — conversion
 * to {@link java.time.ZoneId} happens inside {@code JobService} when building the {@code Job}.
 */
public record SubmitCommand(
        String jobType,
        String payload,
        String idempotencyKey,
        DeliveryPolicy deliveryPolicy,
        Instant scheduledFor,
        String cronExpr,
        String timezone,
        MisfirePolicy misfirePolicy,
        RetryPolicy retryPolicy,
        int priority,
        String tenantId,
        /** Optional URL to POST when the job reaches a terminal state. */
        String callbackUrl
) {
    /** Backwards-compatible constructor without callbackUrl — existing callers unaffected. */
    public SubmitCommand(String jobType, String payload, String idempotencyKey,
                         DeliveryPolicy deliveryPolicy, Instant scheduledFor,
                         String cronExpr, String timezone, MisfirePolicy misfirePolicy,
                         RetryPolicy retryPolicy, int priority, String tenantId) {
        this(jobType, payload, idempotencyKey, deliveryPolicy, scheduledFor,
             cronExpr, timezone, misfirePolicy, retryPolicy, priority, tenantId, null);
    }
}
