package io.kinetis.api.dto;

import io.kinetis.core.model.Job;

import java.time.Instant;
import java.util.UUID;

/** Read model for a job definition. */
public record JobView(
        UUID id,
        String jobType,
        String payload,
        String idempotencyKey,
        String deliveryPolicy,
        String cronExpr,
        String timezone,
        String misfirePolicy,
        int maxAttempts,
        long backoffBaseMs,
        double backoffFactor,
        Instant createdAt
) {
    public static JobView from(Job j) {
        return new JobView(
                j.id(), j.jobType(), j.payload(), j.idempotencyKey(),
                j.deliveryPolicy().name(), j.cronExpr(),
                j.timezone().getId(),
                j.misfirePolicy().name(),
                j.retryPolicy().maxAttempts(), j.retryPolicy().backoffBaseMs(),
                j.retryPolicy().backoffFactor(), j.createdAt());
    }
}
