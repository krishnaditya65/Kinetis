package io.kinetis.api.dto;

import com.fasterxml.jackson.databind.JsonNode;
import io.kinetis.core.model.DeliveryPolicy;
import io.kinetis.core.model.MisfirePolicy;
import jakarta.validation.constraints.NotBlank;

/**
 * Job submission payload. Only {@code jobType} is required.
 * {@code scheduleAt} accepts an ISO-8601 instant or a relative offset: {@code +5s}, {@code +2m},
 * {@code +1h}, {@code +500ms}, {@code +3d}. Absent = due immediately.
 */
public record SubmitJobRequest(
        @NotBlank String jobType,
        JsonNode payload,
        String idempotencyKey,
        DeliveryPolicy deliveryPolicy,
        String scheduleAt,
        String cronExpr,
        String timezone,
        MisfirePolicy misfirePolicy,
        Integer maxAttempts,
        Long backoffBaseMs,
        Double backoffFactor,
        Integer priority,
        String tenantId,
        /** Optional URL to POST when this job reaches a terminal state. */
        String callbackUrl
) {
}
