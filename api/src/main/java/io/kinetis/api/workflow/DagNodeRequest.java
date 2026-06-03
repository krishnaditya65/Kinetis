package io.kinetis.api.workflow;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotBlank;

/** One node in a workflow submission request. */
public record DagNodeRequest(
        @NotBlank String id,
        @NotBlank String jobType,
        JsonNode payload,
        Integer maxAttempts,
        Long backoffBaseMs,
        Double backoffFactor,
        Integer priority,
        String tenantId
) {}
