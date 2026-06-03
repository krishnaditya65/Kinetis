package io.kinetis.core.workflow;

import java.util.Map;
import java.util.UUID;

/** Result of {@link WorkflowService#submit}. Maps caller node-id → assigned run UUID. */
public record WorkflowSubmission(UUID workflowId, Map<String, UUID> nodeRunIds) {}
