package io.kinetis.core.workflow;

import java.time.Instant;
import java.util.UUID;

/**
 * A DAG workflow definition — the header row that groups a set of {@code job_runs} and
 * expresses their dependency ordering.
 */
public record Workflow(
        UUID id,
        FailurePolicy failurePolicy,
        WorkflowState state,
        Instant createdAt
) {}
