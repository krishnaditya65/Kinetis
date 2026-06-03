package io.kinetis.core.workflow;

import io.kinetis.core.model.RetryPolicy;

/**
 * One node in a DAG submission — maps to exactly one {@code job_run}.
 *
 * @param id        caller-assigned stable label; used in {@link DagEdge} references
 * @param jobType   handler name (looked up in HandlerRegistry at execution time)
 * @param payload   JSON payload, opaque to the scheduler
 * @param retryPolicy if null, job-level defaults apply
 */
public record DagNode(
        String id,
        String jobType,
        String payload,
        RetryPolicy retryPolicy,
        int priority,
        String tenantId
) {}
