package io.kinetis.core.workflow;

/**
 * Aggregate lifecycle state of a {@link Workflow}.
 * Derived from the terminal states of all its nodes.
 */
public enum WorkflowState {
    /** At least one node is still active (SCHEDULED, LEASED, RUNNING, or PENDING_DEPS). */
    RUNNING,
    /** All nodes reached SUCCEEDED or SKIPPED. */
    SUCCEEDED,
    /** At least one node reached DEAD_LETTER or FAILED (with no retries left). */
    FAILED,
    /** Cancelled by the user. */
    CANCELLED
}
