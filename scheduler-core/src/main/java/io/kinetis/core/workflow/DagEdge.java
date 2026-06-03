package io.kinetis.core.workflow;

/**
 * A directed dependency edge in a DAG: node {@code from} must SUCCEED before node {@code to}
 * transitions from PENDING_DEPS to SCHEDULED.
 */
public record DagEdge(String from, String to) {}
