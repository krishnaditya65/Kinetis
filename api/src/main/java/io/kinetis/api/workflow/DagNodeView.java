package io.kinetis.api.workflow;

import io.kinetis.core.workflow.WorkflowStore;

import java.util.UUID;

/** Read model for a single node in a workflow. */
public record DagNodeView(String nodeId, UUID runId, String state, UUID jobId) {

    public static DagNodeView from(WorkflowStore.WorkflowNodeRow row) {
        return new DagNodeView(row.nodeId(), row.runId(), row.state().name(), row.jobId());
    }
}
