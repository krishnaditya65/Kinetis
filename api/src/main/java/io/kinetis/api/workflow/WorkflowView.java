package io.kinetis.api.workflow;

import io.kinetis.core.workflow.Workflow;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Read model for a complete workflow with its nodes. */
public record WorkflowView(
        UUID id,
        String state,
        String onFailure,
        Instant createdAt,
        List<DagNodeView> nodes
) {
    public static WorkflowView from(Workflow w, List<DagNodeView> nodes) {
        return new WorkflowView(w.id(), w.state().name(), w.failurePolicy().name(),
                w.createdAt(), nodes);
    }
}
