package io.kinetis.api.workflow;

import io.kinetis.core.model.RetryPolicy;
import io.kinetis.core.workflow.DagEdge;
import io.kinetis.core.workflow.DagNode;
import io.kinetis.core.workflow.FailurePolicy;
import io.kinetis.core.workflow.Workflow;
import io.kinetis.core.workflow.WorkflowService;
import io.kinetis.core.workflow.WorkflowSubmission;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/workflows")
public class WorkflowController {

    private final WorkflowService workflowService;

    public WorkflowController(WorkflowService workflowService) {
        this.workflowService = workflowService;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> submit(
            @Valid @RequestBody SubmitWorkflowRequest req) {

        List<DagNode> nodes = req.nodes().stream().map(n -> new DagNode(
                n.id(), n.jobType(),
                n.payload() == null ? "{}" : n.payload().toString(),
                retryPolicy(n),
                n.priority() != null ? n.priority() : 0,
                n.tenantId())).toList();

        List<DagEdge> edges = req.edges() == null ? List.of() :
                req.edges().stream().map(e -> new DagEdge(e.from(), e.to())).toList();

        FailurePolicy policy = req.onFailure() == null ? FailurePolicy.FAIL_FAST :
                FailurePolicy.valueOf(req.onFailure().toUpperCase());

        WorkflowSubmission submission = workflowService.submit(nodes, edges, policy, req.callbackUrl());

        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "workflowId", submission.workflowId(),
                "nodeRunIds", submission.nodeRunIds()));
    }

    @GetMapping("/{workflowId}")
    public WorkflowView getWorkflow(@PathVariable UUID workflowId) {
        Workflow wf = workflowService.findWorkflow(workflowId)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "workflow not found: " + workflowId));
        List<DagNodeView> nodes = workflowService.findNodes(workflowId).stream()
                .map(DagNodeView::from).toList();
        return WorkflowView.from(wf, nodes);
    }

    @DeleteMapping("/{workflowId}")
    public Map<String, Object> cancel(@PathVariable UUID workflowId) {
        workflowService.findWorkflow(workflowId)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "workflow not found: " + workflowId));
        int cancelled = workflowService.cancel(workflowId);
        return Map.of("workflowId", workflowId, "cancelledNodes", cancelled);
    }

    private static RetryPolicy retryPolicy(DagNodeRequest n) {
        if (n.maxAttempts() == null && n.backoffBaseMs() == null && n.backoffFactor() == null)
            return null;
        RetryPolicy d = RetryPolicy.defaults();
        return new RetryPolicy(
                n.maxAttempts()   != null ? n.maxAttempts()   : d.maxAttempts(),
                n.backoffBaseMs() != null ? n.backoffBaseMs() : d.backoffBaseMs(),
                n.backoffFactor() != null ? n.backoffFactor() : d.backoffFactor());
    }
}
