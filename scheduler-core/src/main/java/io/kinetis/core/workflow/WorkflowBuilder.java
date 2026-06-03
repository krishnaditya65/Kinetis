package io.kinetis.core.workflow;

import java.util.ArrayList;
import java.util.List;

/**
 * Fluent Java DSL for building and submitting DAG workflows programmatically.
 *
 * <pre>
 * WorkflowSubmission result = WorkflowBuilder.create()
 *     .failFast()
 *     .node("extract",   "etl.extract",   "{\"source\":\"s3://bucket/data\"}")
 *     .node("transform", "etl.transform", "{}")
 *     .node("load",      "etl.load",      "{\"target\":\"warehouse\"}")
 *     .edge("extract",   "transform")
 *     .edge("transform", "load")
 *     .submit(workflowService);
 * </pre>
 *
 * <p>Compiles to the same {@link DagNode}/{@link DagEdge} lists passed to
 * {@link WorkflowService#submit} — no separate code path.
 */
public final class WorkflowBuilder {

    private final List<DagNode> nodes = new ArrayList<>();
    private final List<DagEdge> edges = new ArrayList<>();
    private FailurePolicy policy = FailurePolicy.FAIL_FAST;

    private WorkflowBuilder() {}

    public static WorkflowBuilder create() {
        return new WorkflowBuilder();
    }

    // ---- failure policy ----

    public WorkflowBuilder failFast()        { policy = FailurePolicy.FAIL_FAST;        return this; }
    public WorkflowBuilder wait()            { policy = FailurePolicy.WAIT;             return this; }
    public WorkflowBuilder skipDownstream()  { policy = FailurePolicy.SKIP_DOWNSTREAM;  return this; }
    public WorkflowBuilder onFailure(FailurePolicy p) { policy = p; return this; }

    // ---- nodes ----

    public WorkflowBuilder node(String id, String jobType) {
        return node(id, jobType, "{}");
    }

    public WorkflowBuilder node(String id, String jobType, String payloadJson) {
        return node(id, jobType, payloadJson, null, 0, null);
    }

    public WorkflowBuilder node(String id, String jobType, String payloadJson,
                                 RetryPolicy retryPolicy, int priority, String tenantId) {
        nodes.add(new DagNode(id, jobType, payloadJson, retryPolicy, priority, tenantId));
        return this;
    }

    // ---- edges ----

    /** {@code from} must succeed before {@code to} starts. */
    public WorkflowBuilder edge(String from, String to) {
        edges.add(new DagEdge(from, to));
        return this;
    }

    /**
     * Chain nodes sequentially: A → B → C → ...
     * Each node must have already been added via {@link #node}.
     */
    public WorkflowBuilder chain(String... nodeIds) {
        for (int i = 0; i < nodeIds.length - 1; i++) {
            edges.add(new DagEdge(nodeIds[i], nodeIds[i + 1]));
        }
        return this;
    }

    /**
     * Fan-out: {@code source} → each target runs in parallel after source succeeds.
     */
    public WorkflowBuilder fanOut(String source, String... targets) {
        for (String target : targets) edges.add(new DagEdge(source, target));
        return this;
    }

    /**
     * Fan-in: all sources must succeed before {@code sink} starts.
     */
    public WorkflowBuilder fanIn(String sink, String... sources) {
        for (String source : sources) edges.add(new DagEdge(source, sink));
        return this;
    }

    // ---- build / submit ----

    public List<DagNode> buildNodes()  { return List.copyOf(nodes); }
    public List<DagEdge> buildEdges()  { return List.copyOf(edges); }
    public FailurePolicy buildPolicy() { return policy; }

    /** Validate and submit to the service in one call. */
    public WorkflowSubmission submit(WorkflowService service) {
        return service.submit(List.copyOf(nodes), List.copyOf(edges), policy);
    }
}
