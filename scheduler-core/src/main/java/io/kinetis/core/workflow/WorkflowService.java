package io.kinetis.core.workflow;

import io.kinetis.core.idempotency.IdempotencyKeys;
import io.kinetis.core.model.DeliveryPolicy;
import io.kinetis.core.model.Job;
import io.kinetis.core.model.JobRun;
import io.kinetis.core.model.JobState;
import io.kinetis.core.model.MisfirePolicy;
import io.kinetis.core.model.RetryPolicy;
import io.kinetis.core.shard.ShardingUtils;
import io.kinetis.core.store.JobRunStore;
import io.kinetis.core.store.JobStore;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Application-facing entry point for DAG workflow operations.
 *
 * <p>On submit: validates the DAG, inserts all job+run rows atomically in one transaction,
 * wires dependency edges, and returns a map of node-id → run-id for the caller.
 */
public class WorkflowService {

    private final WorkflowStore workflowStore;
    private final JobStore jobStore;
    private final JobRunStore runStore;
    private final JdbcTemplate jdbc;
    private final Clock clock;
    private final int totalShards;

    public WorkflowService(WorkflowStore workflowStore, JobStore jobStore,
                           JobRunStore runStore, JdbcTemplate jdbc,
                           Clock clock, int totalShards) {
        this.workflowStore = workflowStore;
        this.jobStore      = jobStore;
        this.runStore      = runStore;
        this.jdbc          = jdbc;
        this.clock         = clock;
        this.totalShards   = totalShards;
    }

    /**
     * Submit a DAG. All rows are written in one transaction — the workflow either fully appears
     * or fully rolls back.
     *
     * @return map of caller node-id → assigned run UUID
     */
    @Transactional
    public WorkflowSubmission submit(List<DagNode> nodes, List<DagEdge> edges,
                                      FailurePolicy policy) {
        DagValidator.validate(nodes, edges);

        UUID workflowId = UUID.randomUUID();
        workflowStore.insertWorkflow(workflowId, policy);

        // Compute which nodes are roots (no incoming edges)
        Set<String> hasIncoming = edges.stream().map(DagEdge::to).collect(Collectors.toSet());

        Instant now = clock.instant();
        Map<String, UUID> nodeToRunId = new HashMap<>();
        Map<String, UUID> nodeToJobId = new HashMap<>();

        // Insert a Job + JobRun for every node
        for (DagNode node : nodes) {
            UUID jobId   = UUID.randomUUID();
            int  shardId = ShardingUtils.computeShardId(jobId, totalShards);
            String jobKey  = IdempotencyKeys.deriveJobKey(node.jobType(),
                    node.payload() == null ? "{}" : node.payload());
            String runKey  = jobKey + ":workflow:" + workflowId + ":" + node.id();
            RetryPolicy retry = node.retryPolicy() != null ? node.retryPolicy() : RetryPolicy.defaults();

            Job job = new Job(jobId, node.jobType(),
                    node.payload() == null ? "{}" : node.payload(),
                    jobKey, DeliveryPolicy.AT_LEAST_ONCE,
                    null, ZoneId.of("UTC"), MisfirePolicy.FIRE_ONCE,
                    retry, now, shardId, node.priority(), node.tenantId());
            jobStore.insertIfAbsent(job);

            boolean isRoot = !hasIncoming.contains(node.id());
            UUID runId = UUID.randomUUID();
            JobRun run = new JobRun(runId, jobId,
                    isRoot ? JobState.SCHEDULED : JobState.PENDING_DEPS,
                    0, now, null, null, 0L, runKey,
                    null, null, now, null, null,
                    shardId, node.priority(), node.tenantId());
            runStore.insert(run);
            workflowStore.linkRunToWorkflow(runId, workflowId, node.id());

            nodeToRunId.put(node.id(), runId);
            nodeToJobId.put(node.id(), jobId);
        }

        // Wire dependency edges
        for (DagEdge edge : edges) {
            UUID downstreamRunId = nodeToRunId.get(edge.to());
            UUID upstreamRunId   = nodeToRunId.get(edge.from());
            workflowStore.insertDependency(downstreamRunId, upstreamRunId);
        }

        return new WorkflowSubmission(workflowId, Map.copyOf(nodeToRunId));
    }

    public Optional<Workflow> findWorkflow(UUID workflowId) {
        return workflowStore.findById(workflowId);
    }

    public List<WorkflowStore.WorkflowNodeRow> findNodes(UUID workflowId) {
        return workflowStore.findNodes(workflowId);
    }

    /** Cancel all non-terminal nodes in the workflow and mark the workflow CANCELLED. */
    @Transactional
    public int cancel(UUID workflowId) {
        int cancelled = jdbc.update("""
                UPDATE job_runs SET state = 'CANCELLED', finished_at = now()
                WHERE workflow_id = ?
                  AND state IN ('PENDING_DEPS', 'SCHEDULED', 'LEASED', 'RUNNING', 'FAILED')
                """, workflowId);
        workflowStore.updateWorkflowState(workflowId, WorkflowState.CANCELLED);
        return cancelled;
    }
}
