package io.kinetis.core.workflow;

import io.kinetis.core.model.JobState;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** DAO for the {@code workflows} table and the {@code job_dependencies} join table. */
public class WorkflowStore {

    private final JdbcTemplate jdbc;

    public WorkflowStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insertWorkflow(UUID id, FailurePolicy policy) {
        jdbc.update("""
                INSERT INTO workflows (id, on_failure, state, created_at)
                VALUES (?, ?, 'RUNNING', now())
                """, id, policy.name());
    }

    /** Record a dependency: {@code runId} depends on {@code upstreamRunId}. */
    public void insertDependency(UUID runId, UUID upstreamRunId) {
        jdbc.update("""
                INSERT INTO job_dependencies (run_id, depends_on_run_id) VALUES (?, ?)
                """, runId, upstreamRunId);
    }

    /** Set workflow_id and node_id on an existing job_run row. */
    public void linkRunToWorkflow(UUID runId, UUID workflowId, String nodeId) {
        jdbc.update("""
                UPDATE job_runs SET workflow_id = ?, node_id = ? WHERE id = ?
                """, workflowId, nodeId, runId);
    }

    public Optional<Workflow> findById(UUID workflowId) {
        List<Workflow> rows = jdbc.query("""
                SELECT id, on_failure, state, created_at FROM workflows WHERE id = ?
                """,
                (rs, r) -> new Workflow(
                        rs.getObject("id", UUID.class),
                        FailurePolicy.valueOf(rs.getString("on_failure")),
                        WorkflowState.valueOf(rs.getString("state")),
                        rs.getTimestamp("created_at").toInstant()),
                workflowId);
        return rows.stream().findFirst();
    }

    /** All run ids and their states for a given workflow, keyed by node_id. */
    public List<WorkflowNodeRow> findNodes(UUID workflowId) {
        return jdbc.query("""
                SELECT id, node_id, state, job_id, attempt, scheduled_for, finished_at
                FROM job_runs WHERE workflow_id = ? ORDER BY enqueued_at
                """,
                (rs, r) -> new WorkflowNodeRow(
                        rs.getObject("id", UUID.class),
                        rs.getString("node_id"),
                        JobState.valueOf(rs.getString("state")),
                        rs.getObject("job_id", UUID.class)),
                workflowId);
    }

    /** Ids of all direct downstream runs that depend on {@code upstreamRunId}. */
    public List<UUID> findDownstreams(UUID upstreamRunId) {
        return jdbc.query("""
                SELECT run_id FROM job_dependencies WHERE depends_on_run_id = ?
                """,
                (rs, r) -> rs.getObject("run_id", UUID.class),
                upstreamRunId);
    }

    /** Count of upstreams of {@code runId} that are NOT yet SUCCEEDED. */
    public int countPendingUpstreams(UUID runId) {
        Integer n = jdbc.queryForObject("""
                SELECT count(*) FROM job_dependencies jd
                JOIN job_runs jr ON jr.id = jd.depends_on_run_id
                WHERE jd.run_id = ? AND jr.state != 'SUCCEEDED'
                """, Integer.class, runId);
        return n == null ? 0 : n;
    }

    public void updateWorkflowState(UUID workflowId, WorkflowState state) {
        jdbc.update("UPDATE workflows SET state = ? WHERE id = ?", state.name(), workflowId);
    }

    /** Workflow id of the workflow that owns this run, or null. */
    public UUID findWorkflowIdForRun(UUID runId) {
        List<UUID> rows = jdbc.query(
                "SELECT workflow_id FROM job_runs WHERE id = ? AND workflow_id IS NOT NULL",
                (rs, r) -> rs.getObject("workflow_id", UUID.class), runId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    /** Simple projection for listing workflow nodes. */
    public record WorkflowNodeRow(UUID runId, String nodeId, JobState state, UUID jobId) {}
}
