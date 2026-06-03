package io.kinetis.core.workflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.UUID;

/**
 * Polling loop that drives {@link DependencyResolver} for completed workflow nodes.
 *
 * <p>Rather than coupling {@link DependencyResolver} into {@link io.kinetis.worker.WorkerPool}
 * (which would add a DAG dependency to the execution path), this loop polls periodically for
 * runs that belong to a workflow and have just reached a terminal state. This keeps the hot
 * path clean and makes the DAG advancement easy to reason about in isolation.
 *
 * <p>The latency trade-off (one poll interval between run completion and dependency unlock) is
 * acceptable for workflow workloads. Tick interval is configured via
 * {@code scheduler.workflow-advance-interval} (default 500ms).
 */
public class WorkflowAdvancer {

    private static final Logger log = LoggerFactory.getLogger(WorkflowAdvancer.class);

    private final DependencyResolver resolver;
    private final JdbcTemplate jdbc;
    private final int batchSize;

    public WorkflowAdvancer(DependencyResolver resolver, JdbcTemplate jdbc, int batchSize) {
        this.resolver  = resolver;
        this.jdbc      = jdbc;
        this.batchSize = batchSize;
    }

    /**
     * One tick: find recently-completed workflow nodes whose downstream dependencies
     * haven't been resolved yet, and advance them.
     */
    public int tick() {
        // Find SUCCEEDED runs in workflows whose direct downstreams are still PENDING_DEPS
        List<UUID> succeeded = jdbc.query("""
                SELECT DISTINCT jr.id FROM job_runs jr
                JOIN job_dependencies jd ON jd.depends_on_run_id = jr.id
                JOIN job_runs downstream ON downstream.id = jd.run_id
                WHERE jr.workflow_id IS NOT NULL
                  AND jr.state = 'SUCCEEDED'
                  AND downstream.state = 'PENDING_DEPS'
                LIMIT ?
                """,
                (rs, r) -> rs.getObject("id", UUID.class),
                batchSize);

        // Find DEAD_LETTER/FAILED runs in workflows (unresolved failure)
        List<UUID> failed = jdbc.query("""
                SELECT DISTINCT jr.id FROM job_runs jr
                WHERE jr.workflow_id IS NOT NULL
                  AND jr.state IN ('DEAD_LETTER')
                  AND EXISTS (
                      SELECT 1 FROM job_dependencies jd
                      JOIN job_runs ds ON ds.id = jd.run_id
                      WHERE jd.depends_on_run_id = jr.id AND ds.state = 'PENDING_DEPS'
                  )
                LIMIT ?
                """,
                (rs, r) -> rs.getObject("id", UUID.class),
                batchSize);

        int processed = 0;
        for (UUID runId : succeeded) {
            try {
                resolver.onRunSucceeded(runId);
                processed++;
            } catch (Exception e) {
                log.warn("error advancing succeeded run {} in workflow", runId, e);
            }
        }
        for (UUID runId : failed) {
            try {
                resolver.onRunFailed(runId);
                processed++;
            } catch (Exception e) {
                log.warn("error advancing failed run {} in workflow", runId, e);
            }
        }
        return processed;
    }
}
