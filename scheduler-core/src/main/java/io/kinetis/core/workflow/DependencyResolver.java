package io.kinetis.core.workflow;

import io.kinetis.core.model.JobState;
import io.kinetis.core.webhook.WebhookDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * Advances DAG state when a node completes. Called by the {@link WorkflowAdvancer} polling loop
 * after each run reaches a terminal state.
 *
 * <h2>On success</h2>
 * For each downstream of the completed run, check if all its upstreams are now SUCCEEDED.
 * If yes → promote it from PENDING_DEPS to SCHEDULED so the normal SchedulerLoop picks it up.
 *
 * <h2>On failure (DEAD_LETTER / exhausted)</h2>
 * Apply the workflow's {@link FailurePolicy}:
 * <ul>
 *   <li>FAIL_FAST — cancel all PENDING_DEPS and SCHEDULED nodes in the workflow.</li>
 *   <li>WAIT — do nothing; dependents stay PENDING_DEPS indefinitely.</li>
 *   <li>SKIP_DOWNSTREAM — mark direct dependents SKIPPED, then recursively resolve their
 *       downstreams (which may now be unblocked if they had other non-skipped upstreams).</li>
 * </ul>
 * In all cases, re-evaluate the workflow's aggregate state after processing.
 */
public class DependencyResolver {

    private static final Logger log = LoggerFactory.getLogger(DependencyResolver.class);

    private final WorkflowStore store;
    private final JdbcTemplate jdbc;
    private final WebhookDispatcher webhookDispatcher;

    public DependencyResolver(WorkflowStore store, JdbcTemplate jdbc) {
        this(store, jdbc, new WebhookDispatcher());
    }

    public DependencyResolver(WorkflowStore store, JdbcTemplate jdbc,
                               WebhookDispatcher webhookDispatcher) {
        this.store              = store;
        this.jdbc               = jdbc;
        this.webhookDispatcher  = webhookDispatcher;
    }

    /** Called after a run has been marked SUCCEEDED. */
    @Transactional
    public void onRunSucceeded(UUID runId) {
        UUID workflowId = store.findWorkflowIdForRun(runId);
        if (workflowId == null) return;

        List<UUID> downstreams = store.findDownstreams(runId);
        for (UUID downstream : downstreams) {
            if (store.countPendingUpstreams(downstream) == 0) {
                promoteToScheduled(downstream);
            }
        }
        recomputeWorkflowState(workflowId);
    }

    /** Called after a run has been dead-lettered (retries exhausted or AT_MOST_ONCE failure). */
    @Transactional
    public void onRunFailed(UUID runId) {
        UUID workflowId = store.findWorkflowIdForRun(runId);
        if (workflowId == null) return;

        var workflow = store.findById(workflowId).orElse(null);
        if (workflow == null) return;

        switch (workflow.failurePolicy()) {
            case FAIL_FAST -> cancelAllActive(workflowId);
            case WAIT      -> log.info("workflow {} WAIT policy — leaving dependents in PENDING_DEPS", workflowId);
            case SKIP_DOWNSTREAM -> skipDownstreams(runId, workflowId);
        }
        recomputeWorkflowState(workflowId);
    }

    // ---- private helpers ---------------------------------------------------

    private void promoteToScheduled(UUID runId) {
        int updated = jdbc.update("""
                UPDATE job_runs SET state = 'SCHEDULED' WHERE id = ? AND state = 'PENDING_DEPS'
                """, runId);
        if (updated == 1) log.debug("promoted run {} from PENDING_DEPS to SCHEDULED", runId);
    }

    private void cancelAllActive(UUID workflowId) {
        int cancelled = jdbc.update("""
                UPDATE job_runs SET state = 'CANCELLED', finished_at = now()
                WHERE workflow_id = ? AND state IN ('PENDING_DEPS', 'SCHEDULED')
                """, workflowId);
        log.info("FAIL_FAST: cancelled {} active nodes in workflow {}", cancelled, workflowId);
    }

    private void skipDownstreams(UUID failedRunId, UUID workflowId) {
        List<UUID> downstreams = store.findDownstreams(failedRunId);
        for (UUID downstream : downstreams) {
            int skipped = jdbc.update("""
                    UPDATE job_runs SET state = 'SKIPPED', finished_at = now()
                    WHERE id = ? AND state = 'PENDING_DEPS'
                    """, downstream);
            if (skipped == 1) {
                log.debug("SKIP_DOWNSTREAM: skipped run {}", downstream);
                // Recursively skip further downstreams of the now-skipped node
                skipDownstreams(downstream, workflowId);
            }
        }
    }

    /**
     * Derive and persist the workflow's aggregate state from its nodes.
     * Called after every node terminal transition.
     */
    private void recomputeWorkflowState(UUID workflowId) {
        // Count nodes still active (not yet terminal)
        Long active = jdbc.queryForObject("""
                SELECT count(*) FROM job_runs
                WHERE workflow_id = ? AND state IN ('PENDING_DEPS','SCHEDULED','LEASED','RUNNING')
                """, Long.class, workflowId);

        if (active != null && active > 0) return; // still running

        // All nodes are terminal — determine aggregate outcome
        Long failed = jdbc.queryForObject("""
                SELECT count(*) FROM job_runs
                WHERE workflow_id = ? AND state IN ('DEAD_LETTER','FAILED')
                """, Long.class, workflowId);

        Long cancelled = jdbc.queryForObject("""
                SELECT count(*) FROM job_runs
                WHERE workflow_id = ? AND state = 'CANCELLED'
                """, Long.class, workflowId);

        WorkflowState newState;
        if (cancelled != null && cancelled > 0)    newState = WorkflowState.CANCELLED;
        else if (failed != null && failed > 0)      newState = WorkflowState.FAILED;
        else                                         newState = WorkflowState.SUCCEEDED;

        store.updateWorkflowState(workflowId, newState);
        log.info("workflow {} reached terminal state: {}", workflowId, newState);

        // Fire webhook if the workflow has one configured
        String cbUrl = jdbc.query(
                "SELECT callback_url FROM workflows WHERE id = ?",
                rs -> rs.next() ? rs.getString(1) : null,
                workflowId);
        String event = newState == WorkflowState.SUCCEEDED ? "workflow.succeeded" : "workflow.failed";
        webhookDispatcher.fire(cbUrl, event, workflowId.toString(), newState.name());
    }
}
