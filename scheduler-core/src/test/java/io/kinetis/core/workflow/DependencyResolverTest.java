package io.kinetis.core.workflow;

import io.kinetis.core.AbstractPgTest;
import io.kinetis.core.model.*;
import io.kinetis.core.store.JobRunStore;
import io.kinetis.core.store.JobStore;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link DependencyResolver} against a real Postgres.
 * Tests the core DAG advancement: promote PENDING_DEPS→SCHEDULED when upstreams complete,
 * and apply failure policies correctly.
 */
class DependencyResolverTest extends AbstractPgTest {

    private WorkflowStore workflowStore() { return new WorkflowStore(jdbc); }
    private JobStore jobStore()           { return new JobStore(jdbc); }
    private JobRunStore runStore()        { return new JobRunStore(jdbc); }

    private DependencyResolver resolver() {
        return new DependencyResolver(workflowStore(), jdbc);
    }

    // ---- helpers ----

    private UUID seedWorkflow(FailurePolicy policy) {
        UUID id = UUID.randomUUID();
        workflowStore().insertWorkflow(id, policy);
        return id;
    }

    private UUID seedRun(UUID workflowId, String nodeId, JobState state) {
        UUID jobId  = UUID.randomUUID();
        UUID runId  = UUID.randomUUID();
        jobStore().insertIfAbsent(new Job(jobId, "noop", "{}", "k-" + UUID.randomUUID(),
                DeliveryPolicy.AT_LEAST_ONCE, null, ZoneId.of("UTC"), MisfirePolicy.FIRE_ONCE,
                RetryPolicy.defaults(), Instant.now(), 0, 0, null));
        runStore().insert(new JobRun(runId, jobId, state, 0, Instant.now(),
                null, null, 0L, "rk-" + UUID.randomUUID(),
                null, null, Instant.now(), null, null, 0, 0, null));
        workflowStore().linkRunToWorkflow(runId, workflowId, nodeId);
        return runId;
    }

    private void succeed(UUID runId) {
        jdbc.update("UPDATE job_runs SET state = 'SUCCEEDED' WHERE id = ?", runId);
    }

    private void deadLetter(UUID runId) {
        jdbc.update("UPDATE job_runs SET state = 'DEAD_LETTER' WHERE id = ?", runId);
    }

    private JobState stateOf(UUID runId) {
        return JobState.valueOf(jdbc.queryForObject(
                "SELECT state FROM job_runs WHERE id = ?", String.class, runId));
    }

    // ---- tests ----

    @Test
    void singleDependency_upstreamSucceeds_downstreamBecomesScheduled() {
        UUID wf         = seedWorkflow(FailurePolicy.FAIL_FAST);
        UUID upstreamId = seedRun(wf, "a", JobState.SUCCEEDED);
        UUID downstream = seedRun(wf, "b", JobState.PENDING_DEPS);
        workflowStore().insertDependency(downstream, upstreamId);

        resolver().onRunSucceeded(upstreamId);

        assertThat(stateOf(downstream)).isEqualTo(JobState.SCHEDULED);
    }

    @Test
    void fanIn_bothUpstreamsMustSucceed_beforeDownstreamScheduled() {
        UUID wf  = seedWorkflow(FailurePolicy.FAIL_FAST);
        UUID ua  = seedRun(wf, "a", JobState.SUCCEEDED);
        UUID ub  = seedRun(wf, "b", JobState.PENDING_DEPS); // not yet done
        UUID sink = seedRun(wf, "c", JobState.PENDING_DEPS);
        workflowStore().insertDependency(sink, ua);
        workflowStore().insertDependency(sink, ub);

        resolver().onRunSucceeded(ua);
        assertThat(stateOf(sink)).isEqualTo(JobState.PENDING_DEPS); // still waiting for ub

        succeed(ub);
        resolver().onRunSucceeded(ub);
        assertThat(stateOf(sink)).isEqualTo(JobState.SCHEDULED); // now both done
    }

    @Test
    void failFast_cancelsPendingDownstreams() {
        UUID wf   = seedWorkflow(FailurePolicy.FAIL_FAST);
        UUID root = seedRun(wf, "root", JobState.DEAD_LETTER);
        UUID dep1 = seedRun(wf, "dep1", JobState.PENDING_DEPS);
        UUID dep2 = seedRun(wf, "dep2", JobState.PENDING_DEPS);
        workflowStore().insertDependency(dep1, root);
        workflowStore().insertDependency(dep2, root);

        resolver().onRunFailed(root);

        assertThat(stateOf(dep1)).isEqualTo(JobState.CANCELLED);
        assertThat(stateOf(dep2)).isEqualTo(JobState.CANCELLED);
    }

    @Test
    void skipDownstream_marksDirectDependentsSkipped() {
        UUID wf   = seedWorkflow(FailurePolicy.SKIP_DOWNSTREAM);
        UUID root = seedRun(wf, "root", JobState.DEAD_LETTER);
        UUID dep  = seedRun(wf, "dep",  JobState.PENDING_DEPS);
        workflowStore().insertDependency(dep, root);

        resolver().onRunFailed(root);

        assertThat(stateOf(dep)).isEqualTo(JobState.SKIPPED);
    }

    @Test
    void wait_leavesDownstreamsInPendingDeps() {
        UUID wf   = seedWorkflow(FailurePolicy.WAIT);
        UUID root = seedRun(wf, "root", JobState.DEAD_LETTER);
        UUID dep  = seedRun(wf, "dep",  JobState.PENDING_DEPS);
        workflowStore().insertDependency(dep, root);

        resolver().onRunFailed(root);

        assertThat(stateOf(dep)).isEqualTo(JobState.PENDING_DEPS);
    }

    @Test
    void workflowStateBecomesSucceededWhenAllNodesDone() {
        UUID wf = seedWorkflow(FailurePolicy.FAIL_FAST);
        UUID a  = seedRun(wf, "a", JobState.SUCCEEDED);
        UUID b  = seedRun(wf, "b", JobState.PENDING_DEPS);
        workflowStore().insertDependency(b, a);

        succeed(b); // manually mark b SUCCEEDED to simulate WorkerPool completion
        resolver().onRunSucceeded(a); // should promote b, then see all terminal

        // Re-invoke resolver as b is now SCHEDULED (in real flow WorkflowAdvancer handles b)
        succeed(b);
        resolver().onRunSucceeded(b);

        var wfState = workflowStore().findById(wf).orElseThrow().state();
        assertThat(wfState).isEqualTo(WorkflowState.SUCCEEDED);
    }
}
