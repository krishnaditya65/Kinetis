package io.kinetis.api.workflow;

import io.kinetis.core.model.JobState;
import io.kinetis.core.workflow.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class WorkflowEndToEndTest {

    @Container
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kinetis").withUsername("kinetis").withPassword("kinetis");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url",      PG::getJdbcUrl);
        r.add("spring.datasource.username", PG::getUsername);
        r.add("spring.datasource.password", PG::getPassword);
        r.add("scheduler.poll-interval",    () -> "100ms");
        r.add("scheduler.reaper-interval",  () -> "500ms");
    }

    @Autowired WorkflowService workflowService;

    // ---- helpers ----

    private WorkflowSubmission linear(String... handlerTypes) {
        WorkflowBuilder b = WorkflowBuilder.create().failFast();
        for (String t : handlerTypes) b.node(t + "-" + System.nanoTime(), t);
        String[] ids = b.buildNodes().stream().map(DagNode::id).toArray(String[]::new);
        b.chain(ids);
        return b.submit(workflowService);
    }

    private void awaitWorkflowState(WorkflowSubmission s, WorkflowState target,
                                     Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            var wf = workflowService.findWorkflow(s.workflowId()).orElseThrow();
            if (wf.state() == target) return;
            Thread.sleep(150);
        }
        var wf = workflowService.findWorkflow(s.workflowId()).orElseThrow();
        fail("workflow did not reach " + target + " within " + timeout
                + " (state=" + wf.state() + ")");
    }

    private JobState nodeState(WorkflowSubmission s, String nodeId) {
        Map<String, UUID> map = s.nodeRunIds();
        return workflowService.findNodes(s.workflowId()).stream()
                .filter(n -> map.get(nodeId) != null && n.runId().equals(map.get(nodeId)))
                .map(WorkflowStore.WorkflowNodeRow::state)
                .findFirst().orElseThrow();
    }

    // ---- tests ----

    @Test
    void linearWorkflow_allNodesSucceed() throws Exception {
        WorkflowSubmission s = linear("noop", "noop", "noop");
        awaitWorkflowState(s, WorkflowState.SUCCEEDED, Duration.ofSeconds(20));

        workflowService.findNodes(s.workflowId()).forEach(n ->
                assertThat(n.state()).isEqualTo(JobState.SUCCEEDED));
    }

    @Test
    void rootNodeOnly_noEdges_succeedsImmediately() throws Exception {
        WorkflowSubmission s = WorkflowBuilder.create()
                .node("only", "noop")
                .submit(workflowService);
        awaitWorkflowState(s, WorkflowState.SUCCEEDED, Duration.ofSeconds(15));
    }

    @Test
    void pendingDepsNode_doesNotExecuteUntilUpstreamSucceeds() throws Exception {
        // Submit a 2-node chain: sleep(500ms) → noop
        WorkflowSubmission s = WorkflowBuilder.create()
                .node("slow", "sleep", "{\"ms\":500}")
                .node("fast", "noop")
                .edge("slow", "fast")
                .submit(workflowService);

        // Give the scheduler one poll cycle — "fast" should still be PENDING_DEPS
        Thread.sleep(300);
        var fastState = workflowService.findNodes(s.workflowId()).stream()
                .filter(n -> n.nodeId().equals("fast")).findFirst().orElseThrow().state();
        assertThat(fastState).isEqualTo(JobState.PENDING_DEPS);

        // Wait for the full workflow to complete
        awaitWorkflowState(s, WorkflowState.SUCCEEDED, Duration.ofSeconds(15));
    }

    @Test
    void failFast_oneFailure_cancelsRemainingNodes() throws Exception {
        WorkflowSubmission s = WorkflowBuilder.create()
                .failFast()
                .node("fail",  "failNTimes", "{\"failTimes\":99}")  // always fails
                .node("after", "noop")
                .edge("fail", "after")
                .submit(workflowService);

        awaitWorkflowState(s, WorkflowState.FAILED, Duration.ofSeconds(20));

        var afterState = workflowService.findNodes(s.workflowId()).stream()
                .filter(n -> n.nodeId().equals("after")).findFirst().orElseThrow().state();
        assertThat(afterState).isIn(JobState.CANCELLED, JobState.PENDING_DEPS);
    }

    @Test
    void cancelWorkflow_stopsAllPendingNodes() throws Exception {
        WorkflowSubmission s = WorkflowBuilder.create()
                .node("a", "sleep", "{\"ms\":60000}")  // very slow
                .node("b", "noop")
                .edge("a", "b")
                .submit(workflowService);

        Thread.sleep(200);
        int cancelled = workflowService.cancel(s.workflowId());
        assertThat(cancelled).isGreaterThanOrEqualTo(1);

        var wf = workflowService.findWorkflow(s.workflowId()).orElseThrow();
        assertThat(wf.state()).isEqualTo(WorkflowState.CANCELLED);
    }

    @Test
    void workflowBuilder_diamondShape_allNodesSucceed() throws Exception {
        // a → b, a → c, b → d, c → d
        WorkflowSubmission s = WorkflowBuilder.create()
                .node("a", "noop").node("b", "noop")
                .node("c", "noop").node("d", "noop")
                .edge("a", "b").edge("a", "c")
                .edge("b", "d").edge("c", "d")
                .submit(workflowService);

        awaitWorkflowState(s, WorkflowState.SUCCEEDED, Duration.ofSeconds(20));
    }
}
