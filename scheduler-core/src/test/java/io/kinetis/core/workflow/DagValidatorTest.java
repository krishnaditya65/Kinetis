package io.kinetis.core.workflow;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class DagValidatorTest {

    private static DagNode node(String id) {
        return new DagNode(id, "noop", "{}", null, 0, null);
    }

    @Test
    void validLinearDagPasses() {
        assertThatNoException().isThrownBy(() ->
                DagValidator.validate(
                        List.of(node("a"), node("b"), node("c")),
                        List.of(new DagEdge("a", "b"), new DagEdge("b", "c"))));
    }

    @Test
    void emptyNodeListIsRejected() {
        assertThatThrownBy(() -> DagValidator.validate(List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one node");
    }

    @Test
    void duplicateNodeIdIsRejected() {
        assertThatThrownBy(() ->
                DagValidator.validate(List.of(node("a"), node("a")), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate node id");
    }

    @Test
    void orphanEdgeSourceIsRejected() {
        assertThatThrownBy(() ->
                DagValidator.validate(List.of(node("a")),
                        List.of(new DagEdge("ghost", "a"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown source node");
    }

    @Test
    void orphanEdgeTargetIsRejected() {
        assertThatThrownBy(() ->
                DagValidator.validate(List.of(node("a")),
                        List.of(new DagEdge("a", "ghost"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown target node");
    }

    @Test
    void selfLoopIsRejected() {
        assertThatThrownBy(() ->
                DagValidator.validate(List.of(node("a")),
                        List.of(new DagEdge("a", "a"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("self-loop");
    }

    @Test
    void directCycleIsRejected() {
        assertThatThrownBy(() ->
                DagValidator.validate(
                        List.of(node("a"), node("b")),
                        List.of(new DagEdge("a", "b"), new DagEdge("b", "a"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cycle");
    }

    @Test
    void longerCycleIsRejected() {
        assertThatThrownBy(() ->
                DagValidator.validate(
                        List.of(node("a"), node("b"), node("c")),
                        List.of(new DagEdge("a", "b"), new DagEdge("b", "c"),
                                new DagEdge("c", "a"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cycle");
    }

    @Test
    void diamondDagPasses() {
        // a → b, a → c, b → d, c → d
        assertThatNoException().isThrownBy(() ->
                DagValidator.validate(
                        List.of(node("a"), node("b"), node("c"), node("d")),
                        List.of(new DagEdge("a", "b"), new DagEdge("a", "c"),
                                new DagEdge("b", "d"), new DagEdge("c", "d"))));
    }

    @Test
    void workflowBuilderChainProducesCorrectEdges() {
        WorkflowBuilder builder = WorkflowBuilder.create()
                .node("a", "noop").node("b", "noop").node("c", "noop")
                .chain("a", "b", "c");
        assertThat(builder.buildEdges()).containsExactly(
                new DagEdge("a", "b"), new DagEdge("b", "c"));
    }

    @Test
    void workflowBuilderFanOutProducesCorrectEdges() {
        WorkflowBuilder builder = WorkflowBuilder.create()
                .node("root", "noop").node("b", "noop").node("c", "noop")
                .fanOut("root", "b", "c");
        assertThat(builder.buildEdges()).containsExactlyInAnyOrder(
                new DagEdge("root", "b"), new DagEdge("root", "c"));
    }
}
