package io.kinetis.core.workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validates a DAG submission before any DB writes. Catches structural problems early so callers
 * get a clear error at submission time rather than silent misbehaviour at execution time.
 *
 * <p>Checks performed:
 * <ol>
 *   <li>At least one node.</li>
 *   <li>No duplicate node ids.</li>
 *   <li>All edge endpoints reference a declared node (no orphan edges).</li>
 *   <li>No cycles (DFS with colour marking).</li>
 * </ol>
 */
public final class DagValidator {

    private DagValidator() {}

    /**
     * @throws IllegalArgumentException describing the first validation failure found
     */
    public static void validate(List<DagNode> nodes, List<DagEdge> edges) {
        if (nodes == null || nodes.isEmpty())
            throw new IllegalArgumentException("workflow must have at least one node");

        // 1. Duplicate node ids
        Set<String> ids = new HashSet<>();
        for (DagNode n : nodes) {
            if (n.id() == null || n.id().isBlank())
                throw new IllegalArgumentException("node id must not be blank");
            if (!ids.add(n.id()))
                throw new IllegalArgumentException("duplicate node id: " + n.id());
        }

        // 2. Orphan edge endpoints
        for (DagEdge e : edges) {
            if (!ids.contains(e.from()))
                throw new IllegalArgumentException("edge references unknown source node: " + e.from());
            if (!ids.contains(e.to()))
                throw new IllegalArgumentException("edge references unknown target node: " + e.to());
            if (e.from().equals(e.to()))
                throw new IllegalArgumentException("self-loop on node: " + e.from());
        }

        // 3. Cycle detection via DFS (white=0 / grey=1 / black=2 colouring)
        Map<String, List<String>> adj = new HashMap<>();
        ids.forEach(id -> adj.put(id, new ArrayList<>()));
        edges.forEach(e -> adj.get(e.from()).add(e.to()));

        Map<String, Integer> colour = new HashMap<>();
        ids.forEach(id -> colour.put(id, 0));

        for (String id : ids) {
            if (colour.get(id) == 0) {
                detectCycle(id, adj, colour);
            }
        }
    }

    private static void detectCycle(String node, Map<String, List<String>> adj,
                                     Map<String, Integer> colour) {
        colour.put(node, 1); // grey — in current DFS path
        for (String neighbour : adj.get(node)) {
            int c = colour.get(neighbour);
            if (c == 1)
                throw new IllegalArgumentException(
                        "cycle detected in DAG involving node: " + neighbour);
            if (c == 0)
                detectCycle(neighbour, adj, colour);
        }
        colour.put(node, 2); // black — fully explored
    }
}
