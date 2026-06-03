package io.kinetis.api.workflow;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

/**
 * Workflow submission payload.
 *
 * <pre>
 * {
 *   "nodes": [
 *     {"id":"extract", "jobType":"etl.extract", "payload":{"source":"s3://..."}, "priority":0},
 *     {"id":"transform", "jobType":"etl.transform"},
 *     {"id":"load", "jobType":"etl.load"}
 *   ],
 *   "edges": [
 *     {"from":"extract",   "to":"transform"},
 *     {"from":"transform", "to":"load"}
 *   ],
 *   "onFailure": "FAIL_FAST"
 * }
 * </pre>
 */
public record SubmitWorkflowRequest(
        @NotEmpty @Valid List<DagNodeRequest> nodes,
        List<DagEdgeRequest> edges,
        String onFailure
) {}
