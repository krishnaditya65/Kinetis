package io.kinetis.worker;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Everything a {@link JobHandler} needs to do its work.
 *
 * <h2>Upstream outputs (DAG workflows)</h2>
 * When this run is a node in a DAG, {@link #upstreamOutputs()} contains the JSON outputs
 * of all direct upstream nodes, keyed by their node id. Call {@link #complete(JsonNode)} to
 * store this run's own output, which downstream nodes will receive via their context.
 *
 * <h2>Retries</h2>
 * {@code attempt} is 0-based — handlers can use it to skip already-sent side effects on
 * retries (e.g. "only send the email on attempt 0").
 */
public class JobContext {

    private final UUID runId;
    private final UUID jobId;
    private final String jobType;
    private final JsonNode payload;
    private final int attempt;
    private final String idempotencyKey;
    private final Map<String, JsonNode> upstreamOutputs;

    /** Captured by WorkerPool after the handler returns. */
    private final AtomicReference<JsonNode> result = new AtomicReference<>();

    public JobContext(UUID runId, UUID jobId, String jobType, JsonNode payload,
                      int attempt, String idempotencyKey) {
        this(runId, jobId, jobType, payload, attempt, idempotencyKey, Map.of());
    }

    public JobContext(UUID runId, UUID jobId, String jobType, JsonNode payload,
                      int attempt, String idempotencyKey,
                      Map<String, JsonNode> upstreamOutputs) {
        this.runId           = runId;
        this.jobId           = jobId;
        this.jobType         = jobType;
        this.payload         = payload;
        this.attempt         = attempt;
        this.idempotencyKey  = idempotencyKey;
        this.upstreamOutputs = Collections.unmodifiableMap(upstreamOutputs);
    }

    // Accessors
    public UUID     runId()          { return runId; }
    public UUID     jobId()          { return jobId; }
    public String   jobType()        { return jobType; }
    public JsonNode payload()        { return payload; }
    public int      attempt()        { return attempt; }
    public String   idempotencyKey() { return idempotencyKey; }

    /**
     * Outputs from all direct upstream nodes in the DAG, keyed by node id.
     * Empty map for standalone (non-workflow) jobs.
     */
    public Map<String, JsonNode> upstreamOutputs() {
        return upstreamOutputs;
    }

    /**
     * Store the result of this run. Called by the handler to pass data to downstream nodes.
     * The output is persisted by {@link WorkerPool} after the handler returns successfully.
     * Calling this more than once overwrites the previous value.
     */
    public void complete(JsonNode output) {
        result.set(output);
    }

    /** The output stored via {@link #complete}, or null if the handler didn't set one. */
    public JsonNode getResult() {
        return result.get();
    }
}
