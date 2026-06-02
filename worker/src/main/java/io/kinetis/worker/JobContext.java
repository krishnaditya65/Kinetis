package io.kinetis.worker;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.UUID;

/**
 * Everything a {@link JobHandler} needs to do its work. {@code attempt} is 0-based — handlers
 * can use it to behave differently on retries (e.g. skip an already-sent notification).
 */
public record JobContext(
        UUID runId,
        UUID jobId,
        String jobType,
        JsonNode payload,
        int attempt,
        String idempotencyKey
) {
}
