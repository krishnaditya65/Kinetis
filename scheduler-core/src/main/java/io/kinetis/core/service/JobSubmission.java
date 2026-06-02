package io.kinetis.core.service;

import java.util.UUID;

/**
 * Result of {@link JobService#submit}. {@code created} is false when the submission was
 * deduplicated — the returned ids point at the existing job and its first run.
 */
public record JobSubmission(UUID jobId, UUID runId, boolean created) {
}
