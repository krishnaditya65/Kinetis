package io.kinetis.api.dto;

import io.kinetis.core.model.JobRun;

import java.time.Instant;
import java.util.UUID;

/** Read model for a job run (omits internal lease plumbing except the token, which is informative). */
public record RunView(
        UUID id,
        UUID jobId,
        String state,
        int attempt,
        Instant scheduledFor,
        String leaseOwner,
        Instant leaseExpiresAt,
        long leaseToken,
        String lastError,
        Instant startedAt,
        Instant finishedAt,
        int priority,
        String tenantId
) {
    public static RunView from(JobRun r) {
        return new RunView(r.id(), r.jobId(), r.state().name(), r.attempt(), r.scheduledFor(),
                r.leaseOwner(), r.leaseExpiresAt(), r.leaseToken(), r.lastError(),
                r.startedAt(), r.finishedAt(), r.priority(), r.tenantId());
    }
}
