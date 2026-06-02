package io.kinetis.api.dto;

import io.kinetis.core.service.JobSubmission;

import java.util.UUID;

/** Response to a submit. {@code created=false} means deduplicated against an existing job. */
public record SubmitJobResponse(UUID jobId, UUID runId, boolean created) {

    public static SubmitJobResponse from(JobSubmission s) {
        return new SubmitJobResponse(s.jobId(), s.runId(), s.created());
    }
}
