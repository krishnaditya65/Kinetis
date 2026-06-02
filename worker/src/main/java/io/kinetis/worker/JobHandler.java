package io.kinetis.worker;

/**
 * Business logic for a job type. Implementations must be <b>idempotent</b>: because delivery is
 * at-least-once, a handler may run more than once for the same logical work (retry, or a slow
 * worker that lost its lease). Use {@link JobContext#idempotencyKey()} to dedup external effects.
 *
 * <p>Throwing any exception signals failure and triggers the retry/dead-letter machinery.
 */
public interface JobHandler {

    /** The {@code job_type} this handler serves. Must be unique within a {@link HandlerRegistry}. */
    String type();

    void handle(JobContext ctx) throws Exception;
}
