package io.kinetis.worker.handlers;

import io.kinetis.worker.JobContext;
import io.kinetis.worker.JobHandler;

/**
 * Fails on the first {@code payload.failTimes} attempts then succeeds. Decision is driven by
 * {@link JobContext#attempt()} only — no shared state — so it deterministically exercises the
 * retry/backoff path and, when {@code failTimes >= maxAttempts}, the dead-letter path.
 */
public class FailNTimesHandler implements JobHandler {

    @Override
    public String type() { return "failNTimes"; }

    @Override
    public void handle(JobContext ctx) {
        int failTimes = ctx.payload().path("failTimes").asInt(1);
        if (ctx.attempt() < failTimes) {
            throw new IllegalStateException(
                    "deliberate failure on attempt " + ctx.attempt() + " (failTimes=" + failTimes + ")");
        }
    }
}
