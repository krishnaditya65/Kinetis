package io.kinetis.worker.handlers;

import io.kinetis.worker.JobContext;
import io.kinetis.worker.JobHandler;

/**
 * Sleeps for {@code payload.ms} milliseconds then succeeds. Exercises long-running jobs and
 * the heartbeat keeping the lease alive. Runs on a virtual thread so the block is cheap.
 */
public class SleepHandler implements JobHandler {

    @Override
    public String type() { return "sleep"; }

    @Override
    public void handle(JobContext ctx) throws InterruptedException {
        long ms = ctx.payload().path("ms").asLong(1_000L);
        Thread.sleep(ms);
    }
}
