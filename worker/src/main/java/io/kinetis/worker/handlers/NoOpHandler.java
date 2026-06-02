package io.kinetis.worker.handlers;

import io.kinetis.worker.JobContext;
import io.kinetis.worker.JobHandler;

/** Does nothing and succeeds. Useful as a smoke test of the full pipeline. */
public class NoOpHandler implements JobHandler {

    @Override
    public String type() { return "noop"; }

    @Override
    public void handle(JobContext ctx) {}
}
