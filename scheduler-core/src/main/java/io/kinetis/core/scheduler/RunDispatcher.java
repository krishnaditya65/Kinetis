package io.kinetis.core.scheduler;

import io.kinetis.core.model.JobRun;

/**
 * Sink for leased runs. Implemented by the worker side — in Phase 1 an in-process pool,
 * in Phase 3 a gRPC client to remote workers. Keeping it an interface means scheduler-core
 * stays unaware of how or where execution happens.
 */
@FunctionalInterface
public interface RunDispatcher {
    void dispatch(JobRun leasedRun);
}
