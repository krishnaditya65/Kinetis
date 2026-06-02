package io.kinetis.api.grpc;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.kinetis.core.model.JobRun;
import io.kinetis.core.scheduler.RunDispatcher;
import io.kinetis.core.store.JobStore;
import io.kinetis.worker.proto.Ack;
import io.kinetis.worker.proto.RunAssignment;
import io.kinetis.worker.proto.WorkerServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * {@link RunDispatcher} that sends leased runs to remote workers over gRPC.
 *
 * <p>Dispatch is fire-and-forget: the scheduler sends {@code RunAssignment} and immediately
 * continues. Outcome is tracked via the DB — the worker writes through {@code LeaseManager}
 * exactly as the in-process {@code WorkerPool} does. If the worker crashes before starting,
 * the lease expires and the reaper reclaims the run — no change to the crash-recovery protocol.
 *
 * <p>If no workers are registered the run stays {@code LEASED} and will be reaped after the TTL.
 */
public class GrpcRunDispatcher implements RunDispatcher {

    private static final Logger log = LoggerFactory.getLogger(GrpcRunDispatcher.class);

    private final WorkerRegistry registry;
    private final JobStore jobStore;
    private final Executor callbackExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public GrpcRunDispatcher(WorkerRegistry registry, JobStore jobStore) {
        this.registry = registry;
        this.jobStore = jobStore;
    }

    @Override
    public void dispatch(JobRun run) {
        WorkerServiceGrpc.WorkerServiceFutureStub stub = registry.nextWorker();
        if (stub == null) {
            log.warn("no workers registered — run {} stays LEASED until reaped", run.id());
            return;
        }

        var maybeJob = jobStore.findById(run.jobId());
        if (maybeJob.isEmpty()) {
            log.warn("job {} not found for run {} — skipping dispatch", run.jobId(), run.id());
            return;
        }
        var job = maybeJob.get();

        RunAssignment assignment = RunAssignment.newBuilder()
                .setRunId(run.id().toString())
                .setJobId(run.jobId().toString())
                .setJobType(job.jobType())
                .setPayloadJson(job.payload() != null ? job.payload() : "{}")
                .setAttempt(run.attempt())
                .setLeaseToken(run.leaseToken())
                .setIdempotencyKey(run.idempotencyKey())
                .setShardId(run.shardId())
                .build();

        ListenableFuture<Ack> future = stub.dispatch(assignment);
        Futures.addCallback(future, new FutureCallback<>() {
            @Override
            public void onSuccess(Ack ack) {
                if (!ack.getAccepted())
                    log.warn("worker rejected run {}: {}", run.id(), ack.getReason());
                else
                    log.debug("dispatched run {} to remote worker", run.id());
            }
            @Override
            public void onFailure(Throwable t) {
                log.warn("gRPC dispatch failed for run {} (will be reaped): {}", run.id(), t.getMessage());
            }
        }, callbackExecutor);
    }
}
