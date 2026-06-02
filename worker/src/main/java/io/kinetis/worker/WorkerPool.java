package io.kinetis.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kinetis.core.lease.LeaseManager;
import io.kinetis.core.metrics.SchedulerMetrics;
import io.kinetis.core.model.Job;
import io.kinetis.core.model.JobRun;
import io.kinetis.core.retry.RetryHandler;
import io.kinetis.core.scheduler.RunDispatcher;
import io.kinetis.core.store.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Executes leased runs. Implements {@link RunDispatcher} so the scheduler hands it work without
 * knowing execution is in-process. Each run gets its own <b>virtual thread</b> (Java 21 Loom),
 * so blocking handlers scale cheaply. A heartbeat loop extends the lease while the handler runs;
 * all writes carry the fencing token so a zombie execution can never corrupt state.
 */
public class WorkerPool implements RunDispatcher, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);

    private final JobStore jobStore;
    private final LeaseManager leases;
    private final HandlerRegistry registry;
    private final RetryHandler retryHandler;
    private final SchedulerMetrics metrics;
    private final ObjectMapper mapper;
    private final Duration leaseTtl;
    private final Duration heartbeatInterval;

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService heartbeatScheduler =
            Executors.newScheduledThreadPool(1, r -> {
                Thread t = new Thread(r, "lease-heartbeat");
                t.setDaemon(true);
                return t;
            });

    public WorkerPool(JobStore jobStore, LeaseManager leases, HandlerRegistry registry,
                      RetryHandler retryHandler, SchedulerMetrics metrics, ObjectMapper mapper,
                      Duration leaseTtl, Duration heartbeatInterval) {
        this.jobStore          = jobStore;
        this.leases            = leases;
        this.registry          = registry;
        this.retryHandler      = retryHandler;
        this.metrics           = metrics;
        this.mapper            = mapper;
        this.leaseTtl          = leaseTtl;
        this.heartbeatInterval = heartbeatInterval;
    }

    @Override
    public void dispatch(JobRun leasedRun) {
        executor.submit(() -> execute(leasedRun));
    }

    private void execute(JobRun run) {
        long token = run.leaseToken();

        // LEASED → RUNNING. If this fails we were fenced or reaped already — drop silently.
        if (!leases.markRunning(run.id(), token)) {
            log.debug("run {} not claimable (fenced or already moved on)", run.id());
            return;
        }

        Optional<Job> maybeJob = jobStore.findById(run.jobId());
        if (maybeJob.isEmpty()) {
            leases.markDeadLetter(run.id(), token, "job definition not found");
            metrics.onDeadLettered();
            return;
        }
        Job job = maybeJob.get();

        Optional<JobHandler> maybeHandler = registry.get(job.jobType());
        if (maybeHandler.isEmpty()) {
            failOrRetry(run, job, token, "no handler registered for type: " + job.jobType());
            return;
        }

        ScheduledFuture<?> heartbeat = heartbeatScheduler.scheduleAtFixedRate(
                () -> safeHeartbeat(run.id(), token),
                heartbeatInterval.toMillis(), heartbeatInterval.toMillis(), TimeUnit.MILLISECONDS);
        try {
            JsonNode payload = mapper.readTree(job.payload() == null ? "{}" : job.payload());
            JobContext ctx = new JobContext(
                    run.id(), run.jobId(), job.jobType(), payload, run.attempt(), run.idempotencyKey());

            maybeHandler.get().handle(ctx);

            if (leases.markSucceeded(run.id(), token)) {
                metrics.onSucceeded();
            } else {
                log.debug("run {} succeeded locally but write was fenced", run.id());
            }
        } catch (Throwable t) {
            failOrRetry(run, job, token, describe(t));
        } finally {
            heartbeat.cancel(true);
        }
    }

    private void failOrRetry(JobRun run, Job job, long token, String error) {
        metrics.onFailed();
        boolean retried = retryHandler.onFailure(run, job, token, error);
        if (retried) metrics.onRetried(); else metrics.onDeadLettered();
        log.debug("run {} failed: {} (retried={})", run.id(), error, retried);
    }

    private void safeHeartbeat(UUID runId, long token) {
        try {
            leases.heartbeat(runId, token, leaseTtl);
        } catch (RuntimeException e) {
            log.debug("heartbeat failed for run {}", runId, e);
        }
    }

    private static String describe(Throwable t) {
        String msg = t.getMessage();
        return t.getClass().getSimpleName() + (msg == null ? "" : ": " + msg);
    }

    @Override
    public void close() {
        heartbeatScheduler.shutdownNow();
        executor.shutdown();
    }
}
