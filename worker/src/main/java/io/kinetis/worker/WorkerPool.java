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
import io.kinetis.core.webhook.WebhookDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Executes leased runs. Implements {@link RunDispatcher} so the scheduler hands it work without
 * knowing execution is in-process. Each run gets its own <b>virtual thread</b> (Java 21 Loom),
 * so blocking handlers scale cheaply.
 *
 * <h2>Batched heartbeats</h2>
 * Rather than scheduling one {@code ScheduledFuture} per active run (O(N) DB calls per interval),
 * a single pool-level heartbeat task calls {@link LeaseManager#batchHeartbeat} once per interval
 * for all currently-active runs. At 1,000 concurrent runs this reduces heartbeat DB calls from
 * 1,000 to 1 per interval — a ~1,000× reduction on the hot path.
 *
 * <p>All state-mutating writes ({@code markRunning}, {@code markSucceeded}, etc.) still carry
 * the per-run fencing token; the batch heartbeat intentionally omits it (see
 * {@link LeaseManager#batchHeartbeat} for the safety argument).
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
    private final WebhookDispatcher webhookDispatcher;

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService heartbeatScheduler =
            Executors.newScheduledThreadPool(1, r -> {
                Thread t = new Thread(r, "lease-heartbeat");
                t.setDaemon(true);
                return t;
            });

    /** Run IDs currently executing in this pool. Used for batch heartbeats. */
    private final Set<UUID> activeRunIds = ConcurrentHashMap.newKeySet();

    public WorkerPool(JobStore jobStore, LeaseManager leases, HandlerRegistry registry,
                      RetryHandler retryHandler, SchedulerMetrics metrics, ObjectMapper mapper,
                      Duration leaseTtl, Duration heartbeatInterval) {
        this(jobStore, leases, registry, retryHandler, metrics, mapper, leaseTtl,
             heartbeatInterval, new WebhookDispatcher());
    }

    public WorkerPool(JobStore jobStore, LeaseManager leases, HandlerRegistry registry,
                      RetryHandler retryHandler, SchedulerMetrics metrics, ObjectMapper mapper,
                      Duration leaseTtl, Duration heartbeatInterval,
                      WebhookDispatcher webhookDispatcher) {
        this.jobStore          = jobStore;
        this.leases            = leases;
        this.registry          = registry;
        this.retryHandler      = retryHandler;
        this.metrics           = metrics;
        this.mapper            = mapper;
        this.leaseTtl          = leaseTtl;
        this.webhookDispatcher = webhookDispatcher;

        // Single pool-wide heartbeat — one DB call for all active runs per interval.
        heartbeatScheduler.scheduleAtFixedRate(
                this::batchHeartbeat,
                heartbeatInterval.toMillis(), heartbeatInterval.toMillis(), TimeUnit.MILLISECONDS);
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

        activeRunIds.add(run.id());
        try {
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

            JsonNode payload = mapper.readTree(job.payload() == null ? "{}" : job.payload());
            JobContext ctx = new JobContext(
                    run.id(), run.jobId(), job.jobType(), payload, run.attempt(), run.idempotencyKey());

            maybeHandler.get().handle(ctx);

            if (leases.markSucceeded(run.id(), token)) {
                metrics.onSucceeded();
                String cbUrl = jobStore.findCallbackUrl(run.jobId());
                webhookDispatcher.fire(cbUrl, "job.succeeded", run.id().toString(), "SUCCEEDED");
            } else {
                log.debug("run {} succeeded locally but write was fenced", run.id());
            }
        } catch (Throwable t) {
            Optional<Job> maybeJob = jobStore.findById(run.jobId());
            maybeJob.ifPresent(job -> failOrRetry(run, job, token, describe(t)));
            if (maybeJob.isEmpty()) log.warn("run {} failed but job not found: {}", run.id(), describe(t));
        } finally {
            activeRunIds.remove(run.id());
        }
    }

    /** One DB call extends leases for every active run in this pool. */
    private void batchHeartbeat() {
        Set<UUID> snapshot = Set.copyOf(activeRunIds);
        if (snapshot.isEmpty()) return;
        try {
            int updated = leases.batchHeartbeat(snapshot, leaseTtl);
            log.debug("batch heartbeat: {} runs refreshed", updated);
        } catch (RuntimeException e) {
            log.debug("batch heartbeat failed", e);
        }
    }

    private void failOrRetry(JobRun run, Job job, long token, String error) {
        metrics.onFailed();
        boolean retried = retryHandler.onFailure(run, job, token, error);
        if (retried) {
            metrics.onRetried();
        } else {
            metrics.onDeadLettered();
            // Only fire webhook on final failure (dead-letter) — not on intermediate retries
            String cbUrl = jobStore.findCallbackUrl(run.jobId());
            webhookDispatcher.fire(cbUrl, "job.failed", run.id().toString(), "DEAD_LETTER");
        }
        log.debug("run {} failed: {} (retried={})", run.id(), error, retried);
    }

    private static String describe(Throwable t) {
        String msg = t.getMessage();
        return t.getClass().getSimpleName() + (msg == null ? "" : ": " + msg);
    }

    /** Active run count — useful for backpressure checks. */
    public int activeCount() {
        return activeRunIds.size();
    }

    @Override
    public void close() {
        heartbeatScheduler.shutdownNow();
        executor.shutdown();
    }
}
