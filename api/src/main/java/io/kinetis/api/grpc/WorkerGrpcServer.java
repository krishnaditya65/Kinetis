package io.kinetis.api.grpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.kinetis.core.lease.LeaseManager;
import io.kinetis.core.metrics.SchedulerMetrics;
import io.kinetis.core.model.Job;
import io.kinetis.core.model.JobRun;
import io.kinetis.core.model.JobState;
import io.kinetis.core.retry.RetryHandler;
import io.kinetis.core.store.JobStore;
import io.kinetis.worker.HandlerRegistry;
import io.kinetis.worker.JobContext;
import io.kinetis.worker.JobHandler;
import io.kinetis.worker.proto.Ack;
import io.kinetis.worker.proto.RunAssignment;
import io.kinetis.worker.proto.WorkerRegistration;
import io.kinetis.worker.proto.WorkerServiceGrpc;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * gRPC server running in the worker role. Listens for {@code Dispatch} calls from the scheduler
 * and executes them using the same {@link HandlerRegistry} and {@link LeaseManager} as the
 * in-process {@code WorkerPool} — the distributed protocol is identical; only the transport changed.
 */
public class WorkerGrpcServer extends WorkerServiceGrpc.WorkerServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(WorkerGrpcServer.class);

    private final int port;
    private final String workerId;
    private final String schedulerHost;
    private final int schedulerPort;
    private final HandlerRegistry handlerRegistry;
    private final LeaseManager leaseManager;
    private final JobStore jobStore;
    private final RetryHandler retryHandler;
    private final SchedulerMetrics metrics;
    private final ObjectMapper mapper;
    private final Duration leaseTtl;
    private final Duration heartbeatInterval;

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService heartbeatScheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "worker-heartbeat");
                t.setDaemon(true);
                return t;
            });

    private Server grpcServer;

    public WorkerGrpcServer(int port, String workerId, String schedulerHost, int schedulerPort,
                             HandlerRegistry handlerRegistry, LeaseManager leaseManager,
                             JobStore jobStore, RetryHandler retryHandler,
                             SchedulerMetrics metrics, ObjectMapper mapper,
                             Duration leaseTtl, Duration heartbeatInterval) {
        this.port              = port;
        this.workerId          = workerId;
        this.schedulerHost     = schedulerHost;
        this.schedulerPort     = schedulerPort;
        this.handlerRegistry   = handlerRegistry;
        this.leaseManager      = leaseManager;
        this.jobStore          = jobStore;
        this.retryHandler      = retryHandler;
        this.metrics           = metrics;
        this.mapper            = mapper;
        this.leaseTtl          = leaseTtl;
        this.heartbeatInterval = heartbeatInterval;
    }

    @PostConstruct
    public void start() throws IOException {
        grpcServer = ServerBuilder.forPort(port)
                .addService(this)
                .executor(executor)
                .build()
                .start();
        log.info("WorkerGrpcServer listening on port {} (workerId={})", port, workerId);
        registerWithScheduler();
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        heartbeatScheduler.shutdownNow();
        executor.shutdownNow();
        if (grpcServer != null) {
            grpcServer.shutdown();
            grpcServer.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    // ---- gRPC RPC handlers -------------------------------------------------

    @Override
    public void dispatch(RunAssignment req, StreamObserver<Ack> out) {
        out.onNext(Ack.newBuilder().setAccepted(true).build());
        out.onCompleted();
        if (handlerRegistry.get(req.getJobType()).isEmpty()) {
            log.warn("no handler for '{}' — run {} will be reaped", req.getJobType(), req.getRunId());
            return;
        }
        executor.submit(() -> execute(req));
    }

    @Override
    public void register(WorkerRegistration req, StreamObserver<Ack> out) {
        out.onNext(Ack.newBuilder().setAccepted(true).build());
        out.onCompleted();
    }

    // ---- execution ---------------------------------------------------------

    private void execute(RunAssignment req) {
        UUID runId = UUID.fromString(req.getRunId());
        UUID jobId = UUID.fromString(req.getJobId());
        long token = req.getLeaseToken();

        if (!leaseManager.markRunning(runId, token)) {
            log.debug("run {} already claimed or fenced — dropping", runId);
            return;
        }

        Optional<Job> maybeJob = jobStore.findById(jobId);
        if (maybeJob.isEmpty()) {
            leaseManager.markDeadLetter(runId, token, "job definition not found");
            metrics.onDeadLettered();
            return;
        }
        Job job = maybeJob.get();

        Optional<JobHandler> maybeHandler = handlerRegistry.get(req.getJobType());
        if (maybeHandler.isEmpty()) {
            failOrRetry(runId, job, req, token, "no handler: " + req.getJobType());
            return;
        }

        ScheduledFuture<?> hb = heartbeatScheduler.scheduleAtFixedRate(
                () -> safeHeartbeat(runId, token),
                heartbeatInterval.toMillis(), heartbeatInterval.toMillis(), TimeUnit.MILLISECONDS);
        try {
            JsonNode payload = mapper.readTree(
                    req.getPayloadJson().isBlank() ? "{}" : req.getPayloadJson());
            maybeHandler.get().handle(new JobContext(
                    runId, jobId, req.getJobType(), payload,
                    req.getAttempt(), req.getIdempotencyKey()));

            if (leaseManager.markSucceeded(runId, token)) metrics.onSucceeded();
            else log.debug("run {} succeeded locally but write was fenced", runId);
        } catch (Throwable t) {
            failOrRetry(runId, job, req, token, describe(t));
        } finally {
            hb.cancel(true);
        }
    }

    private void failOrRetry(UUID runId, Job job, RunAssignment req, long token, String error) {
        metrics.onFailed();
        JobRun stub = new JobRun(runId, UUID.fromString(req.getJobId()),
                JobState.RUNNING, req.getAttempt(), null,
                workerId, null, token, req.getIdempotencyKey(),
                null, null, null, null, null,
                req.getShardId(), 0, null);
        boolean retried = retryHandler.onFailure(stub, job, token, error);
        if (retried) metrics.onRetried(); else metrics.onDeadLettered();
        log.debug("run {} failed (retry={}): {}", runId, retried, error);
    }

    private void safeHeartbeat(UUID runId, long token) {
        try {
            leaseManager.heartbeat(runId, token, leaseTtl);
        } catch (Exception e) {
            log.debug("heartbeat failed for run {}", runId, e);
        }
    }

    private static String describe(Throwable t) {
        String msg = t.getMessage();
        return t.getClass().getSimpleName() + (msg == null ? "" : ": " + msg);
    }

    private void registerWithScheduler() {
        if (schedulerHost == null || schedulerHost.isBlank()) {
            log.info("SCHEDULER_HOST not set — skipping self-registration");
            return;
        }
        try {
            var channel = io.grpc.ManagedChannelBuilder
                    .forAddress(schedulerHost, schedulerPort)
                    .usePlaintext().build();
            WorkerServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .register(WorkerRegistration.newBuilder()
                            .setWorkerId(workerId)
                            .setHost(resolveHostname())
                            .setPort(port)
                            .build());
            channel.shutdown();
            log.info("registered with scheduler at {}:{}", schedulerHost, schedulerPort);
        } catch (Exception e) {
            log.warn("failed to register with scheduler: {}", e.getMessage());
        }
    }

    private static String resolveHostname() {
        try {
            return java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (java.net.UnknownHostException e) {
            return "localhost";
        }
    }
}
