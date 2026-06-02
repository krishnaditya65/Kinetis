package io.kinetis.api.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.kinetis.worker.proto.WorkerServiceGrpc;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory registry of connected worker nodes. Workers register via the {@code Register} RPC;
 * the scheduler picks one when dispatching using round-robin selection.
 *
 * <p>Stale detection: a worker is removed if it hasn't re-registered within 30 seconds.
 * Workers should call {@code Register} every {@code heartbeat-interval}.
 */
@Component
public class WorkerRegistry {

    private static final Logger log = LoggerFactory.getLogger(WorkerRegistry.class);
    private static final long REGISTRATION_TTL_MS = 30_000;

    private record WorkerEntry(
            String workerId, String host, int port,
            ManagedChannel channel,
            WorkerServiceGrpc.WorkerServiceFutureStub stub,
            AtomicLong lastSeenMs) {}

    private final Map<String, WorkerEntry> workers = new ConcurrentHashMap<>();
    private final AtomicInteger roundRobinIdx = new AtomicInteger(0);

    /** Register or refresh a worker. Called from the gRPC Register handler. */
    public void register(String workerId, String host, int port) {
        WorkerEntry existing = workers.get(workerId);
        if (existing != null && existing.host().equals(host) && existing.port() == port) {
            existing.lastSeenMs().set(System.currentTimeMillis());
            return;
        }
        if (existing != null) shutdownChannel(existing.channel());

        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .keepAliveTime(10, TimeUnit.SECONDS)
                .keepAliveTimeout(5, TimeUnit.SECONDS)
                .build();
        workers.put(workerId, new WorkerEntry(workerId, host, port, channel,
                WorkerServiceGrpc.newFutureStub(channel),
                new AtomicLong(System.currentTimeMillis())));
        log.info("worker registered: {} @ {}:{}", workerId, host, port);
    }

    /**
     * Pick the next available worker (round-robin). Prunes stale entries first.
     * Returns null if no workers are available.
     */
    public WorkerServiceGrpc.WorkerServiceFutureStub nextWorker() {
        pruneStale();
        List<WorkerEntry> live = new ArrayList<>(workers.values());
        if (live.isEmpty()) return null;
        int idx = Math.abs(roundRobinIdx.getAndIncrement() % live.size());
        return live.get(idx).stub();
    }

    public int workerCount() {
        pruneStale();
        return workers.size();
    }

    private void pruneStale() {
        long cutoff = System.currentTimeMillis() - REGISTRATION_TTL_MS;
        workers.entrySet().removeIf(e -> {
            if (e.getValue().lastSeenMs().get() < cutoff) {
                log.warn("removing stale worker: {}", e.getKey());
                shutdownChannel(e.getValue().channel());
                return true;
            }
            return false;
        });
    }

    private static void shutdownChannel(ManagedChannel channel) {
        channel.shutdown();
        try {
            channel.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @PreDestroy
    public void close() {
        workers.values().forEach(e -> shutdownChannel(e.channel()));
        workers.clear();
    }
}
