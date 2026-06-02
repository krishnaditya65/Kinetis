package io.kinetis.api.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kinetis.api.grpc.GrpcRunDispatcher;
import io.kinetis.api.grpc.WorkerGrpcServer;
import io.kinetis.api.grpc.WorkerRegistry;
import io.kinetis.core.lease.LeaseManager;
import io.kinetis.core.metrics.SchedulerMetrics;
import io.kinetis.core.retry.RetryHandler;
import io.kinetis.core.store.JobStore;
import io.kinetis.worker.HandlerRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Wires gRPC components conditionally on {@code app.role}:
 *
 * <ul>
 *   <li>{@code scheduler} — activates {@link GrpcRunDispatcher} (sends runs to remote workers).
 *       {@code @Primary} makes it win over the in-process {@code WorkerPool} bean.</li>
 *   <li>{@code worker} — activates {@link WorkerGrpcServer} (listens for dispatch from scheduler).
 *       Scheduler loops are not started in this role.</li>
 *   <li>{@code standalone} (default) — neither bean activates; in-process worker + scheduler
 *       run together with no gRPC. Right for single-node and Phase 1/2/3 deployments.</li>
 * </ul>
 */
@Configuration
public class WorkerGrpcConfig {

    @Bean
    @Primary
    @ConditionalOnProperty(name = "app.role", havingValue = "scheduler")
    public GrpcRunDispatcher grpcRunDispatcher(WorkerRegistry registry, JobStore jobStore) {
        return new GrpcRunDispatcher(registry, jobStore);
    }

    @Bean
    @ConditionalOnProperty(name = "app.role", havingValue = "worker")
    public WorkerGrpcServer workerGrpcServer(SchedulerProperties props,
                                              HandlerRegistry handlerRegistry,
                                              LeaseManager leaseManager,
                                              JobStore jobStore,
                                              RetryHandler retryHandler,
                                              SchedulerMetrics metrics,
                                              ObjectMapper mapper) {
        return new WorkerGrpcServer(
                props.getWorkerGrpcPort(),
                props.getWorkerGrpcId(),
                props.getSchedulerGrpcHost(),
                props.getSchedulerGrpcPort(),
                handlerRegistry, leaseManager, jobStore, retryHandler, metrics, mapper,
                props.getLeaseTtl(), props.getHeartbeatInterval());
    }
}
