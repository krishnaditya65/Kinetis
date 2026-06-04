package io.kinetis.api.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kinetis.core.admin.ArchivalService;
import io.kinetis.core.admin.MaintenanceFlag;
import io.kinetis.core.cron.CronScheduler;
import io.kinetis.core.lease.LeaseManager;
import io.kinetis.core.output.JobRunOutputStore;
import io.kinetis.core.workflow.DependencyResolver;
import io.kinetis.core.workflow.WorkflowAdvancer;
import io.kinetis.core.workflow.WorkflowService;
import io.kinetis.core.workflow.WorkflowStore;
import io.kinetis.core.metrics.SchedulerMetrics;
import io.kinetis.core.queue.FairShareDispatcher;
import io.kinetis.core.queue.RateLimiter;
import io.kinetis.core.reaper.ReaperLoop;
import io.kinetis.core.retry.BackoffCalculator;
import io.kinetis.core.retry.RetryHandler;
import io.kinetis.core.scheduler.RunDispatcher;
import io.kinetis.core.scheduler.SchedulerLoop;
import io.kinetis.core.service.JobService;
import io.kinetis.core.shard.ShardOwnershipProvider;
import io.kinetis.core.shard.StaticShardOwnership;
import io.kinetis.core.store.JobRunStore;
import io.kinetis.core.store.JobStore;
import io.kinetis.worker.HandlerRegistry;
import io.kinetis.worker.JobHandler;
import io.kinetis.worker.WorkerPool;
import io.kinetis.worker.handlers.FailNTimesHandler;
import io.kinetis.worker.handlers.NoOpHandler;
import io.kinetis.worker.handlers.SleepHandler;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Clock;
import java.util.List;
import java.util.Random;
import java.util.random.RandomGenerator;

/** Wires the framework-light core + worker components into Spring-managed beans. */
@Configuration
public class CoreConfig {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public RandomGenerator backoffRandom() {
        return new Random();
    }

    @Bean
    public SchedulerMetrics schedulerMetrics(MeterRegistry registry) {
        return new SchedulerMetrics(registry);
    }

    @Bean
    public JobStore jobStore(JdbcTemplate jdbc) {
        return new JobStore(jdbc);
    }

    @Bean
    public JobRunStore jobRunStore(JdbcTemplate jdbc) {
        return new JobRunStore(jdbc);
    }

    @Bean
    public LeaseManager leaseManager(JdbcTemplate jdbc) {
        return new LeaseManager(jdbc);
    }

    @Bean
    public BackoffCalculator backoffCalculator(RandomGenerator random) {
        return new BackoffCalculator(random);
    }

    @Bean
    public RetryHandler retryHandler(LeaseManager leases, BackoffCalculator backoff, Clock clock) {
        return new RetryHandler(leases, backoff, clock);
    }

    @Bean
    public ShardOwnershipProvider shardOwnershipProvider(SchedulerProperties props) {
        return new StaticShardOwnership(props.getTotalShards(), props.getOwnedShards());
    }

    @Bean
    public JobService jobService(JobStore jobStore, JobRunStore runStore,
                                 JdbcTemplate jdbc, Clock clock, SchedulerProperties props) {
        return new JobService(jobStore, runStore, jdbc, clock, props.getTotalShards());
    }

    @Bean
    public RateLimiter rateLimiter(JdbcTemplate jdbc) {
        return new RateLimiter(jdbc);
    }

    /**
     * Wraps the active RunDispatcher with rate limiting and fair-share per-tenant caps.
     * SchedulerLoop calls this; it delegates to WorkerPool or GrpcRunDispatcher after checks.
     */
    @Bean
    public FairShareDispatcher fairShareDispatcher(RunDispatcher delegate,
                                                    RateLimiter rateLimiter,
                                                    LeaseManager leaseManager,
                                                    SchedulerProperties props) {
        return new FairShareDispatcher(delegate, rateLimiter, leaseManager, props.getBatchSize());
    }

    @Bean
    public JobRunOutputStore jobRunOutputStore(JdbcTemplate jdbc) {
        return new JobRunOutputStore(jdbc);
    }

    @Bean(destroyMethod = "close")
    public WorkerPool workerPool(JobStore jobStore, LeaseManager leases,
                                 HandlerRegistry registry, RetryHandler retryHandler,
                                 SchedulerMetrics metrics, ObjectMapper mapper,
                                 JobRunOutputStore outputStore,
                                 SchedulerProperties props) {
        return new WorkerPool(jobStore, leases, registry, retryHandler, metrics, mapper,
                props.getLeaseTtl(), props.getHeartbeatInterval(),
                new io.kinetis.core.webhook.WebhookDispatcher(), outputStore);
    }

    @Bean public NoOpHandler noOpHandler()             { return new NoOpHandler(); }
    @Bean public SleepHandler sleepHandler()           { return new SleepHandler(); }
    @Bean public FailNTimesHandler failNTimesHandler() { return new FailNTimesHandler(); }

    @Bean
    public HandlerRegistry handlerRegistry(List<JobHandler> handlers) {
        return new HandlerRegistry(handlers);
    }

    @Bean
    public SchedulerLoop schedulerLoop(LeaseManager leases, FairShareDispatcher dispatcher,
                                       SchedulerMetrics metrics,
                                       ShardOwnershipProvider shardOwnership,
                                       JdbcTemplate jdbc, SchedulerProperties props) {
        return new SchedulerLoop(leases, dispatcher, metrics, shardOwnership, jdbc,
                resolveNodeId(props), props.getBatchSize(),
                props.getMaxConcurrentRuns(), props.getLeaseTtl());
    }

    @Bean
    public ReaperLoop reaperLoop(LeaseManager leases, JobStore jobStore,
                                 RetryHandler retryHandler, SchedulerMetrics metrics,
                                 ShardOwnershipProvider shardOwnership, SchedulerProperties props) {
        return new ReaperLoop(leases, jobStore, retryHandler, metrics, shardOwnership,
                props.getReaperBatchSize());
    }

    @Bean
    public CronScheduler cronScheduler(JdbcTemplate jdbc, JobRunStore runStore,
                                       SchedulerMetrics metrics,
                                       ShardOwnershipProvider shardOwnership,
                                       Clock clock, SchedulerProperties props) {
        return new CronScheduler(jdbc, runStore, metrics, shardOwnership, clock, props.getBatchSize());
    }

    // ---- admin beans ----

    @Bean
    public MaintenanceFlag maintenanceFlag(JdbcTemplate jdbc) {
        return new MaintenanceFlag(jdbc);
    }

    @Bean
    public ArchivalService archivalService(JdbcTemplate jdbc, SchedulerProperties props) {
        return new ArchivalService(jdbc, props.getArchivalRetentionDays(), props.getReaperBatchSize());
    }

    // ---- workflow beans ----

    @Bean
    public WorkflowStore workflowStore(JdbcTemplate jdbc) {
        return new WorkflowStore(jdbc);
    }

    @Bean
    public DependencyResolver dependencyResolver(WorkflowStore workflowStore, JdbcTemplate jdbc) {
        return new DependencyResolver(workflowStore, jdbc);
    }

    @Bean
    public WorkflowAdvancer workflowAdvancer(DependencyResolver resolver, JdbcTemplate jdbc,
                                              SchedulerProperties props) {
        return new WorkflowAdvancer(resolver, jdbc, props.getBatchSize());
    }

    @Bean
    public WorkflowService workflowService(WorkflowStore workflowStore, JobStore jobStore,
                                            JobRunStore runStore, JdbcTemplate jdbc,
                                            Clock clock, SchedulerProperties props) {
        return new WorkflowService(workflowStore, jobStore, runStore, jdbc,
                clock, props.getTotalShards());
    }

    private static String resolveNodeId(SchedulerProperties props) {
        if (props.getNodeId() != null && !props.getNodeId().isBlank()) return props.getNodeId();
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "node-" + ProcessHandle.current().pid();
        }
    }
}
