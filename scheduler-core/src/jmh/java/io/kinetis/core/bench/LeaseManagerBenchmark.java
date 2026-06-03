package io.kinetis.core.bench;

import io.kinetis.core.lease.LeaseManager;
import io.kinetis.core.model.*;
import io.kinetis.core.store.JobRunStore;
import io.kinetis.core.store.JobStore;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * JMH microbenchmarks for the hot-path {@link LeaseManager} operations.
 *
 * <p>Requires a Postgres instance. Set {@code BENCHMARK_DB_URL}, {@code BENCHMARK_DB_USER},
 * and {@code BENCHMARK_DB_PASSWORD} environment variables before running:
 *
 * <pre>
 *   export BENCHMARK_DB_URL=jdbc:postgresql://localhost:5432/kinetis_bench
 *   ./gradlew :scheduler-core:jmh
 * </pre>
 *
 * Results are written to {@code scheduler-core/build/reports/jmh/}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(1)
public class LeaseManagerBenchmark {

    private static final Set<Integer> ALL_SHARDS =
            Set.of(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15);

    private LeaseManager leaseManager;
    private JobRunStore runStore;
    private JdbcTemplate jdbc;
    private UUID seedJobId;

    @Setup(Level.Trial)
    public void setUp() {
        String url  = System.getenv().getOrDefault("BENCHMARK_DB_URL",
                "jdbc:postgresql://localhost:5432/kinetis_bench");
        String user = System.getenv().getOrDefault("BENCHMARK_DB_USER",  "kinetis");
        String pass = System.getenv().getOrDefault("BENCHMARK_DB_PASSWORD", "kinetis");

        DriverManagerDataSource ds = new DriverManagerDataSource(url, user, pass);
        ds.setDriverClassName("org.postgresql.Driver");
        jdbc = new JdbcTemplate(ds);
        leaseManager = new LeaseManager(jdbc);
        runStore     = new JobRunStore(jdbc);
        JobStore jobStore = new JobStore(jdbc);

        // Seed a single job to attach runs to
        seedJobId = UUID.randomUUID();
        jobStore.insertIfAbsent(new Job(seedJobId, "bench", "{}", "bench-key-" + seedJobId,
                DeliveryPolicy.AT_LEAST_ONCE, null, ZoneId.of("UTC"), MisfirePolicy.FIRE_ONCE,
                RetryPolicy.defaults(), Instant.now(), 0, 0, null));
    }

    @Setup(Level.Iteration)
    public void seedRuns() {
        // Insert 100 due runs before each iteration
        for (int i = 0; i < 100; i++) {
            runStore.insert(new JobRun(UUID.randomUUID(), seedJobId,
                    JobState.SCHEDULED, 0, Instant.now().minusSeconds(1),
                    null, null, 0L, "bk-" + UUID.randomUUID(),
                    null, null, Instant.now(), null, null, 0, 0, null));
        }
    }

    @TearDown(Level.Iteration)
    public void cleanup() {
        jdbc.update("TRUNCATE job_runs RESTART IDENTITY CASCADE");
    }

    @Benchmark
    public void claimDue_batch10(Blackhole bh) {
        List<JobRun> claimed = leaseManager.claimDue("bench-node", 10,
                Duration.ofSeconds(30), ALL_SHARDS);
        bh.consume(claimed);
    }

    @Benchmark
    public void claimDue_batch100(Blackhole bh) {
        List<JobRun> claimed = leaseManager.claimDue("bench-node", 100,
                Duration.ofSeconds(30), ALL_SHARDS);
        bh.consume(claimed);
    }

    @Benchmark
    public void markSucceeded_singleRow(Blackhole bh) {
        // Claim one run, then immediately mark it succeeded
        List<JobRun> claimed = leaseManager.claimDue("bench-node", 1,
                Duration.ofSeconds(30), ALL_SHARDS);
        if (!claimed.isEmpty()) {
            JobRun r = claimed.get(0);
            leaseManager.markRunning(r.id(), r.leaseToken());
            bh.consume(leaseManager.markSucceeded(r.id(), r.leaseToken()));
        }
    }

    @Benchmark
    public void heartbeat_singleRow(Blackhole bh) {
        List<JobRun> claimed = leaseManager.claimDue("bench-node", 1,
                Duration.ofSeconds(30), ALL_SHARDS);
        if (!claimed.isEmpty()) {
            JobRun r = claimed.get(0);
            leaseManager.markRunning(r.id(), r.leaseToken());
            bh.consume(leaseManager.heartbeat(r.id(), r.leaseToken(), Duration.ofSeconds(30)));
        }
    }
}
