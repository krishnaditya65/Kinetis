package io.kinetis.core.bench;

import io.kinetis.core.idempotency.IdempotencyKeys;
import io.kinetis.core.lease.LeaseManager;
import io.kinetis.core.model.*;
import io.kinetis.core.store.JobRunStore;
import io.kinetis.core.store.JobStore;
import org.flywaydb.core.Flyway;
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
 * JMH microbenchmarks for the hot-path {@link LeaseManager} and {@link JobStore} operations.
 *
 * <p>Requires a Postgres instance. Set {@code BENCHMARK_DB_URL}, {@code BENCHMARK_DB_USER},
 * and {@code BENCHMARK_DB_PASSWORD} before running:
 *
 * <pre>
 *   export BENCHMARK_DB_URL=jdbc:postgresql://localhost:5432/kinetis_bench
 *   export BENCHMARK_DB_USER=kinetis
 *   export BENCHMARK_DB_PASSWORD=kinetis
 *   ./gradlew :scheduler-core:jmh
 *   # Results → scheduler-core/build/reports/jmh/results.txt
 * </pre>
 *
 * <h2>Benchmarks</h2>
 * <ul>
 *   <li>{@code claimDue_batch10}   — SKIP LOCKED claim, 10 rows per call</li>
 *   <li>{@code claimDue_batch100}  — SKIP LOCKED claim, 100 rows per call</li>
 *   <li>{@code markSucceeded}      — claim 1 → markRunning → markSucceeded cycle</li>
 *   <li>{@code heartbeat}          — claim 1 → markRunning → heartbeat cycle</li>
 *   <li>{@code submitJob}          — {@link JobStore#insertIfAbsent} with a unique key</li>
 *   <li>{@code fullLifecycleSingleRun} — submit + insert run + claimDue + markRunning + markSucceeded</li>
 * </ul>
 *
 * <p>Each benchmark iteration re-seeds the {@code job_runs} table so there are always rows
 * to claim. The submit benchmarks generate UUID-keyed jobs to avoid idempotency dedup.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class LeaseManagerBenchmark {

    private static final Set<Integer> ALL_SHARDS =
            Set.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);

    private LeaseManager  leaseManager;
    private JobRunStore   runStore;
    private JobStore      jobStore;
    private JdbcTemplate  jdbc;
    private UUID          seedJobId;

    @Setup(Level.Trial)
    public void setUp() {
        String url  = System.getenv().getOrDefault("BENCHMARK_DB_URL",
                "jdbc:postgresql://localhost:5432/kinetis_bench");
        String user = System.getenv().getOrDefault("BENCHMARK_DB_USER",  "kinetis");
        String pass = System.getenv().getOrDefault("BENCHMARK_DB_PASSWORD", "kinetis");

        DriverManagerDataSource ds = new DriverManagerDataSource(url, user, pass);
        ds.setDriverClassName("org.postgresql.Driver");
        jdbc         = new JdbcTemplate(ds);
        leaseManager = new LeaseManager(jdbc);
        runStore     = new JobRunStore(jdbc);
        jobStore     = new JobStore(jdbc);

        // Apply schema migrations so the benchmark table structure is current
        Flyway.configure()
                .dataSource(ds)
                .locations("classpath:db/migration")
                .baselineOnMigrate(true)
                .load()
                .migrate();

        // Seed one persistent job to attach runs to
        seedJobId = UUID.randomUUID();
        jobStore.insertIfAbsent(new Job(seedJobId, "bench", "{}",
                "bench-key-" + seedJobId,
                DeliveryPolicy.AT_LEAST_ONCE, null, ZoneId.of("UTC"),
                MisfirePolicy.FIRE_ONCE, RetryPolicy.defaults(),
                Instant.now(), 0, 0, null));
    }

    @Setup(Level.Iteration)
    public void seedRuns() {
        // Insert 200 due runs before each measurement iteration
        for (int i = 0; i < 200; i++) {
            String runKey = IdempotencyKeys.deriveRunKey(
                    "bench-key-" + seedJobId,
                    Instant.now().minusSeconds(1).plusMillis(i));
            runStore.insert(new JobRun(
                    UUID.randomUUID(), seedJobId,
                    JobState.SCHEDULED, 0,
                    Instant.now().minusSeconds(1),
                    null, null, 0L,
                    runKey,
                    null, null, Instant.now(), null, null,
                    0, 0, null));
        }
    }

    @TearDown(Level.Iteration)
    public void cleanup() {
        jdbc.update("DELETE FROM job_runs WHERE job_id = ?", seedJobId);
    }

    // ------------------------------------------------------------------
    // Claim benchmarks
    // ------------------------------------------------------------------

    /** Throughput of SKIP LOCKED batch-claim, 10 rows per call. */
    @Benchmark
    public void claimDue_batch10(Blackhole bh) {
        List<JobRun> claimed = leaseManager.claimDue("bench-node", 10,
                Duration.ofSeconds(30), ALL_SHARDS);
        bh.consume(claimed);
    }

    /** Throughput of SKIP LOCKED batch-claim, 100 rows per call. */
    @Benchmark
    public void claimDue_batch100(Blackhole bh) {
        List<JobRun> claimed = leaseManager.claimDue("bench-node", 100,
                Duration.ofSeconds(30), ALL_SHARDS);
        bh.consume(claimed);
    }

    // ------------------------------------------------------------------
    // State-transition benchmarks
    // ------------------------------------------------------------------

    /** End-to-end cost of: claim 1 → markRunning → markSucceeded (3 SQL statements). */
    @Benchmark
    public void markSucceeded_singleRow(Blackhole bh) {
        List<JobRun> claimed = leaseManager.claimDue("bench-node", 1,
                Duration.ofSeconds(30), ALL_SHARDS);
        if (!claimed.isEmpty()) {
            JobRun r = claimed.get(0);
            leaseManager.markRunning(r.id(), r.leaseToken());
            bh.consume(leaseManager.markSucceeded(r.id(), r.leaseToken()));
        }
    }

    /** End-to-end cost of: claim 1 → markRunning → heartbeat (3 SQL statements). */
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

    // ------------------------------------------------------------------
    // Submit benchmarks
    // ------------------------------------------------------------------

    /**
     * Raw submit throughput: one {@link JobStore#insertIfAbsent} call per invocation.
     * Each call uses a fresh UUID key so none are deduped — this measures the hot-path
     * INSERT … ON CONFLICT DO NOTHING pattern.
     */
    @Benchmark
    public void submitJob(Blackhole bh) {
        UUID id  = UUID.randomUUID();
        String key = "bench-submit-" + id;
        UUID inserted = jobStore.insertIfAbsent(new Job(
                id, "bench", "{}",
                key,
                DeliveryPolicy.AT_LEAST_ONCE, null, ZoneId.of("UTC"),
                MisfirePolicy.FIRE_ONCE, RetryPolicy.defaults(),
                Instant.now(), 0, 0, null));
        bh.consume(inserted);
    }

    /**
     * Full single-run lifecycle: insert job + insert run + claimDue + markRunning + markSucceeded.
     * This is the minimum cost of processing one job end-to-end through the database.
     * 5 SQL round-trips total.
     */
    @Benchmark
    public void fullLifecycleSingleRun(Blackhole bh) {
        // Submit
        UUID jobId   = UUID.randomUUID();
        String jobKey = "bench-full-" + jobId;
        jobStore.insertIfAbsent(new Job(
                jobId, "bench", "{}",
                jobKey,
                DeliveryPolicy.AT_LEAST_ONCE, null, ZoneId.of("UTC"),
                MisfirePolicy.FIRE_ONCE, RetryPolicy.defaults(),
                Instant.now(), 0, 0, null));

        // Enqueue
        Instant scheduledFor = Instant.now().minusMillis(1);
        String runKey = IdempotencyKeys.deriveRunKey(jobKey, scheduledFor);
        runStore.insert(new JobRun(
                UUID.randomUUID(), jobId,
                JobState.SCHEDULED, 0,
                scheduledFor,
                null, null, 0L,
                runKey,
                null, null, Instant.now(), null, null,
                0, 0, null));

        // Claim → run → succeed
        List<JobRun> claimed = leaseManager.claimDue("bench-node", 1,
                Duration.ofSeconds(30), ALL_SHARDS);
        if (!claimed.isEmpty()) {
            JobRun r = claimed.get(0);
            leaseManager.markRunning(r.id(), r.leaseToken());
            bh.consume(leaseManager.markSucceeded(r.id(), r.leaseToken()));
        }
    }
}
