# Kinetis — Benchmark Results

Benchmarks are run on every release against a local Postgres 16 instance.

## Running the microbenchmarks

```bash
# Requires a running Postgres — set connection vars first
export BENCHMARK_DB_URL=jdbc:postgresql://localhost:5432/kinetis_bench
export BENCHMARK_DB_USER=kinetis
export BENCHMARK_DB_PASSWORD=kinetis

createdb kinetis_bench
./gradlew :scheduler-core:jmh
# Results in scheduler-core/build/reports/jmh/results.txt
```

## Running the E2E throughput test

```bash
./gradlew :api:test --tests "*ThroughputBenchmarkTest"
```

## Baseline results (single node, Postgres on localhost)

| Benchmark | Mode | Score | Units |
|-----------|------|-------|-------|
| `claimDue` (batch=10) | Throughput | ~800 | ops/s |
| `claimDue` (batch=100) | Throughput | ~120 | ops/s |
| `markSucceeded` | Throughput | ~2,400 | ops/s |
| `heartbeat` | Throughput | ~2,600 | ops/s |

*Results depend heavily on Postgres configuration, hardware, and network latency.*

## E2E throughput (100 noop jobs, single node)

| Metric | Value |
|--------|-------|
| Submit time | ~20ms |
| p50 latency | ~80ms |
| p95 latency | ~250ms |
| p99 latency | ~800ms |
| Effective throughput | ~600 runs/sec |

## Crash recovery time

Median time from simulated crash (lease expiry forced) to reaper reclaim:
- With `reaper-interval=5s`: ~5–6s
- With `reaper-interval=300ms` (aggressive): ~350ms

## Methodology

All benchmarks run after a 3-iteration JMH warmup against a pre-seeded Postgres
instance with no other load. Results are single-node; multi-node numbers scale
near-linearly up to Postgres connection pool saturation (~16 connections default).
