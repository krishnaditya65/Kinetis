# ADR-0001: Java 21 + Spring Boot (thin)

- **Status:** Accepted
- **Date:** 2026-06-02
- **Deciders:** project owner

## Context

We are building a distributed job scheduler from scratch, intended to scale to advanced features
(cron, high-throughput queue, DAGs). We need a language/runtime with mature concurrency, strong
persistence/coordination libraries, and operational maturity. The owner prefers Java.

## Decision

Use **Java 21** with **Spring Boot 3.3 used thinly**: only the `api` module is a Spring Boot
application (web, JDBC, Actuator, validation). The `scheduler-core` and `worker` modules are
framework-light libraries (plain Java + Spring JDBC).

## Options considered

- **Java 21 + Spring Boot (thin)** — chosen. Loom virtual threads make "one thread per running job"
  cheap; vast ecosystem (JDBC, Flyway, Micrometer, gRPC); the reference schedulers (Quartz, db-
  scheduler, Temporal Java SDK) are JVM, so prior art is abundant.
- **Go** — lighter footprint, great concurrency; but loses the JVM persistence/observability depth
  and the owner's familiarity.
- **Full Spring Boot everywhere** — simpler wiring, but couples the core distributed logic to the
  framework, hurting testability and the Phase 3 split of the worker into a separate process.

## Consequences

- (+) Virtual threads, mature tooling, easy observability, abundant prior art.
- (+) Core logic stays portable and unit-testable without a Spring context.
- (−) GC tail latency must be watched at scale (mitigated by ZGC/Shenandoah).
- (−) Two "styles" in one repo (framework-light core vs. Spring app) — documented to avoid drift.
- The toolchain targets Java 21 even though newer JDKs may be installed locally (see README).
