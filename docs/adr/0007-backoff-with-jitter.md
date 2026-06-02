# ADR-0007: Exponential backoff with full jitter

- **Status:** Accepted
- **Date:** 2026-06-02

## Context

Failed runs are retried. If many jobs fail together (e.g. a downstream dependency outage) and all
back off by the same fixed schedule, their retries synchronize into a **thundering herd** that hits
the dependency simultaneously when it recovers.

## Decision

Compute the retry delay for 0-based attempt `n` as a uniformly random value in
`[0, min(cap, base · factor^n)]` — exponential backoff with **full jitter**, capped at 1 hour.
Per-job `base` and `factor` are configurable; the cap prevents `factor^n` overflow.

## Options considered

- **Full jitter** — chosen. Spreads correlated retries across time; AWS-recommended.
- **Fixed/linear backoff** — simple but synchronizes retries; no spread.
- **Exponential, no jitter** — reduces frequency but still aligns retry waves.
- **Decorrelated jitter** — also good; full jitter chosen for simplicity and predictable bounds.

## Consequences

- (+) Smooths retry load; avoids stampedes on dependency recovery.
- (+) Bounded, configurable, overflow-safe.
- (−) Individual retry timing is non-deterministic — tests assert outcomes/attempt counts, not exact
  delays, and seed the RNG where determinism is needed.
