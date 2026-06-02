# ADR-0009: Cron expression parser written from scratch

- **Status:** Accepted
- **Date:** 2026-06-03

## Context

Phase 2 requires parsing cron expressions (Unix 5-field and Quartz 6/7-field formats) and computing
the next fire time correctly across time zones and DST transitions. A library (`com.cronutils:cron-utils`)
was the planned default; after discussion the decision was to write the parser ourselves.

## Decision

Write the cron expression parser and evaluator from scratch as `io.kinetis.core.cron.*`:
- A sealed `FieldConstraint` hierarchy covers every specifier (`*`, `?`, ranges, steps, lists,
  `L`, `W`, `#`) as typed value objects.
- `CronParser` tokenises each field and produces a `CronExpression` record — no mutable state.
- `CronEvaluator.next()` walks the candidate ZonedDateTime field-by-field (year → second), rolling
  forward on each mismatch. All arithmetic uses `ZonedDateTime` so the JDK handles DST math.

## Options considered

| Option | Pros | Cons |
|---|---|---|
| **Write from scratch** (chosen) | Full understanding of every edge case; no third-party dependency; portable; DST handling verifiable in tests | More code to write and own; risk of undiscovered edge cases |
| `cron-utils` library | Battle-tested, broad format support, maintained | Black-box DST behaviour; adds a dependency; reduces learning value |
| Other JVM libraries (Quartz, Spring) | Also well-tested | Pull in large transitive dependency graphs for a single concern |

## DST handling rationale

All date arithmetic runs through `ZonedDateTime`:
- **Spring-forward gap:** adding 1 second to the pre-gap time naturally skips to after the gap;
  a fire time that would land in the gap is transparently advanced.
- **Fall-back overlap:** `ZonedDateTime` defaults to the first (pre-transition) occurrence; the
  evaluator finds the second occurrence on the next tick, so a job fires at both wall-clock
  occurrences of the duplicate hour.
- The misfire threshold (60 s) means a job that misfires by a DST hour gets a misfire policy
  applied, which is the correct user-observable behaviour.

## Consequences

- (+) Complete control over parsing, evaluation, and DST behaviour; all verified by tests including
  an explicit spring-forward and fall-back case.
- (+) `FieldConstraint` sealed hierarchy is closed (compiler-enforced exhaustive switch) — adding a
  new specifier requires updating the evaluator, not silently being ignored.
- (−) We own the implementation; edge cases that arise in production must be fixed here.
- (−) Quartz's more obscure specifiers (`LW`, multiple `#` in one field) are not supported —
  acceptable for our use cases, documented as a known limitation.
