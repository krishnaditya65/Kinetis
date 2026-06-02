# Run state machine & transition guards

Every transition on `job_runs` is a single SQL statement guarded so that stale/concurrent actors
lose the race. The fencing token (`lease_token`) is the linchpin: it is bumped on every (re)lease,
and mutating writes carry the token they were issued.

```
            ┌───────────┐
  submit───▶│ SCHEDULED │◀────────────────────────┐
            └─────┬─────┘                          │
        claimDue  │ (SKIP LOCKED, token++)         │ rescheduleForRetry (token++)
            ┌─────▼─────┐                          │  — worker caught error, or
            │  LEASED   │                          │     reaper reclaimed expired lease
            └─────┬─────┘                          │
       markRunning│                                │
            ┌─────▼─────┐   heartbeat (extend)     │
            │  RUNNING  │───────────────┐          │
            └─────┬─────┘               │          │
       markSucceeded│            failure│          │
            ┌───────▼───┐         ┌─────▼──────────┴───┐
            │ SUCCEEDED │         │ retries left?       │
            └───────────┘         │  yes → SCHEDULED    │
                                  │  no  → DEAD_LETTER  │
       cancelByJobId              └─────────────────────┘
   (SCHEDULED/LEASED/RUNNING/FAILED → CANCELLED)
```

## Transitions

| From | To | Method | Guard (`WHERE …`) | Token |
|---|---|---|---|---|
| SCHEDULED | LEASED | `claimDue` | `state='SCHEDULED' AND scheduled_for<=now()` + `FOR UPDATE SKIP LOCKED` | `+1` |
| LEASED | RUNNING | `markRunning` | `id=? AND lease_token=? AND state='LEASED'` | — |
| LEASED/RUNNING | (same) | `heartbeat` | `id=? AND lease_token=? AND state IN ('LEASED','RUNNING')` | — |
| RUNNING | SUCCEEDED | `markSucceeded` | `id=? AND lease_token=? AND state='RUNNING'` | — |
| LEASED/RUNNING | SCHEDULED | `rescheduleForRetry` | `id=? AND lease_token=? AND state IN ('LEASED','RUNNING')` | `+1` |
| LEASED/RUNNING | DEAD_LETTER | `markDeadLetter` | `id=? AND lease_token=? AND state IN ('LEASED','RUNNING')` | `+1` |
| non-terminal | CANCELLED | `cancelByJobId` | `job_id=? AND state IN ('SCHEDULED','LEASED','RUNNING','FAILED')` | — |

Each mutator returns `true` only if it changed exactly one row. A `false` means the caller lost the
row — fenced by a newer token, already terminal, or reclaimed by the reaper — and must drop the work.

## Why the guards matter (the zombie scenario)

```
t0  worker A: claimDue → token=7, markRunning
t1  worker A stalls (40s GC pause)
t6  lease expires; reaper: rescheduleForRetry(token=7) → SCHEDULED, token=8
t7  worker B: claimDue → token=9, runs the job
t8  worker A wakes, markSucceeded(token=7) → 0 rows (7≠9) → returns false  ✅ fenced
```

Worker A never learns it lost the lease; correctness is enforced at the row, not the actor. This is
why fencing — not lease-checking or heartbeats alone — is what makes at-least-once delivery safe.
External side effects (which the token can't fence) are covered separately by idempotency keys.
