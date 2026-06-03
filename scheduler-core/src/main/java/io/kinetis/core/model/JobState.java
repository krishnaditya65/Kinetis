package io.kinetis.core.model;

/**
 * Lifecycle states of a single {@link JobRun}.
 *
 * <pre>
 *   SCHEDULED ──lease──▶ LEASED ──start──▶ RUNNING ──success──▶ SUCCEEDED
 *       ▲                                     │
 *       │                              failure│
 *       │ (retry / reaped)                    ▼
 *       └──────────────── FAILED ──┬── retries left ──▶ SCHEDULED
 *                                  └── exhausted ─────▶ DEAD_LETTER
 *   any non-terminal ──cancel──▶ CANCELLED
 *
 *   DAG nodes:
 *   PENDING_DEPS ──all upstreams succeeded──▶ SCHEDULED
 *   PENDING_DEPS ──upstream failed + SKIP_DOWNSTREAM──▶ SKIPPED
 * </pre>
 */
public enum JobState {
    SCHEDULED,
    LEASED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    DEAD_LETTER,
    CANCELLED,
    /** DAG node waiting for upstream dependencies to complete. */
    PENDING_DEPS,
    /** DAG node skipped because an upstream failed under SKIP_DOWNSTREAM policy. */
    SKIPPED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == DEAD_LETTER || this == CANCELLED || this == SKIPPED;
    }

    public boolean isActive() {
        return this == SCHEDULED || this == LEASED || this == RUNNING || this == PENDING_DEPS;
    }
}
