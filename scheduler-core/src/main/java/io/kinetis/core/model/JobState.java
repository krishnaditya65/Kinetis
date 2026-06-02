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
 * </pre>
 */
public enum JobState {
    SCHEDULED,
    LEASED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    DEAD_LETTER,
    CANCELLED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == DEAD_LETTER || this == CANCELLED;
    }
}
